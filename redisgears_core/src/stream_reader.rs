/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::raw::RedisModuleStreamID;
use redis_module::Context;
use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;

use std::collections::HashMap;

use std::cell::RefCell;
use std::collections::LinkedList;
use std::sync::{Arc, Weak};

use std::time::{SystemTime, UNIX_EPOCH};

use crate::RefCellWrapper;

pub type RecordAcknowledgeCallback = dyn Fn(&Context, &[u8], u64, u64);
pub type StreamReaderCallback<T> = dyn Fn(&Context, &[u8], Option<RedisModuleStreamID>, bool) -> Result<Option<T>, String>
    + Sync
    + Send;
pub type StreamTrimmerCallback = dyn Fn(&Context, &[u8], RedisModuleStreamID) + Sync + Send;
pub(crate) type AcknowledgeCallback = dyn FnOnce(&Context, StreamReaderAck) + Send;

pub(crate) trait StreamReaderRecord {
    fn get_id(&self) -> RedisModuleStreamID;
}

pub(crate) trait StreamReader<R>
where
    R: StreamReaderRecord,
{
    fn read(&self, key: &str, from: Option<RedisModuleStreamID>) -> Result<Option<R>, String>;
}

pub(crate) enum StreamReaderAck {
    Ack,
    Nack(GearsApiError),
}

pub(crate) trait StreamConsumer<T: StreamReaderRecord> {
    fn new_data(
        &self,
        ctx: &Context,
        stream_name: &[u8],
        record: T,
        ack_callback: Box<AcknowledgeCallback>,
    ) -> Option<StreamReaderAck>;
}

pub(crate) struct TrackedStream {
    name: Vec<u8>,
    consumers_data: Vec<Weak<RefCellWrapper<ConsumerInfo>>>,
    stream_trimmer: Arc<Box<StreamTrimmerCallback>>,
}

impl TrackedStream {
    fn trim(&mut self, ctx: &Context) {
        let mut id_to_trim: RedisModuleStreamID = RedisModuleStreamID {
            ms: u64::MAX,
            seq: u64::MAX,
        };
        let mut indexes_to_delete = Vec::new();
        for (i, weak_consumer_info) in self.consumers_data.iter().enumerate() {
            let weak_consumer_info = weak_consumer_info.upgrade();
            if weak_consumer_info.is_none() {
                indexes_to_delete.push(i);
                continue;
            }

            let weak_consumer_info = weak_consumer_info.unwrap();
            let consumer_info = weak_consumer_info.ref_cell.borrow();
            let first_id = {
                let first_id = consumer_info.pending_ids.front();
                if let Some(first_id) = first_id {
                    RedisModuleStreamID {
                        ms: first_id.ms,
                        seq: first_id.seq,
                    }
                } else if let Some(last_read_id) = consumer_info.last_read_id.as_ref() {
                    // if we do not have pending id's it means that last_read_id can be trimmed.
                    // Increase the seq value to make sure we keep everything which is greater than last_read_id.
                    RedisModuleStreamID {
                        ms: last_read_id.ms,
                        seq: last_read_id.seq + 1,
                    }
                } else {
                    continue;
                }
            };
            if first_id.ms < id_to_trim.ms
                || (first_id.ms == id_to_trim.ms && first_id.seq < id_to_trim.seq)
            {
                id_to_trim = first_id;
            }
        }

        if id_to_trim.ms < u64::MAX {
            // do not accidently trimm by u64::MAX
            (self.stream_trimmer)(ctx, &self.name, id_to_trim);
        }

        for id in indexes_to_delete.iter().rev() {
            self.consumers_data.swap_remove(*id);
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConsumerInfo {
    pub(crate) last_processed_time: u128, // last processed time in ms
    pub(crate) total_processed_time: u128, // last processed time in ms
    pub(crate) last_lag: u128,            // last lag in ms
    pub(crate) total_lag: u128,           // average lag in ms
    pub(crate) records_processed: usize,  // average lag in ms
    pub(crate) pending_ids: LinkedList<RedisModuleStreamID>,
    pub(crate) last_read_id: Option<RedisModuleStreamID>,
    pub(crate) last_error: Option<GearsApiError>,
}

impl ConsumerInfo {
    fn ack_id(&mut self, id: RedisModuleStreamID, start_time: u128) -> bool {
        self.records_processed += 1;
        let since_the_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let lag = since_the_epoch - id.ms as u128;
        self.last_processed_time = since_the_epoch - start_time;
        self.total_processed_time += self.last_processed_time;
        self.last_lag = lag;
        self.total_lag += lag;

        let mut temp_list = LinkedList::new();
        while let Some(curr) = self.pending_ids.pop_front() {
            if curr.ms == id.ms && curr.seq == id.seq {
                break;
            }
            temp_list.push_back(curr);
        }
        if temp_list.is_empty() {
            return true; // indicate that the first element was removed
        }
        self.pending_ids.append(&mut temp_list);
        false
    }
}

pub(crate) struct ConsumerData<T: StreamReaderRecord, C: StreamConsumer<T>> {
    pub(crate) prefix: Vec<u8>,
    pub(crate) consumer: Option<C>,
    pub(crate) consumed_streams: HashMap<Vec<u8>, Arc<RefCellWrapper<ConsumerInfo>>>,
    pub(crate) window: usize, // represent the max amount of elements that can be processed at the same time
    pub(crate) trim: bool,
    pub(crate) on_record_acked: Option<Box<RecordAcknowledgeCallback>>,
    pub(crate) description: Option<String>,
    phantom: std::marker::PhantomData<T>,
}

impl<T, C> ConsumerData<T, C>
where
    T: StreamReaderRecord,
    C: StreamConsumer<T>,
{
    pub(crate) fn set_consumer(&mut self, consumer: C) -> C {
        let old_consumer = self.consumer.take();
        self.consumer = Some(consumer);
        old_consumer.unwrap()
    }

    pub(crate) fn set_window(&mut self, window: usize) -> usize {
        let old_window = self.window;
        self.window = window;
        old_window
    }

    pub(crate) fn set_trim(&mut self, trim: bool) -> bool {
        let old_trim = self.trim;
        self.trim = trim;
        old_trim
    }

    pub(crate) fn set_description(&mut self, description: Option<String>) -> Option<String> {
        let old_description = self.description.take();
        self.description = description;
        old_description
    }

    pub(crate) fn get_or_create_consumed_stream(
        &mut self,
        name: &[u8],
    ) -> (Arc<RefCellWrapper<ConsumerInfo>>, bool) {
        let mut is_new = false;
        let res = self
            .consumed_streams
            .entry(name.to_vec())
            .or_insert_with(|| {
                is_new = true;
                Arc::new(RefCellWrapper {
                    ref_cell: RefCell::new(ConsumerInfo {
                        last_processed_time: 0,
                        total_processed_time: 0,
                        last_lag: 0,
                        total_lag: 0,
                        records_processed: 0,
                        pending_ids: LinkedList::new(),
                        last_error: None,
                        last_read_id: None,
                    }),
                })
            });
        (Arc::clone(res), is_new)
    }

    pub(crate) fn get_streams_info<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, u64, u64)> + 'a> {
        Box::new(
            self.consumed_streams
                .iter()
                .filter_map(|(s, v)| {
                    let v = v.ref_cell.borrow();
                    v.last_read_id?;
                    let v = v.last_read_id.as_ref().unwrap();
                    Some((s.clone(), v.ms, v.seq))
                })
                .collect::<Vec<(Vec<u8>, u64, u64)>>()
                .into_iter(),
        )
    }

    pub(crate) fn clear_streams_info(&mut self) {
        self.consumed_streams.clear();
    }
}

pub(crate) struct StreamReaderCtx<T, C>
where
    T: StreamReaderRecord,
    C: StreamConsumer<T>,
{
    // map between consumers to streams the consumer is reading from
    consumers: Vec<Weak<RefCellWrapper<ConsumerData<T, C>>>>,
    stream_reader: Arc<Box<StreamReaderCallback<T>>>,
    stream_trimmer: Arc<Box<StreamTrimmerCallback>>,
    tracked_streams: HashMap<Vec<u8>, Arc<RefCellWrapper<TrackedStream>>>,
}

fn read_next_data<T: StreamReaderRecord>(
    ctx: &Context,
    name: &[u8],
    id: Option<RedisModuleStreamID>,
    include_id: bool,
    consumer_info: &Arc<RefCellWrapper<ConsumerInfo>>,
    stream_reader: &Arc<Box<StreamReaderCallback<T>>>,
) -> Result<Option<T>, String> {
    let r = stream_reader(ctx, name, id, include_id);
    r.as_ref()?;
    let record = r.as_ref().unwrap();
    if record.is_none() {
        return r;
    }
    let record = record.as_ref().unwrap();
    let new_id = record.get_id();
    let mut c_i = consumer_info.ref_cell.borrow_mut();
    c_i.last_read_id = Some(new_id);
    r
}

fn send_new_data<T: StreamReaderRecord + 'static, C: StreamConsumer<T> + 'static>(
    ctx: &Context,
    stream: Arc<RefCellWrapper<TrackedStream>>,
    consumer_weak: Weak<RefCellWrapper<ConsumerData<T, C>>>,
    mut actual_record: Result<Option<T>, String>,
    consumer_info: Arc<RefCellWrapper<ConsumerInfo>>,
    stream_reader: Arc<Box<StreamReaderCallback<T>>>,
) {
    let consumer = match consumer_weak.upgrade() {
        Some(c) => c,
        None => return,
    };
    let trim = { consumer.ref_cell.borrow().trim };
    loop {
        let (id, record) = {
            let mut c_i = consumer_info.ref_cell.borrow_mut();
            if actual_record.is_err() {
                return;
            }
            let record = actual_record.unwrap();
            if record.is_none() {
                return;
            }
            let record = record.unwrap();
            let id = record.get_id();
            c_i.pending_ids.push_back(id);
            (id, record)
        };
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let res = {
            let t_s = stream.ref_cell.borrow();
            let c = consumer.ref_cell.borrow();
            let clone_consumer_weak = Weak::clone(&consumer_weak);
            let clone_consumer_info = Arc::downgrade(&consumer_info);
            let clone_stream = Arc::clone(&stream);
            let clone_stream_reader = Arc::clone(&stream_reader);
            c.consumer.as_ref().unwrap().new_data(
                ctx,
                &t_s.name,
                record,
                Box::new(move |ctx, ack| {
                    // if weak ref returns None it means that stream was deleted
                    if let Some(clone_consumer_info) = clone_consumer_info.upgrade() {
                        let record = {
                            let mut t_s = clone_stream.ref_cell.borrow_mut();
                            let last_read_id = {
                                let (trimmed_first, last_read_id) = {
                                    let mut c_i = clone_consumer_info.ref_cell.borrow_mut();
                                    let mut trimmed_first = c_i.ack_id(id, start_time);
                                    if let Some(c) = clone_consumer_weak.upgrade() {
                                        // consumer is still allive, fire the on acked event.
                                        if trimmed_first {
                                            // only if we trimmed the first element we
                                            // can fire the acked callback to notify
                                            // that it is safe to continue from this ID
                                            // in case of a crash.
                                            if let Some(on_record_acked) =
                                                c.ref_cell.borrow().on_record_acked.as_ref()
                                            {
                                                on_record_acked(ctx, &t_s.name, id.ms, id.seq);
                                            }
                                        }
                                    } else {
                                        // consumer is dead, lets not trim the stream.
                                        trimmed_first = false;
                                    }
                                    match ack {
                                        StreamReaderAck::Ack => {}
                                        StreamReaderAck::Nack(msg) => c_i.last_error = Some(msg),
                                    }
                                    (trimmed_first, c_i.last_read_id)
                                };
                                if trimmed_first && trim {
                                    t_s.trim(ctx);
                                }
                                last_read_id
                            };
                            read_next_data(
                                ctx,
                                &t_s.name,
                                last_read_id,
                                false,
                                &clone_consumer_info,
                                &clone_stream_reader,
                            )
                        };
                        send_new_data(
                            ctx,
                            clone_stream,
                            clone_consumer_weak,
                            record,
                            clone_consumer_info,
                            clone_stream_reader,
                        );
                    }
                }),
            )
        };

        let mut t_s = stream.ref_cell.borrow_mut();
        let last_read_id = match res {
            Some(r) => {
                let (trimmed_first, last_read_id) = {
                    let mut c_i = consumer_info.ref_cell.borrow_mut();
                    let mut trimmed_first = c_i.ack_id(id, start_time);
                    if let Some(c) = consumer_weak.upgrade() {
                        // consumer is still allive, fire the on acked event.
                        if trimmed_first {
                            // only if we trimmed the first element we
                            // can fire the acked callback to notify
                            // that it is safe to continue from this ID
                            // in case of a crash.
                            if let Some(on_record_acked) =
                                c.ref_cell.borrow().on_record_acked.as_ref()
                            {
                                on_record_acked(ctx, &t_s.name, id.ms, id.seq);
                            }
                        }
                    } else {
                        // consumer is dead, lets not trim the stream.
                        trimmed_first = false;
                    }
                    match r {
                        StreamReaderAck::Ack => {}
                        StreamReaderAck::Nack(msg) => c_i.last_error = Some(msg),
                    }
                    (trimmed_first, c_i.last_read_id)
                };
                if trimmed_first && trim {
                    t_s.trim(ctx);
                }
                last_read_id
            }
            None => {
                let window = { consumer.ref_cell.borrow().window };
                let c_i = consumer_info.ref_cell.borrow();
                if c_i.pending_ids.len() >= window {
                    return;
                }
                c_i.last_read_id
            }
        };
        actual_record = read_next_data(
            ctx,
            &t_s.name,
            last_read_id,
            false,
            &consumer_info,
            &stream_reader,
        );
    }
}

unsafe impl<T: StreamReaderRecord + 'static, C: StreamConsumer<T> + 'static> Sync
    for StreamReaderCtx<T, C>
{
}
unsafe impl<T: StreamReaderRecord + 'static, C: StreamConsumer<T> + 'static> Send
    for StreamReaderCtx<T, C>
{
}

impl<T, C> StreamReaderCtx<T, C>
where
    T: StreamReaderRecord + 'static,
    C: StreamConsumer<T> + 'static,
{
    pub(crate) fn new(
        stream_reader: Box<StreamReaderCallback<T>>,
        stream_trimmer: Box<StreamTrimmerCallback>,
    ) -> Self {
        StreamReaderCtx {
            consumers: Vec::new(),
            stream_reader: Arc::new(stream_reader),
            stream_trimmer: Arc::new(stream_trimmer),
            tracked_streams: HashMap::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.consumers.clear();
        self.tracked_streams.clear();
    }

    pub(crate) fn add_consumer(
        &'static mut self,
        prefix: &[u8],
        consumer: C,
        window: usize,
        trim: bool,
        on_record_acked: Option<Box<RecordAcknowledgeCallback>>,
        description: Option<String>,
    ) -> Arc<RefCellWrapper<ConsumerData<T, C>>> {
        let consumer_data = Arc::new(RefCellWrapper {
            ref_cell: RefCell::new(ConsumerData {
                prefix: prefix.to_vec(),
                consumer: Some(consumer),
                consumed_streams: HashMap::new(),
                phantom: std::marker::PhantomData::<T>,
                window,
                trim,
                on_record_acked,
                description,
            }),
        });
        self.consumers.push(Arc::downgrade(&consumer_data));
        consumer_data
    }

    pub(crate) fn on_stream_deleted(&mut self, _event: &str, key: &[u8]) {
        let mut ids_to_remove = Vec::new();
        self.tracked_streams.remove(key);
        for (i, c) in self.consumers.iter().enumerate() {
            let c = c.upgrade();
            if c.is_none() {
                ids_to_remove.push(i);
                continue;
            }
            let c = c.unwrap();
            let mut consumer_data = c.ref_cell.borrow_mut();
            consumer_data.consumed_streams.remove(key);
        }
    }

    fn get_or_create_tracked_stream(
        &mut self,
        name: &[u8],
    ) -> &std::sync::Arc<RefCellWrapper<TrackedStream>> {
        self.tracked_streams
            .entry(name.to_vec())
            .or_insert(Arc::new(RefCellWrapper {
                ref_cell: RefCell::new(TrackedStream {
                    name: name.to_vec(),
                    consumers_data: Vec::new(),
                    stream_trimmer: Arc::clone(&self.stream_trimmer),
                }),
            }))
    }

    pub(crate) fn update_stream_for_consumer(
        &mut self,
        stream_name: &[u8],
        consumer_data: &Arc<RefCellWrapper<ConsumerData<T, C>>>,
        ms: u64,
        seq: u64,
    ) {
        let mut c_d = consumer_data.ref_cell.borrow_mut();
        let (stream_info, is_new) = c_d.get_or_create_consumed_stream(stream_name);
        if is_new {
            let mut t_s = self
                .get_or_create_tracked_stream(stream_name)
                .ref_cell
                .borrow_mut();
            t_s.consumers_data.push(Arc::downgrade(&stream_info));
        }
        stream_info.ref_cell.borrow_mut().last_read_id = Some(RedisModuleStreamID { ms, seq });
    }

    pub(crate) fn clear_tracked_streams(&mut self) {
        self.tracked_streams.clear();
    }

    pub(crate) fn on_stream_touched(&mut self, ctx: &Context, _event: &str, key: &[u8]) {
        let mut ids_to_remove = Vec::new();

        let tracked_stream = Arc::clone(self.get_or_create_tracked_stream(key));

        let _ = self
            .consumers
            .iter()
            .enumerate()
            .filter(|(i, v)| {
                let v = v.upgrade();
                if v.is_none() {
                    ids_to_remove.push(*i);
                    return false;
                }
                let v = v.unwrap();
                let v = v.ref_cell.borrow();
                key.starts_with(&v.prefix)
            })
            .map(|(_, v)| {
                let consumer = v.upgrade().unwrap();
                let (record, consumer_info) = {
                    let mut c = consumer.ref_cell.borrow_mut();
                    let (consumer_info, is_new) = c.get_or_create_consumed_stream(key);
                    if is_new {
                        let mut t_s = tracked_stream.ref_cell.borrow_mut();
                        t_s.consumers_data.push(Arc::downgrade(&consumer_info));
                    }
                    let last_read_id = {
                        let c_i = consumer_info.ref_cell.borrow();
                        if c_i.pending_ids.len() >= c.window {
                            return None;
                        }
                        c_i.last_read_id
                    };

                    (
                        read_next_data(
                            ctx,
                            key,
                            last_read_id,
                            false,
                            &consumer_info,
                            &self.stream_reader,
                        ),
                        Arc::clone(&consumer_info),
                    )
                };
                let res = (Weak::clone(v), record, consumer_info);
                Some(res)
            })
            .collect::<Vec<
                Option<(
                    Weak<RefCellWrapper<ConsumerData<T, C>>>,
                    Result<Option<T>, String>,
                    Arc<RefCellWrapper<ConsumerInfo>>,
                )>,
            >>()
            .into_iter()
            .map(|res| {
                if let Some((consumer_weak, record, consumer_info)) = res {
                    let stream_reader = Arc::clone(&self.stream_reader);
                    let tracked_stream = Arc::clone(&tracked_stream);
                    ctx.add_post_notification_job(move |ctx| {
                        send_new_data(
                            ctx,
                            tracked_stream,
                            consumer_weak,
                            record,
                            consumer_info,
                            stream_reader,
                        );
                    });
                }
            })
            .collect::<Vec<()>>();

        for id in ids_to_remove.iter().rev() {
            self.consumers.swap_remove(*id);
        }
    }
}
