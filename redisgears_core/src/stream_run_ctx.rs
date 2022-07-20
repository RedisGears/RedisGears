use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    stream_ctx::StreamCtxInterface, stream_ctx::StreamProcessCtxInterface,
    stream_ctx::StreamRecordAck, stream_ctx::StreamRecordInterface,
};

use redis_module::{
    context::AclPermissions, raw::RedisModuleStreamID, stream::StreamRecord, RedisString,
    ThreadSafeContext,
};

use crate::{
    background_run_ctx::BackgroundRunCtx,
    get_ctx,
    run_ctx::{RedisClient, RedisClientCallOptions},
};

use crate::stream_reader::{StreamConsumer, StreamReaderAck};

use crate::get_notification_blocker;

use crate::RefCellWrapper;
use std::sync::Arc;

pub(crate) struct StreamRunCtx {
    user: String,
    flags: u8,
}

impl StreamRunCtx {
    fn new(user: String, flags: u8) -> StreamRunCtx {
        StreamRunCtx {
            user: user,
            flags: flags,
        }
    }
}

impl StreamProcessCtxInterface for StreamRunCtx {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient::new(Some(self.user.clone()), self.flags))
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            Some(self.user.clone()),
            RedisClientCallOptions::new(self.flags),
        ))
    }
}

pub(crate) struct GearsStreamRecord {
    pub(crate) record: StreamRecord,
}

unsafe impl Sync for GearsStreamRecord {}
unsafe impl Send for GearsStreamRecord {}

impl crate::stream_reader::StreamReaderRecord for GearsStreamRecord {
    fn get_id(&self) -> RedisModuleStreamID {
        self.record.id
    }
}

impl StreamRecordInterface for GearsStreamRecord {
    fn get_id(&self) -> (u64, u64) {
        (self.record.id.ms, self.record.id.seq)
    }

    fn fields<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a [u8], &'a [u8])> + 'a> {
        let res = self
            .record
            .fields
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect::<Vec<(&'a [u8], &'a [u8])>>();
        Box::new(res.into_iter())
    }
}

pub(crate) struct GearsStreamConsumer {
    pub(crate) ctx: Box<dyn StreamCtxInterface>,
    user: Arc<RefCellWrapper<String>>,
    flags: u8,
    permissions: AclPermissions,
}

impl GearsStreamConsumer {
    pub(crate) fn new(
        user: &Arc<RefCellWrapper<String>>,
        flags: u8,
        ctx: Box<dyn StreamCtxInterface>,
    ) -> GearsStreamConsumer {
        let mut permissions = AclPermissions::new();
        permissions.add_full_permission();
        GearsStreamConsumer {
            ctx: ctx,
            user: Arc::clone(user),
            flags: flags,
            permissions: permissions,
        }
    }
}

impl StreamConsumer<GearsStreamRecord> for GearsStreamConsumer {
    fn new_data(
        &self,
        stream_name: &str,
        record: GearsStreamRecord,
        ack_callback: Box<dyn FnOnce(StreamReaderAck) + Send>,
    ) -> Option<StreamReaderAck> {
        let user = self.user.ref_cell.borrow();
        let key_redis_str = RedisString::create(std::ptr::null_mut(), stream_name);
        if let Err(e) = get_ctx().acl_check_key_permission(&user, &key_redis_str, &self.permissions)
        {
            return Some(StreamReaderAck::Nack(format!(
                "User '{}' has no permissions on key '{}', {}.",
                user, stream_name, e
            )));
        }

        let res = {
            let _notification_blocker = get_notification_blocker();
            self.ctx.process_record(
                stream_name,
                Box::new(record),
                &mut StreamRunCtx::new(self.user.ref_cell.borrow().clone(), self.flags),
                Box::new(|ack| {
                    // here we must take the redis lock
                    let ctx = ThreadSafeContext::new();
                    let _gaurd = ctx.lock();
                    ack_callback(match ack {
                        StreamRecordAck::Ack => StreamReaderAck::Ack,
                        StreamRecordAck::Nack(msg) => StreamReaderAck::Nack(msg),
                    })
                }),
            )
        };
        res.map_or(None, |r| {
            Some(match r {
                StreamRecordAck::Ack => StreamReaderAck::Ack,
                StreamRecordAck::Nack(msg) => StreamReaderAck::Nack(msg),
            })
        })
    }
}
