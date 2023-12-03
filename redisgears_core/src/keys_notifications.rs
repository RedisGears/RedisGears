/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::Context;
use redisgears_plugin_api::redisgears_plugin_api::{GearsApiError, RefCellWrapper};
use std::cell::RefCell;
use std::sync::{Arc, Weak};
use std::time::SystemTime;

/// A callback that will be provider to the user to call when he finished to
/// processes the notification
type AckCallback = Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>;

/// A callback that is provided by the user that will be called when a
/// key space notification arrives.
pub(crate) type NotificationCallback = Box<dyn Fn(&Context, &str, &[u8], AckCallback)>;

#[derive(Debug)]
pub(crate) enum ConsumerKey {
    Key(Vec<u8>),
    Prefix(Vec<u8>),
}

#[derive(Debug, Clone)]
pub(crate) struct NotificationConsumerStats {
    pub(crate) num_trigger: usize,
    pub(crate) num_success: usize,
    pub(crate) num_failed: usize,
    pub(crate) num_finished: usize,
    pub(crate) last_error: Option<GearsApiError>,
    pub(crate) last_execution_time: u128,
    pub(crate) total_execution_time: u128,
}

pub(crate) struct NotificationConsumer {
    key: Option<ConsumerKey>,
    callback: Option<NotificationCallback>,
    stats: Arc<RefCellWrapper<NotificationConsumerStats>>,
    description: Option<String>,
    /// If set, the event notification flags which should be checked
    /// prior to invoking the [`Self::callback`].
    event_notification_flags: Option<redis_module::NotifyEvent>,
}

impl std::fmt::Debug for NotificationConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let callback = if let Some(callback) = self.callback.as_ref() {
            format!("Some({callback:p})")
        } else {
            "None".to_owned()
        };

        f.debug_struct("NotificationConsumer")
            .field("key", &self.key)
            .field("callback", &callback)
            .field("stats", &self.stats)
            .field("description", &self.description)
            .field("event_notification_flags", &self.event_notification_flags)
            .finish()
    }
}

impl NotificationConsumer {
    fn new(
        key: ConsumerKey,
        callback: NotificationCallback,
        description: Option<String>,
        event_notification_flags: Option<redis_module::NotifyEvent>,
    ) -> NotificationConsumer {
        NotificationConsumer {
            key: Some(key),
            callback: Some(callback),
            stats: Arc::new(RefCellWrapper {
                ref_cell: RefCell::new(NotificationConsumerStats {
                    num_trigger: 0,
                    num_success: 0,
                    num_failed: 0,
                    num_finished: 0,
                    last_error: None,
                    last_execution_time: 0,
                    total_execution_time: 0,
                }),
            }),
            description,
            event_notification_flags: event_notification_flags,
        }
    }

    pub(crate) fn set_callback(&mut self, callback: NotificationCallback) -> NotificationCallback {
        let old_callback = self.callback.take();
        self.callback = Some(callback);
        old_callback.unwrap()
    }

    pub(crate) fn set_key(&mut self, key: ConsumerKey) -> ConsumerKey {
        let old_key = self.key.take();
        self.key = Some(key);
        old_key.unwrap()
    }

    pub(crate) fn set_description(&mut self, description: Option<String>) -> Option<String> {
        let old_description = self.description.take();
        self.description = description;
        old_description
    }

    pub(crate) fn get_stats(&self) -> NotificationConsumerStats {
        self.stats.ref_cell.borrow().clone()
    }

    pub(crate) fn get_description(&self) -> Option<String> {
        self.description.clone()
    }
}

fn fire_event(
    ctx: &Context,
    consumer: &Arc<RefCell<NotificationConsumer>>,
    event: &str,
    key: &[u8],
) {
    let c = consumer.borrow();
    {
        let mut stats = c.stats.ref_cell.borrow_mut();
        stats.num_trigger += 1;
    }
    let stats_ref = Arc::clone(&c.stats);
    let start_time = SystemTime::now();

    (c.callback.as_ref().unwrap())(
        ctx,
        event,
        key,
        Box::new(move |res| {
            let duration = match SystemTime::now().duration_since(start_time) {
                Ok(d) => d.as_millis(),
                Err(_) => 0,
            };
            let mut stats = stats_ref.ref_cell.borrow_mut();
            stats.num_finished += 1;
            stats.last_execution_time = duration;
            stats.total_execution_time += duration;
            if let Err(e) = res {
                stats.num_failed += 1;
                stats.last_error = Some(e);
            } else {
                stats.num_success += 1;
            }
        }),
    );
}

pub(crate) struct KeysNotificationsCtx {
    consumers: Vec<Weak<RefCell<NotificationConsumer>>>,
}

impl KeysNotificationsCtx {
    pub(crate) fn new() -> KeysNotificationsCtx {
        KeysNotificationsCtx {
            consumers: Vec::new(),
        }
    }

    pub(crate) fn add_consumer_on_prefix(
        &mut self,
        prefix: &[u8],
        callback: NotificationCallback,
        description: Option<String>,
        event_notification_flags: Option<redis_module::NotifyEvent>,
    ) -> Arc<RefCell<NotificationConsumer>> {
        let consumer = Arc::new(RefCell::new(NotificationConsumer::new(
            ConsumerKey::Prefix(prefix.to_vec()),
            callback,
            description,
            event_notification_flags,
        )));
        self.consumers.push(Arc::downgrade(&consumer));
        consumer
    }

    pub(crate) fn add_consumer_on_key(
        &mut self,
        key: &[u8],
        callback: NotificationCallback,
        description: Option<String>,
        event_notification_flags: Option<redis_module::NotifyEvent>,
    ) -> Arc<RefCell<NotificationConsumer>> {
        let consumer = Arc::new(RefCell::new(NotificationConsumer::new(
            ConsumerKey::Key(key.to_vec()),
            callback,
            description,
            event_notification_flags,
        )));
        self.consumers.push(Arc::downgrade(&consumer));
        consumer
    }

    pub(crate) fn on_key_touched(
        &self,
        ctx: &Context,
        event_notification_type: redis_module::NotifyEvent,
        event: &str,
        key: &[u8],
    ) {
        for consumer in self.consumers.iter() {
            let consumer = match consumer.upgrade() {
                Some(c) => c,
                None => continue,
            };

            let res = {
                let c = consumer.borrow_mut();

                // A fix for 1.2
                if let Some(flags) = c.event_notification_flags {
                    if !flags.contains(event_notification_type) {
                        continue;
                    }
                } else if event_notification_type.contains(redis_module::NotifyEvent::NEW) {
                    continue;
                }

                match c.key.as_ref().unwrap() {
                    ConsumerKey::Key(k) => key == k,
                    ConsumerKey::Prefix(prefix) => key.starts_with(prefix),
                }
            };
            if res {
                fire_event(ctx, &consumer, event, key);
            }
        }
    }
}
