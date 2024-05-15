/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::FunctionFlags, run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::RedisClientCtxInterface, stream_ctx::StreamCtxInterface,
    stream_ctx::StreamProcessCtxInterface, stream_ctx::StreamRecordAck,
    stream_ctx::StreamRecordInterface,
};

use redis_module::{
    raw::RedisModuleStreamID, stream::StreamRecord, AclPermissions, Context, RedisString,
    ThreadSafeContext,
};

use super::background_run_ctx::BackgroundRunCtx;
use super::run_ctx::{RedisClient, RedisClientCallOptions};
use super::GearsLibraryMetaData;

use super::stream_reader::{StreamConsumer, StreamReaderAck};

use super::get_notification_blocker;

use std::sync::Arc;

use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;

pub(crate) struct StreamRunCtx<'ctx> {
    ctx: &'ctx Context,
    lib_meta_data: Arc<GearsLibraryMetaData>,
    flags: FunctionFlags,
}

impl<'ctx> StreamRunCtx<'ctx> {
    fn new(
        ctx: &'ctx Context,
        lib_meta_data: &Arc<GearsLibraryMetaData>,
        flags: FunctionFlags,
    ) -> StreamRunCtx<'ctx> {
        StreamRunCtx {
            ctx,
            lib_meta_data: Arc::clone(lib_meta_data),
            flags,
        }
    }
}

impl<'ctx> StreamProcessCtxInterface for StreamRunCtx<'ctx> {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface + '_> {
        Box::new(RedisClient::new(
            self.ctx,
            self.lib_meta_data.clone(),
            self.lib_meta_data.user.safe_clone(self.ctx),
            self.flags,
        ))
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            self.lib_meta_data.user.safe_clone(self.ctx),
            &self.lib_meta_data,
            RedisClientCallOptions::new(self.flags),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct GearsStreamRecord {
    pub(crate) record: StreamRecord,
}

unsafe impl Sync for GearsStreamRecord {}
unsafe impl Send for GearsStreamRecord {}

impl super::stream_reader::StreamReaderRecord for GearsStreamRecord {
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
    lib_meta_data: Arc<GearsLibraryMetaData>,
    flags: FunctionFlags,
    permissions: AclPermissions,
}

impl GearsStreamConsumer {
    pub(crate) fn new(
        user: &Arc<GearsLibraryMetaData>,
        flags: FunctionFlags,
        ctx: Box<dyn StreamCtxInterface>,
    ) -> GearsStreamConsumer {
        let permissions = AclPermissions::all();
        GearsStreamConsumer {
            ctx,
            lib_meta_data: Arc::clone(user),
            flags,
            permissions,
        }
    }
}

impl std::fmt::Debug for GearsStreamConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GearsStreamConsumer")
            .field("ctx", &format!("{:p}", &self.ctx))
            .field("lib_meta_data", &self.lib_meta_data)
            .field("flags", &self.flags)
            .field("permissions", &self.permissions)
            .finish()
    }
}

impl StreamConsumer<GearsStreamRecord> for GearsStreamConsumer {
    fn new_data(
        &self,
        ctx: &Context,
        stream_name: &[u8],
        record: GearsStreamRecord,
        ack_callback: Box<dyn FnOnce(&Context, StreamReaderAck) + Send>,
    ) -> Option<StreamReaderAck> {
        let user = &self.lib_meta_data.user;
        let key_redis_str = RedisString::create_from_slice(std::ptr::null_mut(), stream_name);
        if let Err(e) = ctx.acl_check_key_permission(user, &key_redis_str, &self.permissions) {
            return Some(StreamReaderAck::Nack(GearsApiError::new(format!(
                "User '{}' has no permissions on key '{}', {}.",
                user,
                std::str::from_utf8(stream_name).unwrap_or("[binary data]"),
                e
            ))));
        }

        let res = {
            let _notification_blocker = get_notification_blocker();
            self.ctx.process_record(
                stream_name,
                Box::new(record),
                &StreamRunCtx::new(ctx, &self.lib_meta_data, self.flags),
                Box::new(|ack| {
                    // here we must take the redis lock
                    let ctx = ThreadSafeContext::new();
                    let gaurd = ctx.lock();
                    ack_callback(
                        &gaurd,
                        match ack {
                            StreamRecordAck::Ack => StreamReaderAck::Ack,
                            StreamRecordAck::Nack(msg) => StreamReaderAck::Nack(msg),
                        },
                    )
                }),
            )
        };
        res.map(|r| match r {
            StreamRecordAck::Ack => StreamReaderAck::Ack,
            StreamRecordAck::Nack(msg) => StreamReaderAck::Nack(msg),
        })
    }
}
