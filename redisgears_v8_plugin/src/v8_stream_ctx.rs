/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;
use v8_rs::v8::{v8_promise::V8PromiseState, v8_value::V8LocalValue, v8_value::V8PersistValue};

use redisgears_plugin_api::redisgears_plugin_api::stream_ctx::{
    StreamCtxInterface, StreamProcessCtxInterface, StreamRecordAck, StreamRecordInterface,
};

use redisgears_plugin_api::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;

use crate::v8_backend::is_safe_to_run_code;
use crate::v8_native_functions::{get_backgrounnd_client, get_redis_client, RedisClient};
use crate::v8_script_ctx::V8ScriptCtx;

use std::cell::RefCell;
use std::sync::Arc;

use std::str;

use v8_derive::new_native_function;

use crate::{get_error_from_object, get_exception_msg};

struct V8StreamAckCtx {
    ack: Option<Box<dyn FnOnce(StreamRecordAck) + Send>>,
}

struct V8StreamCtxInternals {
    persisted_function: V8PersistValue,
    script_ctx: Arc<V8ScriptCtx>,
}

pub struct V8StreamCtx {
    internals: Arc<V8StreamCtxInternals>,
    is_async: bool,
}

impl V8StreamCtx {
    pub(crate) fn new(
        mut persisted_function: V8PersistValue,
        script_ctx: &Arc<V8ScriptCtx>,
        is_async: bool,
    ) -> Self {
        persisted_function.forget();
        Self {
            internals: Arc::new(V8StreamCtxInternals {
                persisted_function,
                script_ctx: Arc::clone(script_ctx),
            }),
            is_async,
        }
    }
}

impl V8StreamCtxInternals {
    fn process_record_internal_sync(
        &self,
        stream_name: &[u8],
        record: Box<dyn StreamRecordInterface>,
        run_ctx: &dyn StreamProcessCtxInterface,
    ) -> Option<StreamRecordAck> {
        let isolate_scope = self.script_ctx.isolate.enter();
        let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
        let trycatch = isolate_scope.new_try_catch();

        let id = record.get_id();
        let id_v8_arr = isolate_scope.new_array(&[
            &isolate_scope.new_long(id.0 as i64),
            &isolate_scope.new_long(id.1 as i64),
        ]);
        let stream_name_v8_str = match std::str::from_utf8(stream_name) {
            Ok(s) => isolate_scope.new_string(s).to_value(),
            Err(_) => isolate_scope.new_null(),
        };

        let vals = record
            .fields()
            .map(|(f, v)| {
                let f = match str::from_utf8(f) {
                    Ok(s) => isolate_scope.new_string(s).to_value(),
                    Err(_) => isolate_scope.new_null(),
                };
                let v = match str::from_utf8(v) {
                    Ok(s) => isolate_scope.new_string(s).to_value(),
                    Err(_) => isolate_scope.new_null(),
                };
                isolate_scope.new_array(&[&f, &v]).to_value()
            })
            .collect::<Vec<V8LocalValue>>();

        let raw_vals = record
            .fields()
            .map(|(f, v)| {
                isolate_scope
                    .new_array(&[
                        &isolate_scope.new_array_buffer(f).to_value(),
                        &isolate_scope.new_array_buffer(v).to_value(),
                    ])
                    .to_value()
            })
            .collect::<Vec<V8LocalValue>>();

        let val_v8_arr = isolate_scope.new_array(&vals.iter().collect::<Vec<&V8LocalValue>>());

        let raw_val_v8_arr =
            isolate_scope.new_array(&raw_vals.iter().collect::<Vec<&V8LocalValue>>());

        let stream_data = isolate_scope.new_object();
        stream_data.set(
            &ctx_scope,
            &isolate_scope.new_string("id").to_value(),
            &id_v8_arr.to_value(),
        );
        stream_data.set(
            &ctx_scope,
            &isolate_scope.new_string("stream_name").to_value(),
            &stream_name_v8_str,
        );
        stream_data.set(
            &ctx_scope,
            &isolate_scope.new_string("stream_name_raw").to_value(),
            &isolate_scope.new_array_buffer(stream_name).to_value(),
        );
        stream_data.set(
            &ctx_scope,
            &isolate_scope.new_string("record").to_value(),
            &val_v8_arr.to_value(),
        );
        stream_data.set(
            &ctx_scope,
            &isolate_scope.new_string("record_raw").to_value(),
            &raw_val_v8_arr.to_value(),
        );

        let c = run_ctx.get_redis_client();
        let redis_client = Arc::new(RefCell::new(RedisClient::with_client(c.as_ref())));
        let r_client =
            get_redis_client(&self.script_ctx, &isolate_scope, &ctx_scope, &redis_client);

        let _block_guard = ctx_scope.set_private_data(0, &true); // indicate we are blocked

        self.script_ctx.before_run();
        self.script_ctx.after_lock_gil();
        let res = self.persisted_function.as_local(&isolate_scope).call(
            &ctx_scope,
            Some(&[&r_client.to_value(), &stream_data.to_value()]),
        );
        self.script_ctx.before_release_gil();
        self.script_ctx.after_run();

        redis_client.borrow_mut().make_invalid();

        Some(match res {
            Some(_) => StreamRecordAck::Ack,
            None => {
                // todo: handle promise
                let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch, &ctx_scope);
                StreamRecordAck::Nack(error_msg)
            }
        })
    }

    fn process_record_internal_async(
        &self,
        stream_name: &[u8],
        record: Box<dyn StreamRecordInterface>,
        redis_client: Box<dyn BackgroundRunFunctionCtxInterface>,
        ack_callback: Box<dyn FnOnce(StreamRecordAck) + Send>,
    ) {
        let ack_callback = Arc::new(RefCell::new(V8StreamAckCtx {
            ack: Some(ack_callback),
        }));
        let res = {
            let isolate_scope = self.script_ctx.isolate.enter();
            let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
            let trycatch = isolate_scope.new_try_catch();

            let id = record.get_id();
            let id_v8_arr = isolate_scope.new_array(&[
                &isolate_scope.new_long(id.0 as i64),
                &isolate_scope.new_long(id.1 as i64),
            ]);
            let stream_name_v8_str = match std::str::from_utf8(stream_name) {
                Ok(s) => isolate_scope.new_string(s).to_value(),
                Err(_) => isolate_scope.new_null(),
            };

            let vals = record
                .fields()
                .map(|(f, v)| {
                    let f = match str::from_utf8(f) {
                        Ok(s) => isolate_scope.new_string(s).to_value(),
                        Err(_) => isolate_scope.new_null(),
                    };
                    let v = match str::from_utf8(v) {
                        Ok(s) => isolate_scope.new_string(s).to_value(),
                        Err(_) => isolate_scope.new_null(),
                    };
                    isolate_scope.new_array(&[&f, &v]).to_value()
                })
                .collect::<Vec<V8LocalValue>>();

            let raw_vals = record
                .fields()
                .map(|(f, v)| {
                    isolate_scope
                        .new_array(&[
                            &isolate_scope.new_array_buffer(f).to_value(),
                            &isolate_scope.new_array_buffer(v).to_value(),
                        ])
                        .to_value()
                })
                .collect::<Vec<V8LocalValue>>();

            let val_v8_arr = isolate_scope.new_array(&vals.iter().collect::<Vec<&V8LocalValue>>());

            let raw_val_v8_arr =
                isolate_scope.new_array(&raw_vals.iter().collect::<Vec<&V8LocalValue>>());

            let stream_data = isolate_scope.new_object();
            stream_data.set(
                &ctx_scope,
                &isolate_scope.new_string("id").to_value(),
                &id_v8_arr.to_value(),
            );
            stream_data.set(
                &ctx_scope,
                &isolate_scope.new_string("stream_name").to_value(),
                &stream_name_v8_str,
            );
            stream_data.set(
                &ctx_scope,
                &isolate_scope.new_string("stream_name_raw").to_value(),
                &isolate_scope.new_array_buffer(stream_name).to_value(),
            );
            stream_data.set(
                &ctx_scope,
                &isolate_scope.new_string("record").to_value(),
                &val_v8_arr.to_value(),
            );
            stream_data.set(
                &ctx_scope,
                &isolate_scope.new_string("record_raw").to_value(),
                &raw_val_v8_arr.to_value(),
            );

            let r_client = get_backgrounnd_client(
                &self.script_ctx,
                &isolate_scope,
                &ctx_scope,
                Arc::new(redis_client),
            );

            self.script_ctx.before_run();
            let res = self.persisted_function.as_local(&isolate_scope).call(
                &ctx_scope,
                Some(&[&r_client.to_value(), &stream_data.to_value()]),
            );
            self.script_ctx.after_run();

            match res {
                Some(res) => {
                    if res.is_promise() {
                        let res = res.as_promise();
                        if res.state() == V8PromiseState::Rejected {
                            let res = res.get_result();
                            let error = get_error_from_object(&res, &ctx_scope);
                            Some(StreamRecordAck::Nack(error))
                        } else if res.state() == V8PromiseState::Fulfilled {
                            Some(StreamRecordAck::Ack)
                        } else {
                            let ack_callback_resolve = Arc::clone(&ack_callback);
                            let ack_callback_reject = Arc::clone(&ack_callback);
                            let resolve =
                                ctx_scope.new_native_function(move |_args, isolate, _context| {
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) = ack_callback_resolve.borrow_mut().ack.take()
                                    {
                                        ack(StreamRecordAck::Ack);
                                    }
                                    None
                                });
                            let reject = ctx_scope.new_native_function(new_native_function!(
                                move |isolate, ctx_scope, res: V8LocalValue| {
                                    let error = get_error_from_object(&res, ctx_scope);
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) = ack_callback_reject.borrow_mut().ack.take() {
                                        ack(StreamRecordAck::Nack(error));
                                    }
                                    Ok::<_, String>(None)
                                }
                            ));
                            res.then(&ctx_scope, &resolve, &reject);
                            None
                        }
                    } else {
                        Some(StreamRecordAck::Ack)
                    }
                }
                None => {
                    // todo: hanlde promise
                    let error_msg =
                        get_exception_msg(&self.script_ctx.isolate, trycatch, &ctx_scope);
                    Some(StreamRecordAck::Nack(error_msg))
                }
            }
        };

        if let Some(r) = res {
            if let Some(ack) = ack_callback.borrow_mut().ack.take() {
                ack(r);
            }
        }
    }
}

impl StreamCtxInterface for V8StreamCtx {
    fn process_record(
        &self,
        stream_name: &[u8],
        record: Box<dyn StreamRecordInterface + Send>,
        run_ctx: &dyn StreamProcessCtxInterface,
        ack_callback: Box<dyn FnOnce(StreamRecordAck) + Send>,
    ) -> Option<StreamRecordAck> {
        if !is_safe_to_run_code() {
            return Some(StreamRecordAck::Nack(GearsApiError::new(
                "JS engine reached OOM state and can not run any more code",
            )));
        }
        if self.is_async {
            let internals = Arc::clone(&self.internals);
            let stream_name: Vec<u8> = stream_name.to_vec();
            let bg_redis_client = run_ctx.get_background_redis_client();
            self.internals
                .script_ctx
                .compiled_library_api
                .run_on_background(Box::new(move || {
                    internals.process_record_internal_async(
                        &stream_name.clone(),
                        record,
                        bg_redis_client,
                        ack_callback,
                    );
                }));
            None
        } else {
            self.internals
                .process_record_internal_sync(stream_name, record, run_ctx)
        }
    }
}
