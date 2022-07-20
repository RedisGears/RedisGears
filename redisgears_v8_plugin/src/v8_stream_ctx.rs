use v8_rs::v8::{v8_promise::V8PromiseState, v8_value::V8LocalValue, v8_value::V8PersistValue};

use redisgears_plugin_api::redisgears_plugin_api::stream_ctx::{
    StreamCtxInterface, StreamProcessCtxInterface, StreamRecordAck, StreamRecordInterface,
};

use redisgears_plugin_api::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;

use crate::v8_native_functions::{get_backgrounnd_client, get_redis_client, RedisClient};
use crate::v8_script_ctx::V8ScriptCtx;

use std::cell::RefCell;
use std::sync::Arc;

use std::str;

use crate::get_exception_msg;

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
        persisted_function: V8PersistValue,
        script_ctx: &Arc<V8ScriptCtx>,
        is_async: bool,
    ) -> V8StreamCtx {
        V8StreamCtx {
            internals: Arc::new(V8StreamCtxInternals {
                persisted_function: persisted_function,
                script_ctx: Arc::clone(script_ctx),
            }),
            is_async: is_async,
        }
    }
}

impl V8StreamCtxInternals {
    fn process_record_internal_sync(
        &self,
        stream_name: &str,
        record: Box<dyn StreamRecordInterface>,
        run_ctx: &dyn StreamProcessCtxInterface,
    ) -> Option<StreamRecordAck> {
        let _isolate_scope = self.script_ctx.isolate.enter();
        let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
        let ctx_scope = self.script_ctx.ctx.enter();
        let trycatch = self.script_ctx.isolate.new_try_catch();

        let id = record.get_id();
        let id_v8_arr = self.script_ctx.isolate.new_array(&[
            &self.script_ctx.isolate.new_long(id.0 as i64),
            &self.script_ctx.isolate.new_long(id.1 as i64),
        ]);
        let stream_name_v8_str = self.script_ctx.isolate.new_string(stream_name);

        let vals = record
            .fields()
            .map(|(f, v)| (str::from_utf8(f), str::from_utf8(v)))
            .filter(|(f, v)| {
                if f.is_err() || v.is_err() {
                    false
                } else {
                    true
                }
            })
            .map(|(f, v)| (f.unwrap(), v.unwrap()))
            .map(|(f, v)| {
                self.script_ctx
                    .isolate
                    .new_array(&[
                        &self.script_ctx.isolate.new_string(f).to_value(),
                        &self.script_ctx.isolate.new_string(v).to_value(),
                    ])
                    .to_value()
            })
            .collect::<Vec<V8LocalValue>>();

        let val_v8_arr = self
            .script_ctx
            .isolate
            .new_array(&vals.iter().collect::<Vec<&V8LocalValue>>());

        let stream_data = self.script_ctx.isolate.new_object();
        stream_data.set(
            &ctx_scope,
            &self.script_ctx.isolate.new_string("id").to_value(),
            &id_v8_arr.to_value(),
        );
        stream_data.set(
            &ctx_scope,
            &self.script_ctx.isolate.new_string("stream_name").to_value(),
            &stream_name_v8_str.to_value(),
        );
        stream_data.set(
            &ctx_scope,
            &self.script_ctx.isolate.new_string("record").to_value(),
            &val_v8_arr.to_value(),
        );

        let c = run_ctx.get_redis_client();
        let mut redis_client = RedisClient::new();
        redis_client.set_client(c);
        let redis_client = Arc::new(RefCell::new(redis_client));
        let r_client = get_redis_client(&self.script_ctx, &ctx_scope, &redis_client);

        ctx_scope.set_private_data(0, Some(&true)); // indicate we are blocked

        self.script_ctx.before_run();
        self.script_ctx.after_lock_gil();
        let res = self
            .persisted_function
            .as_local(&self.script_ctx.isolate)
            .call(
                &ctx_scope,
                Some(&[&r_client.to_value(), &stream_data.to_value()]),
            );
        self.script_ctx.before_release_gil();
        self.script_ctx.after_run();

        ctx_scope.set_private_data::<bool>(0, None); // indicate we are not blocked

        redis_client.borrow_mut().make_invalid();

        Some(match res {
            Some(_) => StreamRecordAck::Ack,
            None => {
                // todo: handle promise
                let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch);
                StreamRecordAck::Nack(error_msg)
            }
        })
    }

    fn process_record_internal_async(
        &self,
        stream_name: &str,
        record: Box<dyn StreamRecordInterface>,
        redis_client: Box<dyn BackgroundRunFunctionCtxInterface>,
        ack_callback: Box<dyn FnOnce(StreamRecordAck) + Send>,
    ) {
        let ack_callback = Arc::new(RefCell::new(V8StreamAckCtx {
            ack: Some(ack_callback),
        }));
        let res = {
            let _isolate_scope = self.script_ctx.isolate.enter();
            let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
            let ctx_scope = self.script_ctx.ctx.enter();
            let trycatch = self.script_ctx.isolate.new_try_catch();

            let id = record.get_id();
            let id_v8_arr = self.script_ctx.isolate.new_array(&[
                &self.script_ctx.isolate.new_long(id.0 as i64),
                &self.script_ctx.isolate.new_long(id.1 as i64),
            ]);
            let stream_name_v8_str = self.script_ctx.isolate.new_string(stream_name);

            let vals = record
                .fields()
                .map(|(f, v)| (str::from_utf8(f), str::from_utf8(v)))
                .filter(|(f, v)| {
                    if f.is_err() || v.is_err() {
                        false
                    } else {
                        true
                    }
                })
                .map(|(f, v)| (f.unwrap(), v.unwrap()))
                .map(|(f, v)| {
                    self.script_ctx
                        .isolate
                        .new_array(&[
                            &self.script_ctx.isolate.new_string(f).to_value(),
                            &self.script_ctx.isolate.new_string(v).to_value(),
                        ])
                        .to_value()
                })
                .collect::<Vec<V8LocalValue>>();

            let val_v8_arr = self
                .script_ctx
                .isolate
                .new_array(&vals.iter().collect::<Vec<&V8LocalValue>>());

            let stream_data = self.script_ctx.isolate.new_object();
            stream_data.set(
                &ctx_scope,
                &self.script_ctx.isolate.new_string("id").to_value(),
                &id_v8_arr.to_value(),
            );
            stream_data.set(
                &ctx_scope,
                &self.script_ctx.isolate.new_string("stream_name").to_value(),
                &stream_name_v8_str.to_value(),
            );
            stream_data.set(
                &ctx_scope,
                &self.script_ctx.isolate.new_string("record").to_value(),
                &val_v8_arr.to_value(),
            );

            let r_client = get_backgrounnd_client(&self.script_ctx, &ctx_scope, redis_client);

            self.script_ctx.before_run();
            let res = self
                .persisted_function
                .as_local(&self.script_ctx.isolate)
                .call(
                    &ctx_scope,
                    Some(&[&r_client.to_value(), &stream_data.to_value()]),
                );
            self.script_ctx.after_run();

            match res {
                Some(res) => {
                    if res.is_promise() {
                        let res = res.as_promise();
                        if res.state() == V8PromiseState::Rejected {
                            let error_utf8 =
                                res.get_result().to_utf8(&self.script_ctx.isolate).unwrap();
                            Some(StreamRecordAck::Nack(error_utf8.as_str().to_string()))
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
                            let reject =
                                ctx_scope.new_native_function(move |args, isolate, _ctx_scope| {
                                    let res = args.get(0);
                                    let res = res.to_utf8(isolate).unwrap();
                                    let res = res.as_str().to_string();
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) = ack_callback_reject.borrow_mut().ack.take() {
                                        ack(StreamRecordAck::Nack(res));
                                    }
                                    None
                                });
                            res.then(&ctx_scope, &resolve, &reject);
                            None
                        }
                    } else {
                        Some(StreamRecordAck::Ack)
                    }
                }
                None => {
                    // todo: hanlde promise
                    let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch);
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
        stream_name: &str,
        record: Box<dyn StreamRecordInterface + Send>,
        run_ctx: &dyn StreamProcessCtxInterface,
        ack_callback: Box<dyn FnOnce(StreamRecordAck) + Send>,
    ) -> Option<StreamRecordAck> {
        if self.is_async {
            let internals = Arc::clone(&self.internals);
            let stream_name = stream_name.to_string();
            let bg_redis_client = run_ctx.get_background_redis_client();
            self.internals
                .script_ctx
                .compiled_library_api
                .run_on_background(Box::new(move || {
                    internals.process_record_internal_async(
                        &stream_name.to_string(),
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
