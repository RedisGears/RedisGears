/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::v8_script_ctx::V8ScriptCtx;
use std::sync::{Arc, Mutex, Weak};

use v8_rs::v8::{
    isolate_scope::V8IsolateScope, v8_array::V8LocalArray, v8_array_buffer::V8LocalArrayBuffer,
    v8_context_scope::V8ContextScope, v8_object::V8LocalObject,
    v8_object_template::V8PersistedObjectTemplate, v8_utf8::V8LocalUtf8, v8_value::V8LocalValue,
};

use crate::v8_native_functions::RedisClient;

use std::cell::RefCell;

use redisgears_plugin_api::redisgears_plugin_api::redisai_interface::AITensorInterface;

use v8_derive::new_native_function;

// Silenced due to actually having a need to return a reference to a
// boxed trait object, as we store boxed trait objects. We could store
// the fat pointers of trait objects but those don't have a stable ABI
// and so we have to deal with another level of indirection.
#[allow(clippy::borrowed_box)]
pub(crate) fn get_tensor_from_js_tensor<'isolate_scope>(
    js_tensor: &'isolate_scope V8LocalObject<'isolate_scope, '_>,
) -> Result<&'isolate_scope Box<dyn AITensorInterface>, String> {
    if js_tensor.get_internal_field_count() != 1 {
        return Err("Data is not a tensor".into());
    }
    let external_data = js_tensor.get_internal_field(0);
    if !external_data.is_external() {
        return Err("Data is not a tensor".into());
    }
    let external_data = external_data.as_external_data();
    Ok(external_data.get_data::<Box<dyn AITensorInterface>>())
}

pub(crate) fn get_js_tensor_from_tensor<'isolate, 'isolate_scope>(
    script_ctx: &Arc<V8ScriptCtx>,
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
    tensor: Box<dyn AITensorInterface>,
) -> V8LocalObject<'isolate_scope, 'isolate> {
    let tensor_external = isolate_scope.new_external_data(tensor);
    let tensor_obj = script_ctx
        .tensor_object_template
        .to_local(isolate_scope)
        .new_instance(ctx_scope);
    tensor_obj.set_internal_field(0, &tensor_external.to_value());
    tensor_obj
}

pub(crate) fn get_tensor_object_template(
    isolate_scope: &V8IsolateScope,
) -> V8PersistedObjectTemplate {
    let mut obj_template = isolate_scope.new_object_template();

    obj_template.add_native_function("get_data", move |args, isolate_scope, _ctx_scope| {
        let curr_self = args.get_self();
        let tensor = get_tensor_from_js_tensor(&curr_self).ok()?;
        let data = tensor.get_data();
        Some(isolate_scope.new_array_buffer(data).to_value())
    });

    obj_template.add_native_function("dims", move |args, isolate_scope, _ctx_scope| {
        let curr_self = args.get_self();
        let tensor = get_tensor_from_js_tensor(&curr_self).ok()?;
        let dims = tensor.dims();
        let res = dims
            .into_iter()
            .map(|v| isolate_scope.new_long(v))
            .collect::<Vec<V8LocalValue>>();
        Some(
            isolate_scope
                .new_array(&res.iter().collect::<Vec<&V8LocalValue>>())
                .to_value(),
        )
    });

    obj_template.add_native_function("element_size", move |args, isolate_scope, _ctx_scope| {
        let curr_self = args.get_self();
        let tensor = get_tensor_from_js_tensor(&curr_self).ok()?;
        let element_size = tensor.element_size();
        Some(isolate_scope.new_long(element_size as i64))
    });

    obj_template.set_internal_field_count(1);
    obj_template.persist()
}

pub(crate) fn get_redisai_api<'isolate, 'isolate_scope>(
    script_ctx: &Arc<V8ScriptCtx>,
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
) -> V8LocalValue<'isolate_scope, 'isolate> {
    let redis_ai = isolate_scope.new_object();

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis_ai.set_native_function(
        ctx_scope,
        "create_tensor",
        new_native_function!(move |isolate_scope,
                                   ctx_scope,
                                   data_type_utf8: V8LocalUtf8,
                                   dims: V8LocalArray,
                                   data: V8LocalArrayBuffer| {
            let mut dims_vec = Vec::new();
            for i in 0..dims.len() {
                let val = dims.get(ctx_scope, i);
                if !val.is_long() {
                    return Err("Tensor dims must be integers".to_string());
                }
                dims_vec.push(val.get_long());
            }

            let s = script_ctx_ref
                .upgrade()
                .ok_or("On redisai_create_tensor, use of invalid script ctx.".to_string())?;
            let tensor = s
                .compiled_library_api
                .redisai_create_tensor(data_type_utf8.as_str(), &dims_vec, data.data())
                .map_err(|e| e.get_msg().to_string())?;
            let tensor_obj = get_js_tensor_from_tensor(&s, isolate_scope, ctx_scope, tensor);
            Ok(Some(tensor_obj.to_value()))
        }),
    );

    redis_ai.to_value()
}

pub(crate) fn get_redisai_client<'isolate, 'isolate_scope>(
    script_ctx: &Arc<V8ScriptCtx>,
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
    redis_client: &Arc<RefCell<RedisClient>>,
) -> V8LocalValue<'isolate_scope, 'isolate> {
    let redis_ai_client = isolate_scope.new_object();

    let script_ctx_ref = Arc::downgrade(script_ctx);
    let redis_client_ref = Arc::clone(redis_client);
    redis_ai_client.set_native_function(ctx_scope, "open_model", new_native_function!(move |isolate_scope, ctx_scope, name_utf8: V8LocalUtf8| {
        let client = redis_client_ref.borrow();
        let client = client.get().ok_or_else(|| "Used on invalid client".to_owned())?;

        let model = match client.open_ai_model(name_utf8.as_str()) {
            Ok(model) => model,
            Err(e) => {
                isolate_scope.raise_exception_str(e.get_msg());
                return Err(e.get_msg().to_string());
            }
        };

        let model_object = isolate_scope.new_object();
        let script_ctx_ref = Weak::clone(&script_ctx_ref);
        model_object.set_native_function(ctx_scope, "get_model_runner", new_native_function!(move |isolate_scope, ctx_scope| {
            let model_runner = Arc::new(Mutex::new(model.get_model_runner()));
            let model_runner_clone = Arc::clone(&model_runner);
            let model_runner_object = isolate_scope.new_object();
            model_runner_object.set_native_function(ctx_scope, "add_input", new_native_function!(move |_isolate_scope, _ctx_scope, input_name_utf8: V8LocalUtf8, tensor_js: V8LocalObject| {
                let tensor = get_tensor_from_js_tensor(&tensor_js)?;

                let mut model_runner = model_runner_clone.lock().unwrap();
                let res = model_runner.as_mut().add_input(input_name_utf8.as_str(), tensor.as_ref());
                if let Err(e) = res {
                    return Err(e.get_msg().to_string());
                }
                Ok(None)
            }));

            let model_runner_clone = Arc::clone(&model_runner);
            model_runner_object.set_native_function(ctx_scope, "add_output", new_native_function!(move |_isolate_scope, _ctx_scope, output_name_utf8: V8LocalUtf8| {
                let mut model_runner = model_runner_clone.lock().unwrap();
                let res = model_runner.as_mut().add_output(output_name_utf8.as_str());
                if let Err(e) = res {
                    return Err(e.get_msg().to_string());
                }
                Ok(None)
            }));

            let script_ctx_ref = Weak::clone(&script_ctx_ref);
            model_runner_object.set_native_function(ctx_scope, "run", new_native_function!(move |_isolate_scope, ctx_scope| {
                let mut model_runner = model_runner.lock().unwrap();
                let resolver = ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let mut persisted_resolver = resolver.to_value().persist();
                let script_ctx_ref = Weak::clone(&script_ctx_ref);
                model_runner.run(Box::new(move |res|{
                    let script_ctx_upgraded = match script_ctx_ref.upgrade() {
                        Some(s) => s,
                        None => {
                            persisted_resolver.forget();
                            crate::v8_backend::log("Use of invalid function context on redisai on_done");
                            return;
                        }
                    };

                    script_ctx_upgraded.compiled_library_api.run_on_background(Box::new(move || {
                        let script_ctx_ref = match script_ctx_ref.upgrade() {
                            Some(s) => s,
                            None => {
                                persisted_resolver.forget();
                                crate::v8_backend::log("Use of invalid function context on redisai on_done");
                                return;
                            }
                        };
                        let isolate_scope = script_ctx_ref.isolate.enter();
                        let ctx_scope = script_ctx_ref.ctx.enter(&isolate_scope);
                        let resolver = persisted_resolver.take_local(&isolate_scope).as_resolver();

                        match res {
                            Ok(res) => {
                                let values = res.into_iter().map(|v| get_js_tensor_from_tensor(&script_ctx_ref, &isolate_scope, &ctx_scope, v).to_value()).collect::<Vec<V8LocalValue>>();
                                let res_js = isolate_scope.new_array(&values.iter().collect::<Vec<&V8LocalValue>>()).to_value();
                                resolver.resolve(&ctx_scope, &res_js);
                            }
                            Err(e) => {
                                resolver.reject(&ctx_scope, &isolate_scope.new_string(e.get_msg()).to_value());
                            }
                        }

                    }));
                }));

                Ok::<Option<_>, String>(Some(promise.to_value()))
            }));
            Ok::<Option<_>, String>(Some(model_runner_object.to_value()))
        }));

        model_object.freeze(ctx_scope);

        Ok(Some(model_object.to_value()))
    }));

    let script_ctx_ref = Arc::downgrade(script_ctx);
    let redis_client_ref = Arc::clone(redis_client);
    redis_ai_client.set_native_function(ctx_scope, "open_script", new_native_function!(move |isolate_scope, ctx_scope, name_utf8: V8LocalUtf8| {
        let client = redis_client_ref.borrow();
        let client = client.get().ok_or_else(|| "Used on invalid client".to_owned())?;

        let script = client.open_ai_script(name_utf8.as_str()).map_err(|e| e.get_msg().to_string())?;

        let script_object = isolate_scope.new_object();
        let script_ctx_ref = Weak::clone(&script_ctx_ref);
        script_object.set_native_function(ctx_scope, "get_script_runner", new_native_function!(move |isolate_scope, ctx_scope, name_utf8: V8LocalUtf8| {
            let script_runner = Arc::new(Mutex::new(script.get_script_runner(name_utf8.as_str())));
            let script_runner_clone = Arc::clone(&script_runner);
            let script_runner_object = isolate_scope.new_object();
            script_runner_object.set_native_function(ctx_scope,"add_input", new_native_function!(move |_isolate_scope, _ctx_scope, tensor_js: V8LocalObject| {
                let tensor = get_tensor_from_js_tensor(&tensor_js)?;
                let mut script_runner = script_runner_clone.lock().unwrap();
                script_runner.as_mut().add_input(tensor.as_ref()).map_err(|e| e.get_msg().to_string())?;
                Ok::<_, String>(None)
            }));

            let script_runner_clone = Arc::clone(&script_runner);
            script_runner_object.set_native_function(ctx_scope, "add_output", new_native_function!(move |_isolate_scope, _ctx_scope| {
                let mut script_runner = script_runner_clone.lock().unwrap();
                script_runner.as_mut().add_output().map_err(|e| e.get_msg().to_string())?;
                Ok::<_, String>(None)
            }));

            let script_ctx_ref = Weak::clone(&script_ctx_ref);
            script_runner_object.set_native_function(ctx_scope, "run", new_native_function!(move |_isolate_scope, ctx_scope| {
                let mut script_runner = script_runner.lock().unwrap();
                let resolver = ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let mut persisted_resolver = resolver.to_value().persist();
                let script_ctx_ref = Weak::clone(&script_ctx_ref);
                script_runner.run(Box::new(move |res|{
                    let script_ctx_upgraded = match script_ctx_ref.upgrade() {
                        Some(s) => s,
                        None => {
                            persisted_resolver.forget();
                            crate::v8_backend::log("Use of invalid function context on redisai on_done");
                            return;
                        }
                    };

                    script_ctx_upgraded.compiled_library_api.run_on_background(Box::new(move || {
                        let script_ctx_ref = match script_ctx_ref.upgrade() {
                            Some(s) => s,
                            None => {
                                persisted_resolver.forget();
                                crate::v8_backend::log("Use of invalid function context on redisai on_done");
                                return;
                            }
                        };
                        let isolate_scope = script_ctx_ref.isolate.enter();
                        let ctx_scope = script_ctx_ref.ctx.enter(&isolate_scope);

                        let resolver = persisted_resolver.take_local(&isolate_scope).as_resolver();

                        match res {
                            Ok(res) => {
                                let values = res.into_iter().map(|v| get_js_tensor_from_tensor(&script_ctx_ref, &isolate_scope, &ctx_scope, v).to_value()).collect::<Vec<V8LocalValue>>();
                                let res_js = isolate_scope.new_array(&values.iter().collect::<Vec<&V8LocalValue>>()).to_value();
                                resolver.resolve(&ctx_scope, &res_js);
                            }
                            Err(e) => {
                                resolver.reject(&ctx_scope, &isolate_scope.new_string(e.get_msg()).to_value());
                            }
                        }

                    }));
                }));

                Ok::<_, String>(Some(promise.to_value()))
            }));
            Ok::<_, String>(Some(script_runner_object.to_value()))
        }));

        script_object.freeze(ctx_scope);

        Ok::<Option<_>, String>(Some(script_object.to_value()))
    }));

    redis_ai_client.to_value()
}
