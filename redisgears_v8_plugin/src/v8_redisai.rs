use crate::v8_script_ctx::V8ScriptCtx;
use std::sync::{Arc, Mutex, Weak};

use v8_rs::v8::{
    isolate_scope::V8IsolateScope, v8_context_scope::V8ContextScope, v8_object::V8LocalObject,
    v8_object_template::V8PersistedObjectTemplate, v8_value::V8LocalValue,
};

use crate::v8_native_functions::RedisClient;

use std::cell::RefCell;

use redisgears_plugin_api::redisgears_plugin_api::redisai_interface::AITensorInterface;

pub(crate) fn get_tensor_from_js_tensor<'isolate, 'isolate_scope>(
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    js_tensor: &'isolate_scope V8LocalObject<'isolate_scope, 'isolate>,
) -> Option<&'isolate_scope Box<dyn AITensorInterface>> {
    if js_tensor.get_internal_field_count() != 1 {
        isolate_scope.raise_exception_str("Data is not a tensor");
        return None;
    }
    let external_data = js_tensor.get_internal_field(0);
    if !external_data.is_external() {
        isolate_scope.raise_exception_str("Data is not a tensor");
        return None;
    }
    let external_data = external_data.as_external_data();
    Some(external_data.get_data::<Box<dyn AITensorInterface>>())
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

pub(crate) fn get_tensor_object_template<'isolate, 'isolate_scope>(
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
) -> V8PersistedObjectTemplate {
    let mut obj_template = isolate_scope.new_object_template();

    obj_template.add_native_function("get_data", move |args, isolate_scope, _ctx_scope| {
        let curr_self = args.get_self();
        let tensor = get_tensor_from_js_tensor(isolate_scope, &curr_self)?;
        let data = tensor.get_data();
        Some(isolate_scope.new_array_buffer(data).to_value())
    });

    obj_template.add_native_function("dims", move |args, isolate_scope, _ctx_scope| {
        let curr_self = args.get_self();
        let tensor = get_tensor_from_js_tensor(isolate_scope, &curr_self)?;
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
        let tensor = get_tensor_from_js_tensor(isolate_scope, &curr_self)?;
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
    redis_ai.set(
        ctx_scope,
        &isolate_scope.new_string("create_tensor").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate_scope, ctx_scope| {
                if args.len() != 3 {
                    isolate_scope.raise_exception_str("Wrong number of arguments to 'redisai_create_tensor' function");
                    return None;
                }

                let data_type = args.get(0);
                if !data_type.is_string() && !data_type.is_string_object() {
                    isolate_scope.raise_exception_str("First argument to 'redisai_create_tensor' must be a string representing data type");
                    return None;
                }
                let data_type_utf8 = data_type.to_utf8().unwrap();

                let dims = args.get(1);
                if !dims.is_array() {
                    isolate_scope.raise_exception_str("Second argument to 'redisai_create_tensor' must be array of dims");
                    return None;
                }

                let mut dims_vec = Vec::new();
                let dims = dims.as_array();
                for i in 0..dims.len() {
                    let val = dims.get(ctx_scope, i);
                    if !val.is_long() {
                        isolate_scope.raise_exception_str("Tensor dims must be integers");
                        return None;
                    }
                    dims_vec.push(val.get_long());
                }

                let data = args.get(2);
                if !data.is_array_buffer() {
                    isolate_scope.raise_exception_str("Third argument to 'redisai_create_tensor' must be tensor data as array buffer");
                    return None;
                }
                let data = data.as_array_buffer();
                let s = match script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate_scope.raise_exception_str("On redisai_create_tensor, use of invalid script ctx.");
                        return None;
                    }
                };
                let tensor = match s.compiled_library_api.redisai_create_tensor(data_type_utf8.as_str(), &dims_vec, data.data()) {
                    Ok(tensor) => tensor,
                    Err(e) => {
                        isolate_scope.raise_exception_str(e.get_msg());
                        return None;
                    }
                };
                let tensor_obj = get_js_tensor_from_tensor(&s, isolate_scope, ctx_scope, tensor);
                Some(tensor_obj.to_value())
            })
            .to_value(),
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
    redis_ai_client.set(
        ctx_scope,
        &isolate_scope.new_string("open_model").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate_scope, ctx_scope| {
                if args.len() != 1 {
                    isolate_scope.raise_exception_str(
                        "Wrong number of arguments to 'open_redisai_model' function",
                    );
                    return None;
                }

                let client = redis_client_ref.borrow();
                let client = match client.client.as_ref() {
                    Some(c) => c,
                    None => {
                        isolate_scope.raise_exception_str("Used on invalid client");
                        return None;
                    }
                };

                let name = args.get(0);
                if !name.is_string() && !name.is_string_object() {
                    isolate_scope.raise_exception_str(
                        "First argument to 'open_redisai_model' must be a string representing key name.",
                    );
                    return None;
                }
                let name_utf8 = name.to_utf8().unwrap();

                let model = match client.open_ai_model(name_utf8.as_str()) {
                    Ok(model) => model,
                    Err(e) => {
                        isolate_scope.raise_exception_str(e.get_msg());
                        return None;
                    }
                };

                let model_object = isolate_scope.new_object();
                let script_ctx_ref = Weak::clone(&script_ctx_ref);
                model_object.set(
                    ctx_scope,
                    &isolate_scope.new_string("get_model_runner").to_value(),
                    &ctx_scope
                    .new_native_function(move |args, isolate_scope, ctx_scope| {
                        if args.len() != 0 {
                            isolate_scope.raise_exception_str(
                                "Wrong number of arguments to 'get_model_runner' function",
                            );
                            return None;
                        }
                        let model_runner = Arc::new(Mutex::new(model.get_model_runner()));
                        let model_runner_clone = Arc::clone(&model_runner);
                        let model_runner_object = isolate_scope.new_object();
                        model_runner_object.set(
                            ctx_scope,
                            &isolate_scope.new_string("add_input").to_value(),
                            &ctx_scope
                            .new_native_function(move |args, isolate_scope, _ctx_scope| {
                                if args.len() != 2 {
                                    isolate_scope.raise_exception_str(
                                        "Wrong number of arguments to 'add_input' function",
                                    );
                                    return None;
                                }

                                let input_name = args.get(0);
                                if !input_name.is_string() && !input_name.is_string_object() {
                                    isolate_scope.raise_exception_str(
                                        "First argument to 'add_input' must be a string representing input name.",
                                    );
                                    return None;
                                }
                                let input_name_utf8 = input_name.to_utf8().unwrap();

                                let tensor_js = args.get(1);
                                if !tensor_js.is_object() {
                                    isolate_scope.raise_exception_str(
                                        "Second argument to 'add_input' must be a tensor.",
                                    );
                                    return None;
                                }
                                let tensor_js = tensor_js.as_object();
                                let tensor = get_tensor_from_js_tensor(isolate_scope, &tensor_js)?;

                                let mut model_runner = model_runner_clone.lock().unwrap();
                                let res = model_runner.as_mut().add_input(input_name_utf8.as_str(), tensor.as_ref());
                                if let Err(e) = res {
                                    isolate_scope.raise_exception_str(e.get_msg());
                                    return None;
                                }
                                None
                            }).to_value(),
                        );

                        let model_runner_clone = Arc::clone(&model_runner);
                        model_runner_object.set(
                            ctx_scope,
                            &isolate_scope.new_string("add_output").to_value(),
                            &ctx_scope
                            .new_native_function(move |args, isolate_scope, _ctx_scope| {
                                if args.len() != 1 {
                                    isolate_scope.raise_exception_str(
                                        "Wrong number of arguments to 'add_output' function",
                                    );
                                    return None;
                                }

                                let output_name = args.get(0);
                                if !output_name.is_string() && !output_name.is_string_object() {
                                    isolate_scope.raise_exception_str(
                                        "First argument to 'add_output' must be a string representing output name.",
                                    );
                                    return None;
                                }
                                let output_name_utf8 = output_name.to_utf8().unwrap();

                                let mut model_runner = model_runner_clone.lock().unwrap();
                                let res = model_runner.as_mut().add_output(output_name_utf8.as_str());
                                if let Err(e) = res {
                                    isolate_scope.raise_exception_str(e.get_msg());
                                    return None;
                                }
                                None
                            }).to_value(),
                        );

                        let script_ctx_ref = Weak::clone(&script_ctx_ref);
                        model_runner_object.set(
                            ctx_scope,
                            &isolate_scope.new_string("run").to_value(),
                            &ctx_scope
                            .new_native_function(move |args, isolate_scope, ctx_scope| {
                                if args.len() != 0 {
                                    isolate_scope.raise_exception_str(
                                        "Wrong number of arguments to 'run' function",
                                    );
                                    return None;
                                }

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

                                Some(promise.to_value())
                            }).to_value(),
                        );
                        Some(model_runner_object.to_value())
                    }).to_value(),
                );

                model_object.freeze(ctx_scope);

                Some(model_object.to_value())
            })
            .to_value(),
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    let redis_client_ref = Arc::clone(redis_client);
    redis_ai_client.set(
        ctx_scope,
        &isolate_scope.new_string("open_script").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate_scope, ctx_scope| {
                if args.len() != 1 {
                    isolate_scope.raise_exception_str(
                        "Wrong number of arguments to 'open_redisai_script' function",
                    );
                    return None;
                }

                let client = redis_client_ref.borrow();
                let client = match client.client.as_ref() {
                    Some(c) => c,
                    None => {
                        isolate_scope.raise_exception_str("Used on invalid client");
                        return None;
                    }
                };

                let name = args.get(0);
                if !name.is_string() && !name.is_string_object() {
                    isolate_scope.raise_exception_str(
                        "First argument to 'open_redisai_script' must be a string representing key name.",
                    );
                    return None;
                }
                let name_utf8 = name.to_utf8().unwrap();

                let script = match client.open_ai_script(name_utf8.as_str()) {
                    Ok(script) => script,
                    Err(e) => {
                        isolate_scope.raise_exception_str(e.get_msg());
                        return None;
                    }
                };

                let script_object = isolate_scope.new_object();
                let script_ctx_ref = Weak::clone(&script_ctx_ref);
                script_object.set(
                    ctx_scope,
                    &isolate_scope.new_string("get_script_runner").to_value(),
                    &ctx_scope
                    .new_native_function(move |args, isolate_scope, ctx_scope| {
                        if args.len() != 1 {
                            isolate_scope.raise_exception_str(
                                "Wrong number of arguments to 'get_model_runner' function",
                            );
                            return None;
                        }

                        let name = args.get(0);
                        if !name.is_string() && !name.is_string_object() {
                            isolate_scope.raise_exception_str(
                                "First argument to 'get_script_runner' must be a string representing function name.",
                            );
                            return None;
                        }
                        let name_utf8 = name.to_utf8().unwrap();

                        let script_runner = Arc::new(Mutex::new(script.get_script_runner(name_utf8.as_str())));
                        let script_runner_clone = Arc::clone(&script_runner);
                        let script_runner_object = isolate_scope.new_object();
                        script_runner_object.set(
                            ctx_scope,
                            &isolate_scope.new_string("add_input").to_value(),
                            &ctx_scope
                            .new_native_function(move |args, isolate_scope, _ctx_scope| {
                                if args.len() != 1 {
                                    isolate_scope.raise_exception_str(
                                        "Wrong number of arguments to 'add_input' function",
                                    );
                                    return None;
                                }

                                let tensor_js = args.get(0);
                                if !tensor_js.is_object() {
                                    isolate_scope.raise_exception_str(
                                        "Second argument to 'add_input' must be a tensor.",
                                    );
                                    return None;
                                }
                                let tensor_js = tensor_js.as_object();
                                let tensor = get_tensor_from_js_tensor(isolate_scope, &tensor_js)?;

                                let mut script_runner = script_runner_clone.lock().unwrap();
                                let res = script_runner.as_mut().add_input(tensor.as_ref());
                                if let Err(e) = res {
                                    isolate_scope.raise_exception_str(e.get_msg());
                                    return None;
                                }
                                None
                            }).to_value(),
                        );

                        let script_runner_clone = Arc::clone(&script_runner);
                        script_runner_object.set(
                            ctx_scope,
                            &isolate_scope.new_string("add_output").to_value(),
                            &ctx_scope
                            .new_native_function(move |args, isolate_scope, _ctx_scope| {
                                if args.len() != 0 {
                                    isolate_scope.raise_exception_str(
                                        "Wrong number of arguments to 'add_output' function",
                                    );
                                    return None;
                                }

                                let mut script_runner = script_runner_clone.lock().unwrap();
                                let res = script_runner.as_mut().add_output();
                                if let Err(e) = res {
                                    isolate_scope.raise_exception_str(e.get_msg());
                                    return None;
                                }
                                None
                            }).to_value(),
                        );

                        let script_ctx_ref = Weak::clone(&script_ctx_ref);
                        script_runner_object.set(
                            ctx_scope,
                            &isolate_scope.new_string("run").to_value(),
                            &ctx_scope
                            .new_native_function(move |args, isolate_scope, ctx_scope| {
                                if args.len() != 0 {
                                    isolate_scope.raise_exception_str(
                                        "Wrong number of arguments to 'run' function",
                                    );
                                    return None;
                                }

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

                                Some(promise.to_value())
                            }).to_value(),
                        );
                        Some(script_runner_object.to_value())
                    }).to_value(),
                );

                script_object.freeze(ctx_scope);

                Some(script_object.to_value())
            })
            .to_value(),
    );

    redis_ai_client.to_value()
}
