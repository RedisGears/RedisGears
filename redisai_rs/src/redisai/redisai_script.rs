/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::redisai_raw::bindings::{
    RAI_Error, RAI_OnFinishCtx, RAI_Script, RAI_ScriptRunCtx,
    RedisAI_ErrorCode_RedisAI_ErrorCode_OK, RedisAI_FreeError, RedisAI_GetAsScriptRunCtx,
    RedisAI_GetError, RedisAI_GetErrorCode, RedisAI_GetScriptFromKeyspace, RedisAI_InitError,
    RedisAI_ScriptFree, RedisAI_ScriptGetShallowCopy, RedisAI_ScriptRunAsync,
    RedisAI_ScriptRunCtxAddInput, RedisAI_ScriptRunCtxAddOutput, RedisAI_ScriptRunCtxCreate,
    RedisAI_ScriptRunCtxFree, RedisAI_ScriptRunCtxNumOutputs, RedisAI_ScriptRunCtxOutputTensor,
    RedisAI_TensorGetShallowCopy, RedisModuleCtx, RedisModuleString, REDISMODULE_OK,
    REDISMODULE_READ,
};

use crate::RedisAIError;
use redis_module::Context;

use std::ffi::{CStr, CString};

use crate::redisai::redisai_tensor::RedisAITensor;

use std::os::raw::c_void;

use redisgears_plugin_api::redisgears_plugin_api::redisai_interface::{
    AIScriptInterface, AIScriptRunnerInterface, AITensorInterface,
};

use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;

pub struct RedisAIScript {
    inner_script: *mut RAI_Script,
}

impl RedisAIScript {
    pub fn open_from_key(ctx: &Context, key_name: &str) -> Result<RedisAIScript, RedisAIError> {
        if !crate::redisai_is_init() {
            return Err("RedisAI is not initialize".to_string());
        }
        let key_redis_str = ctx.create_string(key_name);
        let mut inner_script: *mut RAI_Script = std::ptr::null_mut();
        let mut err: *mut RAI_Error = std::ptr::null_mut();
        unsafe { RedisAI_InitError.unwrap()(&mut err) };
        let res = unsafe {
            RedisAI_GetScriptFromKeyspace.unwrap()(
                ctx.ctx as *mut RedisModuleCtx,
                key_redis_str.inner as *mut RedisModuleString,
                &mut inner_script,
                REDISMODULE_READ as i32,
                err,
            )
        };
        if res != REDISMODULE_OK as i32 {
            let err_str = unsafe { RedisAI_GetError.unwrap()(err) };
            let err_c_str = unsafe { CStr::from_ptr(err_str) };
            let err_rust_string = err_c_str.to_str().unwrap().to_string();
            unsafe { RedisAI_FreeError.unwrap()(err) };
            return Err(err_rust_string);
        }

        unsafe { RedisAI_FreeError.unwrap()(err) };
        let inner_script = unsafe { RedisAI_ScriptGetShallowCopy.unwrap()(inner_script) };
        Ok(RedisAIScript { inner_script })
    }

    pub fn create_run_ctx(&self, func_name: &str) -> RedisAIScriptRunCtx {
        let func_name_c_str = CString::new(func_name).unwrap();
        let inner_run_ctx = unsafe {
            RedisAI_ScriptRunCtxCreate.unwrap()(self.inner_script, func_name_c_str.as_ptr())
        };
        RedisAIScriptRunCtx { inner_run_ctx }
    }
}

impl AIScriptInterface for RedisAIScript {
    fn get_script_runner(&self, func_name: &str) -> Box<dyn AIScriptRunnerInterface> {
        Box::new(self.create_run_ctx(func_name))
    }
}

impl Drop for RedisAIScript {
    fn drop(&mut self) {
        let mut err: *mut RAI_Error = std::ptr::null_mut();
        unsafe { RedisAI_InitError.unwrap()(&mut err) };
        unsafe { RedisAI_ScriptFree.unwrap()(self.inner_script, err) };
        unsafe { RedisAI_FreeError.unwrap()(err) };
    }
}

pub struct RedisAIScriptRunCtx {
    inner_run_ctx: *mut RAI_ScriptRunCtx,
}

extern "C" fn script_run_done<Callback: FnOnce(Result<Vec<RedisAITensor>, RedisAIError>)>(
    ctx: *mut RAI_OnFinishCtx,
    private_data: *mut ::std::os::raw::c_void,
) {
    let on_done = unsafe { Box::from_raw(private_data as *mut Callback) };

    let mut err: *mut RAI_Error = std::ptr::null_mut();
    unsafe { RedisAI_InitError.unwrap()(&mut err) };
    let inner_run_ctx = unsafe { RedisAI_GetAsScriptRunCtx.unwrap()(ctx, err) };

    if unsafe { RedisAI_GetErrorCode.unwrap()(err) } != RedisAI_ErrorCode_RedisAI_ErrorCode_OK {
        let err_str = unsafe { RedisAI_GetError.unwrap()(err) };
        let err_c_str = unsafe { CStr::from_ptr(err_str) };
        let err_rust_string = err_c_str.to_str().unwrap().to_string();
        unsafe { RedisAI_FreeError.unwrap()(err) };
        on_done(Err(err_rust_string));
        return;
    }

    unsafe { RedisAI_FreeError.unwrap()(err) };
    let num_outputs = unsafe { RedisAI_ScriptRunCtxNumOutputs.unwrap()(inner_run_ctx) };
    let mut outputs = Vec::new();
    for i in 0..num_outputs {
        let inner_tensor = unsafe {
            RedisAI_TensorGetShallowCopy.unwrap()(RedisAI_ScriptRunCtxOutputTensor.unwrap()(
                inner_run_ctx,
                i,
            ))
        };
        outputs.push(RedisAITensor::from_inner(inner_tensor));
    }
    on_done(Ok(outputs));
    unsafe { RedisAI_ScriptRunCtxFree.unwrap()(inner_run_ctx) };
}

impl RedisAIScriptRunCtx {
    pub fn add_input(&mut self, tensor: &RedisAITensor) -> Result<(), RedisAIError> {
        if self.inner_run_ctx.is_null() {
            return Err("Invalid run ctx was used".to_string());
        }
        unsafe {
            RedisAI_ScriptRunCtxAddInput.unwrap()(
                self.inner_run_ctx,
                tensor.inner_tensor,
                std::ptr::null_mut(),
            )
        };
        Ok(())
    }

    pub fn add_output(&mut self) -> Result<(), RedisAIError> {
        if self.inner_run_ctx.is_null() {
            return Err("Invalid run ctx was used".to_string());
        }
        unsafe { RedisAI_ScriptRunCtxAddOutput.unwrap()(self.inner_run_ctx) };
        Ok(())
    }

    pub fn run<Callback: FnOnce(Result<Vec<RedisAITensor>, RedisAIError>)>(
        &mut self,
        on_done: Callback,
    ) {
        if self.inner_run_ctx.is_null() {
            on_done(Err("Invalid run ctx was used".to_string()))
        } else {
            let on_done = Box::into_raw(Box::new(on_done));
            unsafe {
                RedisAI_ScriptRunAsync.unwrap()(
                    self.inner_run_ctx,
                    Some(script_run_done::<Callback>),
                    on_done as *mut c_void,
                )
            };
            self.inner_run_ctx = std::ptr::null_mut();
        }
    }
}

impl AIScriptRunnerInterface for RedisAIScriptRunCtx {
    fn add_input(&mut self, tensor: &dyn AITensorInterface) -> Result<(), GearsApiError> {
        let tensor = unsafe { &*(tensor as *const dyn AITensorInterface as *const RedisAITensor) };
        self.add_input(tensor).map_err(GearsApiError::new)
    }

    fn add_output(&mut self) -> Result<(), GearsApiError> {
        self.add_output().map_err(GearsApiError::new)
    }

    fn run(
        &mut self,
        on_done: Box<dyn FnOnce(Result<Vec<Box<dyn AITensorInterface + Send>>, GearsApiError>)>,
    ) {
        self.run(move |res| match res {
            Ok(res) => {
                let mut v: Vec<Box<dyn AITensorInterface + Send>> = Vec::new();
                for r in res {
                    v.push(Box::new(r));
                }
                on_done(Ok(v));
            }
            Err(e) => on_done(Err(GearsApiError::new(e))),
        });
    }
}

impl Drop for RedisAIScriptRunCtx {
    fn drop(&mut self) {
        if !self.inner_run_ctx.is_null() {
            unsafe { RedisAI_ScriptRunCtxFree.unwrap()(self.inner_run_ctx) };
        }
    }
}
