use crate::redisai_raw::bindings::{
    RAI_Error, RAI_Model, RAI_ModelRunCtx, RAI_OnFinishCtx, RedisAI_ErrorCode_RedisAI_ErrorCode_OK,
    RedisAI_FreeError, RedisAI_GetAsModelRunCtx, RedisAI_GetError, RedisAI_GetErrorCode,
    RedisAI_GetModelFromKeyspace, RedisAI_InitError, RedisAI_ModelFree,
    RedisAI_ModelGetShallowCopy, RedisAI_ModelRunAsync, RedisAI_ModelRunCtxAddInput,
    RedisAI_ModelRunCtxAddOutput, RedisAI_ModelRunCtxCreate, RedisAI_ModelRunCtxFree,
    RedisAI_ModelRunCtxNumOutputs, RedisAI_ModelRunCtxOutputTensor, RedisAI_TensorGetShallowCopy,
    RedisModuleCtx, RedisModuleString, REDISMODULE_OK, REDISMODULE_READ,
};

use crate::redisai::redisai_tensor::RedisAITensor;

use redis_module::context::Context;

use std::ffi::{CStr, CString};

use crate::RedisAIError;

use std::os::raw::c_void;

use redisgears_plugin_api::redisgears_plugin_api::redisai_interface::{
    AIModelInterface, AIModelRunnerInterface, AITensorInterface,
};

use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;

pub struct RedisAIModel {
    inner_model: *mut RAI_Model,
}

impl RedisAIModel {
    pub fn open_from_key(ctx: &Context, key_name: &str) -> Result<RedisAIModel, RedisAIError> {
        if !crate::redisai_is_init() {
            return Err("RedisAI is not initialize".to_string());
        }
        let key_redis_str = ctx.create_string(key_name);
        let mut inner_model: *mut RAI_Model = std::ptr::null_mut();
        let mut err: *mut RAI_Error = std::ptr::null_mut();
        unsafe { RedisAI_InitError.unwrap()(&mut err) };
        let res = unsafe {
            RedisAI_GetModelFromKeyspace.unwrap()(
                ctx.ctx as *mut RedisModuleCtx,
                key_redis_str.inner as *mut RedisModuleString,
                &mut inner_model,
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
        let inner_model = unsafe { RedisAI_ModelGetShallowCopy.unwrap()(inner_model) };
        Ok(RedisAIModel { inner_model })
    }

    pub fn create_run_ctx(&self) -> RedisAIModelRunCtx {
        let inner_run_ctx = unsafe { RedisAI_ModelRunCtxCreate.unwrap()(self.inner_model) };
        RedisAIModelRunCtx { inner_run_ctx }
    }
}

impl AIModelInterface for RedisAIModel {
    fn get_model_runner(&self) -> Box<dyn AIModelRunnerInterface> {
        Box::new(self.create_run_ctx())
    }
}

impl Drop for RedisAIModel {
    fn drop(&mut self) {
        let mut err: *mut RAI_Error = std::ptr::null_mut();
        unsafe { RedisAI_InitError.unwrap()(&mut err) };
        unsafe { RedisAI_ModelFree.unwrap()(self.inner_model, err) };
        unsafe { RedisAI_FreeError.unwrap()(err) };
    }
}

pub struct RedisAIModelRunCtx {
    inner_run_ctx: *mut RAI_ModelRunCtx,
}

extern "C" fn model_run_done<Callback: FnOnce(Result<Vec<RedisAITensor>, RedisAIError>)>(
    ctx: *mut RAI_OnFinishCtx,
    private_data: *mut ::std::os::raw::c_void,
) {
    let on_done = unsafe { Box::from_raw(private_data as *mut Callback) };

    let mut err: *mut RAI_Error = std::ptr::null_mut();
    unsafe { RedisAI_InitError.unwrap()(&mut err) };
    let inner_run_ctx = unsafe { RedisAI_GetAsModelRunCtx.unwrap()(ctx, err) };

    if unsafe { RedisAI_GetErrorCode.unwrap()(err) } != RedisAI_ErrorCode_RedisAI_ErrorCode_OK {
        let err_str = unsafe { RedisAI_GetError.unwrap()(err) };
        let err_c_str = unsafe { CStr::from_ptr(err_str) };
        let err_rust_string = err_c_str.to_str().unwrap().to_string();
        unsafe { RedisAI_FreeError.unwrap()(err) };
        on_done(Err(err_rust_string));
        return;
    }

    unsafe { RedisAI_FreeError.unwrap()(err) };
    let num_outputs = unsafe { RedisAI_ModelRunCtxNumOutputs.unwrap()(inner_run_ctx) };
    let mut outputs = Vec::new();
    for i in 0..num_outputs {
        let inner_tensor = unsafe {
            RedisAI_TensorGetShallowCopy.unwrap()(RedisAI_ModelRunCtxOutputTensor.unwrap()(
                inner_run_ctx,
                i,
            ))
        };
        outputs.push(RedisAITensor::from_inner(inner_tensor));
    }
    on_done(Ok(outputs));
    unsafe { RedisAI_ModelRunCtxFree.unwrap()(inner_run_ctx) };
}

impl RedisAIModelRunCtx {
    pub fn add_input(&mut self, name: &str, tensor: &RedisAITensor) -> Result<(), RedisAIError> {
        if self.inner_run_ctx.is_null() {
            return Err("Invalid run ctx was used".to_string());
        }
        let name_c_string = CString::new(name).unwrap();
        unsafe {
            RedisAI_ModelRunCtxAddInput.unwrap()(
                self.inner_run_ctx,
                name_c_string.as_ptr(),
                tensor.inner_tensor,
            )
        };
        Ok(())
    }

    pub fn add_output(&mut self, name: &str) -> Result<(), RedisAIError> {
        if self.inner_run_ctx.is_null() {
            return Err("Invalid run ctx was used".to_string());
        }
        let name_c_string = CString::new(name).unwrap();
        unsafe {
            RedisAI_ModelRunCtxAddOutput.unwrap()(self.inner_run_ctx, name_c_string.as_ptr())
        };
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
                RedisAI_ModelRunAsync.unwrap()(
                    self.inner_run_ctx,
                    Some(model_run_done::<Callback>),
                    on_done as *mut c_void,
                )
            };
            self.inner_run_ctx = std::ptr::null_mut();
        }
    }
}

impl AIModelRunnerInterface for RedisAIModelRunCtx {
    fn add_input(
        &mut self,
        name: &str,
        tensor: &dyn AITensorInterface,
    ) -> Result<(), GearsApiError> {
        let tensor = unsafe { &*(tensor as *const dyn AITensorInterface as *const RedisAITensor) };
        self.add_input(name, tensor)
            .map_err(|e| GearsApiError::Msg(e))
    }

    fn add_output(&mut self, name: &str) -> Result<(), GearsApiError> {
        self.add_output(name).map_err(|e| GearsApiError::Msg(e))
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
            Err(e) => on_done(Err(GearsApiError::Msg(e))),
        });
    }
}

impl Drop for RedisAIModelRunCtx {
    fn drop(&mut self) {
        if !self.inner_run_ctx.is_null() {
            unsafe { RedisAI_ModelRunCtxFree.unwrap()(self.inner_run_ctx) };
        }
    }
}
