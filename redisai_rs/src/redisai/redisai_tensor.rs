/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::redisai_raw::bindings::{
    RAI_Tensor, RedisAI_TensorByteSize, RedisAI_TensorCreate, RedisAI_TensorData,
    RedisAI_TensorDataSize, RedisAI_TensorDim, RedisAI_TensorFree, RedisAI_TensorLength,
    RedisAI_TensorNumDims, RedisAI_TensorSetData,
};
use std::ffi::CString;

use crate::RedisAIError;
use redisgears_plugin_api::redisgears_plugin_api::redisai_interface::AITensorInterface;

use std::os::raw::{c_char, c_int};

pub struct RedisAITensor {
    pub(crate) inner_tensor: *mut RAI_Tensor,
}

impl RedisAITensor {
    pub fn create(data_type: &str, dims: &[i64]) -> Result<RedisAITensor, RedisAIError> {
        if !crate::redisai_is_init() {
            return Err("RedisAI is not initialize".to_string());
        }
        let data_type_c_str = CString::new(data_type).unwrap();
        let inner_tensor = unsafe {
            RedisAI_TensorCreate.unwrap()(
                data_type_c_str.as_ptr(),
                dims.as_ptr() as *mut i64,
                dims.len() as i32,
            )
        };
        Ok(RedisAITensor { inner_tensor })
    }

    pub(crate) fn from_inner(inner_tensor: *mut RAI_Tensor) -> RedisAITensor {
        RedisAITensor { inner_tensor }
    }

    pub fn len(&self) -> usize {
        unsafe { RedisAI_TensorLength.unwrap()(self.inner_tensor) }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn num_dims(&self) -> usize {
        unsafe { RedisAI_TensorNumDims.unwrap()(self.inner_tensor) as usize }
    }

    pub fn dim(&self, index: i32) -> i64 {
        unsafe { RedisAI_TensorDim.unwrap()(self.inner_tensor, index) }
    }

    pub fn bytes_len(&self) -> usize {
        unsafe { RedisAI_TensorByteSize.unwrap()(self.inner_tensor) }
    }

    pub fn data_size(&self) -> usize {
        unsafe { RedisAI_TensorDataSize.unwrap()(self.inner_tensor) }
    }

    pub fn set_data(&mut self, data: &[u8]) -> Result<(), RedisAIError> {
        if data.len() != self.bytes_len() {
            return Err(format!(
                "Buffer size {} but expected {}",
                data.len(),
                self.bytes_len()
            ));
        }
        if unsafe {
            RedisAI_TensorSetData.unwrap()(
                self.inner_tensor,
                data.as_ptr() as *const c_char,
                data.len(),
            )
        } != 1 as c_int
        {
            Err("Failed setting data to tensor".to_string())
        } else {
            Ok(())
        }
    }

    pub fn data(&self) -> &[u8] {
        let size = self.bytes_len();
        let data = unsafe { RedisAI_TensorData.unwrap()(self.inner_tensor) };
        unsafe { std::slice::from_raw_parts(data as *const u8, size) }
    }
}

impl AITensorInterface for RedisAITensor {
    fn get_data(&self) -> &[u8] {
        self.data()
    }

    fn dims(&self) -> Vec<i64> {
        let num_dims = self.num_dims();
        let mut res = Vec::new();
        for i in 0..num_dims {
            res.push(self.dim(i as i32));
        }
        res
    }

    fn element_size(&self) -> usize {
        self.data_size()
    }
}

impl Drop for RedisAITensor {
    fn drop(&mut self) {
        unsafe { RedisAI_TensorFree.unwrap()(self.inner_tensor) };
    }
}

unsafe impl Send for RedisAITensor {}
