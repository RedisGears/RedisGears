use std::os::raw::{c_int};
use redis_module::context::Context;

pub mod redisai;
pub mod redisai_raw;


pub type RedisAIError = String;

// This is the one static function we need to initialize a module.
// bindgen does not generate it for us (probably since it's defined as static in redismodule.h).
#[allow(improper_ctypes)]
#[link(name = "redismodule", kind = "static")]
extern "C" {
    pub fn Export_RedisAI_Init(
        ctx: *mut redisai_raw::bindings::RedisModuleCtx,
    ) -> c_int;
}

static mut IS_INIT: bool = false;

pub(crate) fn redisai_is_init() -> bool {
    return unsafe{IS_INIT}
}

pub fn redisai_init(ctx: &Context) -> Result<(), RedisAIError> {
    if redisai_is_init() {
        return Ok(())
    }
    if unsafe{Export_RedisAI_Init(ctx.ctx as *mut redisai_raw::bindings::RedisModuleCtx)} != redisai_raw::bindings::REDISMODULE_OK as c_int {
        Err("RedisAI initialization failed".to_string())
    } else {
        unsafe{IS_INIT = true};
        Ok(())
    }
}