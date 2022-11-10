#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/redisai.rs"));
}

// See: https://users.rust-lang.org/t/bindgen-generate-options-and-some-are-none/14027
