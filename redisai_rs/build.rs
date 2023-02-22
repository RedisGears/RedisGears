/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::env;
use std::path::PathBuf;

fn main() {
    const EXPERIMENTAL_API: &str = "REDISMODULE_EXPERIMENTAL_API";

    let mut build = cc::Build::new();
    build.define(EXPERIMENTAL_API, None);

    build
        .file("src/redisai_raw/redisai.c")
        .include("src/redisai_raw/")
        .compile("redisai");

    let build = bindgen::Builder::default().clang_arg(format!("-D{}", EXPERIMENTAL_API).as_str());

    let bindings = build
        .header("src/redisai_raw/redisai.h")
        .size_t_is_usize(true)
        .layout_tests(false)
        .generate()
        .expect("error generating bindings");

    let output_dir = env::var("OUT_DIR").expect("Can not find out directory");
    let out_path = PathBuf::from(&output_dir);
    bindings
        .write_to_file(out_path.join("redisai.rs"))
        .expect("failed to write bindings to file");
}
