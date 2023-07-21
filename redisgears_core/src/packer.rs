/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::process::Command;

pub const GIT_BRANCH_OR_TAG: Option<&str> = std::option_env!("GIT_BRANCH_OR_TAG");
pub const BUILD_OS: Option<&str> = std::option_env!("BUILD_OS");
pub const BUILD_OS_NICK: Option<&str> = std::option_env!("BUILD_OS_NICK");
pub const BUILD_OS_ARCH: Option<&str> = std::option_env!("BUILD_OS_ARCH");

fn get_dylib_ext() -> &'static str {
    if BUILD_OS.as_ref().map(|v| v.to_lowercase()) == Some("macos".to_owned()) {
        "dylib"
    } else {
        "so"
    }
}

fn main() {
    let mut curr_path = std::env::current_exe().expect("Could not get binary location");
    curr_path.pop();

    let mut ramp_yml_path = curr_path.clone();
    ramp_yml_path.pop();
    ramp_yml_path.pop();
    ramp_yml_path.push("ramp.yml");

    let mut redisgears_so_path = curr_path.clone();
    redisgears_so_path.push(format!("libredisgears.{}", get_dylib_ext()));

    let mut redisgears_v8_plugin_so_path = curr_path.clone();
    redisgears_v8_plugin_so_path.push(format!("libredisgears_v8_plugin.{}", get_dylib_ext()));

    println!("{:?}", redisgears_v8_plugin_so_path);

    let gears_snapeshot_file_name = format!(
        "redisgears.{}-{}-{}.{}.zip",
        BUILD_OS.unwrap(),
        BUILD_OS_NICK.unwrap(),
        BUILD_OS_ARCH.unwrap(),
        GIT_BRANCH_OR_TAG.unwrap()
    )
    .replace(' ', "_");

    let mut gears_snapeshot_file_path = curr_path.clone();
    gears_snapeshot_file_path.push(gears_snapeshot_file_name);

    if !Command::new("ramp")
        .args([
            "pack",
            "--debug",
            "-m",
            ramp_yml_path.to_str().unwrap(),
            "-o",
            gears_snapeshot_file_path.to_str().unwrap(),
            redisgears_so_path.to_str().unwrap(),
        ])
        .env(
            "REDISGEARS_V8_PLUGIN_PATH",
            redisgears_v8_plugin_so_path.to_str().unwrap(),
        )
        .status()
        .expect("Failed ramp snapeshot")
        .success()
    {
        println!("Failed ramp snapeshot");
        std::process::exit(1);
    }
}
