/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

extern crate clap;

use regex::Regex;
use std::process::Command;

fn main() {
    // Expose GIT_SHA env var
    let git_sha = Command::new("git").args(["rev-parse", "HEAD"]).output();
    if let Ok(sha) = git_sha {
        let sha = String::from_utf8(sha.stdout).unwrap();
        println!("cargo:rustc-env=GIT_SHA={}", sha);
    }
    // Expose GIT_BRANCH env var
    let git_branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output();
    if let Ok(branch) = git_branch {
        let branch = String::from_utf8(branch.stdout).unwrap();
        println!("cargo:rustc-env=GIT_BRANCH={}", branch);
    }

    let version_str = String::from(clap::crate_version!());
    println!("cargo:rustc-env=VERSION_STR={}", version_str);

    let mut version_num = 0;
    let re = Regex::new(r"(\d+).(\d+).(\d+)").unwrap();
    for cap in re.captures_iter(&version_str) {
        let major = (cap[1]).parse::<i32>().unwrap();
        let minor = (cap[2]).parse::<i32>().unwrap();
        let patch = (cap[3]).parse::<i32>().unwrap();
        version_num = major * 10000 + minor * 100 + patch;
    }
    println!("cargo:rustc-env=VERSION_NUM={}", version_num);

    let info = os_info::get();

    println!(
        "cargo:rustc-env=BUILD_OS={}",
        std::env::consts::OS.to_string().to_lowercase()
    );
    println!(
        "cargo:rustc-env=BUILD_OS_TYPE={}",
        info.os_type().to_string().to_lowercase()
    );
    println!("cargo:rustc-env=BUILD_OS_VERSION={}", info.version());
    println!("cargo:rustc-env=BUILD_OS_ARCH={}", std::env::consts::ARCH);
    println!(
        "cargo:rustc-env=BUILD_TYPE={}",
        std::env::var("PROFILE").expect("Can not get PROFILE env var")
    );
}
