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
    match git_sha {
        Ok(sha) => {
            if sha.status.success() {
                let sha = String::from_utf8(sha.stdout).unwrap();
                println!("cargo:rustc-env=GIT_SHA={}", sha);
            } else {
                // print the stdout and stderr
                println!(
                    "Failed extracting git sha value, stdout='{}', stderr='{}'",
                    String::from_utf8(sha.stdout).unwrap(),
                    String::from_utf8(sha.stderr).unwrap()
                );
            }
        }
        Err(e) => {
            println!("Failed extracting git sha value, {}.", e);
        }
    }
    // Expose GIT_BRANCH env var
    let git_branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output();
    match git_branch {
        Ok(branch) => {
            let branch = String::from_utf8(branch.stdout).unwrap();
            println!("cargo:rustc-env=GIT_BRANCH={}", branch);
        }
        Err(e) => {
            println!("Failed extracting git branch value, {}.", e);
        }
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

    println!(
        "cargo:rustc-env=BUILD_OS={}",
        std::env::consts::OS.to_string().to_lowercase()
    );

    let (os_type, os_ver) = if std::env::consts::OS == "linux" {
        // on linux we have lsb release, let use it.
        let os_type = Command::new("lsb_release").args(["-i", "-s"]).output();
        let os_type = match os_type {
            Ok(os) => {
                if os.status.success() {
                    String::from_utf8(os.stdout).unwrap().trim().to_lowercase()
                } else {
                    // print the stdout and stderr
                    println!(
                        "Failed extracting os, stdout='{}', stderr='{}'",
                        String::from_utf8(os.stdout).unwrap(),
                        String::from_utf8(os.stderr).unwrap()
                    );
                    "unknown".to_string()
                }
            }
            Err(e) => {
                println!("Failed extracting os, {}.", e);
                "unknown".to_string()
            }
        };

        // extrac os version using lsb_release
        let os_ver = Command::new("lsb_release").args(["-r", "-s"]).output();
        let os_ver = match os_ver {
            Ok(os_ver) => {
                if os_ver.status.success() {
                    String::from_utf8(os_ver.stdout)
                        .unwrap()
                        .trim()
                        .to_lowercase()
                } else {
                    // print the stdout and stderr
                    println!(
                        "Failed extracting os version, stdout='{}', stderr='{}'",
                        String::from_utf8(os_ver.stdout).unwrap(),
                        String::from_utf8(os_ver.stderr).unwrap()
                    );
                    "0.0".to_string()
                }
            }
            Err(e) => {
                println!("Failed extracting os version, {}.", e);
                "0.0".to_string()
            }
        };
        (os_type, os_ver)
    } else {
        // fallback to os_info library
        let info = os_info::get();
        (
            info.os_type().to_string().to_lowercase(),
            info.version().to_string().to_lowercase(),
        )
    };

    let rhel_like_os = vec!["centos".to_string(), "rocky".to_string()];
    let (os_type, os_version) = if rhel_like_os.contains(&os_type) {
        (
            "rhel".to_string(),
            os_ver
                .split(".")
                .into_iter()
                .next()
                .expect("Failed getting os version")
                .to_string(),
        )
    } else {
        (os_type, os_ver)
    };
    println!("cargo:rustc-env=BUILD_OS_TYPE={}", os_type);
    println!("cargo:rustc-env=BUILD_OS_VERSION={}", os_version);
    println!("cargo:rustc-env=BUILD_OS_ARCH={}", std::env::consts::ARCH);
    println!(
        "cargo:rustc-env=BUILD_TYPE={}",
        std::env::var("PROFILE").expect("Can not get PROFILE env var")
    );
}
