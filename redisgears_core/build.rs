/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

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
    let git_branch_or_tag = Command::new("git")
        .args(["describe", "--tags", "--exact-match"])
        .output()
        .map_err(|e| format!("Failed invoking git command to get git tag, {}", e))
        .and_then(|v| {
            if !v.status.success() {
                return Err(format!(
                    "Failed extracting git branch, {}",
                    String::from_utf8_lossy(&v.stderr)
                ));
            }
            Ok(v)
        })
        .or_else(|_| {
            println!("no tag");
            Command::new("git")
                .args(["symbolic-ref", "-q", "--short", "HEAD"])
                .output()
        });
    match git_branch_or_tag {
        Ok(branch) => {
            let branch = String::from_utf8(branch.stdout).unwrap();
            println!("cargo:rustc-env=GIT_BRANCH_OR_TAG={}", branch);
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

    let mut os = std::env::consts::OS.to_string().to_lowercase();
    if !os.is_empty() {
        os[0..1].make_ascii_uppercase(); // make only the first char upper case
    }
    println!("cargo:rustc-env=BUILD_OS={}", os);

    let (os_type, os_ver) = if std::env::consts::OS == "linux" {
        // on linux we have lsb release, let use it.
        let os_type = Command::new("lsb_release")
            .args(["-i", "-s"])
            .output()
            .map_or_else(
                |e| {
                    println!("Failed extracting os, {}.", e);
                    "unknown".to_string()
                },
                |os| {
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
                },
            );

        // extrac os version using lsb_release
        let os_ver = Command::new("lsb_release")
            .args(["-r", "-s"])
            .output()
            .map_or_else(
                |e| {
                    println!("Failed extracting os version, {}.", e);
                    "x.x".to_string()
                },
                |os_ver| {
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
                        "x.x".to_string()
                    }
                },
            );
        (os_type, os_ver)
    } else {
        // fallback to os_info library
        let info = os_info::get();
        (
            info.os_type().to_string().to_lowercase(),
            info.version().to_string().to_lowercase(),
        )
    };

    let os_nick = match os_type.as_str() {
        "centos" | "rocky" => format!(
            "rhel{}",
            os_ver.split('.').next().expect("Failed getting os version")
        ),
        "amazon" => format!(
            "amzn{}",
            os_ver.split('.').next().expect("Failed getting os version")
        ),
        "debian" if os_ver == "11" => "bullseye".to_owned(),
        _ => format!("{os_type}{os_ver}"),
    };
    println!("cargo:rustc-env=BUILD_OS_NICK={}", os_nick);
    println!("cargo:rustc-env=BUILD_OS_ARCH={}", std::env::consts::ARCH);
    println!(
        "cargo:rustc-env=BUILD_TYPE={}",
        std::env::var("PROFILE").expect("Can not get PROFILE env var")
    );
}
