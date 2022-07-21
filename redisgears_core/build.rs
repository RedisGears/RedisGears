extern crate clap;

use std::process::Command;
use regex::Regex;

fn main() {
    // Expose GIT_SHA env var
    let git_sha = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output();
    if let Ok(sha) = git_sha {
        let sha = String::from_utf8(sha.stdout).unwrap();
        println!("cargo:rustc-env=GIT_SHA={}", sha);
    }
    // Expose GIT_BRANCH env var
    let git_branch = Command::new("git")
        .args(&["rev-parse", "--abbrev-ref", "HEAD"])
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
        let major = (&cap[1]).parse::<i32>().unwrap();
        let minor = (&cap[2]).parse::<i32>().unwrap();
        let patch = (&cap[3]).parse::<i32>().unwrap();
        version_num = major * 10000 + minor * 100 + patch;
    }
    println!("cargo:rustc-env=VERSION_NUM={}", version_num);
}
