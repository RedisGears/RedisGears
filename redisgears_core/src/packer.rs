use std::process::Command;

pub const GIT_SHA: Option<&str> = std::option_env!("GIT_SHA");
pub const GIT_BRANCH: Option<&str> = std::option_env!("GIT_BRANCH");
pub const VERSION_STR: Option<&str> = std::option_env!("VERSION_STR");
pub const VERSION_NUM: Option<&str> = std::option_env!("VERSION_NUM");
pub const BUILD_OS: Option<&str> = std::option_env!("BUILD_OS");
pub const BUILD_OS_TYPE: Option<&str> = std::option_env!("BUILD_OS_TYPE");
pub const BUILD_OS_VERSION: Option<&str> = std::option_env!("BUILD_OS_VERSION");
pub const BUILD_OS_ARCH: Option<&str> = std::option_env!("BUILD_OS_ARCH");
pub const BUILD_TYPE: Option<&str> = std::option_env!("BUILD_TYPE");

fn main() {
    let mut curr_path = std::env::current_exe().expect("Could not get binary location");
    curr_path.pop();

    let mut ramp_yml_path = curr_path.clone();
    ramp_yml_path.pop();
    ramp_yml_path.pop();
    ramp_yml_path.push("ramp.yml");

    let mut redisgears_so_path = curr_path.clone();
    redisgears_so_path.push("libredisgears.so");

    let mut redisgears_v8_plugin_so_path = curr_path.clone();
    redisgears_v8_plugin_so_path.push("libredisgears_v8_plugin.so");

    println!("{:?}", redisgears_v8_plugin_so_path);
    
    let gears_snapeshot_file_name = format!("redisgears2-{}.{}-{}.{}-{}.{}.zip", BUILD_TYPE.unwrap(), BUILD_OS.unwrap(), BUILD_OS_TYPE.unwrap(), BUILD_OS_VERSION.unwrap(), BUILD_OS_ARCH.unwrap(), GIT_BRANCH.unwrap());
    let gears_release_file_name = format!("redisgears2-{}.{}-{}.{}-{}.{}.zip", BUILD_TYPE.unwrap(), BUILD_OS.unwrap(), BUILD_OS_TYPE.unwrap(), BUILD_OS_VERSION.unwrap(), BUILD_OS_ARCH.unwrap(), VERSION_NUM.unwrap());

    let mut gears_snapeshot_file_path = curr_path.clone();
    gears_snapeshot_file_path.push(gears_snapeshot_file_name);

    let mut gears_release_file_path = curr_path.clone();
    gears_release_file_path.push(gears_release_file_name);
    
    if !Command::new("ramp").args(["pack", "--debug", "-m", ramp_yml_path.to_str().unwrap(), "-o", &gears_snapeshot_file_path.to_str().unwrap(), redisgears_so_path.to_str().unwrap()]).env("REDISGEARS_V8_PLUGIN_PATH", redisgears_v8_plugin_so_path.to_str().unwrap()).status().expect("Failed ramp snapeshot").success() {
        println!("Failed ramp snapeshot");
        std::process::exit(1);
    }
    
    if !Command::new("ramp").args(["pack", "--debug", "-m", ramp_yml_path.to_str().unwrap(), "-o", &gears_release_file_path.to_str().unwrap(), redisgears_so_path.to_str().unwrap()]).env("REDISGEARS_V8_PLUGIN_PATH", redisgears_v8_plugin_so_path.to_str().unwrap()).status().expect("Failed ramp release").success() {
        println!("Failed ramp release");
        std::process::exit(1);
    }
}
