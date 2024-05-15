//! RedisGears v2 module.

mod config;
#[cfg(not(feature = "noop"))]
mod full;
#[cfg(feature = "noop")]
mod noop;

#[cfg(not(feature = "noop"))]
pub use full::gears_module;
#[cfg(feature = "noop")]
pub use noop::gears_module;

/// GIT commit hash used for this build.
pub const GIT_SHA: Option<&str> = std::option_env!("GIT_SHA");
/// Crate version (string) used for this build.
pub const VERSION_STR: Option<&str> = std::option_env!("VERSION_STR");
/// Crate version (number) used for this build.
pub const VERSION_NUM: Option<&str> = std::option_env!("VERSION_NUM");
/// The operating system used for building the crate.
pub const BUILD_OS: Option<&str> = std::option_env!("BUILD_OS");
/// The type of the operating system used for building the crate.
pub const BUILD_OS_NICK: Option<&str> = std::option_env!("BUILD_OS_NICK");
/// The CPU architeture of the operating system used for building the crate.
pub const BUILD_OS_ARCH: Option<&str> = std::option_env!("BUILD_OS_ARCH");
/// The build type of the crate.
pub const BUILD_TYPE: Option<&str> = std::option_env!("BUILD_TYPE");
