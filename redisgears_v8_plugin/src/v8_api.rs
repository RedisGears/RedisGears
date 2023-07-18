use std::sync::{Arc, Weak};

use redisgears_plugin_api::redisgears_plugin_api::{
    prologue::{self, ApiVersion},
    GearsApiError,
};
use v8_rs::v8::{isolate_scope::V8IsolateScope, v8_context_scope::V8ContextScope};

use crate::{get_exception_msg, get_function_flags_globals, v8_script_ctx::V8ScriptCtx};

const FUNCTION_FLAGS_GLOBAL_NAME: &str = "functionFlags";

/// A type defining an API version implementation.
pub(crate) type ApiVersionImplementation = fn() -> &'static [JSApiRegisterFunction];

type JSApiRegisterFunction =
    fn(script_ctx: Weak<V8ScriptCtx>, isolate_scope: &V8IsolateScope, ctx_scope: &V8ContextScope);

#[linkme::distributed_slice]
pub(crate) static JS_API_V1_0: [JSApiRegisterFunction] = [..];

fn get_js_api_v1_0() -> &'static [JSApiRegisterFunction] {
    &JS_API_V1_0
}

#[linkme::distributed_slice]
pub(crate) static JS_API_V1_1: [JSApiRegisterFunction] = [..];

fn get_js_api_v1_1() -> &'static [JSApiRegisterFunction] {
    &JS_API_V1_1
}

/// Defines a supported API version.
/// An object of type [`ApiVersionSupported`] is impossible to create if
/// the version isn't supported.
///
/// # Example
///
/// The only way to create an object of this type is to use the
/// [`std::convert::TryFrom`] with an object of [`ApiVersion`]:
///
/// ```rust,no_run,ignore
/// use redisgears_plugin_api::redisgears_plugin_api::prologue::ApiVersion;
///
/// let api_version = ApiVersion(1, 0);
/// let api_version_supported: ApiVersionSupported = api_version.try_into().unwrap();
/// ```
#[derive(Copy, Clone)]
pub struct ApiVersionSupported {
    version: ApiVersion,
    implementation: ApiVersionImplementation,
    is_deprecated: bool,
}
impl PartialOrd for ApiVersionSupported {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.version.partial_cmp(&other.version)
    }
}
impl Ord for ApiVersionSupported {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.version.cmp(&other.version)
    }
}
impl PartialEq for ApiVersionSupported {
    fn eq(&self, other: &Self) -> bool {
        self.version.eq(&other.version)
    }
}
impl Eq for ApiVersionSupported {}
impl std::fmt::Debug for ApiVersionSupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiVersionSupported")
            .field("version", &self.version)
            .field(
                "implementation",
                &(self.implementation as *const std::ffi::c_void),
            )
            .field("is_deprecated", &self.is_deprecated)
            .finish()
    }
}

impl ApiVersionSupported {
    /// A list of all currently supported and deprecated versions.
    const SUPPORTED: [ApiVersionSupported; 2] = [
        Self::new(ApiVersion(1, 0), get_js_api_v1_0, false),
        Self::new(ApiVersion(1, 1), get_js_api_v1_1, false),
    ];

    const fn new(
        version: ApiVersion,
        implementation: ApiVersionImplementation,
        is_deprecated: bool,
    ) -> Self {
        Self {
            version,
            implementation,
            is_deprecated,
        }
    }

    /// Returns the version stored.
    pub fn get_version(&self) -> ApiVersion {
        self.version
    }

    /// Returns a pointer to the API implementation of this version.
    pub(crate) fn get_implementation(&self) -> &ApiVersionImplementation {
        &self.implementation
    }

    /// Returns the minimum supported version.
    ///
    /// # Panics
    ///
    /// Panics if there are no supported versions available.
    pub fn minimum_supported() -> Self {
        Self::SUPPORTED
            .iter()
            .min()
            .cloned()
            .expect("No supported versions found.")
    }

    /// Returns the maximum supported version.
    ///
    /// # Panics
    ///
    /// Panics if there are no supported versions available.
    #[allow(dead_code)]
    pub fn maximum_supported() -> Self {
        Self::SUPPORTED
            .iter()
            .max()
            .cloned()
            .expect("No supported versions found.")
    }

    /// Returns all the version supported.
    pub const fn all_supported() -> &'static [ApiVersionSupported] {
        &Self::SUPPORTED
    }

    /// Returns all the version deprecated.
    #[allow(dead_code)]
    pub fn all_deprecated() -> Vec<ApiVersion> {
        Self::SUPPORTED
            .iter()
            .filter(|v| v.is_deprecated)
            .map(|v| v.version)
            .collect()
    }

    /// Returns `true` if the version is supported.
    #[allow(dead_code)]
    pub fn is_supported(version: ApiVersion) -> bool {
        Self::SUPPORTED.iter().any(|v| v.version == version)
    }

    /// Returns `true` if the version is supported but deprecated.
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }

    /// Converts the current version into the latest compatible,
    /// following the semantic versioning scheme.
    ///
    /// # Example
    ///
    /// If there are versions supported: 1.0, 1.1, 1.2 and 1.3, then
    /// for the any of those versions, the latest compatible one is the
    /// version 1.3, so with the same major number (1) but the maximum
    /// minor number (3).
    ///
    /// ```rust,no_run,ignore
    /// use redisgears_v8_plugin::v8_native_functions::ApiVersionSupported;
    ///
    /// let api_version = ApiVersionSupported::default();
    /// assert_eq!(
    ///    api_version.into_latest_compatible(),
    ///    ApiVersionSupported::maximum_supported()
    /// );
    /// ```
    pub fn into_latest_compatible(self) -> ApiVersionSupported {
        Self::SUPPORTED
            .iter()
            .filter(|v| v.get_version().get_major() == self.get_version().get_major())
            .max()
            .cloned()
            .unwrap_or(self)
    }

    /// Validates the code against using this API version.
    pub(crate) fn validate_code(&self, _code: &str) -> Vec<GearsApiError> {
        let mut messages = Vec::new();

        if self.is_deprecated() {
            messages.push(GearsApiError::new(format!(
                "The code uses a deprecated version of the API: {self}"
            )));
        }

        // Potentially check if uses deprecated symbols here using a JS parser.

        messages
    }
}

impl Default for ApiVersionSupported {
    fn default() -> Self {
        Self::minimum_supported()
    }
}

impl TryFrom<ApiVersion> for ApiVersionSupported {
    type Error = prologue::Error;

    fn try_from(value: ApiVersion) -> Result<Self, Self::Error> {
        Self::SUPPORTED
            .iter()
            .find(|v| v.version == value)
            .cloned()
            .ok_or_else(|| Self::Error::UnsupportedApiVersion {
                requested: value,
                supported: Self::all_supported().iter().map(|v| v.version).collect(),
            })
    }
}

impl std::fmt::Display for ApiVersionSupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.version.fmt(f)
    }
}

/// Initialises the global `redis` object by populating it with methods
/// for the provided supported API version.
pub(crate) fn initialize_globals_for_version(
    script_ctx: &Arc<V8ScriptCtx>,
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    config: Option<&String>,
) -> Result<(), GearsApiError> {
    let globals = ctx_scope.get_globals();
    let redis = isolate_scope.new_object();
    globals.set(
        ctx_scope,
        &isolate_scope.new_string("redis").to_value(),
        &redis.to_value(),
    );
    match config {
        Some(c) => {
            let string = isolate_scope.new_string(c);
            let trycatch = isolate_scope.new_try_catch();
            let config_json = ctx_scope.new_object_from_json(&string);
            if config_json.is_none() {
                return Err(get_exception_msg(&script_ctx.isolate, trycatch, ctx_scope));
            }
            redis.set(
                ctx_scope,
                &isolate_scope.new_string("config").to_value(),
                &config_json.unwrap(),
            )
        }
        None => {
            // setting empty config
            redis.set(
                ctx_scope,
                &isolate_scope.new_string("config").to_value(),
                &isolate_scope.new_object().to_value(),
            )
        }
    }

    // add function flags
    let function_flags = get_function_flags_globals(ctx_scope, isolate_scope);
    redis.set(
        ctx_scope,
        &isolate_scope
            .new_string(FUNCTION_FLAGS_GLOBAL_NAME)
            .to_value(),
        &function_flags,
    );

    let api_version_supported = script_ctx.api_version.into_latest_compatible();
    let apis = api_version_supported.get_implementation()();
    apis.iter().for_each(|v| {
        v(Arc::downgrade(script_ctx), isolate_scope, ctx_scope);
    });
    Ok(())
}

pub(crate) struct JSApiFunction<'c_s, 'i_s, 'i> {
    pub(crate) api_name: &'static str,
    pub(crate) script_ctx: Arc<V8ScriptCtx>,
    pub(crate) isolate_scope: &'i_s V8IsolateScope<'i>,
    pub(crate) ctx_scope: &'c_s V8ContextScope<'i_s, 'i>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latest_supported_version_available() {
        let api_version = ApiVersionSupported::default();
        assert_eq!(
            api_version.into_latest_compatible(),
            ApiVersionSupported::maximum_supported()
        );
    }
}
