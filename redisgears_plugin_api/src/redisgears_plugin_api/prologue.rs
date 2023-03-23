/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
//! The validation mechanism of the user modules.
//!
//! Performs the basic validation and API compatibility checks.

use std::collections::HashMap;
use std::str::FromStr;

use super::GearsApiError;

/// A string indicating that right after it an engine name follows.
const ENGINE_PREFIX: &str = "#!";
/// The key string for the api version within the prologue.
const PROLOGUE_API_VERSION_KEY: &str = "api_version";
/// The key string for the module name within the prologue.
const PROLOGUE_MODULE_NAME_KEY: &str = "name";

/// The RedisGears API version.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ApiVersion(pub u8, pub u8);
impl ApiVersion {
    /// Returns the major part of the version.
    pub const fn get_major(&self) -> u8 {
        self.0
    }

    /// Returns the minor part of the version.
    pub const fn get_minor(&self) -> u8 {
        self.1
    }
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.0, self.1))
    }
}

impl FromStr for ApiVersion {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        const EXPECTED_FORMAT: &str = "<major>.<minor>";
        const EXAMPLE: &str = "1.1";

        let create_error = || Error::ApiVersionSyntaxViolation {
            current: s.to_owned(),
            expected_format: EXPECTED_FORMAT.to_owned(),
            example: EXAMPLE.to_owned(),
        };

        let mut version = s.split('.');

        let major = version
            .next()
            .ok_or_else(create_error)?
            .parse()
            .map_err(|_| create_error())?;

        let minor = version
            .next()
            .ok_or_else(create_error)?
            .parse()
            .map_err(|_| create_error())?;

        Ok(Self(major, minor))
    }
}

/// The prologue of the module.
#[derive(Debug, Clone)]
pub struct Prologue<'a> {
    /// A hint as to what engine should be used for the user code.
    pub engine: &'a str,
    /// The version the user code is written for.
    pub api_version: ApiVersion,
    /// The name of the user module.
    pub module_name: &'a str,
}

/// The errors which the validator may find.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Error {
    /// Indicates that the user module requested an API
    /// version which is unsupported. Returns the requested
    /// version alongside with the minimum and maximum supported
    /// versions.
    UnsupportedApiVersion {
        requested: ApiVersion,
        supported: Vec<ApiVersion>,
    },
    /// Indicates that the version has been incorrectly specified (
    /// syntax violation).
    ApiVersionSyntaxViolation {
        current: String,
        expected_format: String,
        example: String,
    },
    /// Indicates that the api version is missing from the prologue.
    MissingApiVersion,
    /// Indicates that the module name is missing from the prologue.
    MissingModuleName,
    /// Indicates that the validator couldn't find the engine
    /// name within the code prologue.
    ///
    /// Returns the list of available engines.
    NoEngineNameFound {
        available_engines: Vec<String>,
    },
    /// Indicates that the user module doesn't have the prologue or it
    /// is invalid.
    InvalidOrMissingPrologue,
    UnknownPrologueProperties {
        specified_properties: Vec<String>,
        known_properties: Vec<String>,
    },
    DuplicatedPrologueProperties {
        duplicated_properties: Vec<String>,
    },
}

impl From<Error> for GearsApiError {
    fn from(value: Error) -> Self {
        let string = match value {
            Error::UnsupportedApiVersion {
                requested,
                supported,
            } => {
                format!(
                    "An unsupported API version was requested: {requested}.
                \nThe supported versions are {}.",
                    supported
                        .into_iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            Error::ApiVersionSyntaxViolation {
                current,
                expected_format,
                example,
            } => {
                format!(
                    "The API version has been incorrectly specified: \"{current}\".\nThe expected format is \"{expected_format}\".\nAn example: \"{example}\""
                )
            }
            Error::MissingApiVersion => "The api version is missing from the prologue.".to_owned(),
            Error::MissingModuleName => "The module name is missing from the prologue.".to_owned(),
            Error::NoEngineNameFound { available_engines } => {
                format!(
                    "No engine found. The available engines are: {}.",
                    available_engines.join(", ")
                )
            }
            Error::InvalidOrMissingPrologue => {
                "Invalid or missing prologue. The rules for the prologue are:
            \n\t1. It starts on the first line.
            \n\t2. It is formatted as follows: \"#!<engine name> <key>=<value>\""
                    .to_owned()
            }
            Error::UnknownPrologueProperties {
                specified_properties,
                known_properties,
            } => {
                format!(
                    "Unknown prologue properties provided: {}.\nKnown properties: {}",
                    specified_properties.join(", "),
                    known_properties.join(", ")
                )
            }
            Error::DuplicatedPrologueProperties {
                duplicated_properties,
            } => {
                format!(
                    "Duplicated prologue properties found: {}",
                    duplicated_properties.join(", ")
                )
            }
        };

        Self::new(string)
    }
}

/// A result of the validator.
pub type Result<T> = std::result::Result<T, Error>;

/// Parses out the prologue (metadata).
///
/// The full user module code is expected to be passed here, or at
/// least the very first line of it.
pub fn parse_prologue(code: &str) -> Result<Prologue> {
    const KNOWN_PROPERTIES: [&str; 2] = [PROLOGUE_API_VERSION_KEY, PROLOGUE_MODULE_NAME_KEY];

    let first_line = code.lines().next().ok_or(Error::InvalidOrMissingPrologue)?;

    if !first_line.starts_with(ENGINE_PREFIX) {
        return Err(Error::InvalidOrMissingPrologue);
    }

    let shebang = first_line
        .strip_prefix(ENGINE_PREFIX)
        .ok_or(Error::InvalidOrMissingPrologue)?;

    let mut properties = shebang.split(&[' ', '=']);
    let engine = properties.next().ok_or(Error::InvalidOrMissingPrologue)?;

    let properties: Vec<&str> = properties.collect();
    let mut duplicated_properties = Vec::new();
    let mut is_prologue_invalid = false;
    let mut properties = properties.chunks(2).fold(HashMap::new(), |mut map, kv| {
        if !is_prologue_invalid {
            if kv.len() == 2 {
                if map.contains_key(kv[0]) {
                    duplicated_properties.push(kv[0].to_owned());
                } else if kv[0].is_empty() || kv[1].is_empty() {
                    is_prologue_invalid = true;
                } else {
                    map.insert(kv[0], kv[1]);
                }
            } else if kv.len() == 1 {
                is_prologue_invalid = true;
            }
        }
        map
    });

    if is_prologue_invalid {
        return Err(Error::InvalidOrMissingPrologue);
    }

    if !duplicated_properties.is_empty() {
        return Err(Error::DuplicatedPrologueProperties {
            duplicated_properties,
        });
    }

    let api_version = properties
        .remove(PROLOGUE_API_VERSION_KEY)
        .ok_or(Error::MissingApiVersion)
        .map(ApiVersion::from_str)??;

    let module_name = properties
        .remove(PROLOGUE_MODULE_NAME_KEY)
        .ok_or(Error::MissingModuleName)?;

    if !properties.is_empty() {
        let specified_properties = properties
            .keys()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<String>>();
        return Err(Error::UnknownPrologueProperties {
            specified_properties,
            known_properties: KNOWN_PROPERTIES.map(|s| s.to_owned()).to_vec(),
        });
    }

    Ok(Prologue {
        engine,
        api_version,
        module_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_correct() {
        let s = "#!js api_version=1.0 name=test_lib";
        let prologue = parse_prologue(s).unwrap();
        assert_eq!(prologue.engine, "js");
        assert_eq!(prologue.api_version, ApiVersion(1, 0));
        assert_eq!(prologue.module_name, "test_lib");
    }

    #[test]
    fn test_missing_api_version() {
        let s = "#!js name=test_lib";
        let err = parse_prologue(s).unwrap_err();
        assert_eq!(err, Error::MissingApiVersion);
    }

    #[test]
    fn test_repeating_properties() {
        let s = "#!js name=test_lib name=X";
        let err = parse_prologue(s).unwrap_err();
        assert_eq!(
            err,
            Error::DuplicatedPrologueProperties {
                duplicated_properties: vec!["name".to_owned()]
            }
        );
    }

    #[test]
    fn test_wrong_version() {
        let s = "#!js api_version=1.2-3x name=test_lib";
        let err = parse_prologue(s).unwrap_err();
        assert_eq!(
            err,
            Error::ApiVersionSyntaxViolation {
                current: "1.2-3x".to_owned(),
                expected_format: "<major>.<minor>".to_owned(),
                example: "1.1".to_owned()
            }
        );
    }

    #[test]
    fn test_unknown_properties() {
        let s = "#!js api_version=1.0 name=test_lib unknown_key=unknown_value";
        let err = parse_prologue(s).unwrap_err();
        assert_eq!(
            err,
            Error::UnknownPrologueProperties {
                specified_properties: vec!["\"unknown_key\"".to_owned()],
                known_properties: vec![
                    PROLOGUE_API_VERSION_KEY.to_owned(),
                    PROLOGUE_MODULE_NAME_KEY.to_owned()
                ]
            }
        );
    }

    #[test]
    fn test_invalid() {
        let s = "#!js name=test_lib X";
        let err = parse_prologue(s).unwrap_err();
        assert_eq!(err, Error::InvalidOrMissingPrologue);

        let s = "#!js name=test_lib =";
        let err = parse_prologue(s).unwrap_err();
        assert_eq!(err, Error::InvalidOrMissingPrologue);
    }

    #[test]
    fn test_missing_module_name() {
        let s = "#!js api_version=1.0";
        let err = parse_prologue(s).unwrap_err();
        assert_eq!(err, Error::MissingModuleName);
    }
}
