/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::config::GEARS_BOX_ADDRESS;
use redis_module::{Context, RedisError};
use serde::{Deserialize, Serialize};

pub(crate) fn do_http_get<T: for<'de> serde::Deserialize<'de>>(url: &str) -> Result<T, RedisError> {
    match reqwest::blocking::get(url) {
        Ok(r) => match r.json::<T>() {
            Ok(r) => Ok(r),
            Err(e) => Err(RedisError::String(e.to_string())),
        },
        Err(e) => Err(RedisError::String(e.to_string())),
    }
}

pub(crate) fn do_http_get_text(url: &str) -> Result<String, RedisError> {
    reqwest::blocking::get(url)
        .map_err(|e| RedisError::String(e.to_string()))?
        .text()
        .map_err(|e| RedisError::String(e.to_string()))
}

pub(crate) fn gears_box_search(
    ctx: &Context,
    token: &str,
) -> Result<serde_json::Value, RedisError> {
    let gears_box_address = GEARS_BOX_ADDRESS.lock(ctx);
    let url = &format!("{}/api/v1/recipes?q={}", *gears_box_address, token);
    do_http_get(url)
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct GearsBoxLibraryVersionInfo {
    pub(crate) id: String,
    #[serde(rename = "minGearsVersion")]
    pub(crate) min_gears_version: String,
    #[serde(rename = "minRedisVersion")]
    pub(crate) min_redis_version: String,
    pub(crate) version: String,
    #[serde(rename = "changeDescription")]
    pub(crate) change_description: String,
    pub(crate) date: isize,
    #[serde(rename = "mimeType")]
    pub(crate) mime_type: String,
    pub(crate) url: String,
    pub(crate) sha256: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct GearsBoxLibraryAuthorInfo {
    pub(crate) id: String,
    pub(crate) email: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub(crate) struct GearsBoxLibraryGeneralInfo {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) author: GearsBoxLibraryAuthorInfo,
    pub(crate) likes: usize,
    pub(crate) created: usize,
    pub(crate) tags: Vec<String>,
    pub(crate) official: bool,
    pub(crate) versions: Vec<GearsBoxLibraryVersionInfo>,
    pub(crate) lastUpdated: usize,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub(crate) lib_type: String,
    pub(crate) active: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub(crate) struct GearsBoxLibraryInfo {
    pub(crate) general_info: GearsBoxLibraryGeneralInfo,
    pub(crate) installed_version_info: GearsBoxLibraryVersionInfo,
}

pub(crate) fn gears_box_get_library(
    ctx: &Context,
    library_id: &str,
) -> Result<GearsBoxLibraryInfo, RedisError> {
    let gears_box_address = GEARS_BOX_ADDRESS.lock(ctx);
    let general_info_url = &format!("{}/api/v1/recipes/{}/", *gears_box_address, library_id);
    let general_info = do_http_get(general_info_url)?;
    let installed_version_url = &format!(
        "{}/api/v1/recipes/{}/versions/latest",
        *gears_box_address, library_id
    );
    let installed_version = do_http_get(installed_version_url)?;
    Ok(GearsBoxLibraryInfo {
        general_info,
        installed_version_info: installed_version,
    })
}
