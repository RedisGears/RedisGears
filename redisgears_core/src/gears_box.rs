use crate::get_globals;
use redis_module::RedisError;
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
    match reqwest::blocking::get(url) {
        Ok(r) => match r.text() {
            Ok(r) => Ok(r),
            Err(e) => Err(RedisError::String(e.to_string())),
        },
        Err(e) => Err(RedisError::String(e.to_string())),
    }
}

pub(crate) fn gears_box_search(token: &str) -> Result<serde_json::Value, RedisError> {
    let gears_box_address = &get_globals().config.gears_box_address.address;
    let url = &format!("{}/api/v1/recipes?q={}", gears_box_address, token);
    let res = do_http_get(url);
    match res {
        Ok(r) => Ok(r),
        Err(e) => Err(e),
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub(crate) struct GearsBoxLibraryVersionInfo {
    pub(crate) id: String,
    pub(crate) minGearsVersion: String,
    pub(crate) minRedisVersion: String,
    pub(crate) version: String,
    pub(crate) changeDescription: String,
    pub(crate) date: isize,
    pub(crate) mimeType: String,
    pub(crate) url: String,
    pub(crate) sha256: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
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

pub(crate) fn gears_box_get_library(library_id: &str) -> Result<GearsBoxLibraryInfo, RedisError> {
    let gears_box_address = &get_globals().config.gears_box_address.address;
    let general_info_url = &format!("{}/api/v1/recipes/{}/", gears_box_address, library_id);
    let general_info = do_http_get(general_info_url)?;
    let installed_version_url = &format!(
        "{}/api/v1/recipes/{}/versions/latest",
        gears_box_address, library_id
    );
    let installed_version = do_http_get(installed_version_url)?;
    Ok(GearsBoxLibraryInfo {
        general_info,
        installed_version_info: installed_version,
    })
}
