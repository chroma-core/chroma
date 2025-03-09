use std::error::Error;
use colored::Colorize;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use serde::Serialize;
use chroma_types::{CreateDatabaseResponse, Database, DeleteDatabaseResponse, GetUserIdentityResponse, ListDatabasesResponse};
use crate::utils::Profile;
use chroma_frontend::server::CreateDatabasePayload;

pub const CHROMA_API_URL: &str = "https://api.trychroma.com:8000";

fn chroma_server_get_request<T: DeserializeOwned>(route: &str, api_key: &str) -> Result<T, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", CHROMA_API_URL, route);

    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);
    
    let response = client
        .get(url)
        .headers(headers)
        .send()?
        .json::<T>()?;

    Ok(response)
}

fn chroma_server_delete_request<T: DeserializeOwned>(route: &str, api_key: &str) -> Result<T, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", CHROMA_API_URL, route);

    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);

    let response = client
        .delete(url)
        .headers(headers)
        .send()?
        .json::<T>()?;

    Ok(response)
}

fn chroma_server_post_request<T: DeserializeOwned, U: Serialize>(
    route: &str,
    api_key: &str,
    body: &U
) -> Result<T, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", CHROMA_API_URL, route);

    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));

    let response = client
        .post(&url)
        .headers(headers)
        .json(body)
        .send()?
        .json::<T>()?;

    Ok(response)
}

pub fn get_tenant_id(api_key: &str) -> Result<String, Box<dyn Error>> {
    let identity_route = "/api/v2/auth/identity";
    let response = chroma_server_get_request::<GetUserIdentityResponse>(identity_route, api_key)?;
    Ok(response.tenant)
}

pub fn create_database(profile: &Profile, name: String) -> Result<(), Box<dyn Error>> {
    let create_db_route = format!("/api/v2/tenants/{}/databases", profile.tenant_id);
    let _response = chroma_server_post_request::<_, CreateDatabasePayload>(&create_db_route, &profile.api_key, &CreateDatabasePayload { name })?;
    Ok(())
}

pub fn delete_database(profile: &Profile, name: String) -> Result<(), Box<dyn Error>> {
    let delete_db_route = format!("/api/v2/tenants/{}/databases/{}", profile.tenant_id, name);
    chroma_server_delete_request(&delete_db_route, &profile.api_key)?;
    Ok(())
}

pub fn list_databases(profile: &Profile) -> Result<(Vec<Database>), Box<dyn Error>> {
    let list_dbs_route = format!("/api/v2/tenants/{}/databases", profile.tenant_id);
    let response = chroma_server_get_request::<ListDatabasesResponse>(&list_dbs_route, &profile.api_key)?;
    Ok(response)
}