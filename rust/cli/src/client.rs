use crate::utils::Profile;
use chroma_frontend::server::{
    AddCollectionRecordsPayload, CreateCollectionPayload, CreateDatabasePayload,
    UpsertCollectionRecordsPayload,
};
use chroma_types::{
    AddCollectionRecordsResponse, Collection, CollectionUuid, Database, DeleteDatabaseResponse,
    GetRequest, GetResponse, GetUserIdentityResponse, Include, IncludeList,
    ListCollectionsResponse, ListDatabasesResponse, UpsertCollectionRecordsResponse,
};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::error::Error;
use std::fmt::format;
use std::str::FromStr;

pub const CHROMA_API_URL: &str = "https://api.trychroma.com:8000";

fn chroma_server_get_request<T: DeserializeOwned>(
    route: &str,
    api_key: &str,
) -> Result<T, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", CHROMA_API_URL, route);

    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);

    let response = client.get(url).headers(headers).send()?.json::<T>()?;

    Ok(response)
}

fn chroma_server_delete_request(route: &str, api_key: &str) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", CHROMA_API_URL, route);

    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);

    let _response = client.delete(url).headers(headers).send();

    Ok(())
}

pub fn chroma_server_post_request<T: DeserializeOwned, U: Serialize>(
    route: &str,
    api_key: &str,
    body: &U,
) -> Result<T, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}{}", CHROMA_API_URL, route);
    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    let response = client.post(&url).headers(headers).json(body).send()?;
    let response_text = response.text()?;
    let deserialized: T = serde_json::from_str(&response_text)?;
    Ok(deserialized)
}

pub fn get_tenant_id(api_key: &str) -> Result<String, Box<dyn Error>> {
    let identity_route = "/api/v2/auth/identity";
    let response = chroma_server_get_request::<GetUserIdentityResponse>(identity_route, api_key)?;
    Ok(response.tenant)
}

pub fn create_database(profile: &Profile, name: String) -> Result<(), Box<dyn Error>> {
    let create_db_route = format!("/api/v2/tenants/{}/databases", profile.tenant_id);
    chroma_server_post_request::<_, CreateDatabasePayload>(
        &create_db_route,
        &profile.api_key,
        &CreateDatabasePayload { name },
    )?;
    Ok(())
}

pub fn delete_database(profile: &Profile, name: String) -> Result<(), Box<dyn Error>> {
    let delete_db_route = format!("/api/v2/tenants/{}/databases/{}", profile.tenant_id, name);
    chroma_server_delete_request(&delete_db_route, &profile.api_key)?;
    Ok(())
}

pub fn list_databases(profile: &Profile) -> Result<(Vec<Database>), Box<dyn Error>> {
    let list_dbs_route = format!("/api/v2/tenants/{}/databases", profile.tenant_id);
    let response =
        chroma_server_get_request::<ListDatabasesResponse>(&list_dbs_route, &profile.api_key)?;
    Ok(response)
}

pub fn list_collections(
    profile: &Profile,
    db: String,
) -> Result<ListCollectionsResponse, Box<dyn Error>> {
    let list_collections_route = format!(
        "/api/v2/tenants/{}/databases/{}/collections",
        profile.tenant_id, db
    );
    let response = chroma_server_get_request::<ListCollectionsResponse>(
        &list_collections_route,
        &profile.api_key,
    )?;
    Ok(response)
}

pub fn collection_get(
    profile: &Profile,
    db: String,
    collection_id: CollectionUuid,
) -> Result<GetResponse, Box<dyn Error>> {
    let get_route = format!(
        "/api/v2/tenants/{}/databases/{}/collections/{}/get",
        profile.tenant_id, db, collection_id
    );
    let request_body = GetRequest::try_new(
        profile.tenant_id.clone(),
        db,
        collection_id,
        None,
        None,
        None,
        0,
        IncludeList::try_from(vec!["documents".to_string(), "embeddings".to_string()]).unwrap(),
    )?;
    let response = chroma_server_post_request::<GetResponse, GetRequest>(
        &get_route,
        &profile.api_key,
        &request_body,
    )?;
    Ok(response)
}

pub fn collection_upsert(
    profile: Profile,
    db: String,
    collection_id: CollectionUuid,
    payload: UpsertCollectionRecordsPayload,
) -> Result<UpsertCollectionRecordsResponse, Box<dyn Error>> {
    let upsert_route = format!(
        "/api/v2/tenants/{}/databases/{}/collections/{}/upsert",
        profile.tenant_id, db, collection_id
    );
    let response = chroma_server_post_request::<
        UpsertCollectionRecordsResponse,
        UpsertCollectionRecordsPayload,
    >(&upsert_route, &profile.api_key, &payload)?;
    Ok(response)
}

pub fn create_collection(
    profile: &Profile,
    db: String,
    payload: CreateCollectionPayload,
) -> Result<Collection, Box<dyn Error>> {
    let create_collection_route = format!(
        "/api/v2/tenants/{}/databases/{}/collections",
        profile.tenant_id, db
    );
    let response = chroma_server_post_request::<Collection, CreateCollectionPayload>(
        &create_collection_route,
        &profile.api_key,
        &payload,
    )?;
    Ok(response)
}

pub fn collection_add(
    profile: &Profile,
    db: String,
    collection_id: CollectionUuid,
    payload: AddCollectionRecordsPayload,
) -> Result<bool, Box<dyn Error>> {
    let add_route = format!(
        "/api/v2/tenants/{}/databases/{}/collections/{}/add",
        profile.tenant_id,
        db,
        collection_id.clone()
    );
    let response = chroma_server_post_request::<bool, AddCollectionRecordsPayload>(
        &add_route,
        &profile.api_key,
        &payload,
    )?;
    Ok(response)
}
