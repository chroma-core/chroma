use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{de::DeserializeOwned, Serialize};
use std::error::Error;
use crate::utils::Profile;
use chroma_frontend::server::CreateDatabasePayload;
use chroma_types::{Database, ListDatabasesResponse};

enum RequestMethod {
    Get,
    Post,
    Delete,
}

fn create_chroma_client(api_key: &str) -> Result<(Client, HeaderMap), Box<dyn Error>> {
    let client = Client::new();
    let mut headers = HeaderMap::new();
    headers.insert("X-Chroma-Token", HeaderValue::from_str(api_key)?);

    Ok((client, headers))
}

fn chroma_server_request<T: DeserializeOwned, U: Serialize + ?Sized>(
    method: RequestMethod,
    api_url: &str,
    route: &str,
    api_key: &str,
    body: Option<&U>,
) -> Result<Option<T>, Box<dyn Error>> {
    let (client, mut headers) = create_chroma_client(api_key)?;
    let url = format!("{}{}", api_url, route);
    
    if let RequestMethod::Post = method {
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    }
    
    let response = match method {
        RequestMethod::Get => client.get(&url).headers(headers).send()?,
        RequestMethod::Post => {
            if let Some(data) = body {
                client.post(&url).headers(headers).json(data).send()?
            } else {
                return Err("POST request requires a body".into());
            }
        },
        RequestMethod::Delete => {
            println!("5");
            let response = client.delete(&url).headers(headers).send()?;
            return Ok(None); // Delete requests don't typically return a body to deserialize
        },
    };

    println!("6");

    if let RequestMethod::Delete = method {
        println!("7");
        Ok(None)
    } else {
        let response_text = response.text()?;
        let deserialized: T = serde_json::from_str(&response_text)?;
        println!("8");
        Ok(Some(deserialized))
    }
}

pub fn chroma_server_get_request<T: DeserializeOwned>(
    api_url: &str,
    route: &str,
    api_key: &str,
) -> Result<T, Box<dyn Error>> {
    let result = chroma_server_request::<T, ()>(
        RequestMethod::Get,
        api_url,
        route,
        api_key,
        None,
    )?;

    result.ok_or_else(|| "No response body".into())
}

pub fn chroma_server_delete_request(
    api_url: &str,
    route: &str,
    api_key: &str,
) -> Result<(), Box<dyn Error>> {
    chroma_server_request::<(), ()>(
        RequestMethod::Delete,
        api_url,
        route,
        api_key,
        None,
    )?;

    Ok(())
}

pub fn chroma_server_post_request<T: DeserializeOwned, U: Serialize>(
    api_url: &str,
    route: &str,
    api_key: &str,
    body: &U,
) -> Result<T, Box<dyn Error>> {
    let result = chroma_server_request::<T, U>(
        RequestMethod::Post,
        api_url,
        route,
        api_key,
        Some(body),
    )?;

    result.ok_or_else(|| "No response body".into())
}

pub fn create_database(api_url: &str, profile: &Profile, name: String) -> Result<(), Box<dyn Error>> {
    let create_db_route = format!("{}/api/v2/tenants/{}/databases", api_url, profile.team_id);
    
    chroma_server_post_request::<(), CreateDatabasePayload>(
        api_url,
        &create_db_route,
        &profile.api_key,
        &CreateDatabasePayload { name },
    )?;
    Ok(())
}

pub fn delete_database(api_url: &str, profile: &Profile, name: String) -> Result<(), Box<dyn Error>> {
    let delete_db_route = format!("/api/v2/tenants/{}/databases/{}", profile.team_id, name);
    chroma_server_delete_request(api_url, &delete_db_route, &profile.api_key)?;
    Ok(())
}

pub fn list_databases(api_url: &str, profile: &Profile) -> Result<(Vec<Database>), Box<dyn Error>> {
    let list_dbs_route = format!("/api/v2/tenants/{}/databases", profile.team_id);
    let response =
        chroma_server_get_request::<ListDatabasesResponse>(api_url, &list_dbs_route, &profile.api_key)?;
    Ok(response)
}