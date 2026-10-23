use reqwest::header::HeaderMap;
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Debug;

#[derive(Deserialize, Default)]
pub struct EmptyResponse {}

pub async fn send_request<T, R>(
    url: &String,
    method: Method,
    route: &str,
    headers: Option<HeaderMap>,
    body: Option<&T>,
) -> Result<R, Box<dyn Error>>
where
    T: Serialize + Debug,
    R: DeserializeOwned,
{
    let url = format!("{}{}", url, route);

    let client = Client::new();
    let mut request_builder = client.request(method, url);

    if let Some(headers) = headers {
        request_builder = request_builder.headers(headers);
    }

    if let Some(b) = body {
        request_builder = request_builder.json(b);
    }

    let response = request_builder.send().await?;
    let status = response.status();
    let text = response.text().await?;

    if !status.is_success() {
        return Err(text.to_string().into());
    }

    let parsed_response = serde_json::from_str::<R>(&text)?;
    Ok(parsed_response)
}
