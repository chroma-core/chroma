use crate::utils::{get_address_book, send_request};
use reqwest::header::{HeaderMap, HeaderValue, COOKIE};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DashboardClientError {
    #[error("Failed to parse session cookies")]
    CookiesParse,
    #[error("Failed to fetch API key for tenant {0}")]
    ApiKeyFetch(String),
    #[error("Failed to fetch teams")]
    TeamFetch(String),
}

#[derive(Deserialize, Debug)]
pub struct Team {
    pub uuid: String,
    pub name: String,
    pub slug: String,
}

#[derive(Serialize, Debug)]
struct CreateApiKeyRequest {
    name: String,
}

#[derive(Deserialize, Debug, Default)]
struct CreateApiKeyResponse {
    key: String,
}

#[derive(Default, Debug, Clone)]
pub struct DashboardClient {
    pub api_url: String,
    pub frontend_url: String,
}

impl DashboardClient {
    pub fn new(api_url: String, frontend_url: String) -> Self {
        DashboardClient {
            api_url,
            frontend_url,
        }
    }

    fn headers(&self, session_cookies: &str) -> Result<Option<HeaderMap>, DashboardClientError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(session_cookies)
                .map_err(|_| DashboardClientError::CookiesParse)?,
        );
        Ok(Some(headers))
    }

    pub async fn get_api_key(
        &self,
        team_slug: &str,
        session_cookies: &str,
    ) -> Result<String, DashboardClientError> {
        let route = format!("/api/v1/teams/{}/api_keys", team_slug);
        let payload = CreateApiKeyRequest {
            name: team_slug.to_string(),
        };
        let response = send_request::<CreateApiKeyRequest, CreateApiKeyResponse>(
            &self.api_url,
            Method::POST,
            &route,
            self.headers(session_cookies)?,
            Some(&payload),
        )
        .await
        .map_err(|_| DashboardClientError::ApiKeyFetch(team_slug.to_string()))?;
        Ok(response.key)
    }

    pub async fn get_teams(
        &self,
        session_cookies: &str,
    ) -> Result<Vec<Team>, DashboardClientError> {
        let route = "/api/v1/teams";
        let response = send_request::<(), Vec<Team>>(
            &self.api_url,
            Method::GET,
            route,
            self.headers(session_cookies)?,
            None,
        )
        .await
        .map_err(|_| DashboardClientError::TeamFetch(session_cookies.to_string()))?;
        Ok(response)
    }
}

pub fn get_dashboard_client(dev: bool) -> DashboardClient {
    let address_book = get_address_book(dev);
    DashboardClient::new(
        address_book.dashboard_api_url,
        address_book.dashboard_frontend_url,
    )
}
