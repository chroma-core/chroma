use crate::client::utils::send_request;
use crate::utils::get_address_book;
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
    #[error("Failed to get CLI token")]
    CliToken,
    #[error("Failed to verify CLI token")]
    CliTokenVerification,
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

#[derive(Deserialize, Debug, Default)]
struct CliLoginResponse {
    token: String,
}

#[derive(Serialize, Debug)]
struct CliVerifyRequest {
    token: String,
}

#[derive(Deserialize, Debug, Default)]
pub struct CliVerifyResponse {
    pub success: bool,
    #[serde(rename = "sessionId")]
    pub session_id: String,
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

    fn headers(&self, session_id: &str) -> Result<Option<HeaderMap>, DashboardClientError> {
        let mut headers = HeaderMap::new();
        let cookie_value = format!("sessionId={}", session_id);
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&cookie_value).map_err(|_| DashboardClientError::CookiesParse)?,
        );
        Ok(Some(headers))
    }

    pub async fn get_api_key(
        &self,
        team_slug: &str,
        session_id: &str,
    ) -> Result<String, DashboardClientError> {
        let route = format!("/api/v1/teams/{}/api_keys", team_slug);
        let payload = CreateApiKeyRequest {
            name: team_slug.to_string(),
        };
        let response = send_request::<CreateApiKeyRequest, CreateApiKeyResponse>(
            &self.api_url,
            Method::POST,
            &route,
            self.headers(session_id)?,
            Some(&payload),
        )
        .await
        .map_err(|_| DashboardClientError::ApiKeyFetch(team_slug.to_string()))?;
        Ok(response.key)
    }

    pub async fn get_teams(&self, session_id: &str) -> Result<Vec<Team>, DashboardClientError> {
        let route = "/api/v1/teams";
        let response = send_request::<(), Vec<Team>>(
            &self.api_url,
            Method::GET,
            route,
            self.headers(session_id)?,
            None,
        )
        .await
        .map_err(|_| DashboardClientError::TeamFetch(session_id.to_string()))?;
        Ok(response)
    }

    pub async fn get_cli_token(&self) -> Result<String, DashboardClientError> {
        let route = "/api/v1/cli-login";
        let response =
            send_request::<(), CliLoginResponse>(&self.api_url, Method::GET, route, None, None)
                .await
                .map_err(|_| DashboardClientError::CliToken)?;
        Ok(response.token)
    }

    pub async fn verify_cli_token(
        &self,
        token: String,
    ) -> Result<CliVerifyResponse, DashboardClientError> {
        let route = "/api/v1/cli-login/verify-token";
        let body = CliVerifyRequest { token };
        let response = send_request::<CliVerifyRequest, CliVerifyResponse>(
            &self.api_url,
            Method::POST,
            route,
            None,
            Some(&body),
        )
        .await
        .map_err(|_| DashboardClientError::CliTokenVerification)?;
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
