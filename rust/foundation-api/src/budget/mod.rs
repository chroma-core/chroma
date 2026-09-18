//! Agent query budget debits (CHR-773).
//!
//! After each `/api/agent` response stream completes, foundation-api prices
//! the run's per-model token usage against the Foundation price card and
//! posts one debit to the sync-frontend's `POST /foundation/budget-debit`,
//! on a detached task with the querying caller's own `x-chroma-token`. Every
//! failure is a `warn!`, never an error: the user's answer is already
//! streamed, and the budget counter is shadow-mode (fail-open) end to end.
//!
//! The card is read from `GET /foundation/price-card` through an in-process
//! cache honoring the response's `Cache-Control: max-age` (the endpoint
//! serves 5 minutes). Rates are micro-USD per MILLION tokens; only input and
//! output tokens are priced — cache reads/writes are deliberately unpriced
//! today, matching the Orb meters (CHR-767 will change the mix).

mod rates;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use rates::PriceCard;
pub(crate) use rates::{price_agent_usage, TokenRates};

/// Fallback card TTL when the response carries no usable `max-age`.
const DEFAULT_CARD_TTL: Duration = Duration::from_secs(300);
/// Outbound call timeout: these calls run on detached tasks, so they must
/// never be able to hang (a poll loop hung on a missing reqwest timeout
/// before — CHR-623).
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

struct CachedRates {
    rates: TokenRates,
    expires_at: Instant,
}

/// Client for the sync-frontend's price card and budget-debit endpoints.
/// Constructed once per process when `foundation.sync_frontend_url` is set.
pub(crate) struct BudgetClient {
    base_url: String,
    http: reqwest::Client,
    cache: RwLock<Option<CachedRates>>,
}

impl BudgetClient {
    pub(crate) fn new(base_url: &str, http: reqwest::Client) -> Self {
        BudgetClient {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            cache: RwLock::new(None),
        }
    }

    /// The card's token rates, from cache when fresh. On a failed refresh the
    /// last known rates are served stale; with nothing cached yet, `None`
    /// (the caller skips the debit and warns).
    async fn token_rates(&self, token: &str) -> Option<TokenRates> {
        if let Some(cached) = self.cache.read().await.as_ref() {
            if cached.expires_at > Instant::now() {
                return Some(cached.rates);
            }
        }
        match self.fetch_card(token).await {
            Ok((rates, ttl)) => {
                *self.cache.write().await = Some(CachedRates {
                    rates,
                    expires_at: Instant::now() + ttl,
                });
                Some(rates)
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to refresh the Foundation price card");
                self.cache.read().await.as_ref().map(|cached| cached.rates)
            }
        }
    }

    async fn fetch_card(&self, token: &str) -> Result<(TokenRates, Duration), reqwest::Error> {
        let response = self
            .http
            .get(format!("{}/foundation/price-card", self.base_url))
            .header("x-chroma-token", token)
            .timeout(HTTP_TIMEOUT)
            .send()
            .await?
            .error_for_status()?;
        let ttl = cache_ttl(response.headers()).unwrap_or(DEFAULT_CARD_TTL);
        let card: PriceCard = response.json().await?;
        Ok((card.tokens, ttl))
    }

    /// Price and post one agent run's debit. Never fails: every miss — no
    /// rates, zero amount, unreachable sync, non-2xx — is logged and dropped,
    /// and the sync side alarms on its own fail-open path.
    pub(crate) async fn debit_agent_query(
        &self,
        token: &str,
        tenant: &str,
        ref_id: &str,
        planner_model: &str,
        usage: &[(String, u64, u64)],
    ) {
        let Some(rates) = self.token_rates(token).await else {
            tracing::warn!(
                tenant,
                ref_id,
                "no price card available; agent query debit skipped"
            );
            return;
        };
        let amount_micros = price_agent_usage(&rates, planner_model, usage);
        if amount_micros == 0 {
            return;
        }
        let body = serde_json::json!({
            "tenant": tenant,
            "spend_class": "query",
            "source": "agent_query",
            "amount_micros": i64::try_from(amount_micros).unwrap_or(i64::MAX),
            "ref_id": ref_id,
        });
        let result = self
            .http
            .post(format!("{}/foundation/budget-debit", self.base_url))
            .header("x-chroma-token", token)
            .timeout(HTTP_TIMEOUT)
            .json(&body)
            .send()
            .await
            .and_then(|response| response.error_for_status());
        if let Err(error) = result {
            tracing::warn!(
                error = %error,
                tenant,
                ref_id,
                amount_micros,
                "failed to post agent query budget debit"
            );
        }
    }
}

/// Everything the detached debit task needs, captured per request before the
/// agent stream starts.
pub(crate) struct QueryDebit {
    pub client: Arc<BudgetClient>,
    /// The querying caller's own token; the debit rides their grant.
    pub token: String,
    /// Idempotency key: the request's trace id (a fresh UUID when tracing is
    /// off, so untraced runs don't all collide on the zero trace id).
    pub ref_id: String,
    /// The model driving the loop, so its usage prices at the planner rate.
    pub planner_model: String,
}

impl QueryDebit {
    pub(crate) async fn post(self, tenant: String, usage: Vec<(String, u64, u64)>) {
        self.client
            .debit_agent_query(
                &self.token,
                &tenant,
                &self.ref_id,
                &self.planner_model,
                &usage,
            )
            .await;
    }
}

/// `max-age` seconds from a `Cache-Control` header, if present and parseable.
fn cache_ttl(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::CACHE_CONTROL)?.to_str().ok()?;
    let seconds = value
        .split(',')
        .filter_map(|directive| directive.trim().strip_prefix("max-age="))
        .find_map(|age| age.parse::<u64>().ok())?;
    Some(Duration::from_secs(seconds))
}
