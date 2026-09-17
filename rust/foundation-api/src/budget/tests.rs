use httpmock::prelude::*;
use serde_json::json;

use super::rates::TokenRate;
use super::*;

const LAUNCH_RATES: TokenRates = TokenRates {
    planner: TokenRate {
        input_micros_per_m_tokens: 3_000_000,
        output_micros_per_m_tokens: 18_000_000,
    },
    context_1: TokenRate {
        input_micros_per_m_tokens: 1_000_000,
        output_micros_per_m_tokens: 1_000_000,
    },
};

const PLANNER: &str = "claude-sonnet-4-5-20250929";

fn card_json() -> serde_json::Value {
    json!({
        "version": "2026-09-17",
        "currency": "USD",
        "unit": "micro_usd",
        "coding_agent_chunk": 25_000,
        "tokens": {
            "planner": {"input_micros_per_m_tokens": 3_000_000, "output_micros_per_m_tokens": 18_000_000},
            "context_1": {"input_micros_per_m_tokens": 1_000_000, "output_micros_per_m_tokens": 1_000_000},
            "wikigen_haiku_4_5": {"input_micros_per_m_tokens": 1_200_000, "output_micros_per_m_tokens": 6_000_000},
        },
    })
}

// Rates apply per million tokens with integer floor division, matching the
// card's own TokenRate::price semantics.
#[test]
fn prices_planner_and_context_usage_separately() {
    let usage = vec![
        (PLANNER.to_string(), 1_000_000, 100_000),
        ("scout".to_string(), 2_000_000, 50_000),
    ];
    // planner: 1M in × $3/M + 100k out × $18/M = 3_000_000 + 1_800_000
    // context-1: 2M in × $1/M + 50k out × $1/M = 2_000_000 + 50_000
    assert_eq!(
        price_agent_usage(&LAUNCH_RATES, PLANNER, &usage),
        3_000_000 + 1_800_000 + 2_000_000 + 50_000
    );
}

#[test]
fn sub_million_usage_rounds_down_not_up() {
    // 1 input token at $3/M = 3 µ$; 1 output token at $18/M = 18 µ$.
    let usage = vec![(PLANNER.to_string(), 1, 1)];
    assert_eq!(price_agent_usage(&LAUNCH_RATES, PLANNER, &usage), 21);
    let none = vec![("scout".to_string(), 0, 0)];
    assert_eq!(price_agent_usage(&LAUNCH_RATES, PLANNER, &none), 0);
}

#[tokio::test]
async fn debit_posts_priced_amount_with_query_class() {
    let server = MockServer::start_async().await;
    let card = server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/foundation/price-card")
                .header("x-chroma-token", "key-1");
            then.status(200)
                .header("cache-control", "public, max-age=300")
                .json_body(card_json());
        })
        .await;
    let debit = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/foundation/budget-debit")
                .header("x-chroma-token", "key-1")
                .json_body(json!({
                    "tenant": "tenant-1",
                    "spend_class": "query",
                    "source": "agent_query",
                    "amount_micros": 4_800_000_i64,
                    "ref_id": "trace-1",
                }));
            then.status(200)
                .json_body(json!({"debited": true, "applied": true, "exceeded": false}));
        })
        .await;

    let client = BudgetClient::new(&server.base_url(), reqwest::Client::new());
    let usage = vec![(PLANNER.to_string(), 1_000_000, 100_000)];
    client
        .debit_agent_query("key-1", "tenant-1", "trace-1", PLANNER, &usage)
        .await;

    card.assert_async().await;
    debit.assert_async().await;
}

// The card cache honors max-age: a second debit inside the window fetches
// the card once.
#[tokio::test]
async fn card_is_cached_across_debits() {
    let server = MockServer::start_async().await;
    let card = server
        .mock_async(|when, then| {
            when.method(GET).path("/foundation/price-card");
            then.status(200)
                .header("cache-control", "public, max-age=300")
                .json_body(card_json());
        })
        .await;
    let debit = server
        .mock_async(|when, then| {
            when.method(POST).path("/foundation/budget-debit");
            then.status(200).json_body(json!({"debited": true}));
        })
        .await;

    let client = BudgetClient::new(&server.base_url(), reqwest::Client::new());
    let usage = vec![(PLANNER.to_string(), 1_000, 1_000)];
    client
        .debit_agent_query("key-1", "tenant-1", "trace-a", PLANNER, &usage)
        .await;
    client
        .debit_agent_query("key-1", "tenant-1", "trace-b", PLANNER, &usage)
        .await;

    card.assert_calls_async(1).await;
    debit.assert_calls_async(2).await;
}

// Zero-priced runs are not posted.
#[tokio::test]
async fn zero_amount_skips_the_post() {
    let server = MockServer::start_async().await;
    let card = server
        .mock_async(|when, then| {
            when.method(GET).path("/foundation/price-card");
            then.status(200).json_body(card_json());
        })
        .await;
    let debit = server
        .mock_async(|when, then| {
            when.method(POST).path("/foundation/budget-debit");
            then.status(200);
        })
        .await;

    let client = BudgetClient::new(&server.base_url(), reqwest::Client::new());
    client
        .debit_agent_query("key-1", "tenant-1", "trace-z", PLANNER, &[])
        .await;

    card.assert_calls_async(1).await;
    debit.assert_calls_async(0).await;
}

// An unreachable card endpoint with an empty cache drops the debit quietly.
#[tokio::test]
async fn unreachable_sync_never_panics() {
    let client = BudgetClient::new("http://127.0.0.1:9", reqwest::Client::new());
    let usage = vec![(PLANNER.to_string(), 1_000_000, 0)];
    client
        .debit_agent_query("key-1", "tenant-1", "trace-x", PLANNER, &usage)
        .await;
}

#[test]
fn cache_ttl_parses_max_age() {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::CACHE_CONTROL,
        "public, max-age=120".parse().unwrap(),
    );
    assert_eq!(cache_ttl(&headers), Some(Duration::from_secs(120)));
    headers.insert(reqwest::header::CACHE_CONTROL, "no-store".parse().unwrap());
    assert_eq!(cache_ttl(&headers), None);
    headers.remove(reqwest::header::CACHE_CONTROL);
    assert_eq!(cache_ttl(&headers), None);
}
