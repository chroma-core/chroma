use std::time::Duration;

use mdac_service::{BucketConfig, Config, UpdateRequest, UpdateResponse};
use reqwest::{Client, StatusCode};
use tokio::{net::TcpListener, sync::oneshot};

#[tokio::test]
async fn http_clients_share_named_buckets() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let buckets = Config {
        listen_address: listener.local_addr().unwrap(),
        open_telemetry: None,
        stdout_tracing: false,
        buckets: [
            ("shared", 5),
            ("other", 5),
            ("Shared", 1),
            ("shared ", 10),
            ("租户/reads", 1000),
            ("", 5),
        ]
        .into_iter()
        .map(|(name, capacity)| {
            (
                name.to_owned(),
                BucketConfig {
                    capacity,
                    interval_ns: 3_600_000_000_000,
                },
            )
        })
        .collect(),
    }
    .buckets()
    .unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(mdac_service::serve(listener, buckets, async move {
        let _ = shutdown_rx.await;
    }));
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let endpoint = format!("{base}/api/v1/token-bucket/put-back-and-drain");

    let mut requests = Vec::new();
    for _ in 0..20 {
        let client = client.clone();
        let endpoint = endpoint.clone();
        requests.push(tokio::spawn(async move {
            let response = client
                .post(endpoint)
                .json(&UpdateRequest {
                    name: "shared".into(),
                    excess: 0,
                    need: 1,
                })
                .send()
                .await
                .unwrap();
            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
                assert!(response.headers().contains_key("chroma-trace-id"));
            }
            let admitted = response.json::<UpdateResponse>().await.unwrap().admitted;
            assert_eq!(
                status,
                if admitted {
                    StatusCode::OK
                } else {
                    StatusCode::TOO_MANY_REQUESTS
                }
            );
            usize::from(admitted)
        }));
    }
    let mut admitted = 0;
    for request in requests {
        admitted += request.await.unwrap();
    }
    assert_eq!(admitted, 5);

    // Distinct names have independent allowances, including names that differ only by case
    // or whitespace. Arbitrary strings travel in JSON without URL path encoding.
    for (name, capacity) in [
        ("other", 5),
        ("Shared", 1),
        ("shared ", 10),
        ("租户/reads", 1000),
        ("", 5),
    ] {
        for expected in [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS] {
            let response = client
                .post(&endpoint)
                .json(&UpdateRequest {
                    name: name.into(),
                    excess: 0,
                    need: capacity,
                })
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
    }
    let response = client
        .post(&endpoint)
        .json(&UpdateRequest {
            name: "unknown".into(),
            excess: 100,
            need: 0,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(!response.json::<UpdateResponse>().await.unwrap().admitted);

    // Refunding one name must not replenish another name.
    for (name, excess, need, expected) in [
        ("other", 5, 0, StatusCode::OK),
        ("shared", 0, 1, StatusCode::TOO_MANY_REQUESTS),
        ("other", 0, 5, StatusCode::OK),
    ] {
        let response = client
            .post(&endpoint)
            .json(&UpdateRequest {
                name: name.into(),
                excess,
                need,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }

    // Health checks stay available when the allowance is exhausted.
    assert_eq!(
        client
            .get(format!("{base}/api/v1/healthcheck"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // The first failed drain must keep its refund; the next call consumes it. Excess credit
    // must be discarded before draining, and even a failed oversized drain keeps its refund.
    for (excess, need, expected) in [
        (2, 3, false),
        (0, 2, true),
        (0, 1, false),
        (u32::MAX, 4, true),
        (0, 1, true),
        (0, 1, false),
        (u32::MAX, 6, false),
        (0, 5, true),
        (0, 0, true),
    ] {
        let response = client
            .post(&endpoint)
            .json(&UpdateRequest {
                name: "shared".into(),
                excess,
                need,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if expected {
                StatusCode::OK
            } else {
                StatusCode::TOO_MANY_REQUESTS
            }
        );
        assert_eq!(
            response.json::<UpdateResponse>().await.unwrap().admitted,
            expected
        );
    }

    // Invalid input must be rejected before applying a refund.
    for body in [
        r#"{"excess":5,"need":0}"#,
        r#"{"name":42,"excess":5,"need":0}"#,
        r#"{"name":"shared","excess":5,"need":-1}"#,
        r#"{"name":"shared","excess":5}"#,
        r#"{"name":"shared","excess":5,"need":4294967296}"#,
        r#"{"name":"shared","excess":5,"need":0,"unexpected":true}"#,
    ] {
        let response = client
            .post(&endpoint)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
    let response = client
        .post(&endpoint)
        .header("content-type", "application/json")
        .body(format!("{}{{\"excess\":5,\"need\":0}}", " ".repeat(1024)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let response = client
        .post(&endpoint)
        .json(&UpdateRequest {
            name: "shared".into(),
            excess: 0,
            need: 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
