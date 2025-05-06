use aws_config::{retry::RetryConfig, timeout::TimeoutConfigBuilder};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    let config = aws_config::load_from_env().await;
    let timeout_config_builder = TimeoutConfigBuilder::default()
        .connect_timeout(Duration::from_millis(5000))
        .read_timeout(Duration::from_millis(60000));
    let retry_config = RetryConfig::standard();
    let config = config
        .to_builder()
        .timeout_config(timeout_config_builder.build())
        .retry_config(retry_config)
        .build();
    let client = aws_sdk_s3::Client::new(&config);

    // Create 8MB file
    let test_data = vec![0; 1 * 1024 * 1024];
    let bucket_name = "chroma-serverless-staging";
    let object_prefix = "hammad_test_data";
    let num_files = 64;

    for i in 0..num_files {
        let test_data = test_data.clone();
        let object_key = format!("{}/{:02}.bin", object_prefix, i);
        // Upload the file
        let result = client
            .put_object()
            .bucket(bucket_name)
            .key(&object_key)
            .body(test_data.into())
            .send()
            .await;
        match result {
            Ok(_) => println!("File uploaded successfully!"),
            Err(e) => eprintln!("Error uploading file: {}", e),
        }
    }

    // Download the file 64 times concurrently
    let start_time = std::time::Instant::now();
    let mut handles = vec![];
    let latencies = Arc::new(tokio::sync::Mutex::new(vec![]));
    for i in 0..64 {
        let latencies = latencies.clone();
        let client = client.clone();
        let bucket_name = bucket_name.to_string();
        let object_key = format!("{}/{:02}.bin", object_prefix, i);
        handles.push(tokio::spawn(async move {
            let req_start_time = std::time::Instant::now();
            let result = client
                .get_object()
                .bucket(&bucket_name)
                .key(&object_key)
                .send()
                .await;
            match result {
                Ok(res) => {
                    let body = res.body.collect().await.unwrap();
                    println!(
                        "Downloaded file {}: {} bytes in {} ms",
                        i,
                        body.into_bytes().len(),
                        req_start_time.elapsed().as_millis()
                    );
                    // Store the latency
                    let mut latencies = latencies.lock().await;
                    latencies.push(req_start_time.elapsed().as_millis());
                }
                Err(e) => eprintln!("Error downloading file: {}", e),
            }
        }));
    }
    // await for all the handles to finish
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Error joining thread: {}", e);
        }
    }
    println!(
        "Took {} seconds to download 64 files",
        start_time.elapsed().as_secs_f32()
    );
    let sorted_latencies = {
        let latency_guard = latencies.lock().await;
        let mut sorted_latencies = latency_guard.clone();
        sorted_latencies.sort();
        sorted_latencies
    };
    let p50 = sorted_latencies[sorted_latencies.len() / 2];
    let p90 = sorted_latencies[(sorted_latencies.len() * 9) / 10];
    let p95 = sorted_latencies[(sorted_latencies.len() * 95) / 100];
    let p99 = sorted_latencies[(sorted_latencies.len() * 99) / 100];
    println!("P50: {} ms", p50);
    println!("P90: {} ms", p90);
    println!("P95: {} ms", p95);
    println!("P99: {} ms", p99);
    println!(
        "Average: {} ms",
        sorted_latencies.iter().sum::<u128>() / sorted_latencies.len() as u128
    );
}
