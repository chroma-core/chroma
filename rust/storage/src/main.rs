use aws_config::{retry::RetryConfig, timeout::TimeoutConfigBuilder};
use chroma_config::{registry::Registry, Configurable};
use chroma_storage::{
    admissioncontrolleds3::AdmissionControlledS3Storage,
    config::{
        AdmissionControlledS3StorageConfig, CountBasedPolicyConfig, RateLimitingConfig,
        S3CredentialsConfig, S3StorageConfig, StorageConfig,
    },
    GetOptions, Storage,
};
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
enum S3Client {
    RawS3Client(aws_sdk_s3::Client),
    StorageClient(Storage),
}

async fn download_file_raw_s3_client(
    client: &S3Client,
    object_key: &str,
    latencies: Arc<tokio::sync::Mutex<Vec<u128>>>,
) {
    let req_start_time = std::time::Instant::now();
    match client {
        S3Client::RawS3Client(client) => {
            let result = client
                .get_object()
                .bucket("chroma-serverless-staging")
                .key(object_key)
                .send()
                .await;
            match result {
                Ok(res) => {
                    let body = res.body.collect().await.unwrap();
                }
                Err(e) => eprintln!("Error downloading file: {}", e),
            }
        }
        S3Client::StorageClient(client) => {
            let result = client.get(object_key, GetOptions::default()).await;
            match result {
                Ok(res) => {}
                Err(e) => eprintln!("Error downloading file: {}", e),
            }
        }
    }
    latencies
        .lock()
        .await
        .push(req_start_time.elapsed().as_millis());
}

#[tokio::main]
async fn main() {
    // parse the command line arguments
    // n <number of files>
    // size <size of each file in MB>
    // c <whether to use the raw s3 client = 0 or the s3 client = 1>
    let args: Vec<String> = std::env::args().collect();
    println!("Args: {:?}", args);
    let num_files = if args.len() > 1 {
        args[2].parse::<usize>().unwrap_or(64)
    } else {
        64
    };
    let mb_size = if args.len() > 2 {
        args[3].parse::<usize>().unwrap_or(8)
    } else {
        8
    };
    let use_raw_client = if args.len() > 3 {
        args[4].parse::<usize>().unwrap_or(0) == 1
    } else {
        0 == 1
    };

    println!(
        "Running benchmark with {} files of size {} MB each using {} client",
        num_files,
        mb_size,
        if use_raw_client {
            "Raw S3"
        } else {
            "Chroma Storage"
        }
    );

    // connect
    let client;
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
    let raw_client = aws_sdk_s3::Client::new(&config);
    if use_raw_client {
        println!("Using raw S3 client");
        client = S3Client::RawS3Client(raw_client.clone());
    } else {
        println!("Using S3 client");
        let storage_config =
            StorageConfig::AdmissionControlledS3(AdmissionControlledS3StorageConfig {
                s3_config: S3StorageConfig {
                    bucket: "chroma-serverless-staging".to_string(),
                    credentials: S3CredentialsConfig::AWS,
                    connect_timeout_ms: 5000,
                    request_timeout_ms: 60000,
                    upload_part_size_bytes: 8 * 1024 * 1024,
                    download_part_size_bytes: 8 * 1024 * 1024,
                },
                rate_limiting_policy: RateLimitingConfig::CountBasedPolicy(
                    CountBasedPolicyConfig {
                        max_concurrent_requests: 128,
                        bandwidth_allocation: vec![0.7, 0.3],
                    },
                ),
            });
        let registry = Registry::new();
        let storage = AdmissionControlledS3Storage::try_from_config(&storage_config, &registry)
            .await
            .expect("Failed to create storage client");
        client = S3Client::StorageClient(Storage::AdmissionControlledS3(storage));
    }

    let test_data = vec![0; mb_size * 1024 * 1024];
    let bucket_name = "chroma-serverless-staging";
    let object_prefix = "hammad_test_data";

    // Upload the files
    for i in 0..num_files {
        let test_data = test_data.clone();
        let object_key = format!("{}/{:02}.bin", object_prefix, i);
        // Upload the file
        let result = raw_client
            .put_object()
            .bucket(bucket_name)
            .key(&object_key)
            .body(test_data.into())
            .send()
            .await;
        match result {
            Ok(_) => {}
            Err(e) => eprintln!("Error uploading file: {}", e),
        }
    }

    // Download the file times concurrently
    let latencies = Arc::new(tokio::sync::Mutex::new(vec![]));
    let throughputs = Arc::new(tokio::sync::Mutex::new(vec![]));

    // run the experiment N times, warming the connections
    let runs = 10;
    for run_num in 0..runs {
        let run_start_time = std::time::Instant::now();
        let run_latencies = Arc::new(tokio::sync::Mutex::new(vec![]));
        let mut handles = vec![];
        println!("\n========== Run {} ==========", run_num);
        for i in 0..num_files {
            let run_latencies_move = run_latencies.clone();
            let client = client.clone();
            let object_key = format!("{}/{:02}.bin", object_prefix, i);
            handles.push(tokio::spawn(async move {
                download_file_raw_s3_client(&client, &object_key, run_latencies_move).await;
            }));
        }

        // await for all the handles to finish
        for handle in handles {
            if let Err(e) = handle.await {
                eprintln!("Error joining task: {}", e);
            }
        }
        println!(
            "Took {} ms to download {} files of size {} MB each. Total throughput: {} MB/s",
            run_start_time.elapsed().as_millis(),
            num_files,
            mb_size,
            (mb_size * num_files) as f64 / (run_start_time.elapsed().as_secs_f64())
        );
        throughputs
            .lock()
            .await
            .push((mb_size * num_files) as f64 / (run_start_time.elapsed().as_secs_f64()));

        // latencies
        let latencies_guard = run_latencies.lock().await;
        let mut sorted_latencies = latencies_guard.clone();
        sorted_latencies.sort();
        let p50 = sorted_latencies[sorted_latencies.len() / 2];
        let p90 = sorted_latencies[(sorted_latencies.len() * 9) / 10];
        let p95 = sorted_latencies[(sorted_latencies.len() * 95) / 100];
        let p99 = sorted_latencies[(sorted_latencies.len() * 99) / 100];
        println!("========== Latency =========");
        println!("P50: {} ms", p50);
        println!("P90: {} ms", p90);
        println!("P95: {} ms", p95);
        println!("P99: {} ms", p99);
        println!(
            "Average: {} ms",
            sorted_latencies.iter().sum::<u128>() / sorted_latencies.len() as u128
        );

        // compute std dev
        let mean = sorted_latencies.iter().sum::<u128>() / sorted_latencies.len() as u128;
        let variance = sorted_latencies
            .iter()
            .map(|x| {
                let diff = *x as f64 - mean as f64;
                diff * diff
            })
            .sum::<f64>()
            / sorted_latencies.len() as f64;

        let std_dev = variance.sqrt();
        println!("Standard Deviation: {} ms", std_dev);

        // add the latencies to the global latencies
        let mut latencies_guard = latencies.lock().await;
        latencies_guard.append(&mut sorted_latencies);
    }

    println!("\n========== Experiment Complete ==========\n");

    // print the throughput
    let throughput_guard = throughputs.lock().await;
    let mut sorted_throughputs = throughput_guard.clone();
    sorted_throughputs.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let p50 = sorted_throughputs[sorted_throughputs.len() / 2];
    let p90 = sorted_throughputs[(sorted_throughputs.len() * 9) / 10];
    let p95 = sorted_throughputs[(sorted_throughputs.len() * 95) / 100];
    let p99 = sorted_throughputs[(sorted_throughputs.len() * 99) / 100];
    println!("========== Throughput =========");
    println!("P50: {} MB/s", p50);
    println!("P90: {} MB/s", p90);
    println!("P95: {} MB/s", p95);
    println!("P99: {} MB/s", p99);
    println!(
        "Average: {} MB/s",
        sorted_throughputs.iter().sum::<f64>() / sorted_throughputs.len() as f64
    );

    let sorted_latencies = {
        let latency_guard = latencies.lock().await;
        let mut sorted_latencies = latency_guard.clone();
        sorted_latencies.sort();
        sorted_latencies
    };
    println!("========== Latency =========");
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
