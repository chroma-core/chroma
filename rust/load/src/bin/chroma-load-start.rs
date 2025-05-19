//! Start a workload on the chroma-load server.

use std::sync::Arc;

use clap::Parser;

use chroma_load::data_sets::{all_data_sets, References};
use chroma_load::rest::StartRequest;
use chroma_load::{humanize_expires, Connection, Throughput, Workload};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    host: String,
    #[arg(long)]
    name: String,
    #[arg(long)]
    expires: String,
    #[arg(long)]
    delay: Option<String>,
    #[arg(long)]
    workload: String,
    #[arg(long)]
    data_set: Option<String>,
    #[arg(long)]
    references_data_set: Option<String>,
    #[arg(long)]
    operates_on_data_set: Option<String>,
    #[arg(long)]
    operates_on_cardinality: Option<usize>,
    #[arg(long)]
    url: String,
    #[arg(long)]
    database: String,
    #[arg(long)]
    constant_throughput: Option<f64>,
    #[arg(long)]
    sinusoid_throughput: Option<String>,
    #[arg(long)]
    sawtooth_throughput: Option<String>,
}

impl Args {
    fn throughput(&self) -> chroma_load::Throughput {
        let mut count = 0;
        if self.constant_throughput.is_some() {
            count += 1;
        }
        if self.sinusoid_throughput.is_some() {
            count += 1;
        }
        if self.sawtooth_throughput.is_some() {
            count += 1;
        }
        if count > 1 {
            eprintln!("Cannot specify multiple throughput types");
            std::process::exit(1);
        }
        if let Some(throughput) = self.constant_throughput {
            Throughput::Constant(throughput)
        } else if let Some(throughput) = self.sinusoid_throughput.as_ref() {
            let mut parts = throughput.split(',');
            let min = parts.next().expect("sinusoidal throughput must have base");
            let min: f64 = min.parse().expect("base must be a floating point number");
            let max = parts.next().expect("sinusoidal throughput must have base");
            let max: f64 = max.parse().expect("base must be a floating point number");
            let periodicity = parts
                .next()
                .expect("sinusoidal throughput must have period");
            let periodicity: usize = periodicity.parse().expect("period must be an integer");
            Throughput::Sinusoidal {
                min,
                max,
                periodicity,
            }
        } else if let Some(throughput) = self.sawtooth_throughput.as_ref() {
            let mut parts = throughput.split(',');
            let min = parts.next().expect("sinusoidal throughput must have base");
            let min: f64 = min.parse().expect("base must be a floating point number");
            let max = parts.next().expect("sinusoidal throughput must have base");
            let max: f64 = max.parse().expect("base must be a floating point number");
            let periodicity = parts
                .next()
                .expect("sinusoidal throughput must have period");
            let periodicity: usize = periodicity.parse().expect("period must be an integer");
            Throughput::Sawtooth {
                min,
                max,
                periodicity,
            }
        } else {
            Throughput::Constant(std::f64::consts::PI)
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Read API key from environment variable.
    let api_key = std::env::var("CHROMA_API_KEY").ok();

    let client = reqwest::Client::new();
    let throughput = args.throughput();
    let mut workload = Workload::ByName(args.workload);
    if let Some(delay) = args.delay {
        let delay = humanize_expires(&delay).expect("delay must be humanizable");
        let delay = delay.parse().expect("delay must be a date time");
        workload = Workload::Delay {
            after: delay,
            wrap: Box::new(workload),
        };
    }
    let (data_set, custom_data_set) = match (
        args.data_set,
        args.references_data_set,
        args.operates_on_data_set,
        args.operates_on_cardinality,
    ) {
        (Some(data_set), None, None, None) => (Some(data_set.clone()), None),
        (None, Some(references), Some(operates_on), Some(cardinality)) => {
            let Some(referred_to) = all_data_sets()
                .iter()
                .find(|ds| ds.name() == references)
                .map(Arc::clone)
            else {
                eprintln!("Could not find a data set to reference named {references}");
                std::process::exit(13);
            };
            let to_start = References {
                references: referred_to.json(),
                operates_on,
                cardinality,
            };
            let to_start = match serde_json::to_value(&to_start) {
                Ok(to_start) => to_start,
                Err(err) => {
                    eprintln!("could not serialize referencing data set: {err:?}");
                    std::process::exit(13);
                }
            };
            (None, Some(to_start))
        }
        _ => {
            eprintln!("must provide data_set XOR (references_data_set, operates_on_data_set, operates_on_cardinality)");
            std::process::exit(13);
        }
    };
    let req = StartRequest {
        name: args.name,
        expires: humanize_expires(&args.expires).unwrap_or(args.expires),
        workload,
        data_set,
        custom_data_set,
        connection: Connection {
            url: args.url,
            api_key,
            database: args.database,
        },
        throughput,
    };
    match client
        .post(format!("{}/start", args.host))
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&req)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                let uuid = match resp.text().await {
                    Ok(uuid) => uuid,
                    Err(err) => {
                        eprintln!("Failed to start workload on {}: {}", args.host, err);
                        return;
                    }
                };
                println!(
                    "Started workload on {}:\n{}",
                    args.host,
                    // SAFETY(rescrv):  serde_json::to_string_pretty should always convert to JSON
                    // when it just parses as JSON.
                    uuid,
                );
            } else {
                eprintln!(
                    "Categorically failed to start workload on {}: {}",
                    args.host,
                    resp.status()
                );
                if let Ok(text) = resp.text().await {
                    eprintln!("{}", text.trim());
                }
            }
        }
        Err(e) => eprintln!("Failed to start workload on {}: {}", args.host, e),
    }
}
