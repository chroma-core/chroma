use chroma_types::chroma_proto;
use clap::{Parser, Subcommand};
use prost_types::value::Kind;
use tonic::transport::Channel;

#[derive(Parser)]
#[command(name = "chroma-function-manager")]
#[command(about = "CLI client for Chroma coordinator attached function management", long_about = None)]
struct Cli {
    #[arg(
        long,
        default_value = "http://localhost:50051",
        help = "Address of the Chroma coordinator service"
    )]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Attach a function to a collection")]
    AttachFunction {
        #[arg(long, help = "Name for this attached function")]
        name: String,
        #[arg(long, help = "ID of the function to attach")]
        function_id: String,
        #[arg(long, help = "ID of the input collection")]
        input_collection_id: String,
        #[arg(long, help = "Name for the output collection")]
        output_collection_name: String,
        #[arg(long, help = "JSON object containing operator parameters")]
        params: String,
        #[arg(long, help = "Tenant ID")]
        tenant_id: String,
        #[arg(long, help = "Database name")]
        database: String,
        #[arg(
            long,
            default_value = "100",
            help = "Minimum number of records required before attached function execution"
        )]
        min_records_for_invocation: u64,
    },
    #[command(about = "Get attached function by name")]
    GetAttachedFunction {
        #[arg(long, help = "ID of the input collection")]
        input_collection_id: String,
        #[arg(long, help = "Name of the attached function to retrieve")]
        name: String,
    },
    #[command(about = "Detach a function")]
    DetachFunction {
        #[arg(long, help = "ID of the attached function to delete")]
        attached_function_id: String,
        #[arg(long, help = "Whether to delete the output collection")]
        delete_output: bool,
    },
    #[command(about = "Mark an attached function run as ready to advance")]
    AdvanceAttachedFunction {
        #[arg(long, help = "ID of the collection")]
        collection_id: String,
        #[arg(long, help = "ID of the attached function")]
        attached_function_id: String,
        #[arg(long, help = "Nonce identifying the specific function run")]
        run_nonce: String,
        #[arg(long, help = "Completion offset")]
        completion_offset: u64,
        #[arg(long, help = "Next run delay in seconds")]
        next_run_delay_secs: u64,
    },
    #[command(about = "Get all available functions")]
    GetFunctions,
    #[command(about = "Peek schedule by collection IDs")]
    PeekSchedule {
        #[arg(
            long,
            value_delimiter = ',',
            help = "Comma-separated list of collection IDs"
        )]
        collection_ids: Vec<String>,
    },
}

fn json_to_prost_value(json: serde_json::Value) -> prost_types::Value {
    let kind = match json {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(b),
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Kind::NumberValue(f)
            } else {
                Kind::NullValue(0)
            }
        }
        serde_json::Value::String(s) => Kind::StringValue(s),
        serde_json::Value::Array(arr) => Kind::ListValue(prost_types::ListValue {
            values: arr.into_iter().map(json_to_prost_value).collect(),
        }),
        serde_json::Value::Object(map) => Kind::StructValue(prost_types::Struct {
            fields: map
                .into_iter()
                .map(|(k, v)| (k, json_to_prost_value(v)))
                .collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let channel = Channel::from_shared(cli.addr.clone())?.connect().await?;

    let mut client = chroma_proto::sys_db_client::SysDbClient::new(channel);

    match cli.command {
        Command::AttachFunction {
            name,
            function_id,
            input_collection_id,
            output_collection_name,
            params,
            tenant_id,
            database,
            min_records_for_invocation,
        } => {
            let params_json: serde_json::Value = serde_json::from_str(&params)?;
            let params_value = json_to_prost_value(params_json);
            let params_struct = match params_value.kind {
                Some(Kind::StructValue(s)) => Some(s),
                _ => {
                    return Err("params must be a JSON object".into());
                }
            };

            let request = chroma_proto::AttachFunctionRequest {
                name,
                function_name: function_id,
                input_collection_id,
                output_collection_name,
                params: params_struct,
                tenant_id,
                database,
                min_records_for_invocation,
            };

            let response = client.attach_function(request).await?;
            println!("Attached Function created: {}", response.into_inner().id);
        }
        Command::GetAttachedFunction {
            input_collection_id,
            name,
        } => {
            let request = chroma_proto::GetAttachedFunctionByNameRequest {
                input_collection_id,
                name,
            };

            let response = client.get_attached_function_by_name(request).await?;
            let attached_function = response.into_inner().attached_function.unwrap();

            println!("Attached Function ID: {:?}", attached_function.id);
            println!("Name: {:?}", attached_function.name);
            println!("Function: {:?}", attached_function.function_name);
            println!(
                "Input Collection: {:?}",
                attached_function.input_collection_id
            );
            println!(
                "Output Collection Name: {:?}",
                attached_function.output_collection_name
            );
            println!(
                "Output Collection ID: {:?}",
                attached_function.output_collection_id
            );
            println!("Params: {:?}", attached_function.params);
            println!(
                "Completion Offset: {:?}",
                attached_function.completion_offset
            );
            println!(
                "Min Records: {:?}",
                attached_function.min_records_for_invocation
            );
        }
        Command::DetachFunction {
            attached_function_id,
            delete_output,
        } => {
            let request = chroma_proto::DetachFunctionRequest {
                attached_function_id,
                delete_output,
            };

            let _response = client.detach_function(request).await?;
            println!("Attached Function deleted successfully");
        }
        Command::AdvanceAttachedFunction {
            collection_id,
            attached_function_id,
            run_nonce,
            completion_offset,
            next_run_delay_secs,
        } => {
            let request = chroma_proto::AdvanceAttachedFunctionRequest {
                collection_id: Some(collection_id),
                id: Some(attached_function_id),
                run_nonce: Some(run_nonce),
                completion_offset: Some(completion_offset),
                next_run_delay_secs: Some(next_run_delay_secs),
            };

            client.advance_attached_function(request).await?;
            println!("Attached Function advanced successfully");
        }
        Command::GetFunctions => {
            let request = chroma_proto::GetFunctionsRequest {};

            let response = client.get_functions(request).await?;
            let functions = response.into_inner().functions;

            for func in functions {
                println!("  {} - {}", func.id, func.name);
            }
        }
        Command::PeekSchedule { collection_ids } => {
            let request = chroma_proto::PeekScheduleByCollectionIdRequest {
                collection_id: collection_ids,
            };

            let response = client.peek_schedule_by_collection_id(request).await?;
            let entries = response.into_inner().schedule;

            println!("Schedule:");
            for entry in entries {
                println!("  Collection: {:?}", entry.collection_id);
                println!("  Attached Function ID: {:?}", entry.attached_function_id);
                println!("  Nonce: {:?}", entry.run_nonce);
                println!("  When: {:?}", entry.when_to_run);
                println!();
            }
        }
    }

    Ok(())
}
