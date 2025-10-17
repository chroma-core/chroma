use chroma_types::chroma_proto;
use clap::{Parser, Subcommand};
use prost_types::value::Kind;
use tonic::transport::Channel;

#[derive(Parser)]
#[command(name = "chroma-sysdb")]
#[command(about = "CLI client for Chroma coordinator task management", long_about = None)]
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
    #[command(about = "Create a new task")]
    CreateTask {
        #[arg(long, help = "Name of the task")]
        name: String,
        #[arg(long, help = "Name of the operator to apply")]
        operator_name: String,
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
            help = "Minimum number of records required before task execution"
        )]
        min_records_for_task: u64,
    },
    #[command(about = "Get task by name")]
    GetTask {
        #[arg(long, help = "ID of the input collection")]
        input_collection_id: String,
        #[arg(long, help = "Name of the task to retrieve")]
        task_name: String,
    },
    #[command(about = "Delete a task")]
    DeleteTask {
        #[arg(long, help = "ID of the input collection")]
        input_collection_id: String,
        #[arg(long, help = "Name of the task to delete")]
        task_name: String,
        #[arg(long, help = "Whether to delete the output collection")]
        delete_output: bool,
    },
    #[command(about = "Mark a task run as ready to advance")]
    AdvanceTask {
        #[arg(long, help = "ID of the collection")]
        collection_id: String,
        #[arg(long, help = "ID of the task")]
        task_id: String,
        #[arg(long, help = "Nonce identifying the specific task run")]
        task_run_nonce: String,
    },
    #[command(about = "Get all operators")]
    GetOperators,
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
        Command::CreateTask {
            name,
            operator_name,
            input_collection_id,
            output_collection_name,
            params,
            tenant_id,
            database,
            min_records_for_task,
        } => {
            let params_json: serde_json::Value = serde_json::from_str(&params)?;
            let params_value = json_to_prost_value(params_json);
            let params_struct = match params_value.kind {
                Some(Kind::StructValue(s)) => Some(s),
                _ => {
                    return Err("params must be a JSON object".into());
                }
            };

            let request = chroma_proto::CreateTaskRequest {
                name,
                operator_name,
                input_collection_id,
                output_collection_name,
                params: params_struct,
                tenant_id,
                database,
                min_records_for_task,
            };

            let response = client.create_task(request).await?;
            println!("Task created: {}", response.into_inner().task_id);
        }
        Command::GetTask {
            input_collection_id,
            task_name,
        } => {
            let request = chroma_proto::GetTaskByNameRequest {
                input_collection_id,
                task_name,
            };

            let response = client.get_task_by_name(request).await?;
            let task = response.into_inner();

            println!("Task ID: {:?}", task.task_id);
            println!("Name: {:?}", task.name);
            println!("Operator: {:?}", task.operator_name);
            println!("Input Collection: {:?}", task.input_collection_id);
            println!("Output Collection Name: {:?}", task.output_collection_name);
            println!("Output Collection ID: {:?}", task.output_collection_id);
            println!("Params: {:?}", task.params);
            println!("Completion Offset: {:?}", task.completion_offset);
            println!("Min Records: {:?}", task.min_records_for_task);
        }
        Command::DeleteTask {
            input_collection_id,
            task_name,
            delete_output,
        } => {
            let request = chroma_proto::DeleteTaskRequest {
                input_collection_id,
                task_name,
                delete_output,
            };

            let response = client.delete_task(request).await?;
            println!("Task deleted: {}", response.into_inner().success);
        }
        Command::AdvanceTask {
            collection_id,
            task_id,
            task_run_nonce,
        } => {
            let request = chroma_proto::AdvanceTaskRequest {
                collection_id: Some(collection_id),
                task_id: Some(task_id),
                task_run_nonce: Some(task_run_nonce),
            };

            client.advance_task(request).await?;
            println!("Task marked as done");
        }
        Command::GetOperators => {
            let request = chroma_proto::GetOperatorsRequest {};

            let response = client.get_operators(request).await?;
            let operators = response.into_inner().operators;

            for op in operators {
                println!("  {} - {}", op.id, op.name);
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
                println!("  Task ID: {:?}", entry.task_id);
                println!("  Nonce: {:?}", entry.task_run_nonce);
                println!("  When: {:?}", entry.when_to_run);
                println!();
            }
        }
    }

    Ok(())
}
