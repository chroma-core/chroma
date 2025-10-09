use chroma_types::chroma_proto;
use clap::{Parser, Subcommand};
use tonic::transport::Channel;

#[derive(Parser)]
#[command(name = "chroma-sysdb")]
#[command(about = "CLI client for Chroma coordinator task management", long_about = None)]
struct Cli {
    #[arg(long, default_value = "http://localhost:50051")]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Create a new task")]
    CreateTask {
        #[arg(long)]
        name: String,
        #[arg(long)]
        operator_name: String,
        #[arg(long)]
        input_collection_id: String,
        #[arg(long)]
        output_collection_name: String,
        #[arg(long)]
        params: String,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "100")]
        min_records_for_task: u64,
    },
    #[command(about = "Get task by name")]
    GetTask {
        #[arg(long)]
        input_collection_id: String,
        #[arg(long)]
        task_name: String,
    },
    #[command(about = "Delete a task")]
    DeleteTask {
        #[arg(long)]
        input_collection_id: String,
        #[arg(long)]
        task_name: String,
        #[arg(long)]
        delete_output: bool,
    },
    #[command(about = "Mark a task run as complete")]
    DoneTask {
        #[arg(long)]
        collection_id: String,
        #[arg(long)]
        task_id: String,
        #[arg(long)]
        task_run_nonce: String,
    },
    #[command(about = "Get all operators")]
    GetOperators,
    #[command(about = "Peek schedule by collection IDs")]
    PeekSchedule {
        #[arg(long, value_delimiter = ',')]
        collection_ids: Vec<String>,
    },
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
            let request = chroma_proto::CreateTaskRequest {
                name,
                operator_name,
                input_collection_id,
                output_collection_name,
                params,
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
        Command::DoneTask {
            collection_id,
            task_id,
            task_run_nonce,
        } => {
            let request = chroma_proto::DoneTaskRequest {
                collection_id: Some(collection_id),
                task_id: Some(task_id),
                task_run_nonce: Some(task_run_nonce),
            };

            client.done_task(request).await?;
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
