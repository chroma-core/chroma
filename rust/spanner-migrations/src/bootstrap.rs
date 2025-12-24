//! Bootstrap functionality for Spanner emulator.

use chroma_config::spanner::SpannerEmulatorConfig;
use google_cloud_gax::conn::Environment;
use google_cloud_googleapis::spanner::admin::database::v1::CreateDatabaseRequest;
use google_cloud_googleapis::spanner::admin::instance::v1::{CreateInstanceRequest, Instance};
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::admin::AdminClientConfig;
use tonic::Code;

/// Bootstrap the emulator by creating instance and database via gRPC.
pub async fn bootstrap_emulator(
    emulator: &SpannerEmulatorConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(
        "Bootstrapping Spanner emulator at {}",
        emulator.grpc_endpoint()
    );

    // Configure client to connect to emulator
    let admin_client_config = AdminClientConfig {
        environment: Environment::Emulator(emulator.grpc_endpoint().to_string()),
    };

    let admin_client = AdminClient::new(admin_client_config).await?;

    // Create instance
    let project_path = format!("projects/{}", emulator.project);
    tracing::info!(
        "Creating instance {} in {}",
        emulator.instance,
        project_path
    );

    let instance_client = admin_client.instance();

    let instance = Instance {
        name: format!(
            "projects/{}/instances/{}",
            emulator.project, emulator.instance
        ),
        config: format!(
            "projects/{}/instanceConfigs/emulator-config",
            emulator.project
        ),
        display_name: emulator.instance.to_string(),
        node_count: 1,
        ..Default::default()
    };

    let create_instance_request = CreateInstanceRequest {
        parent: project_path.clone(),
        instance_id: emulator.instance.to_string(),
        instance: Some(instance),
    };

    // Try to create instance, handle already exists error
    match instance_client
        .create_instance(create_instance_request, None)
        .await
    {
        Ok(_) => {
            tracing::info!("Created instance: {}", emulator.instance);
        }
        Err(e) if e.code() == Code::AlreadyExists => {
            tracing::info!("Instance {} already exists", emulator.instance);
        }
        Err(e) => {
            return Err(format!("Failed to create instance: {}", e).into());
        }
    }

    // Create database
    let instance_path = format!(
        "projects/{}/instances/{}",
        emulator.project, emulator.instance
    );
    tracing::info!(
        "Creating database {} in {}",
        emulator.database,
        instance_path
    );

    let database_client = admin_client.database();

    let create_database_request = CreateDatabaseRequest {
        parent: instance_path,
        create_statement: format!("CREATE DATABASE `{}`", emulator.database),
        ..Default::default()
    };

    // Try to create database, handle already exists error
    match database_client
        .create_database(create_database_request, None)
        .await
    {
        Ok(_) => {
            tracing::info!("Created database: {}", emulator.database);
        }
        Err(e) if e.code() == Code::AlreadyExists => {
            tracing::info!("Database {} already exists", emulator.database);
        }
        Err(e) => {
            return Err(format!("Failed to create database: {}", e).into());
        }
    }

    tracing::info!("Spanner emulator bootstrap complete");
    Ok(())
}
