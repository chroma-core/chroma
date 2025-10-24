mod compactor;
mod server;
mod utils;

use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_memberlist::memberlist_provider::{
    CustomResourceMemberlistProvider, MemberlistProvider,
};
use clap::Parser;
use compactor::compaction_client::CompactionClient;
use compactor::compaction_server::CompactionServer;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};

// Required for benchmark
pub mod config;
pub mod execution;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn query_service_entrypoint() {
    // Check if the config path is set in the env var
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => {
            eprintln!("loading from {config_path}");
            config::RootConfig::load_from_path(&config_path)
        }
        Err(err) => {
            eprintln!("loading from default path because {err}");
            config::RootConfig::load()
        }
    };

    let config = config.query_service;
    let registry = Registry::new();

    chroma_tracing::init_otel_tracing(
        &config.service_name,
        &config.otel_filters,
        &config.otel_endpoint,
    );

    let system = chroma_system::System::new();
    let dispatcher =
        match chroma_system::Dispatcher::try_from_config(&config.dispatcher, &registry).await {
            Ok(dispatcher) => dispatcher,
            Err(err) => {
                println!("Failed to create dispatcher component: {:?}", err);
                return;
            }
        };
    let mut dispatcher_handle = system.start_component(dispatcher);
    let mut worker_server =
        match server::WorkerServer::try_from_config(&(config, system.clone()), &registry).await {
            Ok(worker_server) => worker_server,
            Err(err) => {
                println!("Failed to create worker server component: {:?}", err);
                return;
            }
        };
    worker_server.set_dispatcher(dispatcher_handle.clone());

    // Server task will run until it receives a shutdown signal
    let _ = tokio::spawn(async move {
        let _ = crate::server::WorkerServer::run(worker_server).await;
    })
    .await;

    println!("Shutting down the query service...");
    dispatcher_handle.stop();
    let _ = dispatcher_handle.join().await;
    system.stop().await;
    system.join().await;
}

pub async fn compaction_service_entrypoint() {
    // Check if the config path is set in the env var
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => {
            eprintln!("loading from {config_path}");
            config::RootConfig::load_from_path(&config_path)
        }
        Err(err) => {
            eprintln!("loading from default path because {err}");
            config::RootConfig::load()
        }
    };

    let config = config.compaction_service;
    let registry = Registry::new();

    chroma_tracing::init_otel_tracing(
        &config.service_name,
        &config.otel_filters,
        &config.otel_endpoint,
    );

    let system = chroma_system::System::new();

    let mut memberlist = match CustomResourceMemberlistProvider::try_from_config(
        &config.memberlist_provider,
        &registry,
    )
    .await
    {
        Ok(memberlist) => memberlist,
        Err(err) => {
            println!("Failed to create memberlist component: {:?}", err);
            return;
        }
    };

    let dispatcher =
        match chroma_system::Dispatcher::try_from_config(&config.dispatcher, &registry).await {
            Ok(dispatcher) => dispatcher,
            Err(err) => {
                println!("Failed to create dispatcher component: {:?}", err);
                return;
            }
        };
    let mut dispatcher_handle = system.start_component(dispatcher);
    let mut compaction_manager = match crate::compactor::CompactionManager::try_from_config(
        &(config.clone(), system.clone()),
        &registry,
    )
    .await
    {
        Ok(compaction_manager) => compaction_manager,
        Err(err) => {
            println!("Failed to create compaction manager component: {:?}", err);
            return;
        }
    };
    compaction_manager.set_dispatcher(dispatcher_handle.clone());

    let mut compaction_manager_handle = system.start_component(compaction_manager);
    memberlist.subscribe(compaction_manager_handle.receiver());

    // Create taskrunner manager if config is present and enabled (runtime config)
    let taskrunner_manager_handle = if let Some(task_config) = &config.task_runner {
        if !task_config.enabled {
            None
        } else {
            match crate::compactor::attach_functionrunner_manager(
                &config,
                task_config,
                system.clone(),
                dispatcher_handle.clone(),
                &registry,
            )
            .await
            {
                Ok(mut task_manager) => {
                    println!("Taskrunner manager created");
                    task_manager.set_dispatcher(dispatcher_handle.clone());
                    let task_handle = system.start_component(task_manager);
                    memberlist.subscribe(task_handle.receiver());
                    Some(task_handle)
                }
                Err(err) => {
                    println!("Failed to create taskrunner manager: {:?}", err);
                    None
                }
            }
        }
    } else {
        None
    };

    let mut memberlist_handle = system.start_component(memberlist);

    let compaction_server = CompactionServer {
        manager: compaction_manager_handle.clone(),
        port: config.my_port,
        jemalloc_pprof_server_port: config.jemalloc_pprof_server_port,
    };

    let server_join_handle = tokio::spawn(async move {
        let _ = compaction_server.run().await;
    });

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(sigterm) => sigterm,
        Err(e) => {
            println!("Failed to create signal handler: {:?}", e);
            return;
        }
    };
    println!("Waiting for SIGTERM to stop the server");
    select! {
        // Kubernetes will send SIGTERM to stop the pod gracefully
        // TODO: add more signal handling
        _ = sigterm.recv() => {
            memberlist_handle.stop();
            let _ = memberlist_handle.join().await;
            dispatcher_handle.stop();
            let _ = dispatcher_handle.join().await;
            compaction_manager_handle.stop();
            let _ = compaction_manager_handle.join().await;
            if let Some(mut handle) = taskrunner_manager_handle {
                handle.stop();
                let _ = handle.join().await;
            }
            system.stop().await;
            system.join().await;
            let _ = server_join_handle.await;
        },
    };
    println!("Server stopped");
}

pub async fn compaction_client_entrypoint() {
    let client = CompactionClient::parse();
    if let Err(e) = client.run().await {
        eprintln!("{e}");
    }
}
