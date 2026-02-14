use rust_sysdb::sysdb_service_entrypoint;

#[tokio::main]
async fn main() {
    Box::pin(sysdb_service_entrypoint()).await;
}
