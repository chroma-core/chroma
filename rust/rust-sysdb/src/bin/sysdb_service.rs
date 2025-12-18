use rust_sysdb::sysdb_service_entrypoint;

#[tokio::main]
async fn main() {
    sysdb_service_entrypoint().await;
}
