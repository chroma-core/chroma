pub async fn sysdb_service_entrypoint() {
    loop {
        println!("Hello, rust sysdb service!");
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}
