use worker::memberlist_provider::{CustomResourceMemberlistProvider, MemberlistProvider};

#[tokio::test]
async fn it_can_work() {
    let provider: &dyn MemberlistProvider =
        &CustomResourceMemberlistProvider::new("worker-memberlist").await;
    let list = provider.get_memberlist().await;
    println!("list: {:?}", list);

    provider.start();

    // sleep to allow time for the watcher to get the initial state
    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

    provider.stop();

    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
}
