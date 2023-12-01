use worker::memberlist_provider::{
    CustomResourceMemberlistProvider, Memberlist, MemberlistProvider,
};

// TODO: move test to not be in integration tests so that we don't have to make everything public

#[tokio::test]
async fn it_can_work() {
    let (tx, mut rx) = tokio::sync::broadcast::channel(10); // TODO: what happens if capacity is exceeded?
    let provider: &dyn MemberlistProvider =
        &CustomResourceMemberlistProvider::new("worker-memberlist", tx).await;
    let list = provider.get_memberlist().await;
    println!("list: {:?}", list);

    provider.start();

    // sleep to allow time for the watcher to get the initial state
    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

    let res = rx.recv().await.unwrap();
    println!("GOT FROM CHANNEL: {:?}", res);

    provider.stop();

    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
}
