use serde::Deserialize;

#[derive(Deserialize)]
/// The type of memberlist provider to use
/// # Options
/// - CustomResource: Use a custom resource to get the memberlist
pub(crate) enum MemberlistProviderType {
    CustomResource,
}

/// The configuration for the memberlist provider.
/// # Options
/// - CustomResource: Use a custom resource to get the memberlist
#[derive(Deserialize)]
pub(crate) enum MemberlistProviderConfig {
    CustomResource(CustomResourceMemberlistProviderConfig),
}

/// The configuration for the custom resource memberlist provider.
/// # Fields
/// - kube_namespace: The namespace to use for the custom resource.
/// - memberlist_name: The name of the custom resource to use for the memberlist.
/// - queue_size: The size of the queue to use for the channel.
#[derive(Deserialize)]
pub(crate) struct CustomResourceMemberlistProviderConfig {
    pub(crate) kube_namespace: String,
    pub(crate) memberlist_name: String,
    pub(crate) queue_size: usize,
}
