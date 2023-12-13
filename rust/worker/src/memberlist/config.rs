use serde::Deserialize;

#[derive(Deserialize)]
/// The type of memberlist provider to use
/// # Options
/// - CustomResource: Use a custom resource to get the memberlist
pub(crate) enum MemberlistProviderType {
    CustomResource,
}

// #[derive(Deserialize)]
// /// The configuration for the memberlist provider.
// /// # Options
// /// - CustomResource: Use a custom resource to get the memberlist
#[derive(Deserialize)]
pub(crate) enum MemberlistProviderConfig {
    CustomResource(CustomResourceMemberlistProviderConfig),
}

// /// The configuration for the custom resource memberlist provider.
// /// # Fields
// /// - memberlist_name: The name of the custom resource to use for the memberlist.
#[derive(Deserialize)]
pub(crate) struct CustomResourceMemberlistProviderConfig {
    pub(crate) memberlist_name: String,
}
