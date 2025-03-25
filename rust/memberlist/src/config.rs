use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
/// The type of memberlist provider to use
/// # Options
/// - CustomResource: Use a custom resource to get the memberlist
pub(crate) enum MemberlistProviderType {
    #[default]
    CustomResource,
}

/// The configuration for the memberlist provider.
/// # Options
/// - CustomResource: Use a custom resource to get the memberlist
#[derive(Deserialize, Clone, Serialize, Debug)]
pub enum MemberlistProviderConfig {
    #[serde(alias = "custom_resource")]
    CustomResource(CustomResourceMemberlistProviderConfig),
}

impl Default for MemberlistProviderConfig {
    fn default() -> Self {
        MemberlistProviderConfig::CustomResource(CustomResourceMemberlistProviderConfig::default())
    }
}

/// The configuration for the custom resource memberlist provider.
/// # Fields
/// - kube_namespace: The namespace to use for the custom resource.
/// - memberlist_name: The name of the custom resource to use for the memberlist.
/// - queue_size: The size of the queue to use for the channel.
#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct CustomResourceMemberlistProviderConfig {
    #[serde(default = "CustomResourceMemberlistProviderConfig::default_kube_namespace")]
    pub kube_namespace: String,
    #[serde(default = "CustomResourceMemberlistProviderConfig::default_memberlist_name")]
    pub memberlist_name: String,
    #[serde(default = "CustomResourceMemberlistProviderConfig::default_queue_size")]
    pub queue_size: usize,
}

impl CustomResourceMemberlistProviderConfig {
    fn default_kube_namespace() -> String {
        "chroma".to_string()
    }

    fn default_memberlist_name() -> String {
        "service-memberlist".to_string()
    }

    fn default_queue_size() -> usize {
        100
    }
}

impl Default for CustomResourceMemberlistProviderConfig {
    fn default() -> Self {
        CustomResourceMemberlistProviderConfig {
            kube_namespace: CustomResourceMemberlistProviderConfig::default_kube_namespace(),
            memberlist_name: CustomResourceMemberlistProviderConfig::default_memberlist_name(),
            queue_size: CustomResourceMemberlistProviderConfig::default_queue_size(),
        }
    }
}
