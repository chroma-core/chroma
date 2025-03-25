use serde::{Deserialize, Serialize};

#[derive(Default, Deserialize, Clone, Serialize, Debug)]
/// The type of hasher to use.
/// # Options
/// - Murmur3: The murmur3 hasher.
pub enum HasherType {
    #[default]
    Murmur3,
}

#[derive(Deserialize, Clone, Serialize, Debug)]
/// The configuration for the assignment policy.
/// # Options
/// - RendezvousHashing: The rendezvous hashing assignment policy.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub enum AssignmentPolicyConfig {
    #[serde(alias = "rendezvous_hashing")]
    RendezvousHashing(RendezvousHashingAssignmentPolicyConfig),
}

impl Default for AssignmentPolicyConfig {
    fn default() -> Self {
        AssignmentPolicyConfig::RendezvousHashing(RendezvousHashingAssignmentPolicyConfig::default())
    }
}

#[derive(Default, Deserialize, Clone, Serialize, Debug)]
/// The configuration for the rendezvous hashing assignment policy.
/// # Fields
/// - hasher: The type of hasher to use.
pub struct RendezvousHashingAssignmentPolicyConfig {
    pub hasher: HasherType,
}
