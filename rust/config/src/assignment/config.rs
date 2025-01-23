use serde::Deserialize;

#[derive(Deserialize)]
/// The type of hasher to use.
/// # Options
/// - Murmur3: The murmur3 hasher.
pub(crate) enum HasherType {
    Murmur3,
}

#[derive(Deserialize)]
/// The configuration for the assignment policy.
/// # Options
/// - RendezvousHashing: The rendezvous hashing assignment policy.
/// # Notes
/// See config.rs in the root of the worker crate for an example of how to use
/// config files to configure the worker.
pub enum AssignmentPolicyConfig {
    RendezvousHashing(RendezvousHashingAssignmentPolicyConfig),
}

#[derive(Deserialize)]
/// The configuration for the rendezvous hashing assignment policy.
/// # Fields
/// - hasher: The type of hasher to use.
pub struct RendezvousHashingAssignmentPolicyConfig {
    pub(crate) hasher: HasherType,
}
