use super::{
    config::{AssignmentPolicyConfig, HasherType},
    rendezvous_hash::{AssignmentError, Hasher, Murmur3Hasher},
};
use crate::{registry::Registry, Configurable};
use async_trait::async_trait;
use chroma_error::ChromaError;
use std::fmt::Debug;

/*
===========================================
Interfaces
===========================================
*/

/// AssignmentPolicy is a trait that defines how to assign a key to a set of members.
/// # Notes
/// This trait mirrors the go and python versions of the assignment policy
/// interface.
/// # Methods
/// - assign: Assign a key to a member.
/// - get_members: Get the members that can be assigned to.
/// - set_members: Set the members that can be assigned to.
pub trait AssignmentPolicy: Send + Sync + AssignmentPolicyClone + Debug {
    fn assign_one(&self, key: &str) -> Result<String, AssignmentError>;
    fn assign(&self, key: &str, k: usize) -> Result<Vec<String>, AssignmentError>;
    fn get_members(&self) -> Vec<String>;
    fn set_members(&mut self, members: Vec<String>);
}

pub trait AssignmentPolicyClone {
    fn clone_box(&self) -> Box<dyn AssignmentPolicy>;
}

impl<T> AssignmentPolicyClone for T
where
    T: 'static + AssignmentPolicy + Clone,
{
    fn clone_box(&self) -> Box<dyn AssignmentPolicy> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn AssignmentPolicy> {
    fn clone(&self) -> Box<dyn AssignmentPolicy> {
        self.clone_box()
    }
}
/*
===========================================
Implementation
===========================================
*/

#[derive(Clone, Debug)]
pub struct RendezvousHashingAssignmentPolicy {
    hasher: Murmur3Hasher,
    members: Vec<String>,
}

impl Default for RendezvousHashingAssignmentPolicy {
    fn default() -> Self {
        Self {
            hasher: Murmur3Hasher {},
            members: vec![],
        }
    }
}

#[async_trait]
impl Configurable<AssignmentPolicyConfig> for RendezvousHashingAssignmentPolicy {
    async fn try_from_config(
        config: &AssignmentPolicyConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let AssignmentPolicyConfig::RendezvousHashing(assignment_policy_config) = config;
        let hasher = match assignment_policy_config.hasher {
            HasherType::Murmur3 => Murmur3Hasher {},
        };
        return Ok(RendezvousHashingAssignmentPolicy {
            hasher,
            members: vec![],
        });
    }
}

impl AssignmentPolicy for RendezvousHashingAssignmentPolicy {
    fn assign_one(&self, key: &str) -> Result<String, AssignmentError> {
        let members = self.get_members();
        self.hasher.assign_one(members, key)
    }

    fn assign(&self, key: &str, k: usize) -> Result<Vec<String>, AssignmentError> {
        let members = self.get_members();
        self.hasher.assign(members, key, k)
    }

    fn get_members(&self) -> Vec<String> {
        // This is not designed to be used frequently for now, nor is the number of members
        // expected to be large, so we can just clone the members
        self.members.clone()
    }

    fn set_members(&mut self, members: Vec<String>) {
        self.members = members;
    }
}
