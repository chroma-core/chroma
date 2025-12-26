//! This module defines the topology-related configuration for multi-cloud, multi-region
//! configuration.
//!
//! There are three core structs:
//! - A [`ProviderRegion`] names a given cloud provider and identifies one geographic region that
//!   they offer.  For example, AWS has us-east-1 and GCP has europe-west1.  By convention the name
//!   is `format!("{provider}-{region}")`.
//! - A [`Topology`] defines a virtual replication overlay that spans multiple provider regions.
//! - A [`MultiCloudMultiRegionConfiguration`] is a self-contained specification of a set of
//!   regions, a set of topologies that refer to those regions, and the name of the preferred
//!   region for operations that have region-affinity.
//!
//! This means the following invariants must be upheld within a
//! `MultiCloudMultiRegionConfiguration`:
//! - `ProviderRegion.name` must be unique within a MultiCloudMultiRegionConfiguration.
//! - `Topology.name` must be unique within a MultiCloudMultiRegionConfiguration.
//! - `Topology.regions` must be refer to a `ProviderRegion.name` within the
//!   MultiCloudMultiRegionConfiguration.
//! - `MultiCloudMultiRegionConfiguration.preferred` must refer to a `ProviderRegion.name` within
//!   the `MultiCloudMultiRegionConfiguration`.
//!
//! # Example
//!
//! ```
//! use chroma_types::{
//!     MultiCloudMultiRegionConfiguration, ProviderRegion, Topology, RegionName, TopologyName,
//! };
//!
//! let config = MultiCloudMultiRegionConfiguration::new(
//!     RegionName::new("aws-us-east-1"),
//!     vec![
//!         ProviderRegion::new(
//!             RegionName::new("aws-us-east-1"),
//!             "aws",
//!             "us-east-1",
//!             (),
//!         ),
//!         ProviderRegion::new(
//!             RegionName::new("gcp-europe-west1"),
//!             "gcp",
//!             "europe-west1",
//!             (),
//!         ),
//!     ],
//!     vec![Topology::new(
//!         TopologyName::new("global"),
//!         vec![
//!             RegionName::new("aws-us-east-1"),
//!             RegionName::new("gcp-europe-west1"),
//!         ],
//!     )],
//! );
//!
//! assert!(config.is_ok());
//! ```
//!
//! # Serde
//!
//! All types in this module support serialization and deserialization via serde.
//! [`MultiCloudMultiRegionConfiguration`] validates its invariants during deserialization,
//! so invalid configurations will fail to deserialize.
//!
//! ```
//! use chroma_types::MultiCloudMultiRegionConfiguration;
//!
//! let json = r#"{
//!     "preferred": "aws-us-east-1",
//!     "regions": [
//!         {"name": "aws-us-east-1", "provider": "aws", "region": "us-east-1", "config": null}
//!     ],
//!     "topologies": []
//! }"#;
//!
//! let config: MultiCloudMultiRegionConfiguration<()> = serde_json::from_str(json).unwrap();
//! assert_eq!(config.preferred().as_str(), "aws-us-east-1");
//! ```

use std::collections::HashSet;
use std::fmt::Debug;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// A strongly-typed region name.
///
/// This newtype wrapper ensures region names cannot be confused with other string types
/// like topology names.
///
/// # Example
///
/// ```
/// use chroma_types::RegionName;
///
/// let name = RegionName::new("aws-us-east-1");
/// assert_eq!(name.as_str(), "aws-us-east-1");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RegionName(String);

impl RegionName {
    /// Creates a new region name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns the region name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RegionName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A strongly-typed topology name.
///
/// This newtype wrapper ensures topology names cannot be confused with other string types
/// like region names.
///
/// # Example
///
/// ```
/// use chroma_types::TopologyName;
///
/// let name = TopologyName::new("global");
/// assert_eq!(name.as_str(), "global");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TopologyName(String);

impl TopologyName {
    /// Creates a new topology name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns the topology name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TopologyName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A cloud provider and geographic region.
///
/// # Example
///
/// ```
/// use chroma_types::{ProviderRegion, RegionName};
///
/// let region = ProviderRegion::new(
///     RegionName::new("aws-us-east-1"),
///     "aws",
///     "us-east-1",
///     (),
/// );
/// assert_eq!(region.name().as_str(), "aws-us-east-1");
/// assert_eq!(region.provider(), "aws");
/// assert_eq!(region.region(), "us-east-1");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Clone + Debug + Eq + PartialEq + Serialize",
    deserialize = "T: Clone + Debug + Eq + PartialEq + serde::de::DeserializeOwned"
))]
pub struct ProviderRegion<T: Clone + Debug + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>> {
    /// The unique name for this provider-region combination.
    name: RegionName,
    /// The cloud provider (e.g., "aws", "gcp").
    provider: String,
    /// The region within the provider (e.g., "us-east-1", "europe-west1").
    region: String,
    /// Additional per-region data.
    config: T,
}

impl<T: Clone + Debug + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>> ProviderRegion<T> {
    /// Creates a new provider region.
    ///
    /// # Example
    ///
    /// ```
    /// use chroma_types::{ProviderRegion, RegionName};
    ///
    /// let region = ProviderRegion::new(
    ///     RegionName::new("gcp-europe-west1"),
    ///     "gcp",
    ///     "europe-west1",
    ///     (),
    /// );
    /// ```
    pub fn new(
        name: RegionName,
        provider: impl Into<String>,
        region: impl Into<String>,
        config: T,
    ) -> Self {
        Self {
            name,
            provider: provider.into(),
            region: region.into(),
            config,
        }
    }

    /// Returns the unique name for this provider-region combination.
    pub fn name(&self) -> &RegionName {
        &self.name
    }

    /// Returns the cloud provider (e.g., "aws", "gcp").
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the region within the provider (e.g., "us-east-1", "europe-west1").
    pub fn region(&self) -> &str {
        &self.region
    }
}

/// A named replication topology spanning multiple provider regions.
///
/// # Example
///
/// ```
/// use chroma_types::{Topology, TopologyName, RegionName};
///
/// let topology = Topology::new(
///     TopologyName::new("us-multi-az"),
///     vec![
///         RegionName::new("aws-us-east-1"),
///         RegionName::new("aws-us-west-2"),
///     ],
/// );
/// assert_eq!(topology.name().as_str(), "us-multi-az");
/// assert_eq!(topology.regions().len(), 2);
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    /// The unique name for this topology.
    name: TopologyName,
    /// The names of provider regions included in this topology.
    regions: Vec<RegionName>,
}

impl Topology {
    /// Creates a new topology.
    ///
    /// # Example
    ///
    /// ```
    /// use chroma_types::{Topology, TopologyName, RegionName};
    ///
    /// let topology = Topology::new(
    ///     TopologyName::new("global"),
    ///     vec![RegionName::new("aws-us-east-1")],
    /// );
    /// ```
    pub fn new(name: TopologyName, regions: Vec<RegionName>) -> Self {
        Self { name, regions }
    }

    /// Returns the unique name for this topology.
    pub fn name(&self) -> &TopologyName {
        &self.name
    }

    /// Returns the names of provider regions included in this topology.
    pub fn regions(&self) -> &[RegionName] {
        &self.regions
    }
}

/// Configuration for multi-cloud, multi-region deployments.
///
/// This type validates its invariants both during construction via [`new`](Self::new)
/// and during deserialization. Invalid configurations will fail with a [`ValidationError`].
///
/// # Example
///
/// ```
/// use chroma_types::{
///     MultiCloudMultiRegionConfiguration, ProviderRegion, Topology,
///     RegionName, TopologyName,
/// };
///
/// let config = MultiCloudMultiRegionConfiguration::new(
///     RegionName::new("aws-us-east-1"),
///     vec![ProviderRegion::new(
///         RegionName::new("aws-us-east-1"),
///         "aws",
///         "us-east-1",
///         (),
///     )],
///     vec![],
/// ).expect("valid configuration");
///
/// assert_eq!(config.preferred().as_str(), "aws-us-east-1");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(into = "RawMultiCloudMultiRegionConfiguration<T>")]
pub struct MultiCloudMultiRegionConfiguration<
    T: Clone + Debug + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>,
> {
    /// The name of the preferred region for operations with region affinity.
    preferred: RegionName,
    /// The set of provider regions available in this configuration.
    regions: Vec<ProviderRegion<T>>,
    /// The set of topologies defined over the provider regions.
    topologies: Vec<Topology>,
}

/// Raw representation for serde deserialization before validation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Clone + Debug + Eq + PartialEq + Serialize",
    deserialize = "T: Clone + Debug + Eq + PartialEq + serde::de::DeserializeOwned"
))]
struct RawMultiCloudMultiRegionConfiguration<
    T: Clone + Debug + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>,
> {
    preferred: RegionName,
    regions: Vec<ProviderRegion<T>>,
    topologies: Vec<Topology>,
}

impl<T: Clone + Debug + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>>
    From<MultiCloudMultiRegionConfiguration<T>> for RawMultiCloudMultiRegionConfiguration<T>
{
    fn from(config: MultiCloudMultiRegionConfiguration<T>) -> Self {
        Self {
            preferred: config.preferred,
            regions: config.regions,
            topologies: config.topologies,
        }
    }
}

impl<T: Clone + Debug + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>>
    TryFrom<RawMultiCloudMultiRegionConfiguration<T>> for MultiCloudMultiRegionConfiguration<T>
{
    type Error = ValidationError;

    fn try_from(raw: RawMultiCloudMultiRegionConfiguration<T>) -> Result<Self, Self::Error> {
        MultiCloudMultiRegionConfiguration::new(raw.preferred, raw.regions, raw.topologies)
    }
}

impl<'de, T: Clone + Debug + Eq + PartialEq + Serialize + serde::de::DeserializeOwned>
    Deserialize<'de> for MultiCloudMultiRegionConfiguration<T>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawMultiCloudMultiRegionConfiguration::<T>::deserialize(deserializer)?;
        MultiCloudMultiRegionConfiguration::try_from(raw).map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur when validating a [`MultiCloudMultiRegionConfiguration`].
#[derive(Clone, Debug, Default, Eq, Error, PartialEq)]
#[error("{}", self.format_message())]
pub struct ValidationError {
    /// Provider region names that appear more than once.
    pub duplicate_region_names: Vec<RegionName>,
    /// Topology names that appear more than once.
    pub duplicate_topology_names: Vec<TopologyName>,
    /// Region names referenced by topologies that do not exist in the configuration.
    pub unknown_topology_regions: Vec<RegionName>,
    /// Set to true if the preferred region does not exist in the configuration.
    pub unknown_preferred_region: bool,
}

impl ValidationError {
    /// Returns true if no validation errors were found.
    pub fn is_ok(&self) -> bool {
        self.duplicate_region_names.is_empty()
            && self.duplicate_topology_names.is_empty()
            && self.unknown_topology_regions.is_empty()
            && !self.unknown_preferred_region
    }

    fn format_message(&self) -> String {
        if self.is_ok() {
            return "no validation errors".to_string();
        }

        let mut parts = Vec::new();

        if !self.duplicate_region_names.is_empty() {
            let names: Vec<&str> = self
                .duplicate_region_names
                .iter()
                .map(|n| n.as_str())
                .collect();
            parts.push(format!("duplicate region names: {}", names.join(", ")));
        }

        if !self.duplicate_topology_names.is_empty() {
            let names: Vec<&str> = self
                .duplicate_topology_names
                .iter()
                .map(|n| n.as_str())
                .collect();
            parts.push(format!("duplicate topology names: {}", names.join(", ")));
        }

        if !self.unknown_topology_regions.is_empty() {
            let names: Vec<&str> = self
                .unknown_topology_regions
                .iter()
                .map(|n| n.as_str())
                .collect();
            parts.push(format!("unknown topology regions: {}", names.join(", ")));
        }

        if self.unknown_preferred_region {
            parts.push("unknown preferred region".to_string());
        }

        parts.join("; ")
    }
}

impl<T: Clone + Debug + Eq + PartialEq + Serialize + for<'a> Deserialize<'a>>
    MultiCloudMultiRegionConfiguration<T>
{
    /// Creates and validates a new multi-cloud, multi-region configuration.
    ///
    /// Returns an error if validation fails.
    ///
    /// # Example
    ///
    /// ```
    /// use chroma_types::{
    ///     MultiCloudMultiRegionConfiguration, ProviderRegion, Topology,
    ///     RegionName, TopologyName,
    /// };
    ///
    /// // Valid configuration
    /// let config = MultiCloudMultiRegionConfiguration::new(
    ///     RegionName::new("aws-us-east-1"),
    ///     vec![ProviderRegion::new(
    ///         RegionName::new("aws-us-east-1"),
    ///         "aws",
    ///         "us-east-1",
    ///         (),
    ///     )],
    ///     vec![Topology::new(
    ///         TopologyName::new("default"),
    ///         vec![RegionName::new("aws-us-east-1")],
    ///     )],
    /// );
    /// assert!(config.is_ok());
    ///
    /// // Invalid configuration - preferred region doesn't exist
    /// let config = MultiCloudMultiRegionConfiguration::<()>::new(
    ///     RegionName::new("nonexistent"),
    ///     vec![ProviderRegion::new(
    ///         RegionName::new("aws-us-east-1"),
    ///         "aws",
    ///         "us-east-1",
    ///         (),
    ///     )],
    ///     vec![],
    /// );
    /// assert!(config.is_err());
    /// ```
    pub fn new(
        preferred: RegionName,
        regions: Vec<ProviderRegion<T>>,
        topologies: Vec<Topology>,
    ) -> Result<Self, ValidationError> {
        let config = Self {
            preferred,
            regions,
            topologies,
        };
        config.validate()?;
        Ok(config)
    }

    /// Returns the preferred region for operations with region affinity.
    pub fn preferred(&self) -> &RegionName {
        &self.preferred
    }

    /// Returns the set of provider regions available in this configuration.
    pub fn regions(&self) -> &[ProviderRegion<T>] {
        &self.regions
    }

    /// Returns the set of topologies defined over the provider regions.
    pub fn topologies(&self) -> &[Topology] {
        &self.topologies
    }

    /// Validates the configuration against the invariants.
    ///
    /// Returns `Ok(())` if validation passes, or a [`ValidationError`] describing any violations.
    fn validate(&self) -> Result<(), ValidationError> {
        let mut error = ValidationError::default();

        let mut region_names: HashSet<&RegionName> = HashSet::new();
        for region in &self.regions {
            if !region_names.insert(&region.name) {
                error.duplicate_region_names.push(region.name.clone());
            }
        }

        let mut topology_names: HashSet<&TopologyName> = HashSet::new();
        for topology in &self.topologies {
            if !topology_names.insert(&topology.name) {
                error.duplicate_topology_names.push(topology.name.clone());
            }
        }

        for topology in &self.topologies {
            for region_ref in &topology.regions {
                if !region_names.contains(region_ref) {
                    error.unknown_topology_regions.push(region_ref.clone());
                }
            }
        }

        if !region_names.contains(&self.preferred) {
            error.unknown_preferred_region = true;
        }

        if error.is_ok() {
            Ok(())
        } else {
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region_name(s: &str) -> RegionName {
        RegionName::new(s)
    }

    fn topology_name(s: &str) -> TopologyName {
        TopologyName::new(s)
    }

    fn provider_region(name: &str, provider: &str, region: &str) -> ProviderRegion<()> {
        ProviderRegion::new(region_name(name), provider, region, ())
    }

    fn topology(name: &str, regions: Vec<&str>) -> Topology {
        Topology::new(
            topology_name(name),
            regions.into_iter().map(region_name).collect(),
        )
    }

    #[test]
    fn region_name_as_str() {
        let name = RegionName::new("aws-us-east-1");
        assert_eq!(name.as_str(), "aws-us-east-1");
    }

    #[test]
    fn region_name_display() {
        let name = RegionName::new("aws-us-east-1");
        assert_eq!(format!("{}", name), "aws-us-east-1");
    }

    #[test]
    fn region_name_equality() {
        let a = RegionName::new("aws-us-east-1");
        let b = RegionName::new("aws-us-east-1");
        let c = RegionName::new("gcp-europe-west1");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn region_name_clone() {
        let a = RegionName::new("aws-us-east-1");
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn region_name_serde_roundtrip() {
        let name = RegionName::new("aws-us-east-1");
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"aws-us-east-1\"");
        let deserialized: RegionName = serde_json::from_str(&json).unwrap();
        assert_eq!(name, deserialized);
    }

    #[test]
    fn topology_name_as_str() {
        let name = TopologyName::new("global");
        assert_eq!(name.as_str(), "global");
    }

    #[test]
    fn topology_name_display() {
        let name = TopologyName::new("global");
        assert_eq!(format!("{}", name), "global");
    }

    #[test]
    fn topology_name_equality() {
        let a = TopologyName::new("global");
        let b = TopologyName::new("global");
        let c = TopologyName::new("regional");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn topology_name_clone() {
        let a = TopologyName::new("global");
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn topology_name_serde_roundtrip() {
        let name = TopologyName::new("global");
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"global\"");
        let deserialized: TopologyName = serde_json::from_str(&json).unwrap();
        assert_eq!(name, deserialized);
    }

    #[test]
    fn provider_region_accessors() {
        let region = ProviderRegion::new(region_name("aws-us-east-1"), "aws", "us-east-1", ());
        assert_eq!(region.name(), &region_name("aws-us-east-1"));
        assert_eq!(region.provider(), "aws");
        assert_eq!(region.region(), "us-east-1");
    }

    #[test]
    fn provider_region_equality() {
        let a = provider_region("aws-us-east-1", "aws", "us-east-1");
        let b = provider_region("aws-us-east-1", "aws", "us-east-1");
        let c = provider_region("gcp-europe-west1", "gcp", "europe-west1");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn provider_region_clone() {
        let a = provider_region("aws-us-east-1", "aws", "us-east-1");
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn provider_region_serde_roundtrip() {
        let region = provider_region("aws-us-east-1", "aws", "us-east-1");
        let json = serde_json::to_string(&region).unwrap();
        let deserialized: ProviderRegion<()> = serde_json::from_str(&json).unwrap();
        assert_eq!(region, deserialized);
    }

    #[test]
    fn topology_accessors() {
        let t = topology("global", vec!["aws-us-east-1", "gcp-europe-west1"]);
        assert_eq!(t.name(), &topology_name("global"));
        assert_eq!(
            t.regions(),
            &[
                region_name("aws-us-east-1"),
                region_name("gcp-europe-west1")
            ]
        );
    }

    #[test]
    fn topology_equality() {
        let a = topology("global", vec!["aws-us-east-1"]);
        let b = topology("global", vec!["aws-us-east-1"]);
        let c = topology("regional", vec!["aws-us-east-1"]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn topology_clone() {
        let a = topology("global", vec!["aws-us-east-1"]);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn topology_serde_roundtrip() {
        let t = topology("global", vec!["aws-us-east-1", "gcp-europe-west1"]);
        let json = serde_json::to_string(&t).unwrap();
        let deserialized: Topology = serde_json::from_str(&json).unwrap();
        assert_eq!(t, deserialized);
    }

    #[test]
    fn valid_configuration() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![
                provider_region("aws-us-east-1", "aws", "us-east-1"),
                provider_region("gcp-europe-west1", "gcp", "europe-west1"),
            ],
            vec![topology(
                "global",
                vec!["aws-us-east-1", "gcp-europe-west1"],
            )],
        );

        assert!(config.is_ok(), "Expected valid configuration: {:?}", config);
    }

    #[test]
    fn valid_configuration_accessors() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![topology("global", vec!["aws-us-east-1"])],
        )
        .expect("valid configuration");

        assert_eq!(config.preferred(), &region_name("aws-us-east-1"));
        assert_eq!(config.regions().len(), 1);
        assert_eq!(config.topologies().len(), 1);
    }

    #[test]
    fn configuration_serde_roundtrip() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![
                provider_region("aws-us-east-1", "aws", "us-east-1"),
                provider_region("gcp-europe-west1", "gcp", "europe-west1"),
            ],
            vec![topology(
                "global",
                vec!["aws-us-east-1", "gcp-europe-west1"],
            )],
        )
        .expect("valid configuration");

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: MultiCloudMultiRegionConfiguration<()> =
            serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn configuration_deserialize_valid() {
        let json = r#"{
            "preferred": "aws-us-east-1",
            "regions": [
                {"name": "aws-us-east-1", "provider": "aws", "region": "us-east-1", "config": null}
            ],
            "topologies": []
        }"#;

        let config: MultiCloudMultiRegionConfiguration<()> = serde_json::from_str(json).unwrap();
        assert_eq!(config.preferred().as_str(), "aws-us-east-1");
    }

    #[test]
    fn configuration_deserialize_invalid_preferred() {
        let json = r#"{
            "preferred": "nonexistent",
            "regions": [
                {"name": "aws-us-east-1", "provider": "aws", "region": "us-east-1", "config": null}
            ],
            "topologies": []
        }"#;

        let result: Result<MultiCloudMultiRegionConfiguration<()>, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown preferred region"),
            "Expected error message to contain 'unknown preferred region', got: {}",
            err_msg
        );
    }

    #[test]
    fn configuration_deserialize_duplicate_regions() {
        let json = r#"{
            "preferred": "aws-us-east-1",
            "regions": [
                {"name": "aws-us-east-1", "provider": "aws", "region": "us-east-1", "config": null},
                {"name": "aws-us-east-1", "provider": "aws", "region": "us-east-1", "config": null}
            ],
            "topologies": []
        }"#;

        let result: Result<MultiCloudMultiRegionConfiguration<()>, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("duplicate region names"),
            "Expected error message to contain 'duplicate region names', got: {}",
            err_msg
        );
    }

    #[test]
    fn configuration_deserialize_unknown_topology_region() {
        let json = r#"{
            "preferred": "aws-us-east-1",
            "regions": [
                {"name": "aws-us-east-1", "provider": "aws", "region": "us-east-1", "config": null}
            ],
            "topologies": [
                {"name": "global", "regions": ["aws-us-east-1", "nonexistent"]}
            ]
        }"#;

        let result: Result<MultiCloudMultiRegionConfiguration<()>, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown topology regions"),
            "Expected error message to contain 'unknown topology regions', got: {}",
            err_msg
        );
    }

    #[test]
    fn empty_configuration() {
        let config = MultiCloudMultiRegionConfiguration::<()>::new(
            region_name("nonexistent"),
            vec![],
            vec![],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![],
                duplicate_topology_names: vec![],
                unknown_topology_regions: vec![],
                unknown_preferred_region: true,
            }
        );
    }

    #[test]
    fn empty_topology_regions() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![topology("empty", vec![])],
        );

        assert!(
            config.is_ok(),
            "Topology with no regions should be valid: {:?}",
            config
        );
    }

    #[test]
    fn duplicate_region_names() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![
                provider_region("aws-us-east-1", "aws", "us-east-1"),
                provider_region("aws-us-east-1", "aws", "us-east-1"),
            ],
            vec![],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![region_name("aws-us-east-1")],
                duplicate_topology_names: vec![],
                unknown_topology_regions: vec![],
                unknown_preferred_region: false,
            }
        );
    }

    #[test]
    fn duplicate_topology_names() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![
                topology("global", vec!["aws-us-east-1"]),
                topology("global", vec!["aws-us-east-1"]),
            ],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![],
                duplicate_topology_names: vec![topology_name("global")],
                unknown_topology_regions: vec![],
                unknown_preferred_region: false,
            }
        );
    }

    #[test]
    fn unknown_topology_region() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![topology(
                "global",
                vec!["aws-us-east-1", "nonexistent-region"],
            )],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![],
                duplicate_topology_names: vec![],
                unknown_topology_regions: vec![region_name("nonexistent-region")],
                unknown_preferred_region: false,
            }
        );
    }

    #[test]
    fn unknown_topology_region_duplicated_in_multiple_topologies() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![
                topology("topo1", vec!["aws-us-east-1", "nonexistent"]),
                topology("topo2", vec!["nonexistent"]),
            ],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![],
                duplicate_topology_names: vec![],
                unknown_topology_regions: vec![
                    region_name("nonexistent"),
                    region_name("nonexistent"),
                ],
                unknown_preferred_region: false,
            }
        );
    }

    #[test]
    fn unknown_preferred_region() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("nonexistent-region"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![],
                duplicate_topology_names: vec![],
                unknown_topology_regions: vec![],
                unknown_preferred_region: true,
            }
        );
    }

    #[test]
    fn empty_preferred_region_string() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name(""),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![],
                duplicate_topology_names: vec![],
                unknown_topology_regions: vec![],
                unknown_preferred_region: true,
            }
        );
    }

    #[test]
    fn empty_region_name_is_valid_if_defined() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name(""),
            vec![provider_region("", "aws", "us-east-1")],
            vec![],
        );

        assert!(
            config.is_ok(),
            "Empty region name should be valid if defined: {:?}",
            config
        );
    }

    #[test]
    fn multiple_validation_errors() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("nonexistent-preferred"),
            vec![
                provider_region("aws-us-east-1", "aws", "us-east-1"),
                provider_region("aws-us-east-1", "aws", "us-east-1"),
            ],
            vec![
                topology("topo1", vec!["unknown-region"]),
                topology("topo1", vec!["aws-us-east-1"]),
            ],
        );

        assert_eq!(
            config.unwrap_err(),
            ValidationError {
                duplicate_region_names: vec![region_name("aws-us-east-1")],
                duplicate_topology_names: vec![topology_name("topo1")],
                unknown_topology_regions: vec![region_name("unknown-region")],
                unknown_preferred_region: true,
            }
        );
    }

    #[test]
    fn display_no_errors() {
        let error = ValidationError::default();
        assert_eq!(error.to_string(), "no validation errors");
    }

    #[test]
    fn display_duplicate_region_names_only() {
        let error = ValidationError {
            duplicate_region_names: vec![region_name("region-a"), region_name("region-b")],
            duplicate_topology_names: vec![],
            unknown_topology_regions: vec![],
            unknown_preferred_region: false,
        };
        assert_eq!(
            error.to_string(),
            "duplicate region names: region-a, region-b"
        );
    }

    #[test]
    fn display_duplicate_topology_names_only() {
        let error = ValidationError {
            duplicate_region_names: vec![],
            duplicate_topology_names: vec![topology_name("topo-x")],
            unknown_topology_regions: vec![],
            unknown_preferred_region: false,
        };
        assert_eq!(error.to_string(), "duplicate topology names: topo-x");
    }

    #[test]
    fn display_unknown_topology_regions_only() {
        let error = ValidationError {
            duplicate_region_names: vec![],
            duplicate_topology_names: vec![],
            unknown_topology_regions: vec![region_name("missing-1"), region_name("missing-2")],
            unknown_preferred_region: false,
        };
        assert_eq!(
            error.to_string(),
            "unknown topology regions: missing-1, missing-2"
        );
    }

    #[test]
    fn display_unknown_preferred_region_only() {
        let error = ValidationError {
            duplicate_region_names: vec![],
            duplicate_topology_names: vec![],
            unknown_topology_regions: vec![],
            unknown_preferred_region: true,
        };
        assert_eq!(error.to_string(), "unknown preferred region");
    }

    #[test]
    fn display_all_errors() {
        let error = ValidationError {
            duplicate_region_names: vec![region_name("dup-region")],
            duplicate_topology_names: vec![topology_name("dup-topo")],
            unknown_topology_regions: vec![region_name("unknown-reg")],
            unknown_preferred_region: true,
        };
        assert_eq!(
            error.to_string(),
            "duplicate region names: dup-region; duplicate topology names: dup-topo; unknown topology regions: unknown-reg; unknown preferred region"
        );
    }

    #[test]
    fn display_empty_string_names() {
        let error = ValidationError {
            duplicate_region_names: vec![region_name("")],
            duplicate_topology_names: vec![topology_name("")],
            unknown_topology_regions: vec![region_name("")],
            unknown_preferred_region: true,
        };
        assert_eq!(
            error.to_string(),
            "duplicate region names: ; duplicate topology names: ; unknown topology regions: ; unknown preferred region"
        );
    }

    #[test]
    fn display_special_characters() {
        let error = ValidationError {
            duplicate_region_names: vec![
                region_name("region-with-dash_and_underscore"),
                region_name("region with spaces"),
            ],
            duplicate_topology_names: vec![topology_name("topo.dot")],
            unknown_topology_regions: vec![region_name("region\nwith\nnewlines")],
            unknown_preferred_region: false,
        };
        assert_eq!(
            error.to_string(),
            "duplicate region names: region-with-dash_and_underscore, region with spaces; duplicate topology names: topo.dot; unknown topology regions: region\nwith\nnewlines"
        );
    }

    #[test]
    fn display_unicode_characters() {
        let error = ValidationError {
            duplicate_region_names: vec![region_name("region-🌍")],
            duplicate_topology_names: vec![topology_name("拓扑名")],
            unknown_topology_regions: vec![region_name("регион-с-кириллицей")],
            unknown_preferred_region: true,
        };
        assert_eq!(
            error.to_string(),
            "duplicate region names: region-🌍; duplicate topology names: 拓扑名; unknown topology regions: регион-с-кириллицей; unknown preferred region"
        );
    }

    #[test]
    fn display_long_names() {
        let long_prefix = "very_long_region_name_that_contains_many_characters_and_pushes_the_boundary_of_reasonableness";
        let error = ValidationError {
            duplicate_region_names: vec![region_name(long_prefix)],
            duplicate_topology_names: vec![],
            unknown_topology_regions: vec![],
            unknown_preferred_region: false,
        };
        assert_eq!(
            error.to_string(),
            format!("duplicate region names: {}", long_prefix)
        );
    }

    #[test]
    fn validation_error_is_ok_default() {
        let error = ValidationError::default();
        assert!(error.is_ok());
    }

    #[test]
    fn validation_error_is_ok_with_duplicate_regions() {
        let error = ValidationError {
            duplicate_region_names: vec![region_name("dup")],
            ..Default::default()
        };
        assert!(!error.is_ok());
    }

    #[test]
    fn validation_error_is_ok_with_duplicate_topologies() {
        let error = ValidationError {
            duplicate_topology_names: vec![topology_name("dup")],
            ..Default::default()
        };
        assert!(!error.is_ok());
    }

    #[test]
    fn validation_error_is_ok_with_unknown_topology_regions() {
        let error = ValidationError {
            unknown_topology_regions: vec![region_name("unknown")],
            ..Default::default()
        };
        assert!(!error.is_ok());
    }

    #[test]
    fn validation_error_is_ok_with_unknown_preferred() {
        let error = ValidationError {
            unknown_preferred_region: true,
            ..Default::default()
        };
        assert!(!error.is_ok());
    }

    #[test]
    fn configuration_clone() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![],
        )
        .expect("valid configuration");

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn configuration_debug() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![],
        )
        .expect("valid configuration");

        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("MultiCloudMultiRegionConfiguration"));
        assert!(debug_str.contains("aws-us-east-1"));
    }
}
