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
//!     RegionName::new("aws-us-east-1").unwrap(),
//!     vec![
//!         ProviderRegion::new(
//!             RegionName::new("aws-us-east-1").unwrap(),
//!             "aws",
//!             "us-east-1",
//!             (),
//!         ),
//!         ProviderRegion::new(
//!             RegionName::new("gcp-europe-west1").unwrap(),
//!             "gcp",
//!             "europe-west1",
//!             (),
//!         ),
//!     ],
//!     vec![Topology::new(
//!         TopologyName::new("global").unwrap(),
//!         vec![
//!             RegionName::new("aws-us-east-1").unwrap(),
//!             RegionName::new("gcp-europe-west1").unwrap(),
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
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Maximum length for region and topology names.
const MAX_NAME_LENGTH: usize = 32;

/// Errors that can occur when creating a [`RegionName`] or [`TopologyName`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NameError {
    /// The name is empty.
    #[error("name cannot be empty")]
    Empty,
    /// The name exceeds the maximum allowed length.
    #[error("name exceeds maximum length of {MAX_NAME_LENGTH} characters: {0} characters")]
    TooLong(usize),
    /// The name contains non-ASCII characters.
    #[error("name contains non-ASCII characters")]
    NonAscii,
}

/// Validates that a name is a non-empty string of at most 32 ASCII characters.
fn validate_name(name: &str) -> Result<(), NameError> {
    if name.is_empty() {
        return Err(NameError::Empty);
    }
    if !name.is_ascii() {
        return Err(NameError::NonAscii);
    }
    if name.len() > MAX_NAME_LENGTH {
        return Err(NameError::TooLong(name.len()));
    }
    Ok(())
}

/// Finds duplicate values in a slice by extracting a key from each item.
///
/// Returns a sorted, deduplicated list of keys that appear more than once.
fn find_duplicates<'a, T, K, F>(items: &'a [T], key_fn: F) -> Vec<K>
where
    K: 'a + Clone + Eq + Hash + Ord,
    F: Fn(&'a T) -> &'a K,
{
    let mut seen = HashSet::new();
    let mut duplicates: Vec<_> = items
        .iter()
        .filter_map(|item| {
            let key = key_fn(item);
            if !seen.insert(key) {
                Some(key.clone())
            } else {
                None
            }
        })
        .collect();
    duplicates.sort();
    duplicates.dedup();
    duplicates
}

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
/// let name = RegionName::new("aws-us-east-1").unwrap();
/// assert_eq!(name.as_str(), "aws-us-east-1");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct RegionName(String);

impl RegionName {
    /// Creates a new region name.
    ///
    /// # Errors
    ///
    /// Returns a [`NameError`] if the name:
    /// - Is empty
    /// - Exceeds 32 characters
    /// - Contains non-ASCII characters
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self(name))
    }

    /// Returns the region name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RegionName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        RegionName::new(s).map_err(serde::de::Error::custom)
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
/// let name = TopologyName::new("global").unwrap();
/// assert_eq!(name.as_str(), "global");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct TopologyName(String);

impl TopologyName {
    /// Creates a new topology name.
    ///
    /// # Errors
    ///
    /// Returns a [`NameError`] if the name:
    /// - Is empty
    /// - Exceeds 32 characters
    /// - Contains non-ASCII characters
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self(name))
    }

    /// Returns the topology name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for TopologyName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TopologyName::new(s).map_err(serde::de::Error::custom)
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
///     RegionName::new("aws-us-east-1").unwrap(),
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
    ///     RegionName::new("gcp-europe-west1").unwrap(),
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

    /// Returns the additional per-region configuration data.
    pub fn config(&self) -> &T {
        &self.config
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
///     TopologyName::new("us-multi-az").unwrap(),
///     vec![
///         RegionName::new("aws-us-east-1").unwrap(),
///         RegionName::new("aws-us-west-2").unwrap(),
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
    ///     TopologyName::new("global").unwrap(),
    ///     vec![RegionName::new("aws-us-east-1").unwrap()],
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
///     RegionName::new("aws-us-east-1").unwrap(),
///     vec![ProviderRegion::new(
///         RegionName::new("aws-us-east-1").unwrap(),
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
    duplicate_region_names: Vec<RegionName>,
    duplicate_topology_names: Vec<TopologyName>,
    unknown_topology_regions: Vec<RegionName>,
    unknown_preferred_region: Option<RegionName>,
}

impl ValidationError {
    #[cfg(test)]
    fn new(
        duplicate_region_names: Vec<RegionName>,
        duplicate_topology_names: Vec<TopologyName>,
        unknown_topology_regions: Vec<RegionName>,
        unknown_preferred_region: Option<RegionName>,
    ) -> Self {
        Self {
            duplicate_region_names,
            duplicate_topology_names,
            unknown_topology_regions,
            unknown_preferred_region,
        }
    }

    /// Returns true if validation errors were found.
    pub fn has_errors(&self) -> bool {
        !self.duplicate_region_names.is_empty()
            || !self.duplicate_topology_names.is_empty()
            || !self.unknown_topology_regions.is_empty()
            || self.unknown_preferred_region.is_some()
    }

    /// Returns the provider region names that appear more than once.
    pub fn duplicate_region_names(&self) -> &[RegionName] {
        &self.duplicate_region_names
    }

    /// Returns the topology names that appear more than once.
    pub fn duplicate_topology_names(&self) -> &[TopologyName] {
        &self.duplicate_topology_names
    }

    /// Returns the region names referenced by topologies that do not exist in the configuration.
    pub fn unknown_topology_regions(&self) -> &[RegionName] {
        &self.unknown_topology_regions
    }

    /// Returns the preferred region name if it does not exist in the configuration.
    pub fn unknown_preferred_region(&self) -> Option<&RegionName> {
        self.unknown_preferred_region.as_ref()
    }

    fn format_message(&self) -> String {
        if !self.has_errors() {
            return "no validation errors".to_string();
        }

        let mut parts = Vec::new();

        if !self.duplicate_region_names.is_empty() {
            parts.push(format!(
                "duplicate region names: {}",
                format_name_list(&self.duplicate_region_names)
            ));
        }

        if !self.duplicate_topology_names.is_empty() {
            parts.push(format!(
                "duplicate topology names: {}",
                format_name_list(&self.duplicate_topology_names)
            ));
        }

        if !self.unknown_topology_regions.is_empty() {
            parts.push(format!(
                "unknown topology regions: {}",
                format_name_list(&self.unknown_topology_regions)
            ));
        }

        if let Some(ref name) = self.unknown_preferred_region {
            parts.push(format!("unknown preferred region: {}", name));
        }

        parts.join("; ")
    }
}

/// Formats a slice of displayable items as a comma-separated string.
fn format_name_list<T: std::fmt::Display>(names: &[T]) -> String {
    names
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(", ")
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
    ///     RegionName::new("aws-us-east-1").unwrap(),
    ///     vec![ProviderRegion::new(
    ///         RegionName::new("aws-us-east-1").unwrap(),
    ///         "aws",
    ///         "us-east-1",
    ///         (),
    ///     )],
    ///     vec![Topology::new(
    ///         TopologyName::new("default").unwrap(),
    ///         vec![RegionName::new("aws-us-east-1").unwrap()],
    ///     )],
    /// );
    /// assert!(config.is_ok());
    ///
    /// // Invalid configuration - preferred region doesn't exist
    /// let config = MultiCloudMultiRegionConfiguration::<()>::new(
    ///     RegionName::new("nonexistent").unwrap(),
    ///     vec![ProviderRegion::new(
    ///         RegionName::new("aws-us-east-1").unwrap(),
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
    /// Each unique error is reported only once, even if it occurs multiple times.
    fn validate(&self) -> Result<(), ValidationError> {
        let mut error = ValidationError::default();
        let all_region_names: HashSet<_> = self.regions.iter().map(|r| &r.name).collect();

        error.duplicate_region_names = find_duplicates(&self.regions, |r| &r.name);
        error.duplicate_topology_names = find_duplicates(&self.topologies, |t| &t.name);

        // Find all unique unknown regions across all topologies.
        let mut unknown_regions: Vec<_> = self
            .topologies
            .iter()
            .flat_map(|t| &t.regions)
            .filter(|r| !all_region_names.contains(r))
            .cloned()
            .collect();
        unknown_regions.sort();
        unknown_regions.dedup();
        error.unknown_topology_regions = unknown_regions;

        // Check if the preferred region is one of the defined regions.
        if !all_region_names.contains(&self.preferred) {
            error.unknown_preferred_region = Some(self.preferred.clone());
        }

        if error.has_errors() {
            Err(error)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region_name(s: impl Into<String>) -> RegionName {
        RegionName::new(s).expect("test region name should be valid")
    }

    fn topology_name(s: impl Into<String>) -> TopologyName {
        TopologyName::new(s).expect("test topology name should be valid")
    }

    fn provider_region(
        name: impl Into<String>,
        provider: impl Into<String>,
        region: impl Into<String>,
    ) -> ProviderRegion<()> {
        ProviderRegion::new(
            RegionName::new(name).expect("test region name should be valid"),
            provider,
            region,
            (),
        )
    }

    fn topology(name: impl Into<String>, regions: Vec<&str>) -> Topology {
        Topology::new(
            TopologyName::new(name).expect("test topology name should be valid"),
            regions
                .into_iter()
                .map(|s| RegionName::new(s).expect("test region name should be valid"))
                .collect(),
        )
    }

    #[test]
    fn region_name_as_str() {
        let name = RegionName::new("aws-us-east-1").expect("valid name");
        assert_eq!(name.as_str(), "aws-us-east-1");
    }

    #[test]
    fn region_name_display() {
        let name = RegionName::new("aws-us-east-1").expect("valid name");
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
        let name = RegionName::new("aws-us-east-1").expect("valid name");
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"aws-us-east-1\"");
        let deserialized: RegionName = serde_json::from_str(&json).unwrap();
        assert_eq!(name, deserialized);
    }

    #[test]
    fn topology_name_as_str() {
        let name = TopologyName::new("global").expect("valid name");
        assert_eq!(name.as_str(), "global");
    }

    #[test]
    fn topology_name_display() {
        let name = TopologyName::new("global").expect("valid name");
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
        let name = TopologyName::new("global").expect("valid name");
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

        let err = config.unwrap_err();
        assert!(err.duplicate_region_names().is_empty());
        assert!(err.duplicate_topology_names().is_empty());
        assert!(err.unknown_topology_regions().is_empty());
        assert_eq!(
            err.unknown_preferred_region(),
            Some(&region_name("nonexistent"))
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

        let err = config.unwrap_err();
        assert_eq!(
            err.duplicate_region_names(),
            &[region_name("aws-us-east-1")]
        );
        assert!(err.duplicate_topology_names().is_empty());
        assert!(err.unknown_topology_regions().is_empty());
        assert_eq!(err.unknown_preferred_region(), None);
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

        let err = config.unwrap_err();
        assert!(err.duplicate_region_names().is_empty());
        assert_eq!(err.duplicate_topology_names(), &[topology_name("global")]);
        assert!(err.unknown_topology_regions().is_empty());
        assert_eq!(err.unknown_preferred_region(), None);
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

        let err = config.unwrap_err();
        assert!(err.duplicate_region_names().is_empty());
        assert!(err.duplicate_topology_names().is_empty());
        assert_eq!(
            err.unknown_topology_regions(),
            &[region_name("nonexistent-region")]
        );
        assert_eq!(err.unknown_preferred_region(), None);
    }

    #[test]
    fn unknown_topology_region_duplicated_in_multiple_topologies() {
        // When the same unknown region is referenced in multiple topologies, it should only
        // appear once in the error output for cleaner, more deterministic error messages.
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("aws-us-east-1"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![
                topology("topo1", vec!["aws-us-east-1", "nonexistent"]),
                topology("topo2", vec!["nonexistent"]),
            ],
        );

        let err = config.unwrap_err();
        assert!(err.duplicate_region_names().is_empty());
        assert!(err.duplicate_topology_names().is_empty());
        assert_eq!(
            err.unknown_topology_regions(),
            &[region_name("nonexistent")]
        );
        assert_eq!(err.unknown_preferred_region(), None);
    }

    #[test]
    fn unknown_preferred_region() {
        let config = MultiCloudMultiRegionConfiguration::new(
            region_name("nonexistent-region"),
            vec![provider_region("aws-us-east-1", "aws", "us-east-1")],
            vec![],
        );

        let err = config.unwrap_err();
        assert!(err.duplicate_region_names().is_empty());
        assert!(err.duplicate_topology_names().is_empty());
        assert!(err.unknown_topology_regions().is_empty());
        assert_eq!(
            err.unknown_preferred_region(),
            Some(&region_name("nonexistent-region"))
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

        let err = config.unwrap_err();
        assert_eq!(
            err.duplicate_region_names(),
            &[region_name("aws-us-east-1")]
        );
        assert_eq!(err.duplicate_topology_names(), &[topology_name("topo1")]);
        assert_eq!(
            err.unknown_topology_regions(),
            &[region_name("unknown-region")]
        );
        assert_eq!(
            err.unknown_preferred_region(),
            Some(&region_name("nonexistent-preferred"))
        );
    }

    #[test]
    fn display_no_errors() {
        let error = ValidationError::default();
        assert_eq!(error.to_string(), "no validation errors");
    }

    #[test]
    fn display_duplicate_region_names_only() {
        let error = ValidationError::new(
            vec![region_name("region-a"), region_name("region-b")],
            vec![],
            vec![],
            None,
        );
        assert_eq!(
            error.to_string(),
            "duplicate region names: region-a, region-b"
        );
    }

    #[test]
    fn display_duplicate_topology_names_only() {
        let error = ValidationError::new(vec![], vec![topology_name("topo-x")], vec![], None);
        assert_eq!(error.to_string(), "duplicate topology names: topo-x");
    }

    #[test]
    fn display_unknown_topology_regions_only() {
        let error = ValidationError::new(
            vec![],
            vec![],
            vec![region_name("missing-1"), region_name("missing-2")],
            None,
        );
        assert_eq!(
            error.to_string(),
            "unknown topology regions: missing-1, missing-2"
        );
    }

    #[test]
    fn display_unknown_preferred_region_only() {
        let error =
            ValidationError::new(vec![], vec![], vec![], Some(region_name("missing-region")));
        assert_eq!(
            error.to_string(),
            "unknown preferred region: missing-region"
        );
    }

    #[test]
    fn display_all_errors() {
        let error = ValidationError::new(
            vec![region_name("dup-region")],
            vec![topology_name("dup-topo")],
            vec![region_name("unknown-reg")],
            Some(region_name("bad-preferred")),
        );
        assert_eq!(
            error.to_string(),
            "duplicate region names: dup-region; duplicate topology names: dup-topo; unknown topology regions: unknown-reg; unknown preferred region: bad-preferred"
        );
    }

    #[test]
    fn display_special_characters() {
        let error = ValidationError::new(
            vec![
                region_name("region-with-dash_and_underscore"),
                region_name("region with spaces"),
            ],
            vec![topology_name("topo.dot")],
            vec![region_name("region\nwith\nnewlines")],
            None,
        );
        assert_eq!(
            error.to_string(),
            "duplicate region names: region-with-dash_and_underscore, region with spaces; duplicate topology names: topo.dot; unknown topology regions: region\nwith\nnewlines"
        );
    }

    #[test]
    fn validation_error_has_errors_default() {
        let error = ValidationError::default();
        assert!(!error.has_errors());
    }

    #[test]
    fn validation_error_has_errors_with_duplicate_regions() {
        let error = ValidationError::new(vec![region_name("dup")], vec![], vec![], None);
        assert!(error.has_errors());
    }

    #[test]
    fn validation_error_has_errors_with_duplicate_topologies() {
        let error = ValidationError::new(vec![], vec![topology_name("dup")], vec![], None);
        assert!(error.has_errors());
    }

    #[test]
    fn validation_error_has_errors_with_unknown_topology_regions() {
        let error = ValidationError::new(vec![], vec![], vec![region_name("unknown")], None);
        assert!(error.has_errors());
    }

    #[test]
    fn validation_error_has_errors_with_unknown_preferred() {
        let error = ValidationError::new(vec![], vec![], vec![], Some(region_name("unknown")));
        assert!(error.has_errors());
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

    #[test]
    fn region_name_valid() {
        assert!(RegionName::new("aws-us-east-1").is_ok());
        assert!(RegionName::new("a").is_ok());
        // 32 chars exactly
        assert!(RegionName::new("12345678901234567890123456789012").is_ok());
    }

    #[test]
    fn region_name_empty() {
        let result = RegionName::new("");
        assert!(result.is_err());
        println!(
            "region_name_empty error: {:?}",
            result.as_ref().unwrap_err()
        );
        assert!(matches!(result, Err(NameError::Empty)));
    }

    #[test]
    fn region_name_too_long() {
        // 33 chars
        let result = RegionName::new("123456789012345678901234567890123");
        assert!(result.is_err());
        println!(
            "region_name_too_long error: {:?}",
            result.as_ref().unwrap_err()
        );
        assert!(matches!(result, Err(NameError::TooLong(33))));
    }

    #[test]
    fn region_name_non_ascii() {
        let result = RegionName::new("region-🌍");
        assert!(result.is_err());
        println!(
            "region_name_non_ascii error: {:?}",
            result.as_ref().unwrap_err()
        );
        assert!(matches!(result, Err(NameError::NonAscii)));
    }

    #[test]
    fn topology_name_valid() {
        assert!(TopologyName::new("global").is_ok());
        assert!(TopologyName::new("a").is_ok());
        // 32 chars exactly
        assert!(TopologyName::new("12345678901234567890123456789012").is_ok());
    }

    #[test]
    fn topology_name_empty() {
        let result = TopologyName::new("");
        assert!(result.is_err());
        println!(
            "topology_name_empty error: {:?}",
            result.as_ref().unwrap_err()
        );
        assert!(matches!(result, Err(NameError::Empty)));
    }

    #[test]
    fn topology_name_too_long() {
        // 33 chars
        let result = TopologyName::new("123456789012345678901234567890123");
        assert!(result.is_err());
        println!(
            "topology_name_too_long error: {:?}",
            result.as_ref().unwrap_err()
        );
        assert!(matches!(result, Err(NameError::TooLong(33))));
    }

    #[test]
    fn topology_name_non_ascii() {
        let result = TopologyName::new("拓扑名");
        assert!(result.is_err());
        println!(
            "topology_name_non_ascii error: {:?}",
            result.as_ref().unwrap_err()
        );
        assert!(matches!(result, Err(NameError::NonAscii)));
    }

    #[test]
    fn name_error_display_empty() {
        let err = NameError::Empty;
        assert_eq!(err.to_string(), "name cannot be empty");
    }

    #[test]
    fn name_error_display_too_long() {
        let err = NameError::TooLong(50);
        assert_eq!(
            err.to_string(),
            "name exceeds maximum length of 32 characters: 50 characters"
        );
    }

    #[test]
    fn name_error_display_non_ascii() {
        let err = NameError::NonAscii;
        assert_eq!(err.to_string(), "name contains non-ASCII characters");
    }

    #[test]
    fn region_name_deserialize_valid() {
        let json = "\"aws-us-east-1\"";
        let name: RegionName = serde_json::from_str(json).unwrap();
        assert_eq!(name.as_str(), "aws-us-east-1");
    }

    #[test]
    fn region_name_deserialize_empty() {
        let json = "\"\"";
        let result: Result<RegionName, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        println!("region_name_deserialize_empty error: {}", err_msg);
        assert!(
            err_msg.contains("name cannot be empty"),
            "Expected error message to contain 'name cannot be empty', got: {}",
            err_msg
        );
    }

    #[test]
    fn region_name_deserialize_too_long() {
        let json = "\"123456789012345678901234567890123\"";
        let result: Result<RegionName, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        println!("region_name_deserialize_too_long error: {}", err_msg);
        assert!(
            err_msg.contains("name exceeds maximum length"),
            "Expected error message to contain 'name exceeds maximum length', got: {}",
            err_msg
        );
    }

    #[test]
    fn region_name_deserialize_non_ascii() {
        let json = "\"region-🌍\"";
        let result: Result<RegionName, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        println!("region_name_deserialize_non_ascii error: {}", err_msg);
        assert!(
            err_msg.contains("non-ASCII"),
            "Expected error message to contain 'non-ASCII', got: {}",
            err_msg
        );
    }

    #[test]
    fn topology_name_deserialize_valid() {
        let json = "\"global\"";
        let name: TopologyName = serde_json::from_str(json).unwrap();
        assert_eq!(name.as_str(), "global");
    }

    #[test]
    fn topology_name_deserialize_empty() {
        let json = "\"\"";
        let result: Result<TopologyName, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        println!("topology_name_deserialize_empty error: {}", err_msg);
        assert!(
            err_msg.contains("name cannot be empty"),
            "Expected error message to contain 'name cannot be empty', got: {}",
            err_msg
        );
    }

    #[test]
    fn topology_name_deserialize_too_long() {
        let json = "\"123456789012345678901234567890123\"";
        let result: Result<TopologyName, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        println!("topology_name_deserialize_too_long error: {}", err_msg);
        assert!(
            err_msg.contains("name exceeds maximum length"),
            "Expected error message to contain 'name exceeds maximum length', got: {}",
            err_msg
        );
    }

    #[test]
    fn topology_name_deserialize_non_ascii() {
        let json = "\"拓扑名\"";
        let result: Result<TopologyName, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        println!("topology_name_deserialize_non_ascii error: {}", err_msg);
        assert!(
            err_msg.contains("non-ASCII"),
            "Expected error message to contain 'non-ASCII', got: {}",
            err_msg
        );
    }
}
