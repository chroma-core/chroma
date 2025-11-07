use chroma_error::{ChromaError, ErrorCodes};
use itertools::Itertools;
use serde::{ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Number, Value};
use sprs::CsVec;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    mem::size_of_val,
    ops::{BitAnd, BitOr},
};
use thiserror::Error;

use crate::chroma_proto;

#[cfg(feature = "pyo3")]
use pyo3::types::PyAnyMethods;

#[cfg(feature = "testing")]
use proptest::prelude::*;

#[derive(Serialize, Deserialize)]
struct SparseVectorSerdeHelper {
    #[serde(rename = "#type")]
    type_tag: Option<String>,
    indices: Vec<u32>,
    values: Vec<f32>,
}

/// Represents a sparse vector using parallel arrays for indices and values.
///
/// On deserialization: accepts both old format `{"indices": [...], "values": [...]}`
/// and new format `{"#type": "sparse_vector", "indices": [...], "values": [...]}`.
///
/// On serialization: always includes `#type` field with value `"sparse_vector"`.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SparseVector {
    /// Dimension indices
    pub indices: Vec<u32>,
    /// Values corresponding to each index
    pub values: Vec<f32>,
}

// Custom deserializer: accept both old and new formats
impl<'de> Deserialize<'de> for SparseVector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let helper = SparseVectorSerdeHelper::deserialize(deserializer)?;

        // If #type is present, validate it
        if let Some(type_tag) = &helper.type_tag {
            if type_tag != "sparse_vector" {
                return Err(serde::de::Error::custom(format!(
                    "Expected #type='sparse_vector', got '{}'",
                    type_tag
                )));
            }
        }

        Ok(SparseVector {
            indices: helper.indices,
            values: helper.values,
        })
    }
}

// Custom serializer: always include #type field
impl Serialize for SparseVector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let helper = SparseVectorSerdeHelper {
            type_tag: Some("sparse_vector".to_string()),
            indices: self.indices.clone(),
            values: self.values.clone(),
        };
        helper.serialize(serializer)
    }
}

impl SparseVector {
    /// Create a new sparse vector from parallel arrays.
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Self {
        Self { indices, values }
    }

    /// Create a sparse vector from an iterator of (index, value) pairs.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (u32, f32)>) -> Self {
        let (indices, values) = pairs.into_iter().unzip();
        Self { indices, values }
    }

    /// Iterate over (index, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u32, f32)> + '_ {
        self.indices
            .iter()
            .copied()
            .zip(self.values.iter().copied())
    }

    /// Validate the sparse vector
    pub fn validate(&self) -> Result<(), MetadataValueConversionError> {
        // Check that indices and values have the same length
        if self.indices.len() != self.values.len() {
            return Err(MetadataValueConversionError::SparseVectorLengthMismatch);
        }

        // Check that indices are sorted in strictly ascending order (no duplicates)
        for i in 1..self.indices.len() {
            if self.indices[i] <= self.indices[i - 1] {
                return Err(MetadataValueConversionError::SparseVectorIndicesNotSorted);
            }
        }

        Ok(())
    }
}

impl Eq for SparseVector {}

impl Ord for SparseVector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.indices.cmp(&other.indices).then_with(|| {
            for (a, b) in self.values.iter().zip(other.values.iter()) {
                match a.total_cmp(b) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            self.values.len().cmp(&other.values.len())
        })
    }
}

impl PartialOrd for SparseVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<chroma_proto::SparseVector> for SparseVector {
    fn from(proto: chroma_proto::SparseVector) -> Self {
        SparseVector::new(proto.indices, proto.values)
    }
}

impl From<SparseVector> for chroma_proto::SparseVector {
    fn from(sparse: SparseVector) -> Self {
        chroma_proto::SparseVector {
            indices: sparse.indices,
            values: sparse.values,
        }
    }
}

/// Convert SparseVector to sprs::CsVec for efficient sparse operations
impl From<&SparseVector> for CsVec<f32> {
    fn from(sparse: &SparseVector) -> Self {
        let (indices, values) = sparse
            .iter()
            .map(|(index, value)| (index as usize, value))
            .unzip();
        CsVec::new(u32::MAX as usize, indices, values)
    }
}

impl From<SparseVector> for CsVec<f32> {
    fn from(sparse: SparseVector) -> Self {
        (&sparse).into()
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::IntoPyObject<'py> for SparseVector {
    type Target = pyo3::PyAny;
    type Output = pyo3::Bound<'py, Self::Target>;
    type Error = pyo3::PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> Result<Self::Output, Self::Error> {
        use pyo3::types::PyDict;
        let dict = PyDict::new(py);
        dict.set_item("indices", self.indices)?;
        dict.set_item("values", self.values)?;
        Ok(dict.into_any())
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for SparseVector {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyDict;

        let dict = ob.downcast::<PyDict>()?;
        let indices_obj = dict.get_item("indices")?;
        let values_obj = dict.get_item("values")?;

        let indices: Vec<u32> = indices_obj.extract()?;
        let values: Vec<f32> = values_obj.extract()?;

        Ok(SparseVector::new(indices, values))
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
#[serde(untagged)]
pub enum UpdateMetadataValue {
    Bool(bool),
    Int(i64),
    #[cfg_attr(
        feature = "testing",
        proptest(
            strategy = "(-1e6..=1e6f32).prop_map(|v| UpdateMetadataValue::Float(v as f64)).boxed()"
        )
    )]
    Float(f64),
    Str(String),
    #[cfg_attr(feature = "testing", proptest(skip))]
    SparseVector(SparseVector),
    None,
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for UpdateMetadataValue {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        if ob.is_none() {
            Ok(UpdateMetadataValue::None)
        } else if let Ok(value) = ob.extract::<bool>() {
            Ok(UpdateMetadataValue::Bool(value))
        } else if let Ok(value) = ob.extract::<i64>() {
            Ok(UpdateMetadataValue::Int(value))
        } else if let Ok(value) = ob.extract::<f64>() {
            Ok(UpdateMetadataValue::Float(value))
        } else if let Ok(value) = ob.extract::<String>() {
            Ok(UpdateMetadataValue::Str(value))
        } else if let Ok(value) = ob.extract::<SparseVector>() {
            Ok(UpdateMetadataValue::SparseVector(value))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Cannot convert Python object to UpdateMetadataValue",
            ))
        }
    }
}

impl From<bool> for UpdateMetadataValue {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<i64> for UpdateMetadataValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<i32> for UpdateMetadataValue {
    fn from(v: i32) -> Self {
        Self::Int(v as i64)
    }
}

impl From<f64> for UpdateMetadataValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<f32> for UpdateMetadataValue {
    fn from(v: f32) -> Self {
        Self::Float(v as f64)
    }
}

impl From<String> for UpdateMetadataValue {
    fn from(v: String) -> Self {
        Self::Str(v)
    }
}

impl From<&str> for UpdateMetadataValue {
    fn from(v: &str) -> Self {
        Self::Str(v.to_string())
    }
}

impl From<SparseVector> for UpdateMetadataValue {
    fn from(v: SparseVector) -> Self {
        Self::SparseVector(v)
    }
}

#[derive(Error, Debug)]
pub enum UpdateMetadataValueConversionError {
    #[error("Invalid metadata value, valid values are: Int, Float, Str, Bool, None")]
    InvalidValue,
}

impl ChromaError for UpdateMetadataValueConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateMetadataValueConversionError::InvalidValue => ErrorCodes::InvalidArgument,
        }
    }
}

impl TryFrom<&chroma_proto::UpdateMetadataValue> for UpdateMetadataValue {
    type Error = UpdateMetadataValueConversionError;

    fn try_from(value: &chroma_proto::UpdateMetadataValue) -> Result<Self, Self::Error> {
        match &value.value {
            Some(chroma_proto::update_metadata_value::Value::BoolValue(value)) => {
                Ok(UpdateMetadataValue::Bool(*value))
            }
            Some(chroma_proto::update_metadata_value::Value::IntValue(value)) => {
                Ok(UpdateMetadataValue::Int(*value))
            }
            Some(chroma_proto::update_metadata_value::Value::FloatValue(value)) => {
                Ok(UpdateMetadataValue::Float(*value))
            }
            Some(chroma_proto::update_metadata_value::Value::StringValue(value)) => {
                Ok(UpdateMetadataValue::Str(value.clone()))
            }
            Some(chroma_proto::update_metadata_value::Value::SparseVectorValue(value)) => {
                Ok(UpdateMetadataValue::SparseVector(value.clone().into()))
            }
            // Used to communicate that the user wants to delete this key.
            None => Ok(UpdateMetadataValue::None),
        }
    }
}

impl From<UpdateMetadataValue> for chroma_proto::UpdateMetadataValue {
    fn from(value: UpdateMetadataValue) -> Self {
        match value {
            UpdateMetadataValue::Bool(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::BoolValue(value)),
            },
            UpdateMetadataValue::Int(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(value)),
            },
            UpdateMetadataValue::Float(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::FloatValue(
                    value,
                )),
            },
            UpdateMetadataValue::Str(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::StringValue(
                    value,
                )),
            },
            UpdateMetadataValue::SparseVector(sparse_vec) => chroma_proto::UpdateMetadataValue {
                value: Some(
                    chroma_proto::update_metadata_value::Value::SparseVectorValue(
                        sparse_vec.into(),
                    ),
                ),
            },
            UpdateMetadataValue::None => chroma_proto::UpdateMetadataValue { value: None },
        }
    }
}

impl TryFrom<&UpdateMetadataValue> for MetadataValue {
    type Error = MetadataValueConversionError;

    fn try_from(value: &UpdateMetadataValue) -> Result<Self, Self::Error> {
        match value {
            UpdateMetadataValue::Bool(value) => Ok(MetadataValue::Bool(*value)),
            UpdateMetadataValue::Int(value) => Ok(MetadataValue::Int(*value)),
            UpdateMetadataValue::Float(value) => Ok(MetadataValue::Float(*value)),
            UpdateMetadataValue::Str(value) => Ok(MetadataValue::Str(value.clone())),
            UpdateMetadataValue::SparseVector(value) => {
                Ok(MetadataValue::SparseVector(value.clone()))
            }
            UpdateMetadataValue::None => Err(MetadataValueConversionError::InvalidValue),
        }
    }
}

/*
===========================================
MetadataValue
===========================================
*/

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject, pyo3::IntoPyObject))]
#[serde(untagged)]
pub enum MetadataValue {
    Bool(bool),
    Int(i64),
    #[cfg_attr(
        feature = "testing",
        proptest(
            strategy = "(-1e6..=1e6f32).prop_map(|v| MetadataValue::Float(v as f64)).boxed()"
        )
    )]
    Float(f64),
    Str(String),
    #[cfg_attr(feature = "testing", proptest(skip))]
    SparseVector(SparseVector),
}

impl std::fmt::Display for MetadataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataValue::Bool(v) => write!(f, "{}", v),
            MetadataValue::Int(v) => write!(f, "{}", v),
            MetadataValue::Float(v) => write!(f, "{}", v),
            MetadataValue::Str(v) => write!(f, "\"{}\"", v),
            MetadataValue::SparseVector(v) => write!(f, "SparseVector(len={})", v.values.len()),
        }
    }
}

impl Eq for MetadataValue {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataValueType {
    Bool,
    Int,
    Float,
    Str,
    SparseVector,
}

impl MetadataValue {
    pub fn value_type(&self) -> MetadataValueType {
        match self {
            MetadataValue::Bool(_) => MetadataValueType::Bool,
            MetadataValue::Int(_) => MetadataValueType::Int,
            MetadataValue::Float(_) => MetadataValueType::Float,
            MetadataValue::Str(_) => MetadataValueType::Str,
            MetadataValue::SparseVector(_) => MetadataValueType::SparseVector,
        }
    }
}

impl From<&MetadataValue> for MetadataValueType {
    fn from(value: &MetadataValue) -> Self {
        value.value_type()
    }
}

impl From<bool> for MetadataValue {
    fn from(v: bool) -> Self {
        MetadataValue::Bool(v)
    }
}

impl From<i64> for MetadataValue {
    fn from(v: i64) -> Self {
        MetadataValue::Int(v)
    }
}

impl From<i32> for MetadataValue {
    fn from(v: i32) -> Self {
        MetadataValue::Int(v as i64)
    }
}

impl From<f64> for MetadataValue {
    fn from(v: f64) -> Self {
        MetadataValue::Float(v)
    }
}

impl From<f32> for MetadataValue {
    fn from(v: f32) -> Self {
        MetadataValue::Float(v as f64)
    }
}

impl From<String> for MetadataValue {
    fn from(v: String) -> Self {
        MetadataValue::Str(v)
    }
}

impl From<&str> for MetadataValue {
    fn from(v: &str) -> Self {
        MetadataValue::Str(v.to_string())
    }
}

impl From<SparseVector> for MetadataValue {
    fn from(v: SparseVector) -> Self {
        MetadataValue::SparseVector(v)
    }
}

/// We need `Eq` and `Ord` since we want to use this as a key in `BTreeMap`
///
/// For cross-type comparisons, we define a consistent ordering based on variant position:
/// Bool < Int < Float < Str < SparseVector
#[allow(clippy::derive_ord_xor_partial_ord)]
impl Ord for MetadataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        // Define type ordering based on variant position
        fn type_order(val: &MetadataValue) -> u8 {
            match val {
                MetadataValue::Bool(_) => 0,
                MetadataValue::Int(_) => 1,
                MetadataValue::Float(_) => 2,
                MetadataValue::Str(_) => 3,
                MetadataValue::SparseVector(_) => 4,
            }
        }

        // Chain type ordering with value ordering
        type_order(self).cmp(&type_order(other)).then_with(|| {
            match (self, other) {
                (MetadataValue::Bool(left), MetadataValue::Bool(right)) => left.cmp(right),
                (MetadataValue::Int(left), MetadataValue::Int(right)) => left.cmp(right),
                (MetadataValue::Float(left), MetadataValue::Float(right)) => left.total_cmp(right),
                (MetadataValue::Str(left), MetadataValue::Str(right)) => left.cmp(right),
                (MetadataValue::SparseVector(left), MetadataValue::SparseVector(right)) => {
                    left.cmp(right)
                }
                _ => Ordering::Equal, // Different types, but type_order already handled this
            }
        })
    }
}

impl PartialOrd for MetadataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&MetadataValue> for bool {
    type Error = MetadataValueConversionError;

    fn try_from(value: &MetadataValue) -> Result<Self, Self::Error> {
        match value {
            MetadataValue::Bool(value) => Ok(*value),
            _ => Err(MetadataValueConversionError::InvalidValue),
        }
    }
}

impl TryFrom<&MetadataValue> for i64 {
    type Error = MetadataValueConversionError;

    fn try_from(value: &MetadataValue) -> Result<Self, Self::Error> {
        match value {
            MetadataValue::Int(value) => Ok(*value),
            _ => Err(MetadataValueConversionError::InvalidValue),
        }
    }
}

impl TryFrom<&MetadataValue> for f64 {
    type Error = MetadataValueConversionError;

    fn try_from(value: &MetadataValue) -> Result<Self, Self::Error> {
        match value {
            MetadataValue::Float(value) => Ok(*value),
            _ => Err(MetadataValueConversionError::InvalidValue),
        }
    }
}

impl TryFrom<&MetadataValue> for String {
    type Error = MetadataValueConversionError;

    fn try_from(value: &MetadataValue) -> Result<Self, Self::Error> {
        match value {
            MetadataValue::Str(value) => Ok(value.clone()),
            _ => Err(MetadataValueConversionError::InvalidValue),
        }
    }
}

impl From<MetadataValue> for UpdateMetadataValue {
    fn from(value: MetadataValue) -> Self {
        match value {
            MetadataValue::Bool(v) => UpdateMetadataValue::Bool(v),
            MetadataValue::Int(v) => UpdateMetadataValue::Int(v),
            MetadataValue::Float(v) => UpdateMetadataValue::Float(v),
            MetadataValue::Str(v) => UpdateMetadataValue::Str(v),
            MetadataValue::SparseVector(v) => UpdateMetadataValue::SparseVector(v),
        }
    }
}

impl From<MetadataValue> for Value {
    fn from(value: MetadataValue) -> Self {
        match value {
            MetadataValue::Bool(val) => Self::Bool(val),
            MetadataValue::Int(val) => Self::Number(
                Number::from_i128(val as i128).expect("i64 should be representable in JSON"),
            ),
            MetadataValue::Float(val) => Self::Number(
                Number::from_f64(val).expect("Inf and NaN should not be present in MetadataValue"),
            ),
            MetadataValue::Str(val) => Self::String(val),
            MetadataValue::SparseVector(val) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "indices".to_string(),
                    Value::Array(
                        val.indices
                            .iter()
                            .map(|&i| Value::Number(i.into()))
                            .collect(),
                    ),
                );
                map.insert(
                    "values".to_string(),
                    Value::Array(
                        val.values
                            .iter()
                            .map(|&v| {
                                Value::Number(
                                    Number::from_f64(v as f64)
                                        .expect("Float number should not be NaN or infinite"),
                                )
                            })
                            .collect(),
                    ),
                );
                Self::Object(map)
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum MetadataValueConversionError {
    #[error("Invalid metadata value, valid values are: Int, Float, Str")]
    InvalidValue,
    #[error("Metadata key cannot start with '#' or '$': {0}")]
    InvalidKey(String),
    #[error("Sparse vector indices and values must have the same length")]
    SparseVectorLengthMismatch,
    #[error("Sparse vector indices must be sorted in strictly ascending order (no duplicates)")]
    SparseVectorIndicesNotSorted,
}

impl ChromaError for MetadataValueConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataValueConversionError::InvalidValue => ErrorCodes::InvalidArgument,
            MetadataValueConversionError::InvalidKey(_) => ErrorCodes::InvalidArgument,
            MetadataValueConversionError::SparseVectorLengthMismatch => ErrorCodes::InvalidArgument,
            MetadataValueConversionError::SparseVectorIndicesNotSorted => {
                ErrorCodes::InvalidArgument
            }
        }
    }
}

impl TryFrom<&chroma_proto::UpdateMetadataValue> for MetadataValue {
    type Error = MetadataValueConversionError;

    fn try_from(value: &chroma_proto::UpdateMetadataValue) -> Result<Self, Self::Error> {
        match &value.value {
            Some(chroma_proto::update_metadata_value::Value::BoolValue(value)) => {
                Ok(MetadataValue::Bool(*value))
            }
            Some(chroma_proto::update_metadata_value::Value::IntValue(value)) => {
                Ok(MetadataValue::Int(*value))
            }
            Some(chroma_proto::update_metadata_value::Value::FloatValue(value)) => {
                Ok(MetadataValue::Float(*value))
            }
            Some(chroma_proto::update_metadata_value::Value::StringValue(value)) => {
                Ok(MetadataValue::Str(value.clone()))
            }
            Some(chroma_proto::update_metadata_value::Value::SparseVectorValue(value)) => {
                Ok(MetadataValue::SparseVector(value.clone().into()))
            }
            _ => Err(MetadataValueConversionError::InvalidValue),
        }
    }
}

impl From<MetadataValue> for chroma_proto::UpdateMetadataValue {
    fn from(value: MetadataValue) -> Self {
        match value {
            MetadataValue::Int(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(value)),
            },
            MetadataValue::Float(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::FloatValue(
                    value,
                )),
            },
            MetadataValue::Str(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::StringValue(
                    value,
                )),
            },
            MetadataValue::Bool(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::BoolValue(value)),
            },
            MetadataValue::SparseVector(sparse_vec) => chroma_proto::UpdateMetadataValue {
                value: Some(
                    chroma_proto::update_metadata_value::Value::SparseVectorValue(
                        sparse_vec.into(),
                    ),
                ),
            },
        }
    }
}

/*
===========================================
UpdateMetadata
===========================================
*/
pub type UpdateMetadata = HashMap<String, UpdateMetadataValue>;

/**
 * Check if two metadata are close to equal. Ignores small differences in float values.
 */
pub fn are_update_metadatas_close_to_equal(
    metadata1: &UpdateMetadata,
    metadata2: &UpdateMetadata,
) -> bool {
    assert_eq!(metadata1.len(), metadata2.len());

    for (key, value) in metadata1.iter() {
        if !metadata2.contains_key(key) {
            return false;
        }
        let other_value = metadata2.get(key).unwrap();

        if let (UpdateMetadataValue::Float(value), UpdateMetadataValue::Float(other_value)) =
            (value, other_value)
        {
            if (value - other_value).abs() > 1e-6 {
                return false;
            }
        } else if value != other_value {
            return false;
        }
    }

    true
}

pub fn are_metadatas_close_to_equal(metadata1: &Metadata, metadata2: &Metadata) -> bool {
    assert_eq!(metadata1.len(), metadata2.len());

    for (key, value) in metadata1.iter() {
        if !metadata2.contains_key(key) {
            return false;
        }
        let other_value = metadata2.get(key).unwrap();

        if let (MetadataValue::Float(value), MetadataValue::Float(other_value)) =
            (value, other_value)
        {
            if (value - other_value).abs() > 1e-6 {
                return false;
            }
        } else if value != other_value {
            return false;
        }
    }

    true
}

impl TryFrom<chroma_proto::UpdateMetadata> for UpdateMetadata {
    type Error = UpdateMetadataValueConversionError;

    fn try_from(proto_metadata: chroma_proto::UpdateMetadata) -> Result<Self, Self::Error> {
        let mut metadata = UpdateMetadata::new();
        for (key, value) in proto_metadata.metadata.iter() {
            let value = match value.try_into() {
                Ok(value) => value,
                Err(_) => return Err(UpdateMetadataValueConversionError::InvalidValue),
            };
            metadata.insert(key.clone(), value);
        }
        Ok(metadata)
    }
}

impl From<UpdateMetadata> for chroma_proto::UpdateMetadata {
    fn from(metadata: UpdateMetadata) -> Self {
        let mut metadata = metadata;
        let mut proto_metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        for (key, value) in metadata.drain() {
            let proto_value = value.into();
            proto_metadata.metadata.insert(key.clone(), proto_value);
        }
        proto_metadata
    }
}

/*
===========================================
Metadata
===========================================
*/

pub type Metadata = HashMap<String, MetadataValue>;
pub type DeletedMetadata = HashSet<String>;

pub fn logical_size_of_metadata(metadata: &Metadata) -> usize {
    metadata
        .iter()
        .map(|(k, v)| {
            k.len()
                + match v {
                    MetadataValue::Bool(b) => size_of_val(b),
                    MetadataValue::Int(i) => size_of_val(i),
                    MetadataValue::Float(f) => size_of_val(f),
                    MetadataValue::Str(s) => s.len(),
                    MetadataValue::SparseVector(v) => {
                        size_of_val(&v.indices[..]) + size_of_val(&v.values[..])
                    }
                }
        })
        .sum()
}

pub fn get_metadata_value_as<'a, T>(
    metadata: &'a Metadata,
    key: &str,
) -> Result<T, Box<MetadataValueConversionError>>
where
    T: TryFrom<&'a MetadataValue, Error = MetadataValueConversionError>,
{
    let res = match metadata.get(key) {
        Some(value) => T::try_from(value),
        None => return Err(Box::new(MetadataValueConversionError::InvalidValue)),
    };
    match res {
        Ok(value) => Ok(value),
        Err(_) => Err(Box::new(MetadataValueConversionError::InvalidValue)),
    }
}

impl TryFrom<chroma_proto::UpdateMetadata> for Metadata {
    type Error = MetadataValueConversionError;

    fn try_from(proto_metadata: chroma_proto::UpdateMetadata) -> Result<Self, Self::Error> {
        let mut metadata = Metadata::new();
        for (key, value) in proto_metadata.metadata.iter() {
            let maybe_value: Result<MetadataValue, Self::Error> = value.try_into();
            if maybe_value.is_err() {
                return Err(MetadataValueConversionError::InvalidValue);
            }
            let value = maybe_value.unwrap();
            metadata.insert(key.clone(), value);
        }
        Ok(metadata)
    }
}

impl From<Metadata> for chroma_proto::UpdateMetadata {
    fn from(metadata: Metadata) -> Self {
        let mut metadata = metadata;
        let mut proto_metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        for (key, value) in metadata.drain() {
            let proto_value = value.into();
            proto_metadata.metadata.insert(key.clone(), proto_value);
        }
        proto_metadata
    }
}

#[derive(Debug, Default)]
pub struct MetadataDelta<'referred_data> {
    pub metadata_to_update: HashMap<
        &'referred_data str,
        (&'referred_data MetadataValue, &'referred_data MetadataValue),
    >,
    pub metadata_to_delete: HashMap<&'referred_data str, &'referred_data MetadataValue>,
    pub metadata_to_insert: HashMap<&'referred_data str, &'referred_data MetadataValue>,
}

impl MetadataDelta<'_> {
    pub fn new() -> Self {
        Self::default()
    }
}

/*
===========================================
Metadata queries
===========================================
*/

#[derive(Clone, Debug, Error, PartialEq)]
pub enum WhereConversionError {
    #[error("Error: {0}")]
    Cause(String),
    #[error("{0} -> {1}")]
    Trace(String, Box<Self>),
}

impl WhereConversionError {
    pub fn cause(msg: impl ToString) -> Self {
        Self::Cause(msg.to_string())
    }

    pub fn trace(self, context: impl ToString) -> Self {
        Self::Trace(context.to_string(), Box::new(self))
    }
}

/// This `Where` enum serves as an unified representation for the `where` and `where_document` clauses.
/// Although this is not unified in the API level due to legacy design choices, in the future we will be
/// unifying them together, and the structure of the unified AST should be identical to the one here.
/// Currently both `where` and `where_document` clauses will be translated into `Where`, and if both are
/// present we simply create a conjunction of both clauses as the actual filter. This is consistent with
/// the semantics we used to have when the `where` and `where_document` clauses are treated seperately.
// TODO: Remove this note once the `where` clause and `where_document` clause is unified in the API level.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum Where {
    Composite(CompositeExpression),
    Document(DocumentExpression),
    Metadata(MetadataExpression),
}

impl std::fmt::Display for Where {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Where::Composite(composite) => {
                let fragment = composite
                    .children
                    .iter()
                    .map(|child| format!("{}", child))
                    .collect::<Vec<_>>()
                    .join(match composite.operator {
                        BooleanOperator::And => " & ",
                        BooleanOperator::Or => " | ",
                    });
                write!(f, "({})", fragment)
            }
            Where::Metadata(expr) => write!(f, "{}", expr),
            Where::Document(expr) => write!(f, "{}", expr),
        }
    }
}

impl serde::Serialize for Where {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Where::Composite(composite) => {
                let mut map = serializer.serialize_map(Some(1))?;
                let op_key = match composite.operator {
                    BooleanOperator::And => "$and",
                    BooleanOperator::Or => "$or",
                };
                map.serialize_entry(op_key, &composite.children)?;
                map.end()
            }
            Where::Document(doc) => {
                let mut outer_map = serializer.serialize_map(Some(1))?;
                let mut inner_map = serde_json::Map::new();
                let op_key = match doc.operator {
                    DocumentOperator::Contains => "$contains",
                    DocumentOperator::NotContains => "$not_contains",
                    DocumentOperator::Regex => "$regex",
                    DocumentOperator::NotRegex => "$not_regex",
                };
                inner_map.insert(
                    op_key.to_string(),
                    serde_json::Value::String(doc.pattern.clone()),
                );
                outer_map.serialize_entry("#document", &inner_map)?;
                outer_map.end()
            }
            Where::Metadata(meta) => {
                let mut outer_map = serializer.serialize_map(Some(1))?;
                let mut inner_map = serde_json::Map::new();

                match &meta.comparison {
                    MetadataComparison::Primitive(op, value) => {
                        let op_key = match op {
                            PrimitiveOperator::Equal => "$eq",
                            PrimitiveOperator::NotEqual => "$ne",
                            PrimitiveOperator::GreaterThan => "$gt",
                            PrimitiveOperator::GreaterThanOrEqual => "$gte",
                            PrimitiveOperator::LessThan => "$lt",
                            PrimitiveOperator::LessThanOrEqual => "$lte",
                        };
                        let value_json =
                            serde_json::to_value(value).map_err(serde::ser::Error::custom)?;
                        inner_map.insert(op_key.to_string(), value_json);
                    }
                    MetadataComparison::Set(op, set_value) => {
                        let op_key = match op {
                            SetOperator::In => "$in",
                            SetOperator::NotIn => "$nin",
                        };
                        let values_json = match set_value {
                            MetadataSetValue::Bool(v) => serde_json::to_value(v),
                            MetadataSetValue::Int(v) => serde_json::to_value(v),
                            MetadataSetValue::Float(v) => serde_json::to_value(v),
                            MetadataSetValue::Str(v) => serde_json::to_value(v),
                        }
                        .map_err(serde::ser::Error::custom)?;
                        inner_map.insert(op_key.to_string(), values_json);
                    }
                }

                outer_map.serialize_entry(&meta.key, &inner_map)?;
                outer_map.end()
            }
        }
    }
}

impl From<bool> for Where {
    fn from(value: bool) -> Self {
        if value {
            Where::conjunction(vec![])
        } else {
            Where::disjunction(vec![])
        }
    }
}

impl Where {
    pub fn conjunction(children: impl IntoIterator<Item = Where>) -> Self {
        // If children.len() == 0, we will return a conjunction that is always true.
        // If children.len() == 1, we will return the single child.
        // Otherwise, we will return a conjunction of the children.

        let mut children: Vec<_> = children
            .into_iter()
            .flat_map(|expr| {
                if let Where::Composite(CompositeExpression {
                    operator: BooleanOperator::And,
                    children,
                }) = expr
                {
                    return children;
                }
                vec![expr]
            })
            .dedup()
            .collect();

        if children.len() == 1 {
            return children.pop().expect("just checked len is 1");
        }

        Self::Composite(CompositeExpression {
            operator: BooleanOperator::And,
            children,
        })
    }
    pub fn disjunction(children: impl IntoIterator<Item = Where>) -> Self {
        // If children.len() == 0, we will return a disjunction that is always false.
        // If children.len() == 1, we will return the single child.
        // Otherwise, we will return a disjunction of the children.

        let mut children: Vec<_> = children
            .into_iter()
            .flat_map(|expr| {
                if let Where::Composite(CompositeExpression {
                    operator: BooleanOperator::Or,
                    children,
                }) = expr
                {
                    return children;
                }
                vec![expr]
            })
            .dedup()
            .collect();

        if children.len() == 1 {
            return children.pop().expect("just checked len is 1");
        }

        Self::Composite(CompositeExpression {
            operator: BooleanOperator::Or,
            children,
        })
    }

    pub fn fts_query_length(&self) -> u64 {
        match self {
            Where::Composite(composite_expression) => composite_expression
                .children
                .iter()
                .map(Where::fts_query_length)
                .sum(),
            // The query length is defined to be the number of trigram tokens
            Where::Document(document_expression) => {
                document_expression.pattern.len().max(3) as u64 - 2
            }
            Where::Metadata(_) => 0,
        }
    }

    pub fn metadata_predicate_count(&self) -> u64 {
        match self {
            Where::Composite(composite_expression) => composite_expression
                .children
                .iter()
                .map(Where::metadata_predicate_count)
                .sum(),
            Where::Document(_) => 0,
            Where::Metadata(metadata_expression) => match &metadata_expression.comparison {
                MetadataComparison::Primitive(_, _) => 1,
                MetadataComparison::Set(_, metadata_set_value) => match metadata_set_value {
                    MetadataSetValue::Bool(items) => items.len() as u64,
                    MetadataSetValue::Int(items) => items.len() as u64,
                    MetadataSetValue::Float(items) => items.len() as u64,
                    MetadataSetValue::Str(items) => items.len() as u64,
                },
            },
        }
    }
}

impl BitAnd for Where {
    type Output = Where;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self::conjunction([self, rhs])
    }
}

impl BitOr for Where {
    type Output = Where;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self::disjunction([self, rhs])
    }
}

impl TryFrom<chroma_proto::Where> for Where {
    type Error = WhereConversionError;

    fn try_from(proto_where: chroma_proto::Where) -> Result<Self, Self::Error> {
        let where_inner = proto_where
            .r#where
            .ok_or(WhereConversionError::cause("Invalid Where"))?;
        Ok(match where_inner {
            chroma_proto::r#where::Where::DirectComparison(direct_comparison) => {
                Self::Metadata(direct_comparison.try_into()?)
            }
            chroma_proto::r#where::Where::Children(where_children) => {
                Self::Composite(where_children.try_into()?)
            }
            chroma_proto::r#where::Where::DirectDocumentComparison(direct_where_document) => {
                Self::Document(direct_where_document.into())
            }
        })
    }
}

impl TryFrom<Where> for chroma_proto::Where {
    type Error = WhereConversionError;

    fn try_from(value: Where) -> Result<Self, Self::Error> {
        let proto_where = match value {
            Where::Composite(composite_expression) => {
                chroma_proto::r#where::Where::Children(composite_expression.try_into()?)
            }
            Where::Document(document_expression) => {
                chroma_proto::r#where::Where::DirectDocumentComparison(document_expression.into())
            }
            Where::Metadata(metadata_expression) => chroma_proto::r#where::Where::DirectComparison(
                chroma_proto::DirectComparison::try_from(metadata_expression)
                    .map_err(|err| err.trace("MetadataExpression"))?,
            ),
        };
        Ok(Self {
            r#where: Some(proto_where),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CompositeExpression {
    pub operator: BooleanOperator,
    pub children: Vec<Where>,
}

impl TryFrom<chroma_proto::WhereChildren> for CompositeExpression {
    type Error = WhereConversionError;

    fn try_from(proto_children: chroma_proto::WhereChildren) -> Result<Self, Self::Error> {
        let operator = proto_children.operator().into();
        let children = proto_children
            .children
            .into_iter()
            .map(Where::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.trace("Child Where of CompositeExpression"))?;
        Ok(Self { operator, children })
    }
}

impl TryFrom<CompositeExpression> for chroma_proto::WhereChildren {
    type Error = WhereConversionError;

    fn try_from(value: CompositeExpression) -> Result<Self, Self::Error> {
        Ok(Self {
            operator: chroma_proto::BooleanOperator::from(value.operator) as i32,
            children: value
                .children
                .into_iter()
                .map(chroma_proto::Where::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum BooleanOperator {
    And,
    Or,
}

impl From<chroma_proto::BooleanOperator> for BooleanOperator {
    fn from(value: chroma_proto::BooleanOperator) -> Self {
        match value {
            chroma_proto::BooleanOperator::And => Self::And,
            chroma_proto::BooleanOperator::Or => Self::Or,
        }
    }
}

impl From<BooleanOperator> for chroma_proto::BooleanOperator {
    fn from(value: BooleanOperator) -> Self {
        match value {
            BooleanOperator::And => Self::And,
            BooleanOperator::Or => Self::Or,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DocumentExpression {
    pub operator: DocumentOperator,
    pub pattern: String,
}

impl std::fmt::Display for DocumentExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op_str = match self.operator {
            DocumentOperator::Contains => "CONTAINS",
            DocumentOperator::NotContains => "NOT CONTAINS",
            DocumentOperator::Regex => "REGEX",
            DocumentOperator::NotRegex => "NOT REGEX",
        };
        write!(f, "#document {} \"{}\"", op_str, self.pattern)
    }
}

impl From<chroma_proto::DirectWhereDocument> for DocumentExpression {
    fn from(value: chroma_proto::DirectWhereDocument) -> Self {
        Self {
            operator: value.operator().into(),
            pattern: value.pattern,
        }
    }
}

impl From<DocumentExpression> for chroma_proto::DirectWhereDocument {
    fn from(value: DocumentExpression) -> Self {
        Self {
            pattern: value.pattern,
            operator: chroma_proto::WhereDocumentOperator::from(value.operator) as i32,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum DocumentOperator {
    Contains,
    NotContains,
    Regex,
    NotRegex,
}
impl From<chroma_proto::WhereDocumentOperator> for DocumentOperator {
    fn from(value: chroma_proto::WhereDocumentOperator) -> Self {
        match value {
            chroma_proto::WhereDocumentOperator::Contains => Self::Contains,
            chroma_proto::WhereDocumentOperator::NotContains => Self::NotContains,
            chroma_proto::WhereDocumentOperator::Regex => Self::Regex,
            chroma_proto::WhereDocumentOperator::NotRegex => Self::NotRegex,
        }
    }
}

impl From<DocumentOperator> for chroma_proto::WhereDocumentOperator {
    fn from(value: DocumentOperator) -> Self {
        match value {
            DocumentOperator::Contains => Self::Contains,
            DocumentOperator::NotContains => Self::NotContains,
            DocumentOperator::Regex => Self::Regex,
            DocumentOperator::NotRegex => Self::NotRegex,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct MetadataExpression {
    pub key: String,
    pub comparison: MetadataComparison,
}

impl std::fmt::Display for MetadataExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.comparison {
            MetadataComparison::Primitive(op, value) => {
                write!(f, "{} {} {}", self.key, op, value)
            }
            MetadataComparison::Set(op, set_value) => {
                write!(f, "{} {} {}", self.key, op, set_value)
            }
        }
    }
}

impl TryFrom<chroma_proto::DirectComparison> for MetadataExpression {
    type Error = WhereConversionError;

    fn try_from(value: chroma_proto::DirectComparison) -> Result<Self, Self::Error> {
        let proto_comparison = value
            .comparison
            .ok_or(WhereConversionError::cause("Invalid MetadataExpression"))?;
        let comparison = match proto_comparison {
            chroma_proto::direct_comparison::Comparison::SingleStringOperand(
                single_string_comparison,
            ) => MetadataComparison::Primitive(
                single_string_comparison.comparator().into(),
                MetadataValue::Str(single_string_comparison.value),
            ),
            chroma_proto::direct_comparison::Comparison::StringListOperand(
                string_list_comparison,
            ) => MetadataComparison::Set(
                string_list_comparison.list_operator().into(),
                MetadataSetValue::Str(string_list_comparison.values),
            ),
            chroma_proto::direct_comparison::Comparison::SingleIntOperand(
                single_int_comparison,
            ) => MetadataComparison::Primitive(
                match single_int_comparison
                    .comparator
                    .ok_or(WhereConversionError::cause(
                        "Invalid scalar integer operator",
                    ))? {
                    chroma_proto::single_int_comparison::Comparator::GenericComparator(op) => {
                        chroma_proto::GenericComparator::try_from(op)
                            .map_err(WhereConversionError::cause)?
                            .into()
                    }
                    chroma_proto::single_int_comparison::Comparator::NumberComparator(op) => {
                        chroma_proto::NumberComparator::try_from(op)
                            .map_err(WhereConversionError::cause)?
                            .into()
                    }
                },
                MetadataValue::Int(single_int_comparison.value),
            ),
            chroma_proto::direct_comparison::Comparison::IntListOperand(int_list_comparison) => {
                MetadataComparison::Set(
                    int_list_comparison.list_operator().into(),
                    MetadataSetValue::Int(int_list_comparison.values),
                )
            }
            chroma_proto::direct_comparison::Comparison::SingleDoubleOperand(
                single_double_comparison,
            ) => MetadataComparison::Primitive(
                match single_double_comparison
                    .comparator
                    .ok_or(WhereConversionError::cause("Invalid scalar float operator"))?
                {
                    chroma_proto::single_double_comparison::Comparator::GenericComparator(op) => {
                        chroma_proto::GenericComparator::try_from(op)
                            .map_err(WhereConversionError::cause)?
                            .into()
                    }
                    chroma_proto::single_double_comparison::Comparator::NumberComparator(op) => {
                        chroma_proto::NumberComparator::try_from(op)
                            .map_err(WhereConversionError::cause)?
                            .into()
                    }
                },
                MetadataValue::Float(single_double_comparison.value),
            ),
            chroma_proto::direct_comparison::Comparison::DoubleListOperand(
                double_list_comparison,
            ) => MetadataComparison::Set(
                double_list_comparison.list_operator().into(),
                MetadataSetValue::Float(double_list_comparison.values),
            ),
            chroma_proto::direct_comparison::Comparison::BoolListOperand(bool_list_comparison) => {
                MetadataComparison::Set(
                    bool_list_comparison.list_operator().into(),
                    MetadataSetValue::Bool(bool_list_comparison.values),
                )
            }
            chroma_proto::direct_comparison::Comparison::SingleBoolOperand(
                single_bool_comparison,
            ) => MetadataComparison::Primitive(
                single_bool_comparison.comparator().into(),
                MetadataValue::Bool(single_bool_comparison.value),
            ),
        };
        Ok(Self {
            key: value.key,
            comparison,
        })
    }
}

impl TryFrom<MetadataExpression> for chroma_proto::DirectComparison {
    type Error = WhereConversionError;

    fn try_from(value: MetadataExpression) -> Result<Self, Self::Error> {
        let comparison = match value.comparison {
            MetadataComparison::Primitive(primitive_operator, metadata_value) => match metadata_value {
                MetadataValue::Bool(value) => chroma_proto::direct_comparison::Comparison::SingleBoolOperand(chroma_proto::SingleBoolComparison { value, comparator: chroma_proto::GenericComparator::try_from(primitive_operator)? as i32 }),
                MetadataValue::Int(value) => chroma_proto::direct_comparison::Comparison::SingleIntOperand(chroma_proto::SingleIntComparison { value, comparator: Some(match primitive_operator {
                                generic_operator @ PrimitiveOperator::Equal | generic_operator @ PrimitiveOperator::NotEqual => chroma_proto::single_int_comparison::Comparator::GenericComparator(chroma_proto::GenericComparator::try_from(generic_operator)? as i32),
                                numeric => chroma_proto::single_int_comparison::Comparator::NumberComparator(chroma_proto::NumberComparator::try_from(numeric)? as i32) }),
                            }),
                MetadataValue::Float(value) => chroma_proto::direct_comparison::Comparison::SingleDoubleOperand(chroma_proto::SingleDoubleComparison { value, comparator: Some(match primitive_operator {
                                generic_operator @ PrimitiveOperator::Equal | generic_operator @ PrimitiveOperator::NotEqual => chroma_proto::single_double_comparison::Comparator::GenericComparator(chroma_proto::GenericComparator::try_from(generic_operator)? as i32),
                                numeric => chroma_proto::single_double_comparison::Comparator::NumberComparator(chroma_proto::NumberComparator::try_from(numeric)? as i32) }),
                            }),
                MetadataValue::Str(value) => chroma_proto::direct_comparison::Comparison::SingleStringOperand(chroma_proto::SingleStringComparison { value, comparator: chroma_proto::GenericComparator::try_from(primitive_operator)? as i32 }),
                MetadataValue::SparseVector(_) => return Err(WhereConversionError::Cause("Comparison with sparse vector is not supported".to_string())),
            },
            MetadataComparison::Set(set_operator, metadata_set_value) => match metadata_set_value {
                MetadataSetValue::Bool(vec) => chroma_proto::direct_comparison::Comparison::BoolListOperand(chroma_proto::BoolListComparison { values: vec, list_operator: chroma_proto::ListOperator::from(set_operator) as i32 }),
                MetadataSetValue::Int(vec) => chroma_proto::direct_comparison::Comparison::IntListOperand(chroma_proto::IntListComparison { values: vec, list_operator: chroma_proto::ListOperator::from(set_operator) as i32 }),
                MetadataSetValue::Float(vec) => chroma_proto::direct_comparison::Comparison::DoubleListOperand(chroma_proto::DoubleListComparison { values: vec, list_operator: chroma_proto::ListOperator::from(set_operator) as i32 }),
                MetadataSetValue::Str(vec) => chroma_proto::direct_comparison::Comparison::StringListOperand(chroma_proto::StringListComparison { values: vec, list_operator: chroma_proto::ListOperator::from(set_operator) as i32 }),
            },
        };
        Ok(Self {
            key: value.key,
            comparison: Some(comparison),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum MetadataComparison {
    Primitive(PrimitiveOperator, MetadataValue),
    Set(SetOperator, MetadataSetValue),
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
pub enum PrimitiveOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl std::fmt::Display for PrimitiveOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op_str = match self {
            PrimitiveOperator::Equal => "=",
            PrimitiveOperator::NotEqual => "≠",
            PrimitiveOperator::GreaterThan => ">",
            PrimitiveOperator::GreaterThanOrEqual => "≥",
            PrimitiveOperator::LessThan => "<",
            PrimitiveOperator::LessThanOrEqual => "≤",
        };
        write!(f, "{}", op_str)
    }
}

impl From<chroma_proto::GenericComparator> for PrimitiveOperator {
    fn from(value: chroma_proto::GenericComparator) -> Self {
        match value {
            chroma_proto::GenericComparator::Eq => Self::Equal,
            chroma_proto::GenericComparator::Ne => Self::NotEqual,
        }
    }
}

impl TryFrom<PrimitiveOperator> for chroma_proto::GenericComparator {
    type Error = WhereConversionError;

    fn try_from(value: PrimitiveOperator) -> Result<Self, Self::Error> {
        match value {
            PrimitiveOperator::Equal => Ok(Self::Eq),
            PrimitiveOperator::NotEqual => Ok(Self::Ne),
            op => Err(WhereConversionError::cause(format!("{op:?} ∉ [=, ≠]"))),
        }
    }
}

impl From<chroma_proto::NumberComparator> for PrimitiveOperator {
    fn from(value: chroma_proto::NumberComparator) -> Self {
        match value {
            chroma_proto::NumberComparator::Gt => Self::GreaterThan,
            chroma_proto::NumberComparator::Gte => Self::GreaterThanOrEqual,
            chroma_proto::NumberComparator::Lt => Self::LessThan,
            chroma_proto::NumberComparator::Lte => Self::LessThanOrEqual,
        }
    }
}

impl TryFrom<PrimitiveOperator> for chroma_proto::NumberComparator {
    type Error = WhereConversionError;

    fn try_from(value: PrimitiveOperator) -> Result<Self, Self::Error> {
        match value {
            PrimitiveOperator::GreaterThan => Ok(Self::Gt),
            PrimitiveOperator::GreaterThanOrEqual => Ok(Self::Gte),
            PrimitiveOperator::LessThan => Ok(Self::Lt),
            PrimitiveOperator::LessThanOrEqual => Ok(Self::Lte),
            op => Err(WhereConversionError::cause(format!(
                "{op:?} ∉ [≤, <, >, ≥]"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
pub enum SetOperator {
    In,
    NotIn,
}

impl std::fmt::Display for SetOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op_str = match self {
            SetOperator::In => "∈",
            SetOperator::NotIn => "∉",
        };
        write!(f, "{}", op_str)
    }
}

impl From<chroma_proto::ListOperator> for SetOperator {
    fn from(value: chroma_proto::ListOperator) -> Self {
        match value {
            chroma_proto::ListOperator::In => Self::In,
            chroma_proto::ListOperator::Nin => Self::NotIn,
        }
    }
}

impl From<SetOperator> for chroma_proto::ListOperator {
    fn from(value: SetOperator) -> Self {
        match value {
            SetOperator::In => Self::In,
            SetOperator::NotIn => Self::Nin,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
pub enum MetadataSetValue {
    Bool(Vec<bool>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    Str(Vec<String>),
}

impl std::fmt::Display for MetadataSetValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataSetValue::Bool(values) => {
                let values_str = values
                    .iter()
                    .map(|v| format!("\"{}\"", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{}]", values_str)
            }
            MetadataSetValue::Int(values) => {
                let values_str = values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{}]", values_str)
            }
            MetadataSetValue::Float(values) => {
                let values_str = values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{}]", values_str)
            }
            MetadataSetValue::Str(values) => {
                let values_str = values
                    .iter()
                    .map(|v| format!("\"{}\"", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{}]", values_str)
            }
        }
    }
}

impl MetadataSetValue {
    pub fn value_type(&self) -> MetadataValueType {
        match self {
            MetadataSetValue::Bool(_) => MetadataValueType::Bool,
            MetadataSetValue::Int(_) => MetadataValueType::Int,
            MetadataSetValue::Float(_) => MetadataValueType::Float,
            MetadataSetValue::Str(_) => MetadataValueType::Str,
        }
    }
}

impl From<Vec<bool>> for MetadataSetValue {
    fn from(values: Vec<bool>) -> Self {
        MetadataSetValue::Bool(values)
    }
}

impl From<Vec<i64>> for MetadataSetValue {
    fn from(values: Vec<i64>) -> Self {
        MetadataSetValue::Int(values)
    }
}

impl From<Vec<i32>> for MetadataSetValue {
    fn from(values: Vec<i32>) -> Self {
        MetadataSetValue::Int(values.into_iter().map(|v| v as i64).collect())
    }
}

impl From<Vec<f64>> for MetadataSetValue {
    fn from(values: Vec<f64>) -> Self {
        MetadataSetValue::Float(values)
    }
}

impl From<Vec<f32>> for MetadataSetValue {
    fn from(values: Vec<f32>) -> Self {
        MetadataSetValue::Float(values.into_iter().map(|v| v as f64).collect())
    }
}

impl From<Vec<String>> for MetadataSetValue {
    fn from(values: Vec<String>) -> Self {
        MetadataSetValue::Str(values)
    }
}

impl From<Vec<&str>> for MetadataSetValue {
    fn from(values: Vec<&str>) -> Self {
        MetadataSetValue::Str(values.into_iter().map(|s| s.to_string()).collect())
    }
}

// TODO: Deprecate where_document
impl TryFrom<chroma_proto::WhereDocument> for Where {
    type Error = WhereConversionError;

    fn try_from(proto_document: chroma_proto::WhereDocument) -> Result<Self, Self::Error> {
        match proto_document.r#where_document {
            Some(chroma_proto::where_document::WhereDocument::Direct(proto_comparison)) => {
                let operator = match TryInto::<chroma_proto::WhereDocumentOperator>::try_into(
                    proto_comparison.operator,
                ) {
                    Ok(operator) => operator,
                    Err(_) => {
                        return Err(WhereConversionError::cause(
                            "[Deprecated] Invalid where document operator",
                        ))
                    }
                };
                let comparison = DocumentExpression {
                    pattern: proto_comparison.pattern,
                    operator: operator.into(),
                };
                Ok(Where::Document(comparison))
            }
            Some(chroma_proto::where_document::WhereDocument::Children(proto_children)) => {
                let operator = match TryInto::<chroma_proto::BooleanOperator>::try_into(
                    proto_children.operator,
                ) {
                    Ok(operator) => operator,
                    Err(_) => {
                        return Err(WhereConversionError::cause(
                            "[Deprecated] Invalid boolean operator",
                        ))
                    }
                };
                let children = CompositeExpression {
                    children: proto_children
                        .children
                        .into_iter()
                        .map(|child| child.try_into())
                        .collect::<Result<_, _>>()?,
                    operator: operator.into(),
                };
                Ok(Where::Composite(children))
            }
            None => Err(WhereConversionError::cause("[Deprecated] Invalid where")),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::Key;

    use super::*;

    #[test]
    fn test_update_metadata_try_from() {
        let mut proto_metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        proto_metadata.metadata.insert(
            "foo".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(42)),
            },
        );
        proto_metadata.metadata.insert(
            "bar".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::FloatValue(42.0)),
            },
        );
        proto_metadata.metadata.insert(
            "baz".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::StringValue(
                    "42".to_string(),
                )),
            },
        );
        // Add sparse vector test
        proto_metadata.metadata.insert(
            "sparse".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(
                    chroma_proto::update_metadata_value::Value::SparseVectorValue(
                        chroma_proto::SparseVector {
                            indices: vec![0, 5, 10],
                            values: vec![0.1, 0.5, 0.9],
                        },
                    ),
                ),
            },
        );
        let converted_metadata: UpdateMetadata = proto_metadata.try_into().unwrap();
        assert_eq!(converted_metadata.len(), 4);
        assert_eq!(
            converted_metadata.get("foo").unwrap(),
            &UpdateMetadataValue::Int(42)
        );
        assert_eq!(
            converted_metadata.get("bar").unwrap(),
            &UpdateMetadataValue::Float(42.0)
        );
        assert_eq!(
            converted_metadata.get("baz").unwrap(),
            &UpdateMetadataValue::Str("42".to_string())
        );
        assert_eq!(
            converted_metadata.get("sparse").unwrap(),
            &UpdateMetadataValue::SparseVector(SparseVector::new(
                vec![0, 5, 10],
                vec![0.1, 0.5, 0.9]
            ))
        );
    }

    #[test]
    fn test_metadata_try_from() {
        let mut proto_metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        proto_metadata.metadata.insert(
            "foo".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(42)),
            },
        );
        proto_metadata.metadata.insert(
            "bar".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::FloatValue(42.0)),
            },
        );
        proto_metadata.metadata.insert(
            "baz".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::StringValue(
                    "42".to_string(),
                )),
            },
        );
        // Add sparse vector test
        proto_metadata.metadata.insert(
            "sparse".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(
                    chroma_proto::update_metadata_value::Value::SparseVectorValue(
                        chroma_proto::SparseVector {
                            indices: vec![1, 10, 100],
                            values: vec![0.2, 0.4, 0.6],
                        },
                    ),
                ),
            },
        );
        let converted_metadata: Metadata = proto_metadata.try_into().unwrap();
        assert_eq!(converted_metadata.len(), 4);
        assert_eq!(
            converted_metadata.get("foo").unwrap(),
            &MetadataValue::Int(42)
        );
        assert_eq!(
            converted_metadata.get("bar").unwrap(),
            &MetadataValue::Float(42.0)
        );
        assert_eq!(
            converted_metadata.get("baz").unwrap(),
            &MetadataValue::Str("42".to_string())
        );
        assert_eq!(
            converted_metadata.get("sparse").unwrap(),
            &MetadataValue::SparseVector(SparseVector::new(vec![1, 10, 100], vec![0.2, 0.4, 0.6]))
        );
    }

    #[test]
    fn test_where_clause_simple_from() {
        let proto_where = chroma_proto::Where {
            r#where: Some(chroma_proto::r#where::Where::DirectComparison(
                chroma_proto::DirectComparison {
                    key: "foo".to_string(),
                    comparison: Some(
                        chroma_proto::direct_comparison::Comparison::SingleIntOperand(
                            chroma_proto::SingleIntComparison {
                                value: 42,
                                comparator: Some(chroma_proto::single_int_comparison::Comparator::GenericComparator(chroma_proto::GenericComparator::Eq as i32)),
                            },
                        ),
                    ),
                },
            )),
        };
        let where_clause: Where = proto_where.try_into().unwrap();
        match where_clause {
            Where::Metadata(comparison) => {
                assert_eq!(comparison.key, "foo");
                match comparison.comparison {
                    MetadataComparison::Primitive(_, value) => {
                        assert_eq!(value, MetadataValue::Int(42));
                    }
                    _ => panic!("Invalid comparison type"),
                }
            }
            _ => panic!("Invalid where type"),
        }
    }

    #[test]
    fn test_where_clause_with_children() {
        let proto_where = chroma_proto::Where {
            r#where: Some(chroma_proto::r#where::Where::Children(
                chroma_proto::WhereChildren {
                    children: vec![
                        chroma_proto::Where {
                            r#where: Some(chroma_proto::r#where::Where::DirectComparison(
                                chroma_proto::DirectComparison {
                                    key: "foo".to_string(),
                                    comparison: Some(
                                        chroma_proto::direct_comparison::Comparison::SingleIntOperand(
                                            chroma_proto::SingleIntComparison {
                                                value: 42,
                                                comparator: Some(chroma_proto::single_int_comparison::Comparator::GenericComparator(chroma_proto::GenericComparator::Eq as i32)),
                                            },
                                        ),
                                    ),
                                },
                            )),
                        },
                        chroma_proto::Where {
                            r#where: Some(chroma_proto::r#where::Where::DirectComparison(
                                chroma_proto::DirectComparison {
                                    key: "bar".to_string(),
                                    comparison: Some(
                                        chroma_proto::direct_comparison::Comparison::SingleIntOperand(
                                            chroma_proto::SingleIntComparison {
                                                value: 42,
                                                comparator: Some(chroma_proto::single_int_comparison::Comparator::GenericComparator(chroma_proto::GenericComparator::Eq as i32)),
                                            },
                                        ),
                                    ),
                                },
                            )),
                        },
                    ],
                    operator: chroma_proto::BooleanOperator::And.into(),
                },
            )),
        };
        let where_clause: Where = proto_where.try_into().unwrap();
        match where_clause {
            Where::Composite(children) => {
                assert_eq!(children.children.len(), 2);
                assert_eq!(children.operator, BooleanOperator::And);
            }
            _ => panic!("Invalid where type"),
        }
    }

    #[test]
    fn test_where_document_simple() {
        let proto_where = chroma_proto::WhereDocument {
            r#where_document: Some(chroma_proto::where_document::WhereDocument::Direct(
                chroma_proto::DirectWhereDocument {
                    pattern: "foo".to_string(),
                    operator: chroma_proto::WhereDocumentOperator::Contains.into(),
                },
            )),
        };
        let where_document: Where = proto_where.try_into().unwrap();
        match where_document {
            Where::Document(comparison) => {
                assert_eq!(comparison.pattern, "foo");
                assert_eq!(comparison.operator, DocumentOperator::Contains);
            }
            _ => panic!("Invalid where document type"),
        }
    }

    #[test]
    fn test_where_document_with_children() {
        let proto_where = chroma_proto::WhereDocument {
            r#where_document: Some(chroma_proto::where_document::WhereDocument::Children(
                chroma_proto::WhereDocumentChildren {
                    children: vec![
                        chroma_proto::WhereDocument {
                            r#where_document: Some(
                                chroma_proto::where_document::WhereDocument::Direct(
                                    chroma_proto::DirectWhereDocument {
                                        pattern: "foo".to_string(),
                                        operator: chroma_proto::WhereDocumentOperator::Contains
                                            .into(),
                                    },
                                ),
                            ),
                        },
                        chroma_proto::WhereDocument {
                            r#where_document: Some(
                                chroma_proto::where_document::WhereDocument::Direct(
                                    chroma_proto::DirectWhereDocument {
                                        pattern: "bar".to_string(),
                                        operator: chroma_proto::WhereDocumentOperator::Contains
                                            .into(),
                                    },
                                ),
                            ),
                        },
                    ],
                    operator: chroma_proto::BooleanOperator::And.into(),
                },
            )),
        };
        let where_document: Where = proto_where.try_into().unwrap();
        match where_document {
            Where::Composite(children) => {
                assert_eq!(children.children.len(), 2);
                assert_eq!(children.operator, BooleanOperator::And);
            }
            _ => panic!("Invalid where document type"),
        }
    }

    #[test]
    fn test_sparse_vector_new() {
        let indices = vec![0, 5, 10];
        let values = vec![0.1, 0.5, 0.9];
        let sparse = SparseVector::new(indices.clone(), values.clone());
        assert_eq!(sparse.indices, indices);
        assert_eq!(sparse.values, values);
    }

    #[test]
    fn test_sparse_vector_from_pairs() {
        let pairs = vec![(0, 0.1), (5, 0.5), (10, 0.9)];
        let sparse = SparseVector::from_pairs(pairs.clone());
        assert_eq!(sparse.indices, vec![0, 5, 10]);
        assert_eq!(sparse.values, vec![0.1, 0.5, 0.9]);
    }

    #[test]
    fn test_sparse_vector_iter() {
        let sparse = SparseVector::new(vec![0, 5, 10], vec![0.1, 0.5, 0.9]);
        let collected: Vec<(u32, f32)> = sparse.iter().collect();
        assert_eq!(collected, vec![(0, 0.1), (5, 0.5), (10, 0.9)]);
    }

    #[test]
    fn test_sparse_vector_ordering() {
        let sparse1 = SparseVector::new(vec![0, 5], vec![0.1, 0.5]);
        let sparse2 = SparseVector::new(vec![0, 5], vec![0.1, 0.5]);
        let sparse3 = SparseVector::new(vec![0, 6], vec![0.1, 0.5]);
        let sparse4 = SparseVector::new(vec![0, 5], vec![0.1, 0.6]);

        assert_eq!(sparse1, sparse2);
        assert!(sparse1 < sparse3);
        assert!(sparse1 < sparse4);
    }

    #[test]
    fn test_sparse_vector_proto_conversion() {
        let sparse = SparseVector::new(vec![1, 10, 100], vec![0.2, 0.4, 0.6]);
        let proto: chroma_proto::SparseVector = sparse.clone().into();
        assert_eq!(proto.indices, vec![1, 10, 100]);
        assert_eq!(proto.values, vec![0.2, 0.4, 0.6]);

        let converted: SparseVector = proto.into();
        assert_eq!(converted, sparse);
    }

    #[test]
    fn test_sparse_vector_logical_size() {
        let metadata = Metadata::from([(
            "sparse".to_string(),
            MetadataValue::SparseVector(SparseVector::new(
                vec![0, 1, 2, 3, 4],
                vec![0.1, 0.2, 0.3, 0.4, 0.5],
            )),
        )]);

        let size = logical_size_of_metadata(&metadata);
        // Size should include the key string length and the sparse vector data
        // "sparse" = 6 bytes + 5 * 4 bytes (u32 indices) + 5 * 4 bytes (f32 values) = 46 bytes
        assert_eq!(size, 46);
    }

    #[test]
    fn test_sparse_vector_validation() {
        // Valid sparse vector
        let sparse = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2, 0.3]);
        assert!(sparse.validate().is_ok());

        // Length mismatch
        let sparse = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2]);
        let result = sparse.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MetadataValueConversionError::SparseVectorLengthMismatch
        ));

        // Unsorted indices (descending order)
        let sparse = SparseVector::new(vec![3, 1, 2], vec![0.3, 0.1, 0.2]);
        let result = sparse.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MetadataValueConversionError::SparseVectorIndicesNotSorted
        ));

        // Duplicate indices (not strictly ascending)
        let sparse = SparseVector::new(vec![1, 2, 2, 3], vec![0.1, 0.2, 0.3, 0.4]);
        let result = sparse.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MetadataValueConversionError::SparseVectorIndicesNotSorted
        ));

        // Descending at one point
        let sparse = SparseVector::new(vec![1, 3, 2], vec![0.1, 0.3, 0.2]);
        let result = sparse.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MetadataValueConversionError::SparseVectorIndicesNotSorted
        ));
    }

    #[test]
    fn test_sparse_vector_deserialize_old_format() {
        // Old format without #type field (backward compatibility)
        let json = r#"{"indices": [0, 1, 2], "values": [1.0, 2.0, 3.0]}"#;
        let sv: SparseVector = serde_json::from_str(json).unwrap();
        assert_eq!(sv.indices, vec![0, 1, 2]);
        assert_eq!(sv.values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_sparse_vector_deserialize_new_format() {
        // New format with #type field
        let json =
            "{\"#type\": \"sparse_vector\", \"indices\": [0, 1, 2], \"values\": [1.0, 2.0, 3.0]}";
        let sv: SparseVector = serde_json::from_str(json).unwrap();
        assert_eq!(sv.indices, vec![0, 1, 2]);
        assert_eq!(sv.values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_sparse_vector_deserialize_new_format_field_order() {
        // New format with different field order (should still work)
        let json = "{\"indices\": [5, 10], \"#type\": \"sparse_vector\", \"values\": [0.5, 1.0]}";
        let sv: SparseVector = serde_json::from_str(json).unwrap();
        assert_eq!(sv.indices, vec![5, 10]);
        assert_eq!(sv.values, vec![0.5, 1.0]);
    }

    #[test]
    fn test_sparse_vector_deserialize_wrong_type_tag() {
        // Wrong #type field value should fail
        let json = "{\"#type\": \"dense_vector\", \"indices\": [0, 1], \"values\": [1.0, 2.0]}";
        let result: Result<SparseVector, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("sparse_vector"));
    }

    #[test]
    fn test_sparse_vector_serialize_always_has_type() {
        // Serialization should always include #type field
        let sv = SparseVector::new(vec![0, 1, 2], vec![1.0, 2.0, 3.0]);
        let json = serde_json::to_value(&sv).unwrap();

        assert_eq!(json["#type"], "sparse_vector");
        assert_eq!(json["indices"], serde_json::json!([0, 1, 2]));
        assert_eq!(json["values"], serde_json::json!([1.0, 2.0, 3.0]));
    }

    #[test]
    fn test_sparse_vector_roundtrip_with_type() {
        // Test that serialize -> deserialize preserves the data
        let original = SparseVector::new(vec![0, 5, 10, 15], vec![0.1, 0.5, 1.0, 1.5]);
        let json = serde_json::to_string(&original).unwrap();

        // Verify the serialized JSON contains #type
        assert!(json.contains("\"#type\":\"sparse_vector\""));

        let deserialized: SparseVector = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_sparse_vector_in_metadata_old_format() {
        // Test that old format works when sparse vector is in metadata
        let json = r#"{"key": "value", "sparse": {"indices": [0, 1], "values": [1.0, 2.0]}}"#;
        let map: HashMap<String, serde_json::Value> = serde_json::from_str(json).unwrap();

        let sparse_value = &map["sparse"];
        let sv: SparseVector = serde_json::from_value(sparse_value.clone()).unwrap();
        assert_eq!(sv.indices, vec![0, 1]);
        assert_eq!(sv.values, vec![1.0, 2.0]);
    }

    #[test]
    fn test_sparse_vector_in_metadata_new_format() {
        // Test that new format works when sparse vector is in metadata
        let json = "{\"key\": \"value\", \"sparse\": {\"#type\": \"sparse_vector\", \"indices\": [0, 1], \"values\": [1.0, 2.0]}}";
        let map: HashMap<String, serde_json::Value> = serde_json::from_str(json).unwrap();

        let sparse_value = &map["sparse"];
        let sv: SparseVector = serde_json::from_value(sparse_value.clone()).unwrap();
        assert_eq!(sv.indices, vec![0, 1]);
        assert_eq!(sv.values, vec![1.0, 2.0]);
    }

    #[test]
    fn test_simplifies_identities() {
        let all: Where = true.into();
        assert_eq!(all.clone() & all.clone(), true.into());
        assert_eq!(all.clone() | all.clone(), true.into());

        let foo = Key::field("foo").eq("bar");
        assert_eq!(foo.clone() & all.clone(), foo.clone());
        assert_eq!(all.clone() & foo.clone(), foo.clone());

        let none: Where = false.into();
        assert_eq!(foo.clone() | none.clone(), foo.clone());
        assert_eq!(none | foo.clone(), foo);
    }

    #[test]
    fn test_flattens() {
        let foo = Key::field("foo").eq("bar");
        let baz = Key::field("baz").eq("quux");

        let and_nested = foo.clone() & (baz.clone() & foo.clone());
        assert_eq!(
            and_nested,
            Where::Composite(CompositeExpression {
                operator: BooleanOperator::And,
                children: vec![foo.clone(), baz.clone(), foo.clone()]
            })
        );

        let or_nested = foo.clone() | (baz.clone() | foo.clone());
        assert_eq!(
            or_nested,
            Where::Composite(CompositeExpression {
                operator: BooleanOperator::Or,
                children: vec![foo.clone(), baz.clone(), foo.clone()]
            })
        );
    }
}
