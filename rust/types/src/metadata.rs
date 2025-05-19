use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};
use thiserror::Error;
use utoipa::ToSchema;

use crate::chroma_proto;

#[cfg(feature = "pyo3")]
use pyo3::{types::PyAnyMethods, FromPyObject, IntoPyObject};

#[cfg(feature = "testing")]
use proptest::prelude::*;

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize, ToSchema)]
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
    None,
}

#[cfg(feature = "pyo3")]
impl FromPyObject<'_> for UpdateMetadataValue {
    fn extract_bound(ob: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        if let Ok(value) = ob.extract::<bool>() {
            Ok(UpdateMetadataValue::Bool(value))
        } else if let Ok(value) = ob.extract::<i64>() {
            Ok(UpdateMetadataValue::Int(value))
        } else if let Ok(value) = ob.extract::<f64>() {
            Ok(UpdateMetadataValue::Float(value))
        } else if let Ok(value) = ob.extract::<String>() {
            Ok(UpdateMetadataValue::Str(value))
        } else {
            Ok(UpdateMetadataValue::None)
        }
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
            UpdateMetadataValue::None => Err(MetadataValueConversionError::InvalidValue),
        }
    }
}

/*
===========================================
MetadataValue
===========================================
*/

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize, ToSchema)]
#[cfg_attr(feature = "pyo3", derive(FromPyObject, IntoPyObject))]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
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
}

impl Eq for MetadataValue {}

/// We need `Eq` and `Ord` since we want to use this as a key in `BTreeMap`
/// We are not planning to support `f64::NaN`s anyway, so the `PartialOrd` and `Ord` should be identical
#[allow(clippy::derive_ord_xor_partial_ord)]
impl Ord for MetadataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
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
        }
    }
}

#[derive(Error, Debug)]
pub enum MetadataValueConversionError {
    #[error("Invalid metadata value, valid values are: Int, Float, Str")]
    InvalidValue,
}

impl ChromaError for MetadataValueConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataValueConversionError::InvalidValue => ErrorCodes::InvalidArgument,
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
#[derive(Clone, Debug, PartialEq, ToSchema)]
pub enum Where {
    Composite(CompositeExpression),
    Document(DocumentExpression),
    Metadata(MetadataExpression),
}

impl serde::Serialize for Where {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        todo!()
    }
}

impl Where {
    pub fn conjunction(children: Vec<Where>) -> Self {
        Self::Composite(CompositeExpression {
            operator: BooleanOperator::And,
            children,
        })
    }
    pub fn disjunction(children: Vec<Where>) -> Self {
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

#[derive(Clone, Debug, PartialEq, ToSchema)]
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

#[derive(Clone, Debug, PartialEq, ToSchema)]
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

#[derive(Clone, Debug, PartialEq, ToSchema)]
pub struct DocumentExpression {
    pub operator: DocumentOperator,
    pub pattern: String,
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

#[derive(Clone, Debug, PartialEq, ToSchema)]
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

#[derive(Clone, Debug, PartialEq, ToSchema)]
pub struct MetadataExpression {
    pub key: String,
    pub comparison: MetadataComparison,
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

#[derive(Clone, Debug, PartialEq, ToSchema)]
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

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
pub enum SetOperator {
    In,
    NotIn,
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

impl From<MetadataValue> for MetadataSetValue {
    fn from(value: MetadataValue) -> Self {
        match value {
            MetadataValue::Bool(value) => Self::Bool(vec![value]),
            MetadataValue::Int(value) => Self::Int(vec![value]),
            MetadataValue::Float(value) => Self::Float(vec![value]),
            MetadataValue::Str(value) => Self::Str(vec![value]),
        }
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
        let converted_metadata: UpdateMetadata = proto_metadata.try_into().unwrap();
        assert_eq!(converted_metadata.len(), 3);
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
        let converted_metadata: Metadata = proto_metadata.try_into().unwrap();
        assert_eq!(converted_metadata.len(), 3);
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
}
