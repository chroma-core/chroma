use chroma_error::{ChromaError, ErrorCodes};
use roaring::RoaringBitmap;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    ops::{BitAnd, BitOr},
};
use thiserror::Error;

use crate::chroma_proto;

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateMetadataValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    None,
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
        let proto_value = match value {
            UpdateMetadataValue::Bool(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::BoolValue(value)),
            },
            UpdateMetadataValue::Int(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(
                    value as i64,
                )),
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
        };
        proto_value
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

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum MetadataValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl Eq for MetadataValue {}

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
        let proto_value = match value {
            MetadataValue::Bool(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::BoolValue(value)),
            },
            MetadataValue::Int(value) => chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(
                    value as i64,
                )),
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
        };
        proto_value
    }
}

/*
===========================================
UpdateMetadata
===========================================
*/
pub type UpdateMetadata = HashMap<String, UpdateMetadataValue>;

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

/*
===========================================
Metadata
===========================================
*/

pub type Metadata = HashMap<String, MetadataValue>;
pub type DeletedMetadata = HashSet<String>;

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

pub struct MetadataDelta<'referred_data> {
    pub metadata_to_update: HashMap<
        &'referred_data str,
        (&'referred_data MetadataValue, &'referred_data MetadataValue),
    >,
    pub metadata_to_delete: HashMap<&'referred_data str, &'referred_data MetadataValue>,
    pub metadata_to_insert: HashMap<&'referred_data str, &'referred_data MetadataValue>,
}

impl<'referred_data> MetadataDelta<'referred_data> {
    pub fn new() -> Self {
        Self {
            metadata_to_update: HashMap::new(),
            metadata_to_delete: HashMap::new(),
            metadata_to_insert: HashMap::new(),
        }
    }
}

/*
===========================================
Metadata queries
===========================================
*/

#[derive(Clone, Debug, PartialEq)]
pub enum Where {
    DirectWhereComparison(DirectWhereComparison),
    DirectWhereDocumentComparison(DirectDocumentComparison),
    WhereChildren(WhereChildren),
}

impl Where {
    pub fn conjunction(children: Vec<Where>) -> Self {
        Self::WhereChildren(WhereChildren {
            operator: BooleanOperator::And,
            children,
        })
    }
    pub fn disjunction(children: Vec<Where>) -> Self {
        Self::WhereChildren(WhereChildren {
            operator: BooleanOperator::Or,
            children,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectWhereComparison {
    pub key: String,
    pub comp: WhereComparison,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WhereComparison {
    Primitive(PrimitiveOperator, MetadataValue),
    Set(SetOperator, MetadataSetValue),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PrimitiveOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SetOperator {
    In,
    NotIn,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MetadataSetValue {
    Bool(Vec<bool>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    Str(Vec<String>),
}

impl MetadataSetValue {
    pub fn into_vec(&self) -> Vec<MetadataValue> {
        use MetadataSetValue::*;
        match self {
            Bool(vec) => vec.iter().map(|b| MetadataValue::Bool(*b)).collect(),
            Int(vec) => vec.iter().map(|i| MetadataValue::Int(*i)).collect(),
            Float(vec) => vec.iter().map(|f| MetadataValue::Float(*f)).collect(),
            Str(vec) => vec.iter().map(|s| MetadataValue::Str(s.clone())).collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct WhereChildren {
    pub operator: BooleanOperator,
    pub children: Vec<Where>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BooleanOperator {
    And,
    Or,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectDocumentComparison {
    pub operator: DocumentOperator,
    pub document: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DocumentOperator {
    Contains,
    NotContains,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WhereConversionError {
    InvalidWhere,
    InvalidWhereComparison,
    InvalidWhereChildren,
}

impl TryFrom<chroma_proto::Where> for Where {
    type Error = WhereConversionError;

    fn try_from(proto_where: chroma_proto::Where) -> Result<Self, Self::Error> {
        match proto_where.r#where {
            Some(chroma_proto::r#where::Where::DirectComparison(proto_comparison)) => {
                let comparison = DirectWhereComparison {
                    key: proto_comparison.key.clone(),
                    comp: proto_comparison.try_into()?,
                };
                Ok(Where::DirectWhereComparison(comparison))
            }
            Some(chroma_proto::r#where::Where::Children(proto_children)) => {
                let operator = match TryInto::<chroma_proto::BooleanOperator>::try_into(
                    proto_children.operator,
                ) {
                    Ok(operator) => operator,
                    Err(_) => return Err(WhereConversionError::InvalidWhereChildren),
                };
                let children = WhereChildren {
                    children: proto_children
                        .children
                        .into_iter()
                        .map(|child| child.try_into())
                        .collect::<Result<Vec<Where>, WhereConversionError>>()?,
                    operator: operator.try_into()?,
                };
                Ok(Where::WhereChildren(children))
            }
            None => Err(WhereConversionError::InvalidWhere),
        }
    }
}

impl TryFrom<chroma_proto::DirectComparison> for WhereComparison {
    type Error = WhereConversionError;

    fn try_from(proto_comparison: chroma_proto::DirectComparison) -> Result<Self, Self::Error> {
        let id_to_generic_comparator = |id| {
            TryInto::<chroma_proto::GenericComparator>::try_into(id)
                .map_err(|_| WhereConversionError::InvalidWhereComparison)?
                .try_into()
        };
        let id_to_number_comparator = |id| {
            TryInto::<chroma_proto::NumberComparator>::try_into(id)
                .map_err(|_| WhereConversionError::InvalidWhereComparison)?
                .try_into()
        };
        let id_to_set_comparator = |id| {
            TryInto::<chroma_proto::ListOperator>::try_into(id)
                .map_err(|_| WhereConversionError::InvalidWhereComparison)?
                .try_into()
        };
        if let Some(proto_comp) = proto_comparison.r#comparison {
            use chroma_proto::direct_comparison::Comparison::*;
            match proto_comp {
                SingleBoolOperand(single_bool_comparison) => Ok(WhereComparison::Primitive(
                    id_to_generic_comparator(single_bool_comparison.comparator)?,
                    MetadataValue::Bool(single_bool_comparison.value),
                )),
                SingleStringOperand(single_string_comparison) => Ok(WhereComparison::Primitive(
                    id_to_generic_comparator(single_string_comparison.comparator)?,
                    MetadataValue::Str(single_string_comparison.value),
                )),
                SingleIntOperand(single_int_comparison) => Ok(WhereComparison::Primitive(
                    match single_int_comparison.comparator {
                        Some(
                            chroma_proto::single_int_comparison::Comparator::GenericComparator(
                                proto_generic_comparator,
                            ),
                        ) => id_to_generic_comparator(proto_generic_comparator)?,
                        Some(
                            chroma_proto::single_int_comparison::Comparator::NumberComparator(
                                proto_number_comparator,
                            ),
                        ) => id_to_number_comparator(proto_number_comparator)?,
                        None => PrimitiveOperator::Equal,
                    },
                    MetadataValue::Int(single_int_comparison.value),
                )),
                SingleDoubleOperand(single_double_comparison) => Ok(WhereComparison::Primitive(
                    match single_double_comparison.comparator {
                        Some(
                            chroma_proto::single_double_comparison::Comparator::GenericComparator(
                                proto_generic_comparator,
                            ),
                        ) => id_to_generic_comparator(proto_generic_comparator)?,
                        Some(
                            chroma_proto::single_double_comparison::Comparator::NumberComparator(
                                proto_number_comparator,
                            ),
                        ) => id_to_number_comparator(proto_number_comparator)?,
                        None => PrimitiveOperator::Equal,
                    },
                    MetadataValue::Float(single_double_comparison.value),
                )),
                BoolListOperand(bool_list_comparison) => Ok(WhereComparison::Set(
                    id_to_set_comparator(bool_list_comparison.list_operator)?,
                    MetadataSetValue::Bool(bool_list_comparison.values),
                )),
                StringListOperand(string_list_comparison) => Ok(WhereComparison::Set(
                    id_to_set_comparator(string_list_comparison.list_operator)?,
                    MetadataSetValue::Str(string_list_comparison.values),
                )),
                IntListOperand(int_list_comparison) => Ok(WhereComparison::Set(
                    id_to_set_comparator(int_list_comparison.list_operator)?,
                    MetadataSetValue::Int(int_list_comparison.values),
                )),
                DoubleListOperand(double_list_comparison) => Ok(WhereComparison::Set(
                    id_to_set_comparator(double_list_comparison.list_operator)?,
                    MetadataSetValue::Float(double_list_comparison.values),
                )),
            }
        } else {
            Err(WhereConversionError::InvalidWhereComparison)
        }
    }
}

impl TryFrom<chroma_proto::NumberComparator> for PrimitiveOperator {
    type Error = WhereConversionError;

    fn try_from(proto_comparator: chroma_proto::NumberComparator) -> Result<Self, Self::Error> {
        match proto_comparator {
            chroma_proto::NumberComparator::Gt => Ok(PrimitiveOperator::GreaterThan),
            chroma_proto::NumberComparator::Gte => Ok(PrimitiveOperator::GreaterThanOrEqual),
            chroma_proto::NumberComparator::Lt => Ok(PrimitiveOperator::LessThan),
            chroma_proto::NumberComparator::Lte => Ok(PrimitiveOperator::LessThanOrEqual),
        }
    }
}

impl TryFrom<chroma_proto::GenericComparator> for PrimitiveOperator {
    type Error = WhereConversionError;

    fn try_from(proto_comparator: chroma_proto::GenericComparator) -> Result<Self, Self::Error> {
        match proto_comparator {
            chroma_proto::GenericComparator::Eq => Ok(PrimitiveOperator::Equal),
            chroma_proto::GenericComparator::Ne => Ok(PrimitiveOperator::NotEqual),
        }
    }
}

impl TryFrom<chroma_proto::ListOperator> for SetOperator {
    type Error = WhereConversionError;

    fn try_from(proto_operator: chroma_proto::ListOperator) -> Result<Self, Self::Error> {
        match proto_operator {
            chroma_proto::ListOperator::In => Ok(SetOperator::In),
            chroma_proto::ListOperator::Nin => Ok(SetOperator::NotIn),
        }
    }
}

impl TryFrom<chroma_proto::WhereChildren> for WhereChildren {
    type Error = WhereConversionError;

    fn try_from(proto_children: chroma_proto::WhereChildren) -> Result<Self, Self::Error> {
        let children = proto_children
            .children
            .into_iter()
            .map(|child| child.try_into())
            .collect::<Result<Vec<Where>, WhereConversionError>>()?;
        let operator: BooleanOperator =
            match TryInto::<chroma_proto::BooleanOperator>::try_into(proto_children.operator) {
                Ok(operator) => operator.try_into()?,
                Err(_) => return Err(WhereConversionError::InvalidWhereChildren),
            };
        Ok(WhereChildren { children, operator })
    }
}

impl TryFrom<chroma_proto::BooleanOperator> for BooleanOperator {
    type Error = WhereConversionError;

    fn try_from(proto_operator: chroma_proto::BooleanOperator) -> Result<Self, Self::Error> {
        match proto_operator {
            chroma_proto::BooleanOperator::And => Ok(BooleanOperator::And),
            chroma_proto::BooleanOperator::Or => Ok(BooleanOperator::Or),
        }
    }
}

impl TryFrom<chroma_proto::WhereDocument> for Where {
    type Error = WhereConversionError;

    fn try_from(proto_document: chroma_proto::WhereDocument) -> Result<Self, Self::Error> {
        match proto_document.r#where_document {
            Some(chroma_proto::where_document::WhereDocument::Direct(proto_comparison)) => {
                let operator = match TryInto::<chroma_proto::WhereDocumentOperator>::try_into(
                    proto_comparison.operator,
                ) {
                    Ok(operator) => operator,
                    Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                };
                let comparison = DirectDocumentComparison {
                    document: proto_comparison.document,
                    operator: operator.try_into()?,
                };
                Ok(Where::DirectWhereDocumentComparison(comparison))
            }
            Some(chroma_proto::where_document::WhereDocument::Children(proto_children)) => {
                let operator = match TryInto::<chroma_proto::BooleanOperator>::try_into(
                    proto_children.operator,
                ) {
                    Ok(operator) => operator,
                    Err(_) => return Err(WhereConversionError::InvalidWhereChildren),
                };
                let children = WhereChildren {
                    children: proto_children
                        .children
                        .into_iter()
                        .map(|child| child.try_into())
                        .collect::<Result<_, _>>()?,
                    operator: operator.try_into()?,
                };
                Ok(Where::WhereChildren(children))
            }
            None => Err(WhereConversionError::InvalidWhere),
        }
    }
}

impl TryFrom<chroma_proto::WhereDocumentOperator> for DocumentOperator {
    type Error = WhereConversionError;

    fn try_from(proto_operator: chroma_proto::WhereDocumentOperator) -> Result<Self, Self::Error> {
        match proto_operator {
            chroma_proto::WhereDocumentOperator::Contains => Ok(DocumentOperator::Contains),
            chroma_proto::WhereDocumentOperator::NotContains => Ok(DocumentOperator::NotContains),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SignedRoaringBitmap {
    Include(RoaringBitmap),
    Exclude(RoaringBitmap),
}

impl SignedRoaringBitmap {
    pub fn empty() -> Self {
        Self::Include(RoaringBitmap::new())
    }

    pub fn full() -> Self {
        Self::Exclude(RoaringBitmap::new())
    }

    pub fn flip(self) -> Self {
        use SignedRoaringBitmap::*;
        match self {
            Include(rbm) => Exclude(rbm),
            Exclude(rbm) => Include(rbm),
        }
    }
}

impl BitAnd for SignedRoaringBitmap {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        use SignedRoaringBitmap::*;
        match (self, rhs) {
            (Include(lhs), Include(rhs)) => Include(lhs & rhs),
            (Include(lhs), Exclude(rhs)) => Include(lhs - rhs),
            (Exclude(lhs), Include(rhs)) => Include(rhs - lhs),
            (Exclude(lhs), Exclude(rhs)) => Exclude(lhs | rhs),
        }
    }
}

impl BitOr for SignedRoaringBitmap {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        use SignedRoaringBitmap::*;
        match (self, rhs) {
            (Include(lhs), Include(rhs)) => Include(lhs | rhs),
            (Include(lhs), Exclude(rhs)) => Exclude(rhs - lhs),
            (Exclude(lhs), Include(rhs)) => Exclude(lhs - rhs),
            (Exclude(lhs), Exclude(rhs)) => Exclude(lhs & rhs),
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
                                comparator: None,
                            },
                        ),
                    ),
                },
            )),
        };
        let where_clause: Where = proto_where.try_into().unwrap();
        match where_clause {
            Where::DirectWhereComparison(comparison) => {
                assert_eq!(comparison.key, "foo");
                match comparison.comp {
                    WhereComparison::Primitive(_, value) => {
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
                                                comparator: None,
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
                                                comparator: None,
                                            },
                                        ),
                                    ),
                                },
                            )),
                        },
                    ],
                    operator: chroma_proto::BooleanOperator::And.try_into().unwrap(),
                },
            )),
        };
        let where_clause: Where = proto_where.try_into().unwrap();
        match where_clause {
            Where::WhereChildren(children) => {
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
                    document: "foo".to_string(),
                    operator: chroma_proto::WhereDocumentOperator::Contains
                        .try_into()
                        .unwrap(),
                },
            )),
        };
        let where_document: Where = proto_where.try_into().unwrap();
        match where_document {
            Where::DirectWhereDocumentComparison(comparison) => {
                assert_eq!(comparison.document, "foo");
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
                                        document: "foo".to_string(),
                                        operator: chroma_proto::WhereDocumentOperator::Contains
                                            .try_into()
                                            .unwrap(),
                                    },
                                ),
                            ),
                        },
                        chroma_proto::WhereDocument {
                            r#where_document: Some(
                                chroma_proto::where_document::WhereDocument::Direct(
                                    chroma_proto::DirectWhereDocument {
                                        document: "bar".to_string(),
                                        operator: chroma_proto::WhereDocumentOperator::Contains
                                            .try_into()
                                            .unwrap(),
                                    },
                                ),
                            ),
                        },
                    ],
                    operator: chroma_proto::BooleanOperator::And.try_into().unwrap(),
                },
            )),
        };
        let where_document: Where = proto_where.try_into().unwrap();
        match where_document {
            Where::WhereChildren(children) => {
                assert_eq!(children.children.len(), 2);
                assert_eq!(children.operator, BooleanOperator::And);
            }
            _ => panic!("Invalid where document type"),
        }
    }
}
