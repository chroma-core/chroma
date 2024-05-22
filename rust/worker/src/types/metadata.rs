use crate::{
    chroma_proto,
    errors::{ChromaError, ErrorCodes},
};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum UpdateMetadataValue {
    Int(i32),
    Float(f64),
    Str(String),
    None,
}

#[derive(Error, Debug)]
pub(crate) enum UpdateMetadataValueConversionError {
    #[error("Invalid metadata value, valid values are: Int, Float, Str, Bool, None")]
    InvalidValue,
}

impl ChromaError for UpdateMetadataValueConversionError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            UpdateMetadataValueConversionError::InvalidValue => ErrorCodes::InvalidArgument,
        }
    }
}

impl TryFrom<&chroma_proto::UpdateMetadataValue> for UpdateMetadataValue {
    type Error = UpdateMetadataValueConversionError;

    fn try_from(value: &chroma_proto::UpdateMetadataValue) -> Result<Self, Self::Error> {
        match &value.value {
            Some(chroma_proto::update_metadata_value::Value::IntValue(value)) => {
                Ok(UpdateMetadataValue::Int(*value as i32))
            }
            Some(chroma_proto::update_metadata_value::Value::FloatValue(value)) => {
                Ok(UpdateMetadataValue::Float(*value))
            }
            Some(chroma_proto::update_metadata_value::Value::StringValue(value)) => {
                Ok(UpdateMetadataValue::Str(value.clone()))
            }
            None => Ok(UpdateMetadataValue::None),
            _ => Err(UpdateMetadataValueConversionError::InvalidValue),
        }
    }
}

impl From<UpdateMetadataValue> for chroma_proto::UpdateMetadataValue {
    fn from(value: UpdateMetadataValue) -> Self {
        let proto_value = match value {
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MetadataValue {
    Int(i32),
    Float(f64),
    Str(String),
}

impl TryFrom<&MetadataValue> for i32 {
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
pub(crate) enum MetadataValueConversionError {
    #[error("Invalid metadata value, valid values are: Int, Float, Str")]
    InvalidValue,
}

impl ChromaError for MetadataValueConversionError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            MetadataValueConversionError::InvalidValue => ErrorCodes::InvalidArgument,
        }
    }
}

impl TryFrom<&chroma_proto::UpdateMetadataValue> for MetadataValue {
    type Error = MetadataValueConversionError;

    fn try_from(value: &chroma_proto::UpdateMetadataValue) -> Result<Self, Self::Error> {
        match &value.value {
            Some(chroma_proto::update_metadata_value::Value::IntValue(value)) => {
                Ok(MetadataValue::Int(*value as i32))
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
pub(crate) type UpdateMetadata = HashMap<String, UpdateMetadataValue>;

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

pub(crate) type Metadata = HashMap<String, MetadataValue>;

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

pub(crate) fn update_metdata_to_metdata(
    update_metdata: &UpdateMetadata,
) -> Result<Metadata, MetadataValueConversionError> {
    let mut metadata = Metadata::new();
    for (key, value) in update_metdata {
        let res = value.try_into();
        match res {
            Ok(value) => {
                metadata.insert(key.clone(), value);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
    Ok(metadata)
}

/*
===========================================
Metadata queries
===========================================
*/

#[derive(Debug, PartialEq)]
pub(crate) enum Where {
    DirectWhereComparison(DirectComparison),
    WhereChildren(WhereChildren),
}

#[derive(Debug, PartialEq)]
pub(crate) struct DirectComparison {
    pub key: String,
    pub comparison: WhereComparison,
}

#[derive(Debug, PartialEq)]
pub(crate) enum WhereComparison {
    SingleStringComparison(String, WhereClauseComparator),
    SingleIntComparison(i32, WhereClauseComparator),
    SingleDoubleComparison(f64, WhereClauseComparator),
    StringListComparison(Vec<String>, WhereClauseListOperator),
    IntListComparison(Vec<i32>, WhereClauseListOperator),
    DoubleListComparison(Vec<f64>, WhereClauseListOperator),
}

#[derive(Debug, PartialEq)]
pub(crate) enum WhereClauseComparator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, PartialEq)]
pub(crate) enum WhereClauseListOperator {
    In,
    NotIn,
}

#[derive(Debug, PartialEq)]
pub(crate) struct WhereChildren {
    pub children: Vec<Where>,
    pub operator: BooleanOperator,
}

#[derive(Debug, PartialEq)]
pub(crate) enum BooleanOperator {
    And,
    Or,
}

#[derive(Debug, PartialEq)]
pub(crate) enum WhereDocument {
    DirectWhereDocumentComparison(DirectDocumentComparison),
    WhereDocumentChildren(WhereDocumentChildren),
}

#[derive(Debug, PartialEq)]
pub(crate) struct DirectDocumentComparison {
    pub document: String,
    pub operator: WhereDocumentOperator,
}

#[derive(Debug, PartialEq)]
pub(crate) enum WhereDocumentOperator {
    Contains,
    NotContains,
}

#[derive(Debug, PartialEq)]
pub(crate) struct WhereDocumentChildren {
    pub children: Vec<WhereDocument>,
    pub operator: BooleanOperator,
}

#[derive(Debug, PartialEq)]
pub(crate) enum WhereConversionError {
    InvalidWhere,
    InvalidWhereComparison,
    InvalidWhereChildren,
}

impl TryFrom<chroma_proto::Where> for Where {
    type Error = WhereConversionError;

    fn try_from(proto_where: chroma_proto::Where) -> Result<Self, Self::Error> {
        match proto_where.r#where {
            Some(chroma_proto::r#where::Where::DirectComparison(proto_comparison)) => {
                let comparison = DirectComparison {
                    key: proto_comparison.key.clone(),
                    comparison: proto_comparison.try_into()?,
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
        match proto_comparison.r#comparison {
            Some(chroma_proto::direct_comparison::Comparison::SingleStringOperand(
                proto_string,
            )) => {
                let comparator = match TryInto::<chroma_proto::GenericComparator>::try_into(
                    proto_string.comparator,
                ) {
                    Ok(comparator) => comparator,
                    Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                };
                Ok(WhereComparison::SingleStringComparison(
                    proto_string.value,
                    comparator.try_into()?,
                ))
            }
            Some(chroma_proto::direct_comparison::Comparison::SingleIntOperand(proto_int)) => {
                let comparator: WhereClauseComparator = match proto_int.comparator {
                    Some(comparator) => match comparator {
                        chroma_proto::single_int_comparison::Comparator::NumberComparator(
                            proto_comparator,
                        ) => {
                            match TryInto::<chroma_proto::NumberComparator>::try_into(
                                proto_comparator,
                            ) {
                                Ok(comparator) => comparator.try_into()?,
                                Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                            }
                        }
                        chroma_proto::single_int_comparison::Comparator::GenericComparator(
                            proto_comparator,
                        ) => {
                            match TryInto::<chroma_proto::GenericComparator>::try_into(
                                proto_comparator,
                            ) {
                                Ok(comparator) => comparator.try_into()?,
                                Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                            }
                        }
                    },
                    None => WhereClauseComparator::Equal,
                };
                Ok(WhereComparison::SingleIntComparison(
                    proto_int.value as i32,
                    comparator,
                ))
            }
            Some(chroma_proto::direct_comparison::Comparison::SingleDoubleOperand(
                proto_double,
            )) => {
                let comparator: WhereClauseComparator = match proto_double.comparator {
                    Some(comparator) => match comparator {
                        chroma_proto::single_double_comparison::Comparator::NumberComparator(
                            proto_comparator,
                        ) => {
                            match TryInto::<chroma_proto::NumberComparator>::try_into(
                                proto_comparator,
                            ) {
                                Ok(comparator) => comparator.try_into()?,
                                Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                            }
                        }
                        chroma_proto::single_double_comparison::Comparator::GenericComparator(
                            proto_comparator,
                        ) => {
                            match TryInto::<chroma_proto::GenericComparator>::try_into(
                                proto_comparator,
                            ) {
                                Ok(comparator) => comparator.try_into()?,
                                Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                            }
                        }
                    },
                    None => WhereClauseComparator::Equal,
                };
                Ok(WhereComparison::SingleDoubleComparison(
                    proto_double.value,
                    comparator,
                ))
            }
            Some(chroma_proto::direct_comparison::Comparison::StringListOperand(proto_list)) => {
                let list_operator =
                    match TryInto::<chroma_proto::ListOperator>::try_into(proto_list.list_operator)
                    {
                        Ok(list_operator) => list_operator,
                        Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                    };
                Ok(WhereComparison::StringListComparison(
                    proto_list.values,
                    list_operator.try_into()?,
                ))
            }
            Some(chroma_proto::direct_comparison::Comparison::IntListOperand(proto_list)) => {
                let list_operator =
                    match TryInto::<chroma_proto::ListOperator>::try_into(proto_list.list_operator)
                    {
                        Ok(list_operator) => list_operator,
                        Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                    };
                Ok(WhereComparison::IntListComparison(
                    proto_list.values.into_iter().map(|v| v as i32).collect(),
                    list_operator.try_into()?,
                ))
            }
            Some(chroma_proto::direct_comparison::Comparison::DoubleListOperand(proto_list)) => {
                let list_operator =
                    match TryInto::<chroma_proto::ListOperator>::try_into(proto_list.list_operator)
                    {
                        Ok(list_operator) => list_operator,
                        Err(_) => return Err(WhereConversionError::InvalidWhereComparison),
                    };
                Ok(WhereComparison::DoubleListComparison(
                    proto_list.values,
                    list_operator.try_into()?,
                ))
            }
            None => Err(WhereConversionError::InvalidWhereComparison),
        }
    }
}

impl TryFrom<chroma_proto::NumberComparator> for WhereClauseComparator {
    type Error = WhereConversionError;

    fn try_from(proto_comparator: chroma_proto::NumberComparator) -> Result<Self, Self::Error> {
        match proto_comparator {
            chroma_proto::NumberComparator::Gt => Ok(WhereClauseComparator::GreaterThan),
            chroma_proto::NumberComparator::Gte => Ok(WhereClauseComparator::GreaterThanOrEqual),
            chroma_proto::NumberComparator::Lt => Ok(WhereClauseComparator::LessThan),
            chroma_proto::NumberComparator::Lte => Ok(WhereClauseComparator::LessThanOrEqual),
        }
    }
}

impl TryFrom<chroma_proto::GenericComparator> for WhereClauseComparator {
    type Error = WhereConversionError;

    fn try_from(proto_comparator: chroma_proto::GenericComparator) -> Result<Self, Self::Error> {
        match proto_comparator {
            chroma_proto::GenericComparator::Eq => Ok(WhereClauseComparator::Equal),
            chroma_proto::GenericComparator::Ne => Ok(WhereClauseComparator::NotEqual),
        }
    }
}

impl TryFrom<chroma_proto::ListOperator> for WhereClauseListOperator {
    type Error = WhereConversionError;

    fn try_from(proto_operator: chroma_proto::ListOperator) -> Result<Self, Self::Error> {
        match proto_operator {
            chroma_proto::ListOperator::In => Ok(WhereClauseListOperator::In),
            chroma_proto::ListOperator::Nin => Ok(WhereClauseListOperator::NotIn),
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

impl TryFrom<chroma_proto::WhereDocument> for WhereDocument {
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
                Ok(WhereDocument::DirectWhereDocumentComparison(comparison))
            }
            Some(chroma_proto::where_document::WhereDocument::Children(proto_children)) => {
                let operator = match TryInto::<chroma_proto::BooleanOperator>::try_into(
                    proto_children.operator,
                ) {
                    Ok(operator) => operator,
                    Err(_) => return Err(WhereConversionError::InvalidWhereChildren),
                };
                let children = WhereDocumentChildren {
                    children: proto_children
                        .children
                        .into_iter()
                        .map(|child| child.try_into())
                        .collect::<Result<Vec<WhereDocument>, WhereConversionError>>()?,
                    operator: operator.try_into()?,
                };
                Ok(WhereDocument::WhereDocumentChildren(children))
            }
            None => Err(WhereConversionError::InvalidWhere),
        }
    }
}

impl TryFrom<chroma_proto::WhereDocumentOperator> for WhereDocumentOperator {
    type Error = WhereConversionError;

    fn try_from(proto_operator: chroma_proto::WhereDocumentOperator) -> Result<Self, Self::Error> {
        match proto_operator {
            chroma_proto::WhereDocumentOperator::Contains => Ok(WhereDocumentOperator::Contains),
            chroma_proto::WhereDocumentOperator::NotContains => {
                Ok(WhereDocumentOperator::NotContains)
            }
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
                match comparison.comparison {
                    WhereComparison::SingleIntComparison(value, _) => {
                        assert_eq!(value, 42);
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
        let where_document: WhereDocument = proto_where.try_into().unwrap();
        match where_document {
            WhereDocument::DirectWhereDocumentComparison(comparison) => {
                assert_eq!(comparison.document, "foo");
                assert_eq!(comparison.operator, WhereDocumentOperator::Contains);
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
        let where_document: WhereDocument = proto_where.try_into().unwrap();
        match where_document {
            WhereDocument::WhereDocumentChildren(children) => {
                assert_eq!(children.children.len(), 2);
                assert_eq!(children.operator, BooleanOperator::And);
            }
            _ => panic!("Invalid where document type"),
        }
    }
}
