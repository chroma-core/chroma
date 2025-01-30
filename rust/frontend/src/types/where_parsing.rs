use super::errors::ValidationError;
use chroma_types::{
    CompositeExpression, DocumentOperator, MetadataExpression, PrimitiveOperator, Where,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct RawWhereFields {
    #[serde(default)]
    r#where: Value,
    #[serde(default)]
    where_document: Value,
}

impl RawWhereFields {
    pub(crate) fn parse(self) -> Result<Option<Where>, ValidationError> {
        let mut where_clause = None;
        if !self.r#where.is_null() {
            let where_payload = &self.r#where;
            where_clause = Some(parse_where(where_payload)?);
        }
        let mut where_document_clause = None;
        if !self.where_document.is_null() {
            let where_document_payload = &self.where_document;
            where_document_clause = Some(parse_where_document(where_document_payload)?);
        }
        let combined_where = match where_clause {
            Some(where_clause) => match where_document_clause {
                Some(where_document_clause) => Some(Where::Composite(CompositeExpression {
                    operator: chroma_types::BooleanOperator::And,
                    children: vec![where_clause, where_document_clause],
                })),
                None => Some(where_clause),
            },
            None => where_document_clause,
        };

        Ok(combined_where)
    }
}

pub fn parse_where_document(json_payload: &Value) -> Result<Where, ValidationError> {
    let where_doc_payload = json_payload
        .as_object()
        .ok_or(ValidationError::WhereDocumentClause)?;
    if where_doc_payload.len() != 1 {
        return Err(ValidationError::WhereDocumentClause);
    }
    let (key, value) = where_doc_payload.iter().next().unwrap();
    // Check if it is a composite expression.
    if key == "$and" {
        let logical_operator = chroma_types::BooleanOperator::And;
        // Check that the value is list type.
        let children = value
            .as_array()
            .ok_or(ValidationError::WhereDocumentClause)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where_document(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    if key == "$or" {
        let logical_operator = chroma_types::BooleanOperator::Or;
        // Check that the value is list type.
        let children = value
            .as_array()
            .ok_or(ValidationError::WhereDocumentClause)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where_document(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    if !value.is_string() {
        return Err(ValidationError::WhereDocumentClause);
    }
    let value_str = value.as_str().unwrap();
    let operator_type;
    if key == "$contains" {
        operator_type = DocumentOperator::Contains;
    } else if key == "not_contains" {
        operator_type = DocumentOperator::NotContains;
    } else {
        return Err(ValidationError::WhereDocumentClause);
    }
    Ok(Where::Document(chroma_types::DocumentExpression {
        operator: operator_type,
        text: value_str.to_string(),
    }))
}

pub fn parse_where(json_payload: &Value) -> Result<Where, ValidationError> {
    let where_payload = json_payload
        .as_object()
        .ok_or(ValidationError::WhereClause)?;
    if where_payload.len() != 1 {
        return Err(ValidationError::WhereClause);
    }
    let (key, value) = where_payload.iter().next().unwrap();
    // Check if it is a composite expression.
    if key == "$and" {
        let logical_operator = chroma_types::BooleanOperator::And;
        // Check that the value is list type.
        let children = value.as_array().ok_or(ValidationError::WhereClause)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    if key == "$or" {
        let logical_operator = chroma_types::BooleanOperator::Or;
        // Check that the value is list type.
        let children = value.as_array().ok_or(ValidationError::WhereClause)?;
        let mut predicate_list = vec![];
        // Recursively parse the children.
        for child in children {
            predicate_list.push(parse_where(child)?);
        }
        return Ok(Where::Composite(CompositeExpression {
            operator: logical_operator,
            children: predicate_list,
        }));
    }
    // At this point we know we're at a direct comparison. It can either
    // be of the form {"key": "value"} or {"key": {"$operator": "value"}}.
    if value.is_string() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Str(value.as_str().unwrap().to_string()),
            ),
        }));
    }
    if value.is_boolean() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Bool(value.as_bool().unwrap()),
            ),
        }));
    }
    if value.is_f64() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Float(value.as_f64().unwrap()),
            ),
        }));
    }
    if value.is_i64() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: chroma_types::MetadataComparison::Primitive(
                chroma_types::PrimitiveOperator::Equal,
                chroma_types::MetadataValue::Int(value.as_i64().unwrap()),
            ),
        }));
    }
    if value.is_object() {
        let value_obj = value.as_object().unwrap();
        // value_obj should have exactly one key.
        if value_obj.len() != 1 {
            return Err(ValidationError::WhereClause);
        }
        let (operator, operand) = value_obj.iter().next().unwrap();
        if operand.is_array() {
            let set_operator;
            if operator == "$in" {
                set_operator = chroma_types::SetOperator::In;
            } else if operator == "$nin" {
                set_operator = chroma_types::SetOperator::NotIn;
            } else {
                return Err(ValidationError::WhereClause);
            }
            let operand = operand.as_array().unwrap();
            if operand.is_empty() {
                return Err(ValidationError::WhereClause);
            }
            if operand[0].is_string() {
                let operand_str = operand
                    .iter()
                    .map(|val| {
                        val.as_str()
                            .ok_or(ValidationError::WhereClause)
                            .map(|s| s.to_string())
                    })
                    .collect::<Result<Vec<String>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Str(operand_str),
                    ),
                }));
            }
            if operand[0].is_boolean() {
                let operand_bool = operand
                    .iter()
                    .map(|val| val.as_bool().ok_or(ValidationError::WhereClause))
                    .collect::<Result<Vec<bool>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Bool(operand_bool),
                    ),
                }));
            }
            if operand[0].is_f64() {
                let operand_f64 = operand
                    .iter()
                    .map(|val| val.as_f64().ok_or(ValidationError::WhereClause))
                    .collect::<Result<Vec<f64>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Float(operand_f64),
                    ),
                }));
            }
            if operand[0].is_i64() {
                let operand_i64 = operand
                    .iter()
                    .map(|val| val.as_i64().ok_or(ValidationError::WhereClause))
                    .collect::<Result<Vec<i64>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: chroma_types::MetadataComparison::Set(
                        set_operator,
                        chroma_types::MetadataSetValue::Int(operand_i64),
                    ),
                }));
            }
            return Err(ValidationError::WhereClause);
        }
        if operand.is_string() {
            let operand_str = operand.as_str().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else {
                return Err(ValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Str(operand_str.to_string()),
                ),
            }));
        }
        if operand.is_boolean() {
            let operand_bool = operand.as_bool().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else {
                return Err(ValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Bool(operand_bool),
                ),
            }));
        }
        if operand.is_f64() {
            let operand_f64 = operand.as_f64().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else if operator == "$lt" {
                operator_type = PrimitiveOperator::LessThan;
            } else if operator == "$lte" {
                operator_type = PrimitiveOperator::LessThanOrEqual;
            } else if operator == "$gt" {
                operator_type = PrimitiveOperator::GreaterThan;
            } else if operator == "$gte" {
                operator_type = PrimitiveOperator::GreaterThanOrEqual;
            } else {
                return Err(ValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Float(operand_f64),
                ),
            }));
        }
        if operand.is_i64() {
            let operand_i64 = operand.as_i64().unwrap();
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else if operator == "$lt" {
                operator_type = PrimitiveOperator::LessThan;
            } else if operator == "$lte" {
                operator_type = PrimitiveOperator::LessThanOrEqual;
            } else if operator == "$gt" {
                operator_type = PrimitiveOperator::GreaterThan;
            } else if operator == "$gte" {
                operator_type = PrimitiveOperator::GreaterThanOrEqual;
            } else {
                return Err(ValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: chroma_types::MetadataComparison::Primitive(
                    operator_type,
                    chroma_types::MetadataValue::Int(operand_i64),
                ),
            }));
        }
        return Err(ValidationError::WhereClause);
    }
    Err(ValidationError::WhereClause)
}
