use crate::regex::{ChromaRegex, ChromaRegexError};
use crate::{
    CompositeExpression, ContainsOperator, DocumentOperator, MetadataExpression, PrimitiveOperator,
    Where,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

#[derive(Default, Deserialize, Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RawWhereFields {
    #[serde(default)]
    pub r#where: Value,
    #[serde(default)]
    pub where_document: Value,
}

impl RawWhereFields {
    pub fn new(r#where: Value, where_document: Value) -> Self {
        Self {
            r#where,
            where_document,
        }
    }

    pub fn from_json_str(
        r#where: Option<&str>,
        where_document: Option<&str>,
    ) -> Result<Self, WhereValidationError> {
        let r#where = r#where
            .map(|r#where| {
                serde_json::from_str(r#where).map_err(|_| WhereValidationError::WhereClause)
            })
            .transpose()?
            .unwrap_or(Value::Null);

        let where_document = where_document
            .map(|where_document| {
                serde_json::from_str(where_document)
                    .map_err(|_| WhereValidationError::WhereDocumentClause)
            })
            .transpose()?
            .unwrap_or(Value::Null);

        Ok(Self {
            r#where,
            where_document,
        })
    }
}

#[derive(Error, Debug)]
pub enum WhereValidationError {
    #[error(transparent)]
    Regex(#[from] ChromaRegexError),
    #[error("Invalid where clause")]
    WhereClause,
    #[error("Invalid where document clause")]
    WhereDocumentClause,
}

impl ChromaError for WhereValidationError {
    fn code(&self) -> chroma_error::ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

impl RawWhereFields {
    pub fn parse(self) -> Result<Option<Where>, WhereValidationError> {
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
                    operator: crate::BooleanOperator::And,
                    children: vec![where_clause, where_document_clause],
                })),
                None => Some(where_clause),
            },
            None => where_document_clause,
        };

        Ok(combined_where)
    }
}

pub fn parse_where_document(json_payload: &Value) -> Result<Where, WhereValidationError> {
    let where_doc_payload = json_payload
        .as_object()
        .ok_or(WhereValidationError::WhereDocumentClause)?;
    if where_doc_payload.len() != 1 {
        return Err(WhereValidationError::WhereDocumentClause);
    }
    let (key, value) = where_doc_payload.iter().next().unwrap();
    // Check if it is a composite expression.
    if key == "$and" {
        let logical_operator = crate::BooleanOperator::And;
        // Check that the value is list type.
        let children = value
            .as_array()
            .ok_or(WhereValidationError::WhereDocumentClause)?;
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
        let logical_operator = crate::BooleanOperator::Or;
        // Check that the value is list type.
        let children = value
            .as_array()
            .ok_or(WhereValidationError::WhereDocumentClause)?;
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
        return Err(WhereValidationError::WhereDocumentClause);
    }
    let value_str = value.as_str().unwrap();
    let operator_type = match key.as_str() {
        "$contains" => DocumentOperator::Contains,
        "$not_contains" => DocumentOperator::NotContains,
        "$regex" => DocumentOperator::Regex,
        "$not_regex" => DocumentOperator::NotRegex,
        _ => return Err(WhereValidationError::WhereDocumentClause),
    };
    if matches!(
        operator_type,
        DocumentOperator::Regex | DocumentOperator::NotRegex
    ) {
        ChromaRegex::try_from(value_str.to_string())?;
    }
    Ok(Where::Document(crate::DocumentExpression {
        operator: operator_type,
        pattern: value_str.to_string(),
    }))
}

/// Returns the [`ContainsOperator`] for `$contains` / `$not_contains`,
/// or `None` for any other operator string.
fn parse_contains_operator(operator: &str) -> Option<ContainsOperator> {
    match operator {
        "$contains" => Some(ContainsOperator::Contains),
        "$not_contains" => Some(ContainsOperator::NotContains),
        _ => None,
    }
}

pub fn parse_where(json_payload: &Value) -> Result<Where, WhereValidationError> {
    let where_payload = json_payload
        .as_object()
        .ok_or(WhereValidationError::WhereClause)?;
    if where_payload.len() != 1 {
        return Err(WhereValidationError::WhereClause);
    }
    let (key, value) = where_payload.iter().next().unwrap();
    // Check if it is a composite expression.
    if key == "$and" {
        let logical_operator = crate::BooleanOperator::And;
        // Check that the value is list type.
        let children = value.as_array().ok_or(WhereValidationError::WhereClause)?;
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
        let logical_operator = crate::BooleanOperator::Or;
        // Check that the value is list type.
        let children = value.as_array().ok_or(WhereValidationError::WhereClause)?;
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
    // Any other $-prefixed key is an operator, not a metadata field name.
    // Operators like $contains, $not_contains, $gt, etc. are only valid
    // inside a field expression (e.g. {"field": {"$contains": val}}).
    if key.starts_with('$') {
        return Err(WhereValidationError::WhereClause);
    }
    // At this point we know we're at a direct comparison. It can either
    // be of the form {"key": "value"} or {"key": {"$operator": "value"}}.
    if value.is_string() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: crate::MetadataComparison::Primitive(
                crate::PrimitiveOperator::Equal,
                crate::MetadataValue::Str(value.as_str().unwrap().to_string()),
            ),
        }));
    }
    if value.is_boolean() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: crate::MetadataComparison::Primitive(
                crate::PrimitiveOperator::Equal,
                crate::MetadataValue::Bool(value.as_bool().unwrap()),
            ),
        }));
    }
    if value.is_f64() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: crate::MetadataComparison::Primitive(
                crate::PrimitiveOperator::Equal,
                crate::MetadataValue::Float(value.as_f64().unwrap()),
            ),
        }));
    }
    if value.is_i64() {
        return Ok(Where::Metadata(MetadataExpression {
            key: key.clone(),
            comparison: crate::MetadataComparison::Primitive(
                crate::PrimitiveOperator::Equal,
                crate::MetadataValue::Int(value.as_i64().unwrap()),
            ),
        }));
    }
    if value.is_object() {
        let value_obj = value.as_object().unwrap();
        // value_obj should have exactly one key.
        if value_obj.len() != 1 {
            return Err(WhereValidationError::WhereClause);
        }
        let (operator, operand) = value_obj.iter().next().unwrap();
        if operand.is_array() {
            let set_operator;
            if operator == "$in" {
                set_operator = crate::SetOperator::In;
            } else if operator == "$nin" {
                set_operator = crate::SetOperator::NotIn;
            } else {
                return Err(WhereValidationError::WhereClause);
            }
            let operand = operand.as_array().unwrap();
            if operand.is_empty() {
                return Err(WhereValidationError::WhereClause);
            }
            if operand[0].is_string() {
                let operand_str = operand
                    .iter()
                    .map(|val| {
                        val.as_str()
                            .ok_or(WhereValidationError::WhereClause)
                            .map(|s| s.to_string())
                    })
                    .collect::<Result<Vec<String>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::Set(
                        set_operator,
                        crate::MetadataSetValue::Str(operand_str),
                    ),
                }));
            }
            if operand[0].is_boolean() {
                let operand_bool = operand
                    .iter()
                    .map(|val| val.as_bool().ok_or(WhereValidationError::WhereClause))
                    .collect::<Result<Vec<bool>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::Set(
                        set_operator,
                        crate::MetadataSetValue::Bool(operand_bool),
                    ),
                }));
            }
            if operand[0].is_f64() {
                let operand_f64 = operand
                    .iter()
                    .map(|val| val.as_f64().ok_or(WhereValidationError::WhereClause))
                    .collect::<Result<Vec<f64>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::Set(
                        set_operator,
                        crate::MetadataSetValue::Float(operand_f64),
                    ),
                }));
            }
            if operand[0].is_i64() {
                let operand_i64 = operand
                    .iter()
                    .map(|val| val.as_i64().ok_or(WhereValidationError::WhereClause))
                    .collect::<Result<Vec<i64>, _>>()?;
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::Set(
                        set_operator,
                        crate::MetadataSetValue::Int(operand_i64),
                    ),
                }));
            }
            return Err(WhereValidationError::WhereClause);
        }
        if operand.is_string() {
            let operand_str = operand.as_str().unwrap();
            // $contains/$not_contains on the "#document" key are document
            // search operators. On any other key they are metadata array
            // contains operators.
            if operator == "$contains" || operator == "$not_contains" {
                if key == "#document" {
                    let doc_op = if operator == "$contains" {
                        DocumentOperator::Contains
                    } else {
                        DocumentOperator::NotContains
                    };
                    return Ok(Where::Document(crate::DocumentExpression {
                        operator: doc_op,
                        pattern: operand_str.to_string(),
                    }));
                }
                let contains_op = if operator == "$contains" {
                    ContainsOperator::Contains
                } else {
                    ContainsOperator::NotContains
                };
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::ArrayContains(
                        contains_op,
                        crate::MetadataValue::Str(operand_str.to_string()),
                    ),
                }));
            }
            if operator == "$regex" || operator == "$not_regex" {
                // Regex operators are only valid on document content.
                if key != "#document" {
                    return Err(WhereValidationError::WhereClause);
                }
                ChromaRegex::try_from(operand_str.to_string())?;
                let doc_op = if operator == "$regex" {
                    DocumentOperator::Regex
                } else {
                    DocumentOperator::NotRegex
                };
                return Ok(Where::Document(crate::DocumentExpression {
                    operator: doc_op,
                    pattern: operand_str.to_string(),
                }));
            }
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else {
                return Err(WhereValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: crate::MetadataComparison::Primitive(
                    operator_type,
                    crate::MetadataValue::Str(operand_str.to_string()),
                ),
            }));
        }
        if operand.is_boolean() {
            let operand_bool = operand.as_bool().unwrap();
            if let Some(contains_op) = parse_contains_operator(operator) {
                // $contains/$not_contains on "#document" requires a string operand.
                if key == "#document" {
                    return Err(WhereValidationError::WhereClause);
                }
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::ArrayContains(
                        contains_op,
                        crate::MetadataValue::Bool(operand_bool),
                    ),
                }));
            }
            let operator_type;
            if operator == "$eq" {
                operator_type = PrimitiveOperator::Equal;
            } else if operator == "$ne" {
                operator_type = PrimitiveOperator::NotEqual;
            } else {
                return Err(WhereValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: crate::MetadataComparison::Primitive(
                    operator_type,
                    crate::MetadataValue::Bool(operand_bool),
                ),
            }));
        }
        if operand.is_f64() {
            let operand_f64 = operand.as_f64().unwrap();
            if let Some(contains_op) = parse_contains_operator(operator) {
                // $contains/$not_contains on "#document" requires a string operand.
                if key == "#document" {
                    return Err(WhereValidationError::WhereClause);
                }
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::ArrayContains(
                        contains_op,
                        crate::MetadataValue::Float(operand_f64),
                    ),
                }));
            }
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
                return Err(WhereValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: crate::MetadataComparison::Primitive(
                    operator_type,
                    crate::MetadataValue::Float(operand_f64),
                ),
            }));
        }
        if operand.is_i64() {
            let operand_i64 = operand.as_i64().unwrap();
            if let Some(contains_op) = parse_contains_operator(operator) {
                // $contains/$not_contains on "#document" requires a string operand.
                if key == "#document" {
                    return Err(WhereValidationError::WhereClause);
                }
                return Ok(Where::Metadata(MetadataExpression {
                    key: key.clone(),
                    comparison: crate::MetadataComparison::ArrayContains(
                        contains_op,
                        crate::MetadataValue::Int(operand_i64),
                    ),
                }));
            }
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
                return Err(WhereValidationError::WhereClause);
            }
            return Ok(Where::Metadata(MetadataExpression {
                key: key.clone(),
                comparison: crate::MetadataComparison::Primitive(
                    operator_type,
                    crate::MetadataValue::Int(operand_i64),
                ),
            }));
        }
        return Err(WhereValidationError::WhereClause);
    }
    Err(WhereValidationError::WhereClause)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_where_direct_eq() {
        let payload = json!({
          "key1": "value1"
        });
        let expected_result = Where::Metadata(MetadataExpression {
            key: "key1".to_string(),
            comparison: crate::MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                crate::MetadataValue::Str("value1".to_string()),
            ),
        });

        let result = parse_where(&payload).expect("This clause to parse successfully");
        assert_eq!(result, expected_result);
    }

    // TODO: add a proptest when there's an Arbitrary impl for Where and WhereDocument
    #[test]
    fn test_parse_where_document() {
        let payloads = [
            // $contains
            json!({
              "$and": [
                  {"$contains": "value1"},
                  {"$or": [
                      {"$contains": "value2"},
                      {"$contains": "value3"}
                  ]}
              ]
            }),
            // $not_contains
            json!({
              "$not_contains": "value1",
            }),
        ];

        let expected_results = [
            // $contains
            Where::Composite(CompositeExpression {
                operator: crate::BooleanOperator::And,
                children: vec![
                    Where::Document(crate::DocumentExpression {
                        operator: DocumentOperator::Contains,
                        pattern: "value1".to_string(),
                    }),
                    Where::Composite(CompositeExpression {
                        operator: crate::BooleanOperator::Or,
                        children: vec![
                            Where::Document(crate::DocumentExpression {
                                operator: DocumentOperator::Contains,
                                pattern: "value2".to_string(),
                            }),
                            Where::Document(crate::DocumentExpression {
                                operator: DocumentOperator::Contains,
                                pattern: "value3".to_string(),
                            }),
                        ],
                    }),
                ],
            }),
            // $not_contains
            Where::Document(crate::DocumentExpression {
                operator: DocumentOperator::NotContains,
                pattern: "value1".to_string(),
            }),
        ];

        for (payload, expected_result) in payloads.iter().zip(expected_results.iter()) {
            let result = parse_where_document(payload);
            assert!(
                result.is_ok(),
                "Parsing failed for payload: {}: {:?}",
                serde_json::to_string_pretty(payload).unwrap(),
                result
            );
            assert_eq!(
                result.unwrap(),
                *expected_result,
                "Parsed result did not match expected result: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }

    #[test]
    fn test_parse_where() {
        let payloads = [
            // $in
            json!({
              "key1": {"$in": ["value1", "value2", "value3"]}
            }),
            // $nin
            json!({
              "key1": {"$nin": ["value1", "value2", "value3"]}
            }),
            // $eq
            json!({
              "key1": {"$eq": "value1"}
            }),
            // $ne
            json!({
              "key1": {"$ne": "value1"}
            }),
        ];

        let expected_results = [
            // $in
            Where::Metadata(MetadataExpression {
                key: "key1".to_string(),
                comparison: crate::MetadataComparison::Set(
                    crate::SetOperator::In,
                    crate::MetadataSetValue::Str(vec![
                        "value1".to_string(),
                        "value2".to_string(),
                        "value3".to_string(),
                    ]),
                ),
            }),
            // $nin
            Where::Metadata(MetadataExpression {
                key: "key1".to_string(),
                comparison: crate::MetadataComparison::Set(
                    crate::SetOperator::NotIn,
                    crate::MetadataSetValue::Str(vec![
                        "value1".to_string(),
                        "value2".to_string(),
                        "value3".to_string(),
                    ]),
                ),
            }),
            // $eq
            Where::Metadata(MetadataExpression {
                key: "key1".to_string(),
                comparison: crate::MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    crate::MetadataValue::Str("value1".to_string()),
                ),
            }),
            // $ne
            Where::Metadata(MetadataExpression {
                key: "key1".to_string(),
                comparison: crate::MetadataComparison::Primitive(
                    PrimitiveOperator::NotEqual,
                    crate::MetadataValue::Str("value1".to_string()),
                ),
            }),
        ];

        for (payload, expected_result) in payloads.iter().zip(expected_results.iter()) {
            let result = parse_where(payload);
            assert!(
                result.is_ok(),
                "Parsing failed for payload: {}: {:?}",
                serde_json::to_string_pretty(payload).unwrap(),
                result
            );
            assert_eq!(
                result.unwrap(),
                *expected_result,
                "Parsed result did not match expected result: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }

    #[test]
    fn test_parse_where_contains_metadata() {
        // $contains on a metadata key should produce MetadataComparison::Contains,
        // NOT a DocumentExpression.
        let payloads = [
            // string contains
            json!({"tags": {"$contains": "action"}}),
            // string not_contains
            json!({"tags": {"$not_contains": "comedy"}}),
            // int contains
            json!({"scores": {"$contains": 42}}),
            // float contains
            json!({"ratings": {"$contains": 4.5}}),
            // bool contains
            json!({"flags": {"$contains": true}}),
        ];

        let expected_results = [
            Where::Metadata(MetadataExpression {
                key: "tags".to_string(),
                comparison: crate::MetadataComparison::ArrayContains(
                    ContainsOperator::Contains,
                    crate::MetadataValue::Str("action".to_string()),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "tags".to_string(),
                comparison: crate::MetadataComparison::ArrayContains(
                    ContainsOperator::NotContains,
                    crate::MetadataValue::Str("comedy".to_string()),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "scores".to_string(),
                comparison: crate::MetadataComparison::ArrayContains(
                    ContainsOperator::Contains,
                    crate::MetadataValue::Int(42),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "ratings".to_string(),
                comparison: crate::MetadataComparison::ArrayContains(
                    ContainsOperator::Contains,
                    crate::MetadataValue::Float(4.5),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "flags".to_string(),
                comparison: crate::MetadataComparison::ArrayContains(
                    ContainsOperator::Contains,
                    crate::MetadataValue::Bool(true),
                ),
            }),
        ];

        for (payload, expected_result) in payloads.iter().zip(expected_results.iter()) {
            let result = parse_where(payload);
            assert!(
                result.is_ok(),
                "Parsing failed for payload: {}: {:?}",
                serde_json::to_string_pretty(payload).unwrap(),
                result
            );
            assert_eq!(
                result.unwrap(),
                *expected_result,
                "Parsed result did not match expected result: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }

    #[test]
    fn test_parse_where_document_contains_in_where() {
        // $contains on the "#document" key within a where clause should still
        // produce a DocumentExpression for backwards compatibility.
        let payload = json!({"#document": {"$contains": "search term"}});
        let result = parse_where(&payload).expect("Should parse successfully");
        assert_eq!(
            result,
            Where::Document(crate::DocumentExpression {
                operator: DocumentOperator::Contains,
                pattern: "search term".to_string(),
            })
        );
    }

    #[test]
    fn test_parse_where_regex_only_on_document() {
        // $regex / $not_regex are only valid on the "#document" key.
        let payload = json!({"#document": {"$regex": "act.*"}});
        let result = parse_where(&payload).expect("Should parse successfully");
        assert_eq!(
            result,
            Where::Document(crate::DocumentExpression {
                operator: DocumentOperator::Regex,
                pattern: "act.*".to_string(),
            })
        );

        let payload = json!({"#document": {"$not_regex": "draft.*"}});
        let result = parse_where(&payload).expect("Should parse successfully");
        assert_eq!(
            result,
            Where::Document(crate::DocumentExpression {
                operator: DocumentOperator::NotRegex,
                pattern: "draft.*".to_string(),
            })
        );

        // $regex on a metadata key should be rejected.
        let payload = json!({"tags": {"$regex": "act.*"}});
        assert!(parse_where(&payload).is_err());

        let payload = json!({"tags": {"$not_regex": "draft.*"}});
        assert!(parse_where(&payload).is_err());
    }

    #[test]
    fn test_where_contains_round_trip() {
        // Verify that serializing a Contains expression and parsing it back
        // produces the same result.
        let original = Where::Metadata(MetadataExpression {
            key: "tags".to_string(),
            comparison: crate::MetadataComparison::ArrayContains(
                ContainsOperator::Contains,
                crate::MetadataValue::Str("action".to_string()),
            ),
        });
        let json_str = serde_json::to_string(&original).unwrap();
        let json_value: Value = serde_json::from_str(&json_str).unwrap();
        let parsed = parse_where(&json_value).expect("Round-trip parsing should succeed");
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_document_contains_rejects_non_string_operand() {
        // $contains / $not_contains on "#document" must have a string operand.
        // Non-string values should be rejected, not silently treated as metadata.
        let payloads = [
            json!({"#document": {"$contains": 42}}),
            json!({"#document": {"$contains": 2.72}}),
            json!({"#document": {"$contains": true}}),
            json!({"#document": {"$not_contains": 42}}),
            json!({"#document": {"$not_contains": false}}),
        ];
        for payload in &payloads {
            let result = parse_where(payload);
            assert!(
                result.is_err(),
                "Expected error for non-string #document contains, but got Ok for: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }

    #[test]
    fn test_parse_where_in_nin_typed_arrays() {
        // $in / $nin with integer, boolean, and float arrays.
        let payloads = [
            // int $in
            json!({"scores": {"$in": [1, 2, 3]}}),
            // int $nin
            json!({"scores": {"$nin": [10, 20]}}),
            // bool $in
            json!({"flags": {"$in": [true, false]}}),
            // float $in
            json!({"ratings": {"$in": [1.5, 2.5, 3.5]}}),
        ];

        let expected_results = [
            Where::Metadata(MetadataExpression {
                key: "scores".to_string(),
                comparison: crate::MetadataComparison::Set(
                    crate::SetOperator::In,
                    crate::MetadataSetValue::Int(vec![1, 2, 3]),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "scores".to_string(),
                comparison: crate::MetadataComparison::Set(
                    crate::SetOperator::NotIn,
                    crate::MetadataSetValue::Int(vec![10, 20]),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "flags".to_string(),
                comparison: crate::MetadataComparison::Set(
                    crate::SetOperator::In,
                    crate::MetadataSetValue::Bool(vec![true, false]),
                ),
            }),
            Where::Metadata(MetadataExpression {
                key: "ratings".to_string(),
                comparison: crate::MetadataComparison::Set(
                    crate::SetOperator::In,
                    crate::MetadataSetValue::Float(vec![1.5, 2.5, 3.5]),
                ),
            }),
        ];

        for (payload, expected_result) in payloads.iter().zip(expected_results.iter()) {
            let result = parse_where(payload);
            assert!(
                result.is_ok(),
                "Parsing failed for payload: {}: {:?}",
                serde_json::to_string_pretty(payload).unwrap(),
                result
            );
            assert_eq!(
                result.unwrap(),
                *expected_result,
                "Parsed result did not match expected result: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }

    #[test]
    fn test_parse_where_in_mixed_types_rejected() {
        // $in / $nin arrays with mixed types should be rejected because the
        // parser requires all elements to match the type of the first element.
        let payloads = [
            json!({"key": {"$in": ["a", 1]}}),
            json!({"key": {"$in": [1, "b"]}}),
            json!({"key": {"$nin": [true, 1]}}),
        ];
        for payload in &payloads {
            let result = parse_where(payload);
            assert!(
                result.is_err(),
                "Expected error for mixed-type array, but got Ok for: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }

    #[test]
    fn test_parse_where_in_empty_array_rejected() {
        // $in / $nin with an empty array should be rejected.
        let payloads = [json!({"key": {"$in": []}}), json!({"key": {"$nin": []}})];
        for payload in &payloads {
            let result = parse_where(payload);
            assert!(
                result.is_err(),
                "Expected error for empty array, but got Ok for: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }

    #[test]
    fn test_parse_where_contains_not_valid_with_array_operand() {
        // $contains / $not_contains expect a scalar operand, not an array.
        let payloads = [
            json!({"tags": {"$contains": ["a", "b"]}}),
            json!({"tags": {"$not_contains": [1, 2]}}),
        ];
        for payload in &payloads {
            let result = parse_where(payload);
            assert!(
                result.is_err(),
                "Expected error for array operand in $contains, but got Ok for: {}",
                serde_json::to_string_pretty(payload).unwrap(),
            );
        }
    }
}
