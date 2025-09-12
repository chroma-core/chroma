use crate::{
    operator::{Rank, RankExpr},
    CollectionMetadataUpdate, Metadata, MetadataValue, UpdateMetadata, UpdateMetadataValue,
};
use regex::Regex;
use std::collections::HashMap;
use std::str::FromStr;
use std::{net::IpAddr, sync::LazyLock};
use validator::ValidationError;

static ALNUM_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{1, 510}[a-zA-Z0-9]$")
        .expect("The alphanumeric regex should be valid")
});

static DP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\.\.").expect("The double period regex should be valid"));

pub(crate) fn validate_non_empty_collection_update_metadata(
    update: &CollectionMetadataUpdate,
) -> Result<(), ValidationError> {
    match update {
        CollectionMetadataUpdate::UpdateMetadata(metadata) => {
            validate_non_empty_metadata(metadata)?;
            validate_update_metadata(metadata)
        }
        CollectionMetadataUpdate::ResetMetadata => Ok(()),
    }
}

pub(crate) fn validate_non_empty_metadata<V>(
    metadata: &HashMap<String, V>,
) -> Result<(), ValidationError> {
    if metadata.is_empty() {
        Err(ValidationError::new("metadata").with_message("Metadata cannot be empty".into()))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_name(name: impl AsRef<str>) -> Result<(), ValidationError> {
    let name_str = name.as_ref();
    if !ALNUM_RE.is_match(name_str) {
        return Err(ValidationError::new("name").with_message(format!("Expected a name containing 3-512 characters from [a-zA-Z0-9._-], starting and ending with a character in [a-zA-Z0-9]. Got: {name_str}").into()));
    }

    if DP_RE.is_match(name_str) {
        return Err(ValidationError::new("name").with_message(
            format!(
            "Expected a name that does not contains two consecutive periods (..). Got {name_str}"
        )
            .into(),
        ));
    }
    if IpAddr::from_str(name_str).is_ok() {
        return Err(ValidationError::new("name").with_message(
            format!("Expected a name that is not a valid ip address. Got {name_str}").into(),
        ));
    }
    Ok(())
}

/// Validate a single metadata key
fn validate_metadata_key(key: &str) -> Result<(), ValidationError> {
    if key.starts_with('#') || key.starts_with('$') {
        Err(ValidationError::new("metadata_key")
            .with_message(format!("Metadata key cannot start with '#' or '$': {}", key).into()))
    } else {
        Ok(())
    }
}

/// Validate metadata
pub fn validate_metadata(metadata: &Metadata) -> Result<(), ValidationError> {
    for (key, value) in metadata {
        validate_metadata_key(key)?;

        if let MetadataValue::SparseVector(sv) = value {
            sv.validate().map_err(|e| {
                ValidationError::new("sparse_vector")
                    .with_message(format!("Invalid sparse vector: {}", e).into())
            })?;
        }
    }
    Ok(())
}

/// Validate update metadata
pub fn validate_update_metadata(metadata: &UpdateMetadata) -> Result<(), ValidationError> {
    for (key, value) in metadata {
        validate_metadata_key(key)?;

        if let UpdateMetadataValue::SparseVector(sv) = value {
            sv.validate().map_err(|e| {
                ValidationError::new("sparse_vector")
                    .with_message(format!("Invalid sparse vector: {}", e).into())
            })?;
        }
    }
    Ok(())
}

/// Validate optional vector of optional metadata
pub fn validate_metadata_vec(metadatas: &Vec<Option<Metadata>>) -> Result<(), ValidationError> {
    for (i, metadata_opt) in metadatas.iter().enumerate() {
        if let Some(metadata) = metadata_opt {
            validate_metadata(metadata).map_err(|_| {
                ValidationError::new("metadata")
                    .with_message(format!("Invalid metadata at index {}", i).into())
            })?;
        }
    }
    Ok(())
}

/// Validate optional vector of optional update metadata
pub fn validate_update_metadata_vec(
    metadatas: &Vec<Option<UpdateMetadata>>,
) -> Result<(), ValidationError> {
    for (i, metadata_opt) in metadatas.iter().enumerate() {
        if let Some(metadata) = metadata_opt {
            validate_update_metadata(metadata).map_err(|_| {
                ValidationError::new("metadata")
                    .with_message(format!("Invalid metadata at index {}", i).into())
            })?;
        }
    }
    Ok(())
}

/// Validate optional metadata (for CreateCollectionRequest)
pub fn validate_optional_metadata(metadata: &Metadata) -> Result<(), ValidationError> {
    // First check it's not empty
    validate_non_empty_metadata(metadata)?;
    // Then validate keys and sparse vectors
    validate_metadata(metadata)?;
    Ok(())
}

/// Validate rank operator for sparse vectors
pub fn validate_rank(rank: &Rank) -> Result<(), ValidationError> {
    if let Some(expr) = &rank.expr {
        validate_rank_expr(expr)?;
    }
    Ok(())
}

fn validate_rank_expr(expr: &RankExpr) -> Result<(), ValidationError> {
    match expr {
        RankExpr::Knn { query, .. } => {
            if let crate::operator::QueryVector::Sparse(sv) = query {
                sv.validate().map_err(|e| {
                    ValidationError::new("sparse_vector")
                        .with_message(format!("Invalid sparse vector in KNN query: {}", e).into())
                })?;
            }
        }
        RankExpr::Absolute(inner)
        | RankExpr::Exponentiation(inner)
        | RankExpr::Logarithm(inner) => validate_rank_expr(inner)?,
        RankExpr::Division { left, right } | RankExpr::Subtraction { left, right } => {
            validate_rank_expr(left)?;
            validate_rank_expr(right)?;
        }
        RankExpr::Maximum(exprs)
        | RankExpr::Minimum(exprs)
        | RankExpr::Multiplication(exprs)
        | RankExpr::Summation(exprs) => {
            for expr in exprs {
                validate_rank_expr(expr)?;
            }
        }
        RankExpr::Value(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetadataValue, SparseVector};

    #[test]
    fn test_metadata_validation() {
        // Valid metadata
        let mut metadata = Metadata::new();
        metadata.insert("valid_key".to_string(), MetadataValue::Int(42));
        let sparse = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2, 0.3]);
        metadata.insert("embedding".to_string(), MetadataValue::SparseVector(sparse));
        assert!(validate_metadata(&metadata).is_ok());

        // Invalid key starting with #
        let mut metadata = Metadata::new();
        metadata.insert("#invalid".to_string(), MetadataValue::Int(42));
        assert!(validate_metadata(&metadata).is_err());

        // Invalid sparse vector (length mismatch)
        let mut metadata = Metadata::new();
        let invalid_sparse = SparseVector::new(vec![1, 2], vec![0.1, 0.2, 0.3]);
        metadata.insert(
            "embedding".to_string(),
            MetadataValue::SparseVector(invalid_sparse),
        );
        assert!(validate_metadata(&metadata).is_err());
    }
}
