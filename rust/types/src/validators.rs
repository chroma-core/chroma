use crate::{
    operator::{Rank, RankExpr},
    CollectionMetadataUpdate, Metadata, MetadataValue, Schema, UpdateMetadata, UpdateMetadataValue,
    DOCUMENT_KEY, EMBEDDING_KEY,
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
    if key.is_empty() {
        Err(ValidationError::new("metadata_key")
            .with_message("Metadata key cannot be empty".into()))
    } else if key.starts_with('#') || key.starts_with('$') {
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
pub fn validate_metadata_vec(metadatas: &[Option<Metadata>]) -> Result<(), ValidationError> {
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
    metadatas: &[Option<UpdateMetadata>],
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

/// Validate schema
pub fn validate_schema(schema: &Schema) -> Result<(), ValidationError> {
    let mut sparse_index_keys = Vec::new();
    if schema
        .defaults
        .float_list
        .as_ref()
        .is_some_and(|vt| vt.vector_index.as_ref().is_some_and(|it| it.enabled))
    {
        return Err(ValidationError::new("schema").with_message("Vector index cannot be enabled by default. It can only be enabled on #embedding field.".into()));
    }
    if schema.defaults.float_list.as_ref().is_some_and(|vt| {
        vt.vector_index
            .as_ref()
            .is_some_and(|it| it.config.hnsw.is_some() && it.config.spann.is_some())
    }) {
        return Err(ValidationError::new("schema").with_message(
            "Both spann and hnsw config cannot be present at the same time.".into(),
        ));
    }
    if schema
        .defaults
        .sparse_vector
        .as_ref()
        .is_some_and(|vt| vt.sparse_vector_index.as_ref().is_some_and(|it| it.enabled))
    {
        return Err(ValidationError::new("schema").with_message("Sparse vector index cannot be enabled by default. Please enable sparse vector index on specific keys. At most one sparse vector index is allowed for the collection.".into()));
    }
    if schema
        .defaults
        .string
        .as_ref()
        .is_some_and(|vt| vt.fts_index.as_ref().is_some_and(|it| it.enabled))
    {
        return Err(ValidationError::new("schema").with_message("Full text search / regular expression index cannot be enabled by default. It can only be enabled on #document field.".into()));
    }
    for (key, config) in &schema.keys {
        // Validate that keys cannot start with # (except system keys)
        if key.starts_with('#') && key != DOCUMENT_KEY && key != EMBEDDING_KEY {
            return Err(ValidationError::new("schema").with_message(
                format!("key cannot begin with '#'. Keys starting with '#' are reserved for system use: {key}")
                    .into(),
            ));
        }

        if key == DOCUMENT_KEY
            && (config.boolean.is_some()
                || config.float.is_some()
                || config.int.is_some()
                || config.float_list.is_some()
                || config.sparse_vector.is_some())
        {
            return Err(ValidationError::new("schema").with_message(
                format!("Document field cannot have any value types other than string: {key}")
                    .into(),
            ));
        }
        if key == EMBEDDING_KEY
            && (config.boolean.is_some()
                || config.float.is_some()
                || config.int.is_some()
                || config.string.is_some()
                || config.sparse_vector.is_some())
        {
            return Err(ValidationError::new("schema").with_message(
                format!("Embedding field cannot have any value types other than float_list: {key}")
                    .into(),
            ));
        }
        if let Some(vit) = config
            .float_list
            .as_ref()
            .and_then(|vt| vt.vector_index.as_ref())
        {
            if vit.enabled && key != EMBEDDING_KEY {
                return Err(ValidationError::new("schema").with_message(
                    format!("Vector index can only be enabled on #embedding field: {key}").into(),
                ));
            }
            if vit
                .config
                .source_key
                .as_ref()
                .is_some_and(|key| key != DOCUMENT_KEY)
            {
                return Err(ValidationError::new("schema")
                    .with_message("Vector index can only source from #document".into()));
            }
        }
        if let Some(svit) = config
            .sparse_vector
            .as_ref()
            .and_then(|vt| vt.sparse_vector_index.as_ref())
        {
            if svit.enabled {
                sparse_index_keys.push(key);
                if sparse_index_keys.len() > 1 {
                    return Err(ValidationError::new("schema").with_message(
                        format!("At most one sparse vector index is allowed for the collection: {sparse_index_keys:?}")
                            .into(),
                    ));
                }
                if svit.config.source_key.is_some() && svit.config.embedding_function.is_none() {
                    return Err(ValidationError::new("schema").with_message(
                        "If source_key is provided then embedding_function must also be provided since there is no default embedding function.".into(),
                    ));
                }
            }
            // Validate source_key for sparse vector index
            if let Some(source_key) = &svit.config.source_key {
                if source_key.starts_with('#') && source_key != DOCUMENT_KEY {
                    return Err(ValidationError::new("schema").with_message(
                        "source_key cannot begin with '#'. The only valid key starting with '#' is Key.DOCUMENT or '#document'.".into(),
                    ));
                }
            }
        }
        if config
            .string
            .as_ref()
            .is_some_and(|vt| vt.fts_index.as_ref().is_some_and(|it| it.enabled))
            && key != DOCUMENT_KEY
        {
            return Err(ValidationError::new("schema").with_message(format!("Full text search / regular expression index can only be enabled on #document field: {key}").into()));
        }
        if config.string.as_ref().is_some_and(|vt| {
            vt.string_inverted_index
                .as_ref()
                .is_some_and(|it| it.enabled)
        }) && key == DOCUMENT_KEY
        {
            return Err(ValidationError::new("schema").with_message(
                format!("String inverted index can not be enabled on #document key: {key}").into(),
            ));
        }
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
        metadata.insert("#embedding".to_string(), MetadataValue::Int(42));
        assert!(validate_metadata(&metadata).is_err());

        // Invalid key starting with #
        let mut metadata = Metadata::new();
        metadata.insert("#invalid".to_string(), MetadataValue::Int(42));
        assert!(validate_metadata(&metadata).is_err());

        // Invalid key starting with $
        let mut metadata = Metadata::new();
        metadata.insert("$invalid".to_string(), MetadataValue::Int(42));
        assert!(validate_metadata(&metadata).is_err());

        // Invalid empty key
        let mut metadata = Metadata::new();
        metadata.insert("".to_string(), MetadataValue::Int(42));
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
