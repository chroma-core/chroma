use crate::{
    execution::plan::SearchPayload,
    operator::{Aggregate, GroupBy, QueryVector, Rank},
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

pub fn validate_name(name: impl AsRef<str>) -> Result<(), ValidationError> {
    let name_str = name.as_ref();

    // A topology is a valid name.  A database name prefixed with a topology is a valid name.  The
    // conjuntion must be separated by a single `+` and not exceed the database name limits.
    // Thus, we recurse after validating no more plusses.
    if let Some((topo, name)) = name_str.split_once('+') {
        if name_str.len() > 512 {
            return Err(ValidationError::new("name").with_message(
                format!(
                    "Expected a name containing 3-512 characters. Got: {}",
                    name_str.len()
                )
                .into(),
            ));
        }
        if name.chars().any(|c| c == '+') {
            return Err(ValidationError::new("name").with_message(
                "Expected a name to contain at most one topology:  Got two `+` characters.".into(),
            ));
        }
        assert!(
            !topo.chars().any(|c| c == '+'),
            "split once should not bypass the split character"
        );
        validate_name(topo)?;
        validate_name(name)?;
        return Ok(());
    }

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
    for knn in rank.knn_queries() {
        if let QueryVector::Sparse(sv) = &knn.query {
            sv.validate().map_err(|e| {
                ValidationError::new("sparse_vector")
                    .with_message(format!("Invalid sparse vector in KNN query: {}", e).into())
            })?;
        }
    }
    Ok(())
}

/// Validate group_by operator
pub fn validate_group_by(group_by: &GroupBy) -> Result<(), ValidationError> {
    let has_keys = !group_by.keys.is_empty();
    let has_aggregate = group_by.aggregate.is_some();

    if has_keys != has_aggregate {
        return Err(ValidationError::new("group_by").with_message(
            "group_by keys and aggregate must both be specified or both be omitted".into(),
        ));
    }

    // Validate group_by keys: only metadata fields are allowed
    for key in &group_by.keys {
        match key {
            crate::operator::Key::MetadataField(_) => {}
            _ => {
                return Err(ValidationError::new("group_by").with_message(
                    "group_by keys must be metadata fields (cannot use #score, #document, #embedding, or #metadata)".into(),
                ));
            }
        }
    }

    match &group_by.aggregate {
        Some(Aggregate::MinK { keys, k }) | Some(Aggregate::MaxK { keys, k }) => {
            if keys.is_empty() {
                return Err(ValidationError::new("group_by")
                    .with_message("aggregate keys must not be empty".into()));
            }
            if *k == 0 {
                return Err(ValidationError::new("group_by")
                    .with_message("aggregate k must be greater than 0".into()));
            }
            // Validate aggregate keys: only metadata fields and score are allowed
            for key in keys {
                match key {
                    crate::operator::Key::MetadataField(_) | crate::operator::Key::Score => {}
                    _ => {
                        return Err(ValidationError::new("group_by").with_message(
                            "aggregate keys must be metadata fields or #score (cannot use #document, #embedding, or #metadata)".into(),
                        ));
                    }
                }
            }
        }
        None => {}
    }

    Ok(())
}

/// Validate SearchPayload
pub fn validate_search_payload(payload: &SearchPayload) -> Result<(), ValidationError> {
    if !payload.group_by.keys.is_empty() && payload.rank.expr.is_none() {
        return Err(ValidationError::new("group_by")
            .with_message("group_by requires rank expression to be specified".into()));
    }
    Ok(())
}

/// Validate schema
pub fn validate_schema(schema: &Schema) -> Result<(), ValidationError> {
    // Prevent users from setting source_attached_function_id - only the system can set this
    if schema.source_attached_function_id.is_some() {
        return Err(ValidationError::new("schema").with_message(
            "Cannot set source_attached_function_id. This field is reserved for system use.".into(),
        ));
    }

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
    if let Some(cmek) = &schema.cmek {
        if !cmek.validate_pattern() {
            return Err(ValidationError::new("schema")
                .with_message(format!("CMEK does not match expected pattern: {cmek:?}").into()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Key;
    use crate::{MetadataValue, SparseVector};

    #[test]
    fn valid_simple_name() {
        assert!(validate_name("abc").is_ok());
        assert!(validate_name("my_collection").is_ok());
        assert!(validate_name("my-collection").is_ok());
        assert!(validate_name("my.collection").is_ok());
        assert!(validate_name("MyCollection123").is_ok());
    }

    #[test]
    fn invalid_simple_name_too_short() {
        assert!(validate_name("ab").is_err());
        assert!(validate_name("a").is_err());
        assert!(validate_name("").is_err());
    }

    #[test]
    fn invalid_simple_name_bad_start_or_end() {
        assert!(validate_name("_abc").is_err());
        assert!(validate_name("-abc").is_err());
        assert!(validate_name(".abc").is_err());
        assert!(validate_name("abc_").is_err());
        assert!(validate_name("abc-").is_err());
        assert!(validate_name("abc.").is_err());
    }

    #[test]
    fn invalid_simple_name_double_period() {
        assert!(validate_name("abc..def").is_err());
        assert!(validate_name("my..collection").is_err());
    }

    #[test]
    fn invalid_simple_name_ip_address() {
        assert!(validate_name("192.168.0.1").is_err());
        assert!(validate_name("127.0.0.1").is_err());
    }

    #[test]
    fn valid_topology_prefixed_name() {
        assert!(validate_name("topo+name").is_ok());
        assert!(validate_name("my_topology+my_collection").is_ok());
        assert!(validate_name("region1+database").is_ok());
        assert!(validate_name("abc+def").is_ok());
    }

    #[test]
    fn invalid_topology_prefixed_name_multiple_plus() {
        assert!(validate_name("a+b+c").is_err());
        assert!(validate_name("topo+name+extra").is_err());
        assert!(validate_name("one+two+three").is_err());
    }

    #[test]
    fn invalid_topology_prefixed_name_invalid_topology() {
        // Topology part must be valid (3+ chars, start/end with alnum)
        assert!(validate_name("ab+valid").is_err());
        assert!(validate_name("_bad+valid").is_err());
        assert!(validate_name("bad_+valid").is_err());
    }

    #[test]
    fn invalid_topology_prefixed_name_invalid_name() {
        // Name part must be valid (3+ chars, start/end with alnum)
        assert!(validate_name("valid+ab").is_err());
        assert!(validate_name("valid+_bad").is_err());
        assert!(validate_name("valid+bad_").is_err());
    }

    #[test]
    fn invalid_topology_prefixed_name_too_long() {
        // Total length exceeds 512
        let long_topo = "a".repeat(256);
        let long_name = "b".repeat(258);
        let too_long = format!("{}+{}", long_topo, long_name);
        assert!(too_long.len() > 512);
        assert!(validate_name(&too_long).is_err());
    }

    #[test]
    fn valid_topology_prefixed_name_at_limit() {
        // Total length exactly 512
        let topo = "a".repeat(255);
        let name = "b".repeat(256);
        let at_limit = format!("{}+{}", topo, name);
        assert_eq!(at_limit.len(), 512);
        assert!(validate_name(&at_limit).is_ok());
    }

    #[test]
    fn test_metadata_validation() {
        // Valid metadata
        let mut metadata = Metadata::new();
        metadata.insert("valid_key".to_string(), MetadataValue::Int(42));
        let sparse = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2, 0.3]).unwrap();
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
        let invalid_sparse = SparseVector {
            indices: vec![1, 2],
            values: vec![0.1, 0.2, 0.3],
            tokens: None,
        };
        metadata.insert(
            "embedding".to_string(),
            MetadataValue::SparseVector(invalid_sparse),
        );
        assert!(validate_metadata(&metadata).is_err());
    }

    #[test]
    fn test_validate_group_by() {
        // Valid: both keys and aggregate present
        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 3,
            }),
        };
        assert!(validate_group_by(&group_by).is_ok());

        // Valid: both empty
        let group_by = GroupBy {
            keys: vec![],
            aggregate: None,
        };
        assert!(validate_group_by(&group_by).is_ok());

        // Invalid: keys present, aggregate missing
        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: None,
        };
        assert!(validate_group_by(&group_by).is_err());

        // Invalid: aggregate present, keys missing
        let group_by = GroupBy {
            keys: vec![],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 3,
            }),
        };
        assert!(validate_group_by(&group_by).is_err());

        // Invalid: aggregate k = 0
        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 0,
            }),
        };
        assert!(validate_group_by(&group_by).is_err());

        // Invalid: aggregate keys empty
        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MaxK { keys: vec![], k: 5 }),
        };
        assert!(validate_group_by(&group_by).is_err());

        // Invalid: group_by key must be metadata field (not #score)
        let group_by = GroupBy {
            keys: vec![Key::Score],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 3,
            }),
        };
        assert!(validate_group_by(&group_by).is_err());

        // Invalid: aggregate key cannot be #document
        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Document],
                k: 3,
            }),
        };
        assert!(validate_group_by(&group_by).is_err());

        // Valid: aggregate key can be metadata field
        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::field("date"), Key::Score],
                k: 3,
            }),
        };
        assert!(validate_group_by(&group_by).is_ok());
    }

    #[test]
    fn test_validate_search_payload() {
        use crate::operator::{QueryVector, RankExpr};

        // Valid: group_by with rank expression
        let payload = SearchPayload {
            rank: Rank {
                expr: Some(RankExpr::Knn {
                    query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
                    key: Key::Embedding,
                    limit: 100,
                    default: None,
                    return_rank: false,
                }),
            },
            group_by: GroupBy {
                keys: vec![Key::field("category")],
                aggregate: Some(Aggregate::MinK {
                    keys: vec![Key::Score],
                    k: 3,
                }),
            },
            ..Default::default()
        };
        assert!(validate_search_payload(&payload).is_ok());

        // Valid: no group_by, no rank
        let payload = SearchPayload::default();
        assert!(validate_search_payload(&payload).is_ok());

        // Valid: rank without group_by
        let payload = SearchPayload {
            rank: Rank {
                expr: Some(RankExpr::Knn {
                    query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
                    key: Key::Embedding,
                    limit: 100,
                    default: None,
                    return_rank: false,
                }),
            },
            ..Default::default()
        };
        assert!(validate_search_payload(&payload).is_ok());

        // Invalid: group_by without rank expression
        let payload = SearchPayload {
            rank: Rank { expr: None },
            group_by: GroupBy {
                keys: vec![Key::field("category")],
                aggregate: Some(Aggregate::MinK {
                    keys: vec![Key::Score],
                    k: 3,
                }),
            },
            ..Default::default()
        };
        assert!(validate_search_payload(&payload).is_err());
    }
}
