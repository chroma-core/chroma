use std::{net::IpAddr, str::FromStr, sync::LazyLock};

use chroma_types::{operator::Filter, UpdateMetadata};
use regex::Regex;

use crate::types::errors::ValidationError;

static ALNUM_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{1, 61}[a-zA-Z0-9]$")
        .expect("The alphanumeric regex should be valid")
});

static DP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\.\.").expect("The double period regex should be valid"));

pub fn validate_dimension(
    dimension: u32,
    embeddings: &Vec<Vec<f32>>,
) -> Result<(), ValidationError> {
    for emb in embeddings {
        if emb.len() as u32 != dimension {
            return Err(ValidationError::DimensionMismatch(
                dimension,
                emb.len() as u32,
            ));
        }
    }
    Ok(())
}

pub fn validate_non_empty_filter(filter: Filter) -> Result<(), ValidationError> {
    if let Filter {
        query_ids: None,
        where_clause: None,
    } = filter
    {
        Err(ValidationError::EmptyDelete)
    } else {
        Ok(())
    }
}

pub fn validate_name(name: impl AsRef<str>) -> Result<(), ValidationError> {
    let name_str = name.as_ref();
    if !ALNUM_RE.is_match(name_str) {
        return Err(ValidationError::Name(format!("Expected a name containing 3-63 characters from [a-zA-Z0-9._-], starting and ending with a character in [a-zA-Z0-9]. Got: {name_str}")));
    }

    if DP_RE.is_match(name_str) {
        return Err(ValidationError::Name(format!(
            "Expected a name that does not contains two consecutive periods (..). Got {name_str}"
        )));
    }
    if IpAddr::from_str(name_str).is_ok() {
        return Err(ValidationError::Name(format!(
            "Expected a name that is not a valid ip address. Got {name_str}"
        )));
    }
    Ok(())
}
