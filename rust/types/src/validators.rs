use crate::CollectionMetadataUpdate;
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
        CollectionMetadataUpdate::UpdateMetadata(metadata) => validate_non_empty_metadata(metadata),
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
