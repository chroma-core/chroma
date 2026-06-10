use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FoundationSourceKindError {
    #[error("unknown foundation source collection: {0}")]
    UnknownSourceCollection(String),
}

impl ChromaError for FoundationSourceKindError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

pub fn source_kind_for_collection_name(
    collection_name: &str,
) -> Result<&'static str, FoundationSourceKindError> {
    if collection_name.contains("slack") {
        return Ok("slack");
    }
    if collection_name.contains("notion") {
        return Ok("notion");
    }
    if collection_name.contains("coding") {
        return Ok("coding_agent_sessions");
    }
    Err(FoundationSourceKindError::UnknownSourceCollection(
        collection_name.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::source_kind_for_collection_name;

    #[test]
    fn detects_slack_source_kind() {
        assert_eq!(source_kind_for_collection_name("slack").unwrap(), "slack");
        assert_eq!(
            source_kind_for_collection_name("slack_master").unwrap(),
            "slack"
        );
    }

    #[test]
    fn detects_notion_source_kind() {
        assert_eq!(source_kind_for_collection_name("notion").unwrap(), "notion");
        assert_eq!(
            source_kind_for_collection_name("notion_master").unwrap(),
            "notion"
        );
    }

    #[test]
    fn rejects_unknown_source_kind() {
        assert!(source_kind_for_collection_name("unknown_source").is_err());
    }

    #[test]
    fn detects_coding_source_kind() {
        assert_eq!(
            source_kind_for_collection_name("coding").unwrap(),
            "coding_agent_sessions"
        );
        assert_eq!(
            source_kind_for_collection_name("my_coding_collection").unwrap(),
            "coding_agent_sessions"
        );
    }
}
