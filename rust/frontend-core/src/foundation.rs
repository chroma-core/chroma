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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FoundationSource {
    kind: &'static str,
    dimension: i32,
}

impl FoundationSource {
    pub const fn kind(self) -> &'static str {
        self.kind
    }

    pub const fn dimension(self) -> i32 {
        self.dimension
    }
}

pub fn foundation_source_for_collection_name(
    collection_name: &str,
) -> Result<FoundationSource, FoundationSourceKindError> {
    let source = if collection_name.contains("slack") {
        FoundationSource {
            kind: "slack",
            dimension: 1024,
        }
    } else if collection_name.contains("notion") {
        FoundationSource {
            kind: "notion",
            dimension: 1024,
        }
    } else if collection_name.contains("gdrive") || collection_name.contains("google_drive") {
        FoundationSource {
            kind: "google_drive",
            dimension: 1,
        }
    } else if collection_name.contains("coding") || collection_name.contains("agent_sessions") {
        FoundationSource {
            kind: "coding_agent_session",
            dimension: 1024,
        }
    } else if collection_name.contains("granola") {
        FoundationSource {
            kind: "granola",
            dimension: 1024,
        }
    } else {
        return Err(FoundationSourceKindError::UnknownSourceCollection(
            collection_name.to_string(),
        ));
    };
    Ok(source)
}

pub fn source_kind_for_collection_name(
    collection_name: &str,
) -> Result<&'static str, FoundationSourceKindError> {
    foundation_source_for_collection_name(collection_name).map(FoundationSource::kind)
}

#[cfg(test)]
mod tests {
    use super::{foundation_source_for_collection_name, source_kind_for_collection_name};

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
    fn detects_gdrive_source_kind() {
        assert_eq!(
            source_kind_for_collection_name("gdrive").unwrap(),
            "google_drive"
        );
        assert_eq!(
            source_kind_for_collection_name("gdrive_master").unwrap(),
            "google_drive"
        );
        assert_eq!(
            source_kind_for_collection_name("google_drive").unwrap(),
            "google_drive"
        );
    }

    #[test]
    fn rejects_unknown_source_kind() {
        assert!(source_kind_for_collection_name("unknown_source").is_err());
    }

    #[test]
    fn carries_source_specific_dimensions() {
        assert_eq!(
            foundation_source_for_collection_name("gdrive_master")
                .unwrap()
                .dimension(),
            1
        );
        assert_eq!(
            foundation_source_for_collection_name("notion_master")
                .unwrap()
                .dimension(),
            1024
        );
    }

    #[test]
    fn detects_coding_source_kind() {
        assert_eq!(
            source_kind_for_collection_name("coding").unwrap(),
            "coding_agent_session"
        );
        assert_eq!(
            source_kind_for_collection_name("my_coding_collection").unwrap(),
            "coding_agent_session"
        );
    }

    #[test]
    fn detects_agent_sessions_source_kind() {
        assert_eq!(
            source_kind_for_collection_name("agent_sessions_hammad").unwrap(),
            "coding_agent_session"
        );
    }

    #[test]
    fn detects_granola_source_kind() {
        assert_eq!(
            source_kind_for_collection_name("granola").unwrap(),
            "granola"
        );
        assert_eq!(
            source_kind_for_collection_name("granola_master").unwrap(),
            "granola"
        );
    }
}
