use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    Operation, OperationRecord, ScalarEncoding, UpdateMetadata, UpdateMetadataValue,
    CHROMA_DOCUMENT_KEY, CHROMA_URI_KEY,
};

#[derive(thiserror::Error, Debug)]
pub(crate) enum ToRecordsError {
    #[error("Inconsistent number of IDs, embeddings, documents, URIs and metadatas")]
    InconsistentLength,
    #[error("Empty ID, ID must have at least one character")]
    EmptyId,
}

impl ChromaError for ToRecordsError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

pub(crate) fn to_records<
    MetadataValue: Into<UpdateMetadataValue>,
    M: IntoIterator<Item = (String, MetadataValue)>,
>(
    ids: Vec<String>,
    embeddings: Option<Vec<Option<Vec<f32>>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<M>>>,
    operation: Operation,
) -> Result<(Vec<OperationRecord>, u64), ToRecordsError> {
    let mut total_bytes = 0;
    let len = ids.len();

    // Check that every present vector has the same length as `ids`.
    if embeddings.as_ref().is_some_and(|v| v.len() != len)
        || documents.as_ref().is_some_and(|v| v.len() != len)
        || uris.as_ref().is_some_and(|v| v.len() != len)
        || metadatas.as_ref().is_some_and(|v| v.len() != len)
    {
        return Err(ToRecordsError::InconsistentLength);
    }

    let mut embeddings_iter = embeddings.into_iter().flat_map(|v| v.into_iter());
    let mut documents_iter = documents.into_iter().flat_map(|v| v.into_iter());
    let mut uris_iter = uris.into_iter().flat_map(|v| v.into_iter());
    let mut metadatas_iter = metadatas.into_iter().flat_map(|v| v.into_iter());

    let mut records = Vec::with_capacity(len);

    for id in ids {
        if id.is_empty() {
            return Err(ToRecordsError::EmptyId);
        }
        let embedding = embeddings_iter.next().flatten();
        let document = documents_iter.next().flatten();
        let uri = uris_iter.next().flatten();
        let metadata = metadatas_iter.next().flatten();

        let encoding = embedding.as_ref().map(|_| ScalarEncoding::FLOAT32);

        let mut metadata = metadata
            .map(|m| {
                m.into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect::<UpdateMetadata>()
            })
            .unwrap_or_default();
        if let Some(document) = document.clone() {
            metadata.insert(
                CHROMA_DOCUMENT_KEY.to_string(),
                UpdateMetadataValue::Str(document),
            );
        }
        if let Some(uri) = uri {
            metadata.insert(CHROMA_URI_KEY.to_string(), UpdateMetadataValue::Str(uri));
        }

        let record = OperationRecord {
            id,
            embedding,
            document,
            encoding,
            metadata: Some(metadata),
            operation,
        };

        total_bytes += record.size_bytes();

        records.push(record);
    }

    Ok((records, total_bytes))
}

#[cfg(test)]
mod tests {
    use chroma_types::Operation;

    use super::*;

    #[test]
    fn test_to_records_empty_id() {
        let ids = vec![String::from("")];
        let embeddings = vec![Some(vec![1.0, 2.0, 3.0])];
        let result = to_records::<
            chroma_types::UpdateMetadataValue,
            Vec<(String, chroma_types::UpdateMetadataValue)>,
        >(ids, Some(embeddings), None, None, None, Operation::Add);
        assert!(matches!(result, Err(ToRecordsError::EmptyId)));
    }

    #[test]
    fn test_normal_ids() {
        let ids = vec![String::from("1"), String::from("2"), String::from("3")];
        let embeddings = vec![
            Some(vec![1.0, 2.0, 3.0]),
            Some(vec![4.0, 5.0, 6.0]),
            Some(vec![7.0, 8.0, 9.0]),
        ];
        let documents = vec![
            Some(String::from("document 1")),
            Some(String::from("document 2")),
            Some(String::from("document 3")),
        ];
        let result = to_records::<
            chroma_types::UpdateMetadataValue,
            Vec<(String, chroma_types::UpdateMetadataValue)>,
        >(
            ids,
            Some(embeddings),
            Some(documents),
            None,
            None,
            Operation::Add,
        );
        assert!(result.is_ok());
        let records = result.unwrap().0;
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].id, "1");
        assert_eq!(records[1].id, "2");
        assert_eq!(records[2].id, "3");
        assert_eq!(records[0].embedding, Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(records[1].embedding, Some(vec![4.0, 5.0, 6.0]));
        assert_eq!(records[2].embedding, Some(vec![7.0, 8.0, 9.0]));
        assert_eq!(records[0].document, Some(String::from("document 1")));
        assert_eq!(records[1].document, Some(String::from("document 2")));
        assert_eq!(records[2].document, Some(String::from("document 3")));
    }
}
