use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{
    spann::quantized_spann::{
        QuantizedSpannError, QuantizedSpannFlusher, QuantizedSpannIds, QuantizedSpannIndexWriter,
    },
    usearch::{USearchIndex, USearchIndexProvider},
    IndexUuid,
};
use chroma_types::{
    Collection, MaterializedLogOperation, Schema, SchemaError, Segment, SegmentScope, SegmentType,
    SegmentUuid, OFFSET_ID_TO_DATA, QUANTIZED_SPANN_CLUSTER, QUANTIZED_SPANN_EMBEDDING_METADATA,
    QUANTIZED_SPANN_QUANTIZED_CENTROID, QUANTIZED_SPANN_RAW_CENTROID,
    QUANTIZED_SPANN_SCALAR_METADATA,
};
use thiserror::Error;

use crate::blockfile_record::ApplyMaterializedLogError;
use crate::types::{ChromaSegmentFlusher, MaterializeLogsResult};

#[derive(Error, Debug)]
pub enum QuantizedSpannSegmentError {
    #[error("quantized spann config error: {0}")]
    Config(String),
    #[error("quantized spann data error: {0}")]
    Data(String),
    #[error(transparent)]
    Index(#[from] QuantizedSpannError),
    #[error(transparent)]
    Schema(#[from] SchemaError),
}

impl ChromaError for QuantizedSpannSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Config(_) => ErrorCodes::InvalidArgument,
            Self::Data(_) => ErrorCodes::Internal,
            Self::Index(e) => e.code(),
            Self::Schema(e) => e.code(),
        }
    }
}

#[derive(Clone)]
pub struct QuantizedSpannSegmentWriter {
    blockfile_provider: BlockfileProvider,
    pub id: SegmentUuid,
    index: QuantizedSpannIndexWriter<USearchIndex>,
    usearch_provider: USearchIndexProvider,
}

impl Debug for QuantizedSpannSegmentWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuantizedSpannSegmentWriter")
            .field("id", &self.id)
            .finish()
    }
}

impl QuantizedSpannSegmentWriter {
    pub async fn from_segment(
        collection: &Collection,
        vector_segment: &Segment,
        record_segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannSegmentError> {
        if vector_segment.r#type != SegmentType::QuantizedSpann
            || vector_segment.scope != SegmentScope::VECTOR
        {
            return Err(QuantizedSpannSegmentError::Config(
                "segment type must be QuantizedSpann with VECTOR scope".to_string(),
            ));
        }

        let schema = match &collection.schema {
            Some(schema) => schema.clone(),
            None => Schema::try_from(&collection.config)?,
        };

        let (spann_config, space) = schema.get_spann_config().ok_or_else(|| {
            QuantizedSpannSegmentError::Config("missing spann configuration".to_string())
        })?;
        let distance_function: DistanceFunction = space.into();
        let cmek = schema.cmek.clone();

        let dimensionality = collection.dimension.ok_or_else(|| {
            QuantizedSpannSegmentError::Config("collection dimension not set".to_string())
        })? as usize;

        // Extract file paths from vector segment metadata.
        let file_path_keys = [
            QUANTIZED_SPANN_CLUSTER,
            QUANTIZED_SPANN_EMBEDDING_METADATA,
            QUANTIZED_SPANN_QUANTIZED_CENTROID,
            QUANTIZED_SPANN_RAW_CENTROID,
            QUANTIZED_SPANN_SCALAR_METADATA,
        ];

        let mut parsed = Vec::new();
        for key in &file_path_keys {
            if let Some(paths) = vector_segment.file_path.get(*key) {
                let path = paths.first().ok_or_else(|| {
                    QuantizedSpannSegmentError::Config(format!("empty file path for {key}"))
                })?;
                let (prefix, id) = Segment::extract_prefix_and_id(path).map_err(|e| {
                    QuantizedSpannSegmentError::Config(format!(
                        "failed to parse file path for {key}: {e}"
                    ))
                })?;
                parsed.push((prefix, id));
            }
        }

        if !parsed.is_empty() && parsed.len() != file_path_keys.len() {
            return Err(QuantizedSpannSegmentError::Config(
                "partial file paths: all or none must be present".to_string(),
            ));
        }

        let prefix_path = if parsed.is_empty() {
            vector_segment.construct_prefix_path(&collection.tenant, &collection.database_id)
        } else {
            let prefix = parsed[0].0;
            for (i, (p, _)) in parsed.iter().enumerate().skip(1) {
                if *p != prefix {
                    return Err(QuantizedSpannSegmentError::Config(format!(
                        "inconsistent prefix path for {}",
                        file_path_keys[i]
                    )));
                }
            }
            prefix.to_string()
        };

        let index = if !parsed.is_empty() {
            // Open the raw embedding reader from the record segment if available.
            let raw_embedding_reader = match record_segment.file_path.get(OFFSET_ID_TO_DATA) {
                Some(paths) => match paths.first() {
                    Some(path) => {
                        let (prefix, id) = Segment::extract_prefix_and_id(path).map_err(|e| {
                            QuantizedSpannSegmentError::Config(format!(
                                "failed to parse record segment file path: {e}"
                            ))
                        })?;
                        let options = BlockfileReaderOptions::new(id, prefix.to_string());
                        let reader = blockfile_provider.read(options).await.map_err(|e| {
                            QuantizedSpannSegmentError::Config(format!(
                                "failed to open record segment reader: {e}"
                            ))
                        })?;
                        Some(reader)
                    }
                    None => None,
                },
                None => None,
            };

            // Order matches file_path_keys: cluster[0], embedding_metadata[1],
            // quantized_centroid[2], raw_centroid[3], scalar_metadata[4].
            let file_ids = QuantizedSpannIds {
                embedding_metadata_id: parsed[1].1,
                prefix_path: prefix_path.clone(),
                quantized_centroid_id: IndexUuid(parsed[2].1),
                quantized_cluster_id: parsed[0].1,
                raw_centroid_id: IndexUuid(parsed[3].1),
                scalar_metadata_id: parsed[4].1,
            };
            QuantizedSpannIndexWriter::open(
                vector_segment.collection,
                spann_config,
                dimensionality,
                distance_function,
                file_ids,
                cmek,
                prefix_path.clone(),
                raw_embedding_reader,
                blockfile_provider,
                usearch_provider,
            )
            .await?
        } else {
            QuantizedSpannIndexWriter::create(
                vector_segment.collection,
                spann_config,
                dimensionality,
                distance_function,
                cmek,
                prefix_path.clone(),
                usearch_provider,
            )
            .await?
        };

        Ok(Self {
            blockfile_provider: blockfile_provider.clone(),
            id: vector_segment.id,
            index,
            usearch_provider: usearch_provider.clone(),
        })
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        materialized_chunk: &MaterializeLogsResult,
    ) -> Result<(), ApplyMaterializedLogError> {
        for record in materialized_chunk {
            match record.get_operation() {
                MaterializedLogOperation::AddNew
                | MaterializedLogOperation::OverwriteExisting => {
                    let embedding =
                        record.embeddings_ref_from_log().ok_or_else(|| {
                            QuantizedSpannSegmentError::Data(
                                "embedding missing for add/overwrite operation".to_string(),
                            )
                        })?;
                    self.index
                        .add(record.get_offset_id(), embedding)
                        .await
                        .map_err(QuantizedSpannSegmentError::from)?;
                }
                MaterializedLogOperation::UpdateExisting => {
                    if let Some(embedding) = record.embeddings_ref_from_log() {
                        self.index
                            .add(record.get_offset_id(), embedding)
                            .await
                            .map_err(QuantizedSpannSegmentError::from)?;
                    }
                }
                MaterializedLogOperation::DeleteExisting => {
                    self.index.remove(record.get_offset_id());
                }
                MaterializedLogOperation::Initial => panic!(
                    "Invariant violation. Materialized records should not contain logs in initial state"
                ),
            }
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .finish(&self.usearch_provider)
            .await
            .map_err(|e| Box::new(QuantizedSpannSegmentError::from(e)) as Box<dyn ChromaError>)
    }

    pub async fn commit(self) -> Result<QuantizedSpannSegmentFlusher, Box<dyn ChromaError>> {
        let flusher = self
            .index
            .commit(&self.blockfile_provider, &self.usearch_provider)
            .await
            .map_err(|e| Box::new(QuantizedSpannSegmentError::from(e)) as Box<dyn ChromaError>)?;
        Ok(QuantizedSpannSegmentFlusher {
            flusher,
            id: self.id,
        })
    }
}

pub struct QuantizedSpannSegmentFlusher {
    flusher: QuantizedSpannFlusher,
    pub id: SegmentUuid,
}

impl Debug for QuantizedSpannSegmentFlusher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuantizedSpannSegmentFlusher")
            .field("id", &self.id)
            .finish()
    }
}

impl QuantizedSpannSegmentFlusher {
    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let ids =
            self.flusher.flush().await.map_err(|e| {
                Box::new(QuantizedSpannSegmentError::from(e)) as Box<dyn ChromaError>
            })?;

        let mut file_paths = HashMap::new();
        file_paths.insert(
            QUANTIZED_SPANN_CLUSTER.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.quantized_cluster_id,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_EMBEDDING_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.embedding_metadata_id,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_QUANTIZED_CENTROID.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.quantized_centroid_id.0,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_RAW_CENTROID.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.raw_centroid_id.0,
            )],
        );
        file_paths.insert(
            QUANTIZED_SPANN_SCALAR_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &ids.prefix_path,
                &ids.scalar_metadata_id,
            )],
        );

        Ok(file_paths)
    }
}
