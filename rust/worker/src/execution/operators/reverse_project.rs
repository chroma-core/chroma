use std::collections::HashMap;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::{KnnProjectionOutput, Rank, RecordDistance},
    Segment,
};
use futures::future::try_join_all;
use tracing::{Instrument, Span};

use crate::execution::operators::{fetch_log::FetchLogOutput, projection::ProjectionError};

#[derive(Clone, Debug)]
pub struct ReverseProjectionInput {
    pub logs: FetchLogOutput,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub projection_outputs: HashMap<Rank, KnnProjectionOutput>,
}

#[derive(Clone, Debug)]
pub struct ReverseProjectionOutput {
    pub rank_records: HashMap<Rank, Vec<RecordDistance>>,
}

// NOTE: This is a temporary operator that aims to reverse
// the projection by converting user id to offset id
#[derive(Clone, Debug)]
pub struct ReverseProjection {}

#[async_trait]
impl Operator<ReverseProjectionInput, ReverseProjectionOutput> for ReverseProjection {
    type Error = ProjectionError;

    async fn run(
        &self,
        input: &ReverseProjectionInput,
    ) -> Result<ReverseProjectionOutput, ProjectionError> {
        tracing::trace!(
            "Reversing projection on {} ranks",
            input.projection_outputs.len()
        );
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;
        let materialized_logs = materialize_logs(&record_segment_reader, input.logs.clone(), None)
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;
        let borrowed_logs = materialized_logs.iter().collect::<Vec<_>>();

        let hydrated_futures = borrowed_logs.iter().map(|log| async {
            let hydrated_log = log.hydrate(record_segment_reader.as_ref()).await?;
            <Result<_, LogMaterializerError>>::Ok((
                hydrated_log.get_user_id(),
                hydrated_log.get_offset_id(),
            ))
        });
        let log_user_id_to_offset_id = try_join_all(hydrated_futures)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();
        let mut rank_records = HashMap::with_capacity(input.projection_outputs.len());

        for (rank, projection_output) in &input.projection_outputs {
            let resolve_futures = projection_output.records.iter().map(|record| async {
                match log_user_id_to_offset_id.get(record.record.id.as_str()) {
                    Some(&offset_id) => Ok(RecordDistance {
                        offset_id,
                        measure: record.distance.unwrap_or_default(),
                    }),
                    None => {
                        if let Some(reader) = &record_segment_reader {
                            match reader.get_offset_id_for_user_id(&record.record.id).await? {
                                Some(offset_id) => Ok(RecordDistance {
                                    offset_id,
                                    measure: record.distance.unwrap_or_default(),
                                }),
                                None => Err(ProjectionError::RecordSegmentPhantomRecord(u32::MAX)),
                            }
                        } else {
                            Err(ProjectionError::RecordSegmentUninitialized)
                        }
                    }
                }
            });
            rank_records.insert(rank.clone(), try_join_all(resolve_futures).await?);
        }

        Ok(ReverseProjectionOutput { rank_records })
    }
}
