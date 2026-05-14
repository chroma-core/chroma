use std::collections::HashSet;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{
        RecordSegmentReaderOptions, RecordSegmentReaderShard, RecordSegmentReaderShardCreationError,
    },
    bloom_filter::BloomFilterManager,
    distributed_spann::{SpannSegmentReaderShard, SpannSegmentReaderShardError},
    spann_provider::SpannProvider,
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::Sample, Chunk, Collection, LogRecord, MaterializedLogOperation, Segment,
    SegmentShard, SegmentShardError, SegmentType, SignedRoaringBitmap,
};
use futures::StreamExt;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use roaring::RoaringBitmap;
use thiserror::Error;
use tracing::{Instrument, Span};

#[derive(Clone, Debug)]
pub struct SampleInput {
    pub logs: Chunk<LogRecord>,
    pub blockfile_provider: BlockfileProvider,
    pub collection: Collection,
    pub record_segment: Segment,
    pub vector_segment: Segment,
    pub log_offset_ids: SignedRoaringBitmap,
    pub compact_offset_ids: SignedRoaringBitmap,
    pub bloom_filter_manager: Option<BloomFilterManager>,
    pub spann_provider: SpannProvider,
    pub shard_index: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SampleOutput {
    pub offset_ids: Vec<u32>,
    pub strata_seen: u64,
}

#[derive(Error, Debug)]
pub enum SampleError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderShardCreationError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error(transparent)]
    SegmentShard(#[from] SegmentShardError),
    #[error("Error reading SPANN HNSW heads: {0}")]
    SpannHnsw(Box<dyn ChromaError>),
    #[error("Error reading SPANN segment: {0}")]
    SpannReader(#[from] SpannSegmentReaderShardError),
}

impl ChromaError for SampleError {
    fn code(&self) -> ErrorCodes {
        match self {
            SampleError::LogMaterializer(e) => e.code(),
            SampleError::RecordReader(e) => e.code(),
            SampleError::RecordSegment(e) => e.code(),
            SampleError::SegmentShard(e) => e.code(),
            SampleError::SpannHnsw(e) => e.code(),
            SampleError::SpannReader(e) => e.code(),
        }
    }
}

struct OffsetReservoir {
    limit: usize,
    strata_seen: u64,
    offsets: Vec<u32>,
}

impl OffsetReservoir {
    fn new(limit: u32) -> Self {
        Self {
            limit: limit as usize,
            strata_seen: 0,
            offsets: Vec::with_capacity(limit as usize),
        }
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn fill(&mut self, offset_id: u32) {
        if self.offsets.len() < self.limit {
            self.offsets.push(offset_id);
        }
    }

    fn consider(&mut self, offset_id: u32, rng: &mut StdRng) {
        self.strata_seen += 1;
        if self.limit == 0 {
            return;
        }
        if self.offsets.len() < self.limit {
            self.offsets.push(offset_id);
            return;
        }
        let replacement = rng.gen_range(0..self.strata_seen);
        if replacement < self.limit as u64 {
            self.offsets[replacement as usize] = offset_id;
        }
    }

    fn finish(mut self, rng: &mut StdRng) -> SampleOutput {
        self.offsets.shuffle(rng);
        SampleOutput {
            offset_ids: self.offsets,
            strata_seen: self.strata_seen,
        }
    }
}

fn rng_from_seed(seed: Option<u64>) -> StdRng {
    match seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    }
}

async fn create_record_segment_reader<'me>(
    record_segment: &'me Segment,
    blockfile_provider: &'me BlockfileProvider,
    bloom_filter_manager: Option<BloomFilterManager>,
    shard_index: u32,
) -> Result<Option<RecordSegmentReaderShard<'me>>, SampleError> {
    let record_segment_shard = SegmentShard::try_from((record_segment, shard_index))?;
    match Box::pin(RecordSegmentReaderShard::from_segment(
        &record_segment_shard,
        blockfile_provider,
        bloom_filter_manager,
    ))
    .instrument(tracing::trace_span!(parent: Span::current(), "Create record segment reader"))
    .await
    {
        Ok(reader) => Ok(Some(reader)),
        Err(e)
            if matches!(
                *e,
                RecordSegmentReaderShardCreationError::UninitializedSegment
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err((*e).into()),
    }
}

async fn materialize_log_offset_ids(
    logs: Chunk<LogRecord>,
    record_segment_reader: &Option<RecordSegmentReaderShard<'_>>,
    log_offset_ids: &SignedRoaringBitmap,
    plan: &RecordSegmentReaderOptions,
) -> Result<RoaringBitmap, SampleError> {
    match log_offset_ids {
        SignedRoaringBitmap::Include(rbm) => Ok(rbm.clone()),
        SignedRoaringBitmap::Exclude(rbm) => {
            let materialized_logs = materialize_logs(record_segment_reader, logs, None, plan)
                .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
                .await?;

            let active_domain: RoaringBitmap = materialized_logs
                .iter()
                .filter_map(|log| {
                    (!matches!(
                        log.get_operation(),
                        MaterializedLogOperation::DeleteExisting
                    ))
                    .then_some(log.get_offset_id())
                })
                .collect();
            Ok(active_domain - rbm)
        }
    }
}

#[async_trait]
impl Operator<SampleInput, SampleOutput> for Sample {
    type Error = SampleError;

    async fn run(&self, input: &SampleInput) -> Result<SampleOutput, SampleError> {
        let mut rng = rng_from_seed(self.seed);
        let mut reservoir = OffsetReservoir::new(self.limit);

        let record_segment_reader = create_record_segment_reader(
            &input.record_segment,
            &input.blockfile_provider,
            input.bloom_filter_manager.clone(),
            input.shard_index,
        )
        .await?;

        let plan = RecordSegmentReaderOptions {
            use_bloom_filter: input
                .bloom_filter_manager
                .as_ref()
                .is_some_and(|mgr| input.logs.len() >= mgr.storage_fetch_threshold()),
        };

        let log_offset_ids = materialize_log_offset_ids(
            input.logs.clone(),
            &record_segment_reader,
            &input.log_offset_ids,
            &plan,
        )
        .await?;
        for offset_id in log_offset_ids {
            reservoir.consider(offset_id, &mut rng);
        }

        let used_spann =
            sample_spann_compacted(input, self.limit, &mut reservoir, &mut rng).await?;
        if !used_spann {
            sample_generic_compacted(input, &record_segment_reader, &mut reservoir, &mut rng)
                .await?;
        }

        Ok(reservoir.finish(&mut rng))
    }
}

async fn sample_spann_compacted(
    input: &SampleInput,
    limit: u32,
    reservoir: &mut OffsetReservoir,
    rng: &mut StdRng,
) -> Result<bool, SampleError> {
    if input.vector_segment.r#type != SegmentType::Spann {
        return Ok(false);
    }
    let Some(dimensionality) = input.collection.dimension else {
        return Ok(false);
    };

    let vector_segment_shard = SegmentShard::try_from((&input.vector_segment, input.shard_index))?;
    let spann_reader = match Box::pin(SpannSegmentReaderShard::from_segment(
        &input.collection,
        &vector_segment_shard,
        &input.blockfile_provider,
        &input.spann_provider.hnsw_provider,
        dimensionality as usize,
        input.spann_provider.adaptive_search_nprobe,
    ))
    .instrument(tracing::trace_span!(parent: Span::current(), "Create SPANN reader"))
    .await
    {
        Ok(reader) => reader,
        Err(SpannSegmentReaderShardError::UninitializedSegment) => return Ok(false),
        Err(err) => return Err(err.into()),
    };

    let (mut heads, _) = spann_reader
        .index_reader
        .hnsw_index
        .inner
        .read()
        .hnsw_index
        .get_all_ids()
        .map_err(SampleError::SpannHnsw)?;
    heads.shuffle(rng);

    let posting_lists_to_touch = posting_lists_to_probe(limit, heads.len());
    heads.truncate(posting_lists_to_touch);

    let mut seen_offsets = HashSet::new();
    let mut top_up_offsets = Vec::new();
    for head in heads {
        let mut chosen = None;
        let mut eligible_seen = 0u64;
        for offset_id in spann_reader
            .fetch_posting_list_offset_ids(head as u32)
            .await?
        {
            if !input.compact_offset_ids.contains(offset_id) || !seen_offsets.insert(offset_id) {
                continue;
            }
            eligible_seen += 1;
            if rng.gen_range(0..eligible_seen) == 0 {
                if let Some(previous) = chosen.replace(offset_id) {
                    top_up_offsets.push(previous);
                }
            } else {
                top_up_offsets.push(offset_id);
            }
        }
        if let Some(offset_id) = chosen {
            reservoir.consider(offset_id, rng);
        }
    }

    if reservoir.len() < limit as usize {
        top_up_offsets.shuffle(rng);
        for offset_id in top_up_offsets {
            if reservoir.len() >= limit as usize {
                break;
            }
            reservoir.fill(offset_id);
        }
    }

    Ok(true)
}

fn posting_lists_to_probe(limit: u32, available_heads: usize) -> usize {
    (limit as usize).saturating_mul(2).min(available_heads)
}

async fn sample_generic_compacted(
    input: &SampleInput,
    record_segment_reader: &Option<RecordSegmentReaderShard<'_>>,
    reservoir: &mut OffsetReservoir,
    rng: &mut StdRng,
) -> Result<(), SampleError> {
    let Some(reader) = record_segment_reader else {
        return Ok(());
    };
    let mut offset_stream = reader.get_offset_stream(..);
    while let Some(offset_id) = offset_stream.next().await.transpose()? {
        if input.compact_offset_ids.contains(offset_id) {
            reservoir.consider(offset_id, rng);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posting_lists_to_probe_is_twice_limit_when_enough_heads_exist() {
        assert_eq!(20, posting_lists_to_probe(10, 100));
    }

    #[test]
    fn posting_lists_to_probe_is_capped_by_available_heads() {
        assert_eq!(7, posting_lists_to_probe(10, 7));
    }

    #[test]
    fn posting_lists_to_probe_is_zero_for_zero_limit() {
        assert_eq!(0, posting_lists_to_probe(0, 100));
    }

    #[test]
    fn offset_reservoir_zero_limit_tracks_strata_without_records() {
        let mut rng = StdRng::seed_from_u64(13);
        let mut reservoir = OffsetReservoir::new(0);

        reservoir.consider(10, &mut rng);
        reservoir.consider(11, &mut rng);

        assert_eq!(
            SampleOutput {
                offset_ids: vec![],
                strata_seen: 2,
            },
            reservoir.finish(&mut rng)
        );
    }

    #[test]
    fn offset_reservoir_fill_does_not_count_extra_strata() {
        let mut rng = StdRng::seed_from_u64(13);
        let mut reservoir = OffsetReservoir::new(3);

        reservoir.consider(10, &mut rng);
        reservoir.fill(11);
        reservoir.fill(12);
        reservoir.fill(13);

        assert_eq!(
            SampleOutput {
                offset_ids: vec![10, 12, 11],
                strata_seen: 1,
            },
            reservoir.finish(&mut rng)
        );
    }
}
