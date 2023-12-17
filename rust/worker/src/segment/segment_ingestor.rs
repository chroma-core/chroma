// A segment ingestor is a component that ingests embeddings into a segment
// Its designed to consume from a async_channel that guarantees exclusive consumption
// They are spawned onto a dedicated thread runtime since ingesting is cpu bound

use async_trait::async_trait;
use std::{fmt::Debug, sync::Arc};

use crate::{
    system::{Component, ComponentContext, ComponentRuntime, Handler},
    types::EmbeddingRecord,
};

pub(crate) struct SegmentIngestor {}

impl Component for SegmentIngestor {
    fn queue_size(&self) -> usize {
        1000
    }
    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Dedicated
    }
}

impl Debug for SegmentIngestor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentIngestor").finish()
    }
}

impl SegmentIngestor {
    pub(crate) fn new() -> Self {
        SegmentIngestor {}
    }
}

#[async_trait]
impl Handler<Arc<EmbeddingRecord>> for SegmentIngestor {
    async fn handle(&mut self, message: Arc<EmbeddingRecord>, ctx: &ComponentContext<Self>) {
        println!("INGEST: ID of embedding is {}", message.id);
        // let segment_manager = ctx.system.get_segment_manager();
        // let segment = segment_manager.get_segment(&tenant);
        // segment.ingest(embedding);
    }
}
