// A segment ingestor is a component that ingests embeddings into a segment
// Its designed to consume from a async_channel that guarantees exclusive consumption
// They are spawned onto a dedicated thread runtime since ingesting is cpu bound

use async_trait::async_trait;
use std::{fmt::Debug, sync::Arc};

use crate::{
    system::{Component, ComponentContext, ComponentRuntime, Handler},
    types::EmbeddingRecord,
};

use super::segment_manager::{self, SegmentManager};

pub(crate) struct SegmentIngestor {
    segment_manager: SegmentManager,
}

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
    pub(crate) fn new(segment_manager: SegmentManager) -> Self {
        SegmentIngestor {
            segment_manager: segment_manager,
        }
    }
}

#[async_trait]
impl Handler<Box<EmbeddingRecord>> for SegmentIngestor {
    async fn handle(&mut self, message: Box<EmbeddingRecord>, ctx: &ComponentContext<Self>) {
        println!("INGEST: ID of embedding is {}", message.id);
        self.segment_manager.write_record(message).await;
    }
}
