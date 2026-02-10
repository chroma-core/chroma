use std::fmt::{Debug, Formatter};

use chroma_blockstore::provider::BlockfileProvider;
use chroma_index::usearch::USearchIndexProvider;
use chroma_types::{Collection, Segment};

use crate::quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentWriter};

#[derive(Clone)]
pub struct QuantizedSpannProvider {
    pub blockfile_provider: BlockfileProvider,
    pub usearch_provider: USearchIndexProvider,
}

impl Debug for QuantizedSpannProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuantizedSpannProvider").finish()
    }
}

impl QuantizedSpannProvider {
    pub async fn write(
        &self,
        collection: &Collection,
        vector_segment: &Segment,
        record_segment: &Segment,
    ) -> Result<QuantizedSpannSegmentWriter, QuantizedSpannSegmentError> {
        QuantizedSpannSegmentWriter::from_segment(
            collection,
            vector_segment,
            record_segment,
            &self.blockfile_provider,
            &self.usearch_provider,
        )
        .await
    }
}
