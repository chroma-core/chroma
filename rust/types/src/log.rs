use crate::CollectionUuid;

////////////////////////////////////// constants and variables /////////////////////////////////////

pub fn dirty_log_path_from_hostname(hostname: &str) -> String {
    format!("dirty-{}", hostname)
}

//////////////////////////////////////////// DirtyMarker ///////////////////////////////////////////

/// Markers for tracking collection compaction state changes.
///
/// DirtyMarker represents state transitions in the compaction lifecycle of collections.
/// The enum is designed for forwards/backwards compatibility - new variants can be added
/// and handled independently while maintaining compatibility with older code.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
// NOTE(rescrv):  This is intentionally an enum for easy forwards/backwards compatibility.  Add a
// new variant, handle both variants, cycle logs, stop handling old variant.
// TODO(rescrv):  Dedupe with log-service crate.
pub enum DirtyMarker {
    /// Marks a collection as needing compaction due to new records.
    #[serde(rename = "mark_dirty")]
    MarkDirty {
        /// The collection requiring compaction.
        collection_id: CollectionUuid,
        /// The position in the write-ahead log where this marker was created.
        log_position: u64,
        /// The total number of records in the collection.
        num_records: u64,
        /// The number of times this collection has been reinserted into the heap.
        reinsert_count: u64,
        /// The epoch time in microseconds when this collection was first marked dirty.
        initial_insertion_epoch_us: u64,
    },
    /// Removes all compaction scheduling for a collection.
    #[serde(rename = "purge")]
    Purge {
        /// The collection to purge from the compaction heap.
        collection_id: CollectionUuid,
    },
    // A Cleared marker is a no-op.  It exists so that a log consisting of mark-dirty markers that
    // map onto purge markers will be cleared and can be erased.
    /// A no-op marker used for log compaction.
    ///
    /// When a log contains mark-dirty markers that have been purged, those entries
    /// can be replaced with Cleared markers to allow log truncation.
    #[serde(rename = "clear")]
    Cleared,
}

impl DirtyMarker {
    /// The collection ID for a given dirty marker.
    pub fn collection_id(&self) -> CollectionUuid {
        match self {
            DirtyMarker::MarkDirty { collection_id, .. } => *collection_id,
            DirtyMarker::Purge { collection_id } => *collection_id,
            DirtyMarker::Cleared => CollectionUuid::default(),
        }
    }

    /// Increment any reinsert counter on the variant.
    pub fn reinsert(&mut self) {
        if let DirtyMarker::MarkDirty {
            collection_id: _,
            log_position: _,
            num_records: _,
            reinsert_count,
            initial_insertion_epoch_us: _,
        } = self
        {
            *reinsert_count = reinsert_count.saturating_add(1);
        }
    }
}
