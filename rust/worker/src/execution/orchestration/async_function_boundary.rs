use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::Segment;

#[derive(Debug, Clone)]
pub(crate) struct AsyncFnBoundaryPlan {
    pub(crate) historical_record_segment: Option<Segment>,
    pub(crate) snapshot_log_position: i64,
    pub(crate) target_log_position: i64,
}

impl AsyncFnBoundaryPlan {
    pub(crate) fn record_segment_for_reader(&self, live_record_segment: &Segment) -> Segment {
        self.historical_record_segment
            .clone()
            .unwrap_or_else(|| live_record_segment.empty_segment())
    }
}

pub(crate) fn resolve_boundary_plan_from_version_file(
    version_file: Option<&CollectionVersionFile>,
    completion_offset: i64,
    target_log_offset: i64,
    max_compaction_size: usize,
    live_record_segment: &Segment,
) -> Result<AsyncFnBoundaryPlan, String> {
    if target_log_offset <= completion_offset {
        return Err(format!(
            "async fn target offset {} must exceed completion offset {}",
            target_log_offset, completion_offset
        ));
    }
    if max_compaction_size == 0 {
        return Err("async fn max_compaction_size must be greater than zero".to_string());
    }

    // Version history is only needed to find the newest readable snapshot at
    // or before the function checkpoint. The execution target comes from the
    // queued work frontier and does not need to be a compaction boundary.
    let historical_version = version_file
        .and_then(|version_file| version_file.version_history.as_ref())
        .into_iter()
        .flat_map(|history| history.versions.iter())
        .filter(|version| !version.marked_for_deletion)
        .filter_map(|version| {
            version
                .collection_info_mutable
                .as_ref()
                .map(|mutable| (version, mutable.current_log_position))
        })
        .filter(|(_, log_position)| *log_position <= completion_offset)
        .max_by_key(|(_, log_position)| *log_position);

    if historical_version.is_none() && completion_offset > 0 {
        return Err(format!(
            "async fn completion offset {} has no live snapshot at or before it",
            completion_offset
        ));
    }

    let snapshot_log_position = historical_version
        .as_ref()
        .map(|(_, log_position)| *log_position)
        .unwrap_or_else(|| completion_offset.min(0));
    let historical_record_segment = match historical_version {
        Some((version, _)) => Some(
            live_record_segment.historical_segment_for_version(version, live_record_segment.id)?,
        ),
        None => None,
    };

    let max_window = i64::try_from(max_compaction_size).unwrap_or(i64::MAX);
    let target_log_position = target_log_offset.min(completion_offset.saturating_add(max_window));

    Ok(AsyncFnBoundaryPlan {
        historical_record_segment,
        snapshot_log_position,
        target_log_position,
    })
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chroma_types::chroma_proto::{
        CollectionInfoMutable, CollectionSegmentInfo, CollectionVersionFile,
        CollectionVersionHistory, CollectionVersionInfo, FilePaths, FlushSegmentCompactionInfo,
    };
    use chroma_types::{CollectionUuid, Segment, SegmentScope, SegmentType, SegmentUuid};

    use super::resolve_boundary_plan_from_version_file;

    fn test_record_segment() -> Segment {
        Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            collection: CollectionUuid::new(),
            metadata: None,
            file_path: HashMap::from([(
                "offset_id_to_data".to_string(),
                vec!["live/path".to_string()],
            )]),
        }
    }

    fn version_info(
        version: i64,
        current_log_position: i64,
        segment_id: SegmentUuid,
        record_path: &str,
    ) -> CollectionVersionInfo {
        CollectionVersionInfo {
            version,
            collection_info_mutable: Some(CollectionInfoMutable {
                current_log_position,
                ..Default::default()
            }),
            segment_info: Some(CollectionSegmentInfo {
                segment_compaction_info: vec![FlushSegmentCompactionInfo {
                    segment_id: segment_id.to_string(),
                    file_paths: HashMap::from([(
                        "offset_id_to_data".to_string(),
                        FilePaths {
                            paths: vec![record_path.to_string()],
                        },
                    )]),
                }],
            }),
            ..Default::default()
        }
    }

    #[test]
    fn initial_checkpoint_uses_empty_state_without_version_file() {
        let record_segment = test_record_segment();
        let plan =
            resolve_boundary_plan_from_version_file(None, -1, 75, 1024, &record_segment).unwrap();

        assert_eq!(plan.snapshot_log_position, -1);
        assert_eq!(plan.target_log_position, 75);
        assert!(plan.historical_record_segment.is_none());
    }

    #[test]
    fn positive_checkpoint_requires_a_live_snapshot() {
        let record_segment = test_record_segment();
        let err = resolve_boundary_plan_from_version_file(None, 25, 75, 1024, &record_segment)
            .unwrap_err();

        assert!(err.contains("no live snapshot"));
    }

    #[test]
    fn target_does_not_need_a_compaction_boundary() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![version_info(1, 100, record_segment.id, "record/v100")],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            125,
            175,
            1024,
            &record_segment,
        )
        .unwrap();

        assert_eq!(plan.snapshot_log_position, 100);
        assert_eq!(plan.target_log_position, 175);
        assert_eq!(
            plan.historical_record_segment.unwrap().file_path["offset_id_to_data"],
            vec!["record/v100".to_string()]
        );
    }

    #[test]
    fn caps_target_by_max_compaction_size_without_a_boundary() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![version_info(1, 100, record_segment.id, "record/v100")],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            125,
            5000,
            1000,
            &record_segment,
        )
        .unwrap();

        assert_eq!(plan.snapshot_log_position, 100);
        assert_eq!(plan.target_log_position, 1125);
    }

    #[test]
    fn chooses_newest_live_snapshot_at_or_before_checkpoint() {
        let record_segment = test_record_segment();
        let mut deleted_version = version_info(2, 150, record_segment.id, "record/v150");
        deleted_version.marked_for_deletion = true;
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    deleted_version,
                    version_info(3, 200, record_segment.id, "record/v200"),
                ],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            175,
            190,
            1024,
            &record_segment,
        )
        .unwrap();

        assert_eq!(plan.snapshot_log_position, 100);
        assert_eq!(plan.target_log_position, 190);
    }

    #[test]
    fn rejects_non_advancing_target() {
        let record_segment = test_record_segment();
        let err = resolve_boundary_plan_from_version_file(None, 25, 25, 1024, &record_segment)
            .unwrap_err();

        assert!(err.contains("must exceed completion offset"));
    }

    #[test]
    fn rejects_zero_sized_windows() {
        let record_segment = test_record_segment();
        let err =
            resolve_boundary_plan_from_version_file(None, -1, 25, 0, &record_segment).unwrap_err();

        assert!(err.contains("must be greater than zero"));
    }
}
