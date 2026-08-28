use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::{FunctionWorkload, Segment};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundarySelection {
    FurthestFitting,
    NextLive,
}

#[derive(Debug, Clone)]
pub(crate) struct AsyncFnBoundaryPlan {
    pub(crate) historical_record_segment: Option<Segment>,
    pub(crate) expected_completion_offset: i64,
    pub(crate) target_log_position: i64,
    pub(crate) function_workload: Option<FunctionWorkload>,
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
    max_compaction_size: usize,
    live_record_segment: &Segment,
    selection: BoundarySelection,
) -> Result<AsyncFnBoundaryPlan, String> {
    let Some(version_file) = version_file else {
        return Err(format!(
            "async fn completion offset {} has no next compaction boundary",
            completion_offset
        ));
    };

    let version_history = match version_file.version_history.as_ref() {
        Some(history) => history,
        None => {
            return Err(format!(
                "async fn completion offset {} has no next compaction boundary",
                completion_offset
            ));
        }
    };

    let mut live_version_infos = version_history
        .versions
        .iter()
        // GC only marks versions for deletion after a newer version supersedes them.
        // Fn-consumers should only resolve boundaries against the still-live versions
        // whose segment files are expected to remain readable. GC makes sure to
        // keep at least one version live below the completion offset.
        .filter(|version| !version.marked_for_deletion)
        .filter_map(|version| {
            version
                .collection_info_mutable
                .as_ref()
                .map(|mutable| (version, mutable.current_log_position))
        })
        .collect::<Vec<_>>();
    live_version_infos.sort_by_key(|(_, log_position)| *log_position);

    // Walk versions newest -> oldest. Boundaries above the completion offset
    // are visited furthest-first, so the first one whose window fits
    // max_compaction_size is the widest eligible target. Tracking the nearest
    // boundary as well preserves the oversized-window error below when no
    // boundary fits.
    let historical_version = live_version_infos
        .iter()
        .rev()
        .find(|(_, log_position)| *log_position <= completion_offset)
        .copied();
    let next_boundaries = live_version_infos
        .iter()
        .filter_map(|(_, log_position)| {
            (*log_position > completion_offset).then_some(*log_position)
        })
        .collect::<Vec<_>>();

    let historical_record_segment = match historical_version {
        Some((_, log_position)) if completion_offset > 0 && log_position < completion_offset => {
            return Err(format!(
                "Invariant violation: async fn completion offset {} does not align to a compaction boundary",
                completion_offset
            ));
        }
        Some((version, _)) => Some(
            live_record_segment.historical_segment_for_version(version, live_record_segment.id)?,
        ),
        None => None,
    };

    // Prefer the furthest boundary that fits: one run then covers every
    // compaction between the completion offset and that boundary, instead of
    // draining a backlog one small compaction window at a time. Skipped
    // intermediate boundaries are safe — the completion offset advances to a
    // real boundary either way, and the work queue retires stale entries by
    // offset comparison.
    let next_boundary = next_boundaries.first().copied().ok_or_else(|| {
        format!(
            "async fn completion offset {} has no next compaction boundary",
            completion_offset
        )
    })?;
    let target_log_position = match selection {
        BoundarySelection::NextLive => next_boundary,
        BoundarySelection::FurthestFitting => next_boundaries
            .iter()
            .rev()
            .find(|log_position| {
                usize::try_from(**log_position - completion_offset)
                    .is_ok_and(|window| window <= max_compaction_size)
            })
            .copied()
            .unwrap_or(next_boundary),
    };
    let log_window_size =
        usize::try_from(target_log_position - completion_offset).map_err(|_| {
            format!(
                "Invariant violation: next compaction boundary {} precedes completion offset {}",
                target_log_position, completion_offset
            )
        })?;
    if log_window_size > max_compaction_size {
        return Err(format!(
            "next compaction boundary window {} exceeds max_compaction_size {}",
            log_window_size, max_compaction_size
        ));
    }

    let mut function_workload: Option<FunctionWorkload> = None;
    for mutable in version_history.versions.iter().filter_map(|version| {
        version.collection_info_mutable.as_ref().filter(|mutable| {
            mutable.current_log_position > completion_offset
                && mutable.current_log_position <= target_log_position
        })
    }) {
        let Some(workload) = mutable.function_workload.map(Into::into) else {
            function_workload = None;
            break;
        };
        match &mut function_workload {
            Some(aggregate) => aggregate.merge(&workload),
            None => function_workload = Some(workload),
        }
    }

    Ok(AsyncFnBoundaryPlan {
        historical_record_segment,
        expected_completion_offset: completion_offset,
        target_log_position,
        function_workload,
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

    use super::{resolve_boundary_plan_from_version_file, BoundarySelection};

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
    fn no_version_file_means_no_executable_boundary() {
        let record_segment = test_record_segment();
        let err = resolve_boundary_plan_from_version_file(
            None,
            -1,
            1024,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap_err();
        assert!(err.contains("no next compaction boundary"));
    }

    #[test]
    fn resolves_exact_boundary_and_next_boundary() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    version_info(2, 150, record_segment.id, "record/v150"),
                ],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            100,
            1024,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap();

        assert_eq!(plan.target_log_position, 150);
        assert_eq!(
            plan.historical_record_segment.unwrap().file_path["offset_id_to_data"],
            vec!["record/v100".to_string()]
        );
    }

    #[test]
    fn completion_offset_zero_uses_empty_state_and_widest_fitting_boundary() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    version_info(2, 150, record_segment.id, "record/v150"),
                ],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            0,
            1024,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap();

        assert_eq!(plan.target_log_position, 150);
        assert!(
            plan.historical_record_segment.is_none(),
            "completion offset zero should use the empty pre-compaction state"
        );
    }

    #[test]
    fn picks_furthest_boundary_that_fits_max_compaction_size() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    version_info(2, 150, record_segment.id, "record/v150"),
                    version_info(3, 200, record_segment.id, "record/v200"),
                ],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            100,
            1024,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap();

        assert_eq!(plan.target_log_position, 200);
        assert_eq!(
            plan.historical_record_segment.unwrap().file_path["offset_id_to_data"],
            vec!["record/v100".to_string()]
        );
    }

    #[test]
    fn skips_boundaries_wider_than_max_compaction_size() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    version_info(2, 150, record_segment.id, "record/v150"),
                    version_info(3, 200, record_segment.id, "record/v200"),
                    version_info(4, 5000, record_segment.id, "record/v5000"),
                ],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            100,
            1000,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap();

        assert_eq!(plan.target_log_position, 200);
    }

    #[test]
    fn errors_when_no_boundary_fits_max_compaction_size() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    version_info(2, 5000, record_segment.id, "record/v5000"),
                ],
            }),
            ..Default::default()
        };

        let err = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            100,
            1000,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap_err();

        assert!(err.contains("exceeds max_compaction_size"));
    }

    #[test]
    fn deleted_versions_are_not_widened_targets() {
        let record_segment = test_record_segment();
        let mut deleted_version = version_info(2, 150, record_segment.id, "record/v150");
        deleted_version.marked_for_deletion = true;

        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    deleted_version,
                    version_info(3, 5000, record_segment.id, "record/v5000"),
                ],
            }),
            ..Default::default()
        };

        // The only live boundary above the offset (5000) does not fit, and the
        // deleted 150 boundary must not be picked in its place.
        let err = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            100,
            1000,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap_err();

        assert!(err.contains("exceeds max_compaction_size"));
    }

    #[test]
    fn rejects_non_boundary_completion_offsets_after_first_compaction() {
        let record_segment = test_record_segment();
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    version_info(2, 150, record_segment.id, "record/v150"),
                ],
            }),
            ..Default::default()
        };

        let err = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            125,
            1024,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap_err();

        assert!(err.contains("does not align to a compaction boundary"));
    }

    #[test]
    fn ignores_deleted_versions_when_finding_next_boundary() {
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
            100,
            1024,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap();

        assert_eq!(plan.target_log_position, 200);
        assert_eq!(
            plan.historical_record_segment.unwrap().file_path["offset_id_to_data"],
            vec!["record/v100".to_string()]
        );
    }

    #[test]
    fn rejects_completion_offsets_that_only_match_deleted_versions() {
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

        let err = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            150,
            1024,
            &record_segment,
            BoundarySelection::FurthestFitting,
        )
        .unwrap_err();

        assert!(err.contains("Invariant violation"));
        assert!(err.contains("does not align to a compaction boundary"));
    }

    #[test]
    fn next_live_boundary_aggregates_descriptors_from_deleted_versions() {
        let record_segment = test_record_segment();
        let mut deleted_version = version_info(2, 150, record_segment.id, "record/v150");
        deleted_version.marked_for_deletion = true;
        deleted_version
            .collection_info_mutable
            .as_mut()
            .unwrap()
            .function_workload = Some(chroma_types::chroma_proto::FunctionWorkload {
            format_version: 1,
            source_log_records: 2,
            source_log_bytes: 20,
            ..Default::default()
        });
        let mut target_version = version_info(3, 200, record_segment.id, "record/v200");
        target_version
            .collection_info_mutable
            .as_mut()
            .unwrap()
            .function_workload = Some(chroma_types::chroma_proto::FunctionWorkload {
            format_version: 1,
            source_log_records: 3,
            source_log_bytes: 30,
            ..Default::default()
        });
        let version_file = CollectionVersionFile {
            version_history: Some(CollectionVersionHistory {
                versions: vec![
                    version_info(1, 100, record_segment.id, "record/v100"),
                    deleted_version,
                    target_version,
                ],
            }),
            ..Default::default()
        };

        let plan = resolve_boundary_plan_from_version_file(
            Some(&version_file),
            100,
            1024,
            &record_segment,
            BoundarySelection::NextLive,
        )
        .unwrap();

        assert_eq!(plan.expected_completion_offset, 100);
        assert_eq!(plan.target_log_position, 200);
        assert_eq!(
            plan.function_workload.unwrap(),
            chroma_types::FunctionWorkload {
                format_version: 1,
                source_log_records: 5,
                source_log_bytes: 50,
                materialized_records: 0,
                non_delete_records: 0,
                id_bytes: 0,
                document_bytes: 0,
                metadata_bytes: 0,
                embedding_bytes: 0,
                metadata_entries: 0,
                max_non_embedding_record_bytes: 0,
            }
        );
    }
}
