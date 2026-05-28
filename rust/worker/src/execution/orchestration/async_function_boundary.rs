use std::collections::HashMap;

use chroma_types::chroma_proto::{CollectionVersionFile, CollectionVersionInfo, FilePaths};
use chroma_types::Segment;

#[derive(Debug, Clone)]
pub(crate) struct AsyncFnBoundaryPlan {
    pub(crate) historical_record_segment: Segment,
    pub(crate) target_log_position: i64,
}

pub(crate) fn resolve_boundary_plan_from_version_file(
    version_file: Option<&CollectionVersionFile>,
    completion_offset: i64,
    max_compaction_size: usize,
    live_record_segment: &Segment,
) -> Result<AsyncFnBoundaryPlan, String> {
    let empty_record_segment = empty_record_segment(live_record_segment);

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

    let version_infos = version_history
        .versions
        .iter()
        .filter(|version| !version.marked_for_deletion)
        .filter_map(|version| {
            version
                .collection_info_mutable
                .as_ref()
                .map(|mutable| (version, mutable.current_log_position))
        });

    let mut historical_version = None;
    let mut next_boundary = None;
    for (version, log_position) in version_infos.rev() {
        if log_position > completion_offset {
            next_boundary = Some(log_position);
            continue;
        }

        historical_version = Some((version, log_position));
        break;
    }

    let historical_record_segment = match historical_version {
        Some((_, log_position)) if completion_offset > 0 && log_position < completion_offset => {
            return Err(format!(
                "async fn completion offset {} does not align to a compaction boundary",
                completion_offset
            ));
        }
        Some((version, _)) => historical_record_segment_for_version(live_record_segment, version),
        None => empty_record_segment,
    };

    if let Some(next_boundary) = next_boundary {
        let log_window_size = usize::try_from(next_boundary - completion_offset)
            .map_err(|_| "next boundary precedes completion offset".to_string())?;
        if log_window_size > max_compaction_size {
            return Err(format!(
                "next compaction boundary window {} exceeds max_compaction_size {}",
                log_window_size, max_compaction_size
            ));
        }
    } else {
        return Err(format!(
            "async fn completion offset {} has no next compaction boundary",
            completion_offset
        ));
    }

    Ok(AsyncFnBoundaryPlan {
        historical_record_segment,
        target_log_position: next_boundary.unwrap(),
    })
}

fn empty_record_segment(record_segment: &Segment) -> Segment {
    let mut segment = record_segment.clone();
    segment.file_path.clear();
    segment
}

fn historical_record_segment_for_version(
    record_segment: &Segment,
    version: &CollectionVersionInfo,
) -> Segment {
    let mut historical_segment = record_segment.clone();
    let Some(segment_info) = version.segment_info.as_ref() else {
        historical_segment.file_path.clear();
        return historical_segment;
    };

    let Some(record_segment_info) = segment_info
        .segment_compaction_info
        .iter()
        .find(|segment_info| segment_info.segment_id == record_segment.id.to_string())
    else {
        historical_segment.file_path.clear();
        return historical_segment;
    };

    historical_segment.file_path = record_segment_info
        .file_paths
        .iter()
        .map(|(key, value)| (key.clone(), file_paths_to_vec(value)))
        .collect::<HashMap<_, _>>();
    historical_segment
}

fn file_paths_to_vec(file_paths: &FilePaths) -> Vec<String> {
    file_paths.paths.clone()
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
    fn no_version_file_means_no_executable_boundary() {
        let record_segment = test_record_segment();
        let err =
            resolve_boundary_plan_from_version_file(None, -1, 1024, &record_segment).unwrap_err();
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
        )
        .unwrap();

        assert_eq!(plan.target_log_position, 150);
        assert_eq!(
            plan.historical_record_segment.file_path["offset_id_to_data"],
            vec!["record/v100".to_string()]
        );
    }

    #[test]
    fn completion_offset_zero_uses_empty_state_and_first_boundary() {
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

        let plan =
            resolve_boundary_plan_from_version_file(Some(&version_file), 0, 1024, &record_segment)
                .unwrap();

        assert_eq!(plan.target_log_position, 100);
        assert!(
            plan.historical_record_segment.file_path.is_empty(),
            "completion offset zero should use the empty pre-compaction state"
        );
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
        )
        .unwrap_err();

        assert!(err.contains("does not align to a compaction boundary"));
    }
}
