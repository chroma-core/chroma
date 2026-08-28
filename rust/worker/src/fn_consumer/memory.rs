//! Pod-local memory sensing, estimation, and admission decisions.

use std::path::{Path, PathBuf};

use chroma_types::{AttachedFunction, FunctionWorkload, FUNCTION_HTTP_GENERATE_ID};

use crate::execution::functions::http_generate::{
    HttpGenerateExecutor, MAX_GENERATE_REQUEST_BYTES,
};

use super::config::MemoryAdmissionConfig;

const MIN_DEFAULT_HEADROOM_BYTES: u64 = 256 * 1024 * 1024;
const JSON_RECORD_OVERHEAD_BYTES: u64 = 128;

#[derive(Clone, Debug)]
pub(crate) struct MemoryAdmission {
    limit_bytes: u64,
    headroom_bytes: u64,
    current_path: Option<PathBuf>,
    peak_path: Option<PathBuf>,
    safety_multiplier: f64,
    fixed_executor_overhead_bytes: u64,
    per_record_overhead_bytes: u64,
    per_metadata_entry_overhead_bytes: u64,
    pub(crate) lookahead_size: u32,
    pub(crate) max_bypass_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionDecision {
    Admit,
    Wait,
    Unaddressable,
}

impl MemoryAdmission {
    pub(crate) fn from_config(
        config: &MemoryAdmissionConfig,
        max_concurrent_workers: usize,
    ) -> Result<Option<Self>, String> {
        if !config.enabled {
            return Ok(None);
        }
        if max_concurrent_workers == 0 {
            return Err("memory admission requires max_concurrent_workers > 0".to_string());
        }
        if !config.safety_multiplier.is_finite() || config.safety_multiplier < 1.0 {
            return Err("memory admission safety_multiplier must be finite and >= 1".to_string());
        }

        let detected = detect_cgroup_memory();
        let (limit_bytes, current_path, peak_path) = match config.memory_limit_bytes {
            Some(0) => return Err("memory_limit_bytes must be greater than zero".to_string()),
            Some(limit) => match detected.as_ref() {
                Some((_, current, peak)) => {
                    (limit, Some(current.to_path_buf()), Some(peak.to_path_buf()))
                }
                None => (limit, None, None),
            },
            None => {
                let (limit, current, peak) = detected.ok_or_else(|| {
                    "memory admission is enabled but no finite cgroup memory limit was detected"
                        .to_string()
                })?;
                (limit, Some(current), Some(peak))
            }
        };
        let headroom_bytes = config
            .headroom_bytes
            .unwrap_or_else(|| (limit_bytes / 10).max(MIN_DEFAULT_HEADROOM_BYTES));
        if headroom_bytes >= limit_bytes {
            return Err(format!(
                "memory admission headroom {headroom_bytes} must be below limit {limit_bytes}"
            ));
        }

        let default_lookahead = max_concurrent_workers
            .saturating_mul(4)
            .max(1)
            .min(u32::MAX as usize) as u32;
        let lookahead_size = config.lookahead_size.unwrap_or(default_lookahead);
        if lookahead_size == 0 {
            return Err("memory admission lookahead_size must be greater than zero".to_string());
        }

        Ok(Some(Self {
            limit_bytes,
            headroom_bytes,
            current_path,
            peak_path,
            safety_multiplier: config.safety_multiplier,
            fixed_executor_overhead_bytes: config.fixed_executor_overhead_bytes,
            per_record_overhead_bytes: config.per_record_overhead_bytes,
            per_metadata_entry_overhead_bytes: config.per_metadata_entry_overhead_bytes,
            lookahead_size,
            max_bypass_count: config.max_bypass_count.unwrap_or(max_concurrent_workers),
        }))
    }

    pub(crate) fn schedulable_budget_bytes(&self) -> u64 {
        self.limit_bytes.saturating_sub(self.headroom_bytes)
    }

    pub(crate) fn headroom_bytes(&self) -> u64 {
        self.headroom_bytes
    }

    pub(crate) fn current_bytes(&self) -> Result<u64, String> {
        match &self.current_path {
            Some(path) => read_u64(path),
            // An explicit limit is also the supported local/test override. In
            // that mode there may be no cgroup usage file to consult.
            None => Ok(0),
        }
    }

    pub(crate) fn peak_bytes(&self) -> Option<Result<u64, String>> {
        self.peak_path.as_deref().map(read_u64)
    }

    pub(crate) fn decide(
        &self,
        reserved_bytes: u64,
        current_bytes: u64,
        estimate_bytes: u64,
    ) -> AdmissionDecision {
        let budget = self.schedulable_budget_bytes();
        if estimate_bytes > budget {
            return AdmissionDecision::Unaddressable;
        }
        if reserved_bytes.saturating_add(estimate_bytes) > budget
            || current_bytes.saturating_add(estimate_bytes) > budget
        {
            return AdmissionDecision::Wait;
        }
        AdmissionDecision::Admit
    }

    pub(crate) fn exclusive_fallback_reservation(&self, current_bytes: u64) -> u64 {
        self.schedulable_budget_bytes()
            .saturating_sub(current_bytes)
    }

    pub(crate) fn estimate(
        &self,
        attached_function: &AttachedFunction,
        workload: &FunctionWorkload,
    ) -> Result<u64, String> {
        if !workload.is_supported() {
            return Err(format!(
                "unsupported workload descriptor format {}",
                workload.format_version
            ));
        }

        let hydrated_bytes = checked_sum(&[
            workload.id_bytes,
            workload.document_bytes,
            workload.metadata_bytes,
            workload.embedding_bytes,
        ])?;
        let container_bytes = checked_sum(&[
            checked_mul(
                workload.materialized_records,
                self.per_record_overhead_bytes,
            )?,
            checked_mul(
                workload.metadata_entries,
                self.per_metadata_entry_overhead_bytes,
            )?,
        ])?;
        let mut estimate = checked_sum(&[
            self.fixed_executor_overhead_bytes,
            workload.source_log_bytes,
            hydrated_bytes,
            container_bytes,
        ])?;

        if attached_function.function_id == FUNCTION_HTTP_GENERATE_ID {
            let batch_size = HttpGenerateExecutor::configured_batch_size(attached_function)
                .map_err(|error| error.to_string())? as u64;
            let batch_records = workload.non_delete_records.min(batch_size);
            let record_payload =
                checked_mul(workload.max_non_embedding_record_bytes, batch_records)?.min(
                    checked_sum(&[
                        workload.id_bytes,
                        workload.document_bytes,
                        workload.metadata_bytes,
                    ])?,
                );
            let json_overhead = checked_mul(batch_records, JSON_RECORD_OVERHEAD_BYTES)?;
            let normal_request = checked_sum(&[record_payload, json_overhead])?
                .min(MAX_GENERATE_REQUEST_BYTES as u64);
            let oversized_single = checked_sum(&[
                workload.max_non_embedding_record_bytes,
                JSON_RECORD_OVERHEAD_BYTES,
            ])?;
            estimate = checked_sum(&[estimate, normal_request.max(oversized_single)])?;
        }

        let scaled = (estimate as f64) * self.safety_multiplier;
        if !scaled.is_finite() || scaled > u64::MAX as f64 {
            return Err("memory estimate overflowed u64".to_string());
        }
        Ok(scaled.ceil() as u64)
    }
}

fn checked_sum(values: &[u64]) -> Result<u64, String> {
    values.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| "memory estimate overflowed u64".to_string())
    })
}

fn checked_mul(left: u64, right: u64) -> Result<u64, String> {
    left.checked_mul(right)
        .ok_or_else(|| "memory estimate overflowed u64".to_string())
}

fn read_u64(path: &Path) -> Result<u64, String> {
    std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?
        .trim()
        .parse::<u64>()
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))
}

fn detect_cgroup_memory() -> Option<(u64, PathBuf, PathBuf)> {
    let candidates = [
        (
            Path::new("/sys/fs/cgroup/memory.max"),
            Path::new("/sys/fs/cgroup/memory.current"),
            Path::new("/sys/fs/cgroup/memory.peak"),
        ),
        (
            Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            Path::new("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
            Path::new("/sys/fs/cgroup/memory/memory.max_usage_in_bytes"),
        ),
    ];

    candidates
        .into_iter()
        .find_map(|(limit_path, current_path, peak_path)| {
            let value = std::fs::read_to_string(limit_path).ok()?;
            let value = value.trim();
            if value == "max" {
                return None;
            }
            let limit = value.parse::<u64>().ok()?;
            // Cgroup v1 represents "unlimited" with a sentinel near i64::MAX.
            (limit > 0 && limit < (1_u64 << 60) && current_path.exists())
                .then(|| (limit, current_path.to_path_buf(), peak_path.to_path_buf()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::{AttachedFunctionUuid, CollectionUuid};

    fn admission() -> MemoryAdmission {
        MemoryAdmission::from_config(
            &MemoryAdmissionConfig {
                enabled: true,
                memory_limit_bytes: Some(1024),
                headroom_bytes: Some(124),
                safety_multiplier: 1.0,
                fixed_executor_overhead_bytes: 100,
                per_record_overhead_bytes: 10,
                per_metadata_entry_overhead_bytes: 5,
                lookahead_size: Some(4),
                max_bypass_count: Some(2),
            },
            2,
        )
        .unwrap()
        .unwrap()
    }

    fn attached_function(function_id: uuid::Uuid) -> AttachedFunction {
        AttachedFunction {
            id: AttachedFunctionUuid::new(),
            name: "test".to_string(),
            function_id,
            input_collection_id: CollectionUuid::new(),
            output_collection_name: "output".to_string(),
            output_collection_id: None,
            params: Some("{\"batch_size\":2}".to_string()),
            tenant_id: "tenant".to_string(),
            database_id: "database".to_string(),
            last_run: None,
            completion_offset: 0,
            min_records_for_invocation: 0,
            is_deleted: false,
            is_async: true,
            failure_count: 0,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn admission_checks_reservations_and_current_usage_independently() {
        let admission = admission();
        assert_eq!(admission.decide(300, 400, 200), AdmissionDecision::Admit);
        assert_eq!(admission.decide(800, 0, 200), AdmissionDecision::Wait);
        assert_eq!(admission.decide(0, 800, 200), AdmissionDecision::Wait);
        assert_eq!(
            admission.decide(0, 0, 901),
            AdmissionDecision::Unaddressable
        );
    }

    #[test]
    fn http_estimate_includes_one_bounded_request() {
        let workload = FunctionWorkload {
            format_version: 1,
            source_log_records: 3,
            source_log_bytes: 30,
            materialized_records: 3,
            non_delete_records: 3,
            id_bytes: 15,
            document_bytes: 300,
            metadata_bytes: 30,
            embedding_bytes: 120,
            metadata_entries: 3,
            max_non_embedding_record_bytes: 115,
        };
        let estimate = admission()
            .estimate(&attached_function(FUNCTION_HTTP_GENERATE_ID), &workload)
            .unwrap();

        assert_eq!(estimate, 1126);
    }
}
