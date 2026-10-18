#[cfg(target_os = "linux")]
pub(crate) fn allocator_stats() -> serde_json::Value {
    if tikv_jemalloc_ctl::epoch::advance().is_err() {
        return serde_json::Value::Null;
    }
    serde_json::json!({
        "allocated_bytes": tikv_jemalloc_ctl::stats::allocated::read().ok(),
        "active_bytes": tikv_jemalloc_ctl::stats::active::read().ok(),
        "resident_bytes": tikv_jemalloc_ctl::stats::resident::read().ok(),
    })
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn allocator_stats() -> serde_json::Value {
    serde_json::Value::Null
}
