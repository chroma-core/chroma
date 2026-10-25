use serde_json::{json, Value};

fn file_number(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

pub(super) fn read() -> Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let rss = status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
            .map(|kb| kb * 1024)
    });
    let current = file_number("/sys/fs/cgroup/memory.current")
        .or_else(|| file_number("/sys/fs/cgroup/memory/memory.usage_in_bytes"));
    let limit = file_number("/sys/fs/cgroup/memory.max")
        .or_else(|| file_number("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
    json!({ "rss_bytes": rss, "cgroup_bytes": current, "cgroup_limit_bytes": limit })
}
