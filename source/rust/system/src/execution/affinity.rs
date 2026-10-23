#[cfg(target_os = "linux")]
pub(crate) fn pin_current_thread(core_id: usize) -> bool {
    if core_id >= libc::CPU_SETSIZE as usize {
        return false;
    }

    // SAFETY: cpu_set_t is initialized before use, core_id bounds are checked,
    // and sched_setaffinity is called for the current thread/process (pid 0).
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core_id, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn pin_current_thread(_core_id: usize) -> bool {
    false
}

/// Return the core to pin for a CPU worker thread.
///
/// Workers cycle through cores `0, 1, ..., affinity_count - 1`.
/// If `affinity_count >= total_cores`, clamp to `total_cores`.
pub(crate) fn cpu_core_for_worker(
    worker_id: usize,
    affinity_count: usize,
    total_cores: usize,
) -> Option<usize> {
    if affinity_count == 0 || total_cores == 0 {
        None
    } else {
        let effective = affinity_count.min(total_cores);
        Some(worker_id % effective)
    }
}

/// Return the core to pin for an IO runtime thread.
///
/// IO threads cycle from the top: `total_cores - 1, total_cores - 2, ...`
/// wrapping after `affinity_count` slots.
/// If `affinity_count >= total_cores`, clamp to `total_cores`.
pub(crate) fn io_core_for_task(
    task_index: usize,
    affinity_count: usize,
    total_cores: usize,
) -> Option<usize> {
    if affinity_count == 0 || total_cores == 0 {
        return None;
    }
    let effective = affinity_count.min(total_cores);
    let offset = task_index % effective;
    Some((total_cores - 1) - offset)
}

#[cfg(test)]
mod tests {
    use super::{cpu_core_for_worker, io_core_for_task};

    #[test]
    fn cpu_affinity_wraps_within_count() {
        // 4 affinity cores on a 4-core machine: cycle 0,1,2,3
        let cores: Vec<usize> = (0..10)
            .map(|worker_id| cpu_core_for_worker(worker_id, 4, 4).unwrap())
            .collect();
        assert_eq!(cores, vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1]);
    }

    #[test]
    fn io_affinity_descends_and_wraps() {
        // 4 affinity cores on a 4-core machine: cycle 3,2,1,0
        let cores: Vec<usize> = (0..10)
            .map(|task_id| io_core_for_task(task_id, 4, 4).unwrap())
            .collect();
        assert_eq!(cores, vec![3, 2, 1, 0, 3, 2, 1, 0, 3, 2]);
    }

    #[test]
    fn nine_cores_three_affinity() {
        // 3 CPU affinity cores on a 9-core machine: cycle 0,1,2
        let cores: Vec<usize> = (0..6)
            .map(|worker_id| cpu_core_for_worker(worker_id, 3, 9).unwrap())
            .collect();
        assert_eq!(cores, vec![0, 1, 2, 0, 1, 2]);
        // 3 IO affinity cores on a 9-core machine: cycle 8,7,6
        let cores: Vec<usize> = (0..6)
            .map(|task_id| io_core_for_task(task_id, 3, 9).unwrap())
            .collect();
        assert_eq!(cores, vec![8, 7, 6, 8, 7, 6]);
    }

    #[test]
    fn affinity_count_exceeds_total_clamps() {
        // 12 affinity cores on a 9-core machine: clamp to 9
        let cores: Vec<usize> = (0..9)
            .map(|worker_id| cpu_core_for_worker(worker_id, 12, 9).unwrap())
            .collect();
        assert_eq!(cores, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let cores: Vec<usize> = (0..9)
            .map(|task_id| io_core_for_task(task_id, 12, 9).unwrap())
            .collect();
        assert_eq!(cores, vec![8, 7, 6, 5, 4, 3, 2, 1, 0]);
    }

    #[test]
    fn zero_affinity_returns_none() {
        assert_eq!(cpu_core_for_worker(0, 0, 9), None);
        assert_eq!(io_core_for_task(0, 0, 9), None);
    }

    #[test]
    fn zero_total_cores_returns_none() {
        assert_eq!(cpu_core_for_worker(0, 3, 0), None);
        assert_eq!(io_core_for_task(0, 3, 0), None);
    }
}
