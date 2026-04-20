//! Process / system memory probes for benchmark instrumentation.
//!
//! On Linux we read `/proc/self/status` (per-process RSS, peak RSS,
//! virtual size, peak virtual size) and `/proc/meminfo` (system memory
//! available). On other platforms, all probes return `None` / zeros so
//! callers can format unconditionally.
//!
//! The intended use is to figure out **why** the OS is killing the
//! benchmark with `SIGKILL`. On Linux that almost always means the OOM
//! killer fired; the kernel does not deliver a signal you can catch
//! ahead of time, so the only way to debug is to log RSS frequently
//! enough that the *previous* checkpoint's logs reveal the trajectory.
//! Confirm with `dmesg | grep -iE 'kill|oom'` after the fact.

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Default, Clone, Copy)]
pub struct MemSnapshot {
    /// Resident set size (physical memory currently mapped) in bytes.
    pub rss: u64,
    /// Peak resident set size since process start (`VmHWM`) in bytes.
    pub rss_peak: u64,
    /// Current virtual memory size (`VmSize`) in bytes.
    pub vmsize: u64,
    /// Peak virtual memory size since process start (`VmPeak`) in bytes.
    pub vmpeak: u64,
    /// Anonymous resident bytes (`RssAnon`) -- pages that came from
    /// `mmap(MAP_ANON)` or `brk()` and are NOT backed by any file.
    /// This is essentially "heap RSS" -- everything jemalloc handed
    /// out to the program. If `rss_anon` ≈ `rss`, the process has no
    /// significant file-mapped memory and the gap between `rss` and
    /// our writer accounting is *all* heap-shaped.
    pub rss_anon: u64,
    /// File-backed resident bytes (`RssFile`) -- pages that came from
    /// `mmap()` of a file (mostly: `.so` libraries, but also any
    /// memory-mapped data files). Should be small (~MB) for this
    /// bench since we don't `mmap()` parquet/blockfile data; if it
    /// climbs into the GBs we have an unexpected mmap somewhere.
    pub rss_file: u64,
}

/// Returns a snapshot of this process's memory usage.
///
/// On non-Linux platforms returns all-zeros; the format helpers will
/// render that as `-`.
#[cfg(target_os = "linux")]
pub fn read_self() -> MemSnapshot {
    let mut snap = MemSnapshot::default();
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return snap;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            snap.rss = parse_kb(rest);
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            snap.rss_peak = parse_kb(rest);
        } else if let Some(rest) = line.strip_prefix("VmSize:") {
            snap.vmsize = parse_kb(rest);
        } else if let Some(rest) = line.strip_prefix("VmPeak:") {
            snap.vmpeak = parse_kb(rest);
        } else if let Some(rest) = line.strip_prefix("RssAnon:") {
            snap.rss_anon = parse_kb(rest);
        } else if let Some(rest) = line.strip_prefix("RssFile:") {
            snap.rss_file = parse_kb(rest);
        }
    }
    snap
}

#[cfg(not(target_os = "linux"))]
pub fn read_self() -> MemSnapshot {
    MemSnapshot::default()
}

/// Returns `MemAvailable` from `/proc/meminfo` in bytes.
#[cfg(target_os = "linux")]
pub fn read_sys_available() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            return Some(parse_kb(rest));
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
pub fn read_sys_available() -> Option<u64> {
    None
}

/// Returns `MemTotal` from `/proc/meminfo` in bytes.
#[cfg(target_os = "linux")]
pub fn read_sys_total() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            return Some(parse_kb(rest));
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
pub fn read_sys_total() -> Option<u64> {
    None
}

/// Parse a `/proc` value formatted as " <number> kB" into bytes.
fn parse_kb(rest: &str) -> u64 {
    let s = rest.trim();
    let n: u64 = s
        .split_whitespace()
        .next()
        .and_then(|tok| tok.parse().ok())
        .unwrap_or(0);
    n.saturating_mul(1024)
}

// =============================================================================
// Background sampler — tracks the *interval* peak RSS between user-defined
// reset points (e.g. between checkpoint boundaries). VmHWM is process-
// lifetime peak and never decreases, so for "what was the peak during
// CP N?" you need a sampler.
// =============================================================================

pub struct RssSampler {
    inner: Arc<RssSamplerInner>,
    handle: Option<JoinHandle<()>>,
}

struct RssSamplerInner {
    stop: AtomicBool,
    interval_peak: AtomicU64,
    /// Minimum `MemAvailable` observed since last reset, in bytes.
    /// `u64::MAX` sentinel means "no sample yet". This is the inverse
    /// of `interval_peak`: it captures the trough of system-available
    /// memory, which is what really matters for OOM avoidance because
    /// jemalloc's `MADV_FREE` pages inflate RSS without actually
    /// pressuring the kernel.
    interval_min_sys_avail: AtomicU64,
}

impl RssSampler {
    /// Spawn a background thread that polls RSS every `interval` and
    /// keeps a running maximum (resettable via `take_interval_peak`).
    /// Returns a sampler with `peak_since_reset() == 0` until the first
    /// poll completes.
    pub fn spawn(interval: Duration) -> Self {
        let inner = Arc::new(RssSamplerInner {
            stop: AtomicBool::new(false),
            interval_peak: AtomicU64::new(0),
            interval_min_sys_avail: AtomicU64::new(u64::MAX),
        });
        let inner_thread = inner.clone();
        let handle = thread::Builder::new()
            .name("mem-probe".into())
            .spawn(move || {
                while !inner_thread.stop.load(Ordering::Relaxed) {
                    let rss = read_self().rss;
                    if rss > 0 {
                        let mut cur = inner_thread.interval_peak.load(Ordering::Relaxed);
                        while rss > cur {
                            match inner_thread.interval_peak.compare_exchange_weak(
                                cur,
                                rss,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => break,
                                Err(observed) => cur = observed,
                            }
                        }
                    }
                    if let Some(avail) = read_sys_available() {
                        let mut cur = inner_thread.interval_min_sys_avail.load(Ordering::Relaxed);
                        while avail < cur {
                            match inner_thread.interval_min_sys_avail.compare_exchange_weak(
                                cur,
                                avail,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => break,
                                Err(observed) => cur = observed,
                            }
                        }
                    }
                    thread::sleep(interval);
                }
            })
            .expect("failed to spawn mem-probe thread");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    /// Returns the maximum RSS observed since the last call to
    /// `take_interval_peak` (or sampler start), and resets the counter
    /// to the current RSS so subsequent intervals start from "now".
    pub fn take_interval_peak(&self) -> u64 {
        let cur = read_self().rss;
        self.inner.interval_peak.swap(cur, Ordering::Relaxed)
    }

    /// Peek without resetting.
    pub fn peek_interval_peak(&self) -> u64 {
        self.inner.interval_peak.load(Ordering::Relaxed)
    }

    /// Returns the minimum `MemAvailable` observed since the last call
    /// to `take_interval_min_sys_avail` (or sampler start) and resets
    /// the counter to the current value. Returns `None` if no sample
    /// has been taken yet (process started in a state where
    /// `/proc/meminfo` was unreadable).
    pub fn take_interval_min_sys_avail(&self) -> Option<u64> {
        let cur = read_sys_available().unwrap_or(u64::MAX);
        let prev = self.inner.interval_min_sys_avail.swap(cur, Ordering::Relaxed);
        if prev == u64::MAX {
            None
        } else {
            Some(prev)
        }
    }

    /// Peek without resetting. `None` if no sample has been taken yet.
    pub fn peek_interval_min_sys_avail(&self) -> Option<u64> {
        let v = self.inner.interval_min_sys_avail.load(Ordering::Relaxed);
        if v == u64::MAX {
            None
        } else {
            Some(v)
        }
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

// =============================================================================
// Formatting helpers
// =============================================================================

pub fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "-".to_string();
    }
    let b = bytes as f64;
    if b < 1024.0 {
        format!("{}B", bytes)
    } else if b < 1024.0 * 1024.0 {
        format!("{:.1}KB", b / 1024.0)
    } else if b < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1}MB", b / (1024.0 * 1024.0))
    } else {
        format!("{:.2}GB", b / (1024.0 * 1024.0 * 1024.0))
    }
}

// =============================================================================
// Jemalloc statistics
//
// Tells us where in jemalloc's internal layers the heap RSS is sitting:
//
//   allocated  -- bytes the program currently holds via malloc/free APIs
//                 (i.e. live, addressable allocations). This is the
//                 closest analogue to "what the application thinks it's
//                 using". If `allocated` is much smaller than RSS, the
//                 gap is jemalloc-internal (decay queue, fragmentation,
//                 metadata) NOT a leak in app code.
//
//   active     -- bytes in pages active for allocation (close to the
//                 sum of `allocated` plus internal fragmentation in
//                 each in-use slab/extent). `active - allocated` is
//                 internal fragmentation.
//
//   resident   -- physical pages backing jemalloc-managed memory
//                 (matches `RssAnon` minus the few MB outside jemalloc).
//                 This is the heap's contribution to RSS.
//
//   mapped     -- virtual address space mapped by jemalloc (≥ resident).
//                 Pages may be MADV_DONTNEED'd back to the kernel and
//                 still counted as mapped.
//
//   retained  -- virtual address space *kept* by jemalloc but unmapped
//                 from any active extent. These pages are MADV_DONTNEED'd
//                 (or `munmap`'d on systems without DONTNEED) so they
//                 don't pressure RSS, but they count toward VmSize. A
//                 large `retained` is normal during steady state and
//                 means the allocator's decay queue is doing its job.
//
// Diagnostic shorthand:
//
//   `resident - allocated` ≈ allocator slack you could in principle
//   reclaim by tuning `dirty_decay_ms` / `muzzy_decay_ms` lower or by
//   restarting the process.
//
//   If `allocated` > expected app size -- you have a real leak.
//   If `resident - allocated` is huge -- the allocator is hoarding.
//   If `mapped >> resident` -- you've fragmented the address space but
//   not RSS; usually fine.
// =============================================================================

#[derive(Debug, Default, Clone, Copy)]
pub struct JemallocSnapshot {
    pub allocated: u64,
    pub active: u64,
    pub resident: u64,
    pub mapped: u64,
    pub retained: u64,
}

#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
pub fn read_jemalloc() -> JemallocSnapshot {
    use tikv_jemalloc_ctl::{epoch, stats};
    // Many jemalloc stats are cached and only refreshed when the
    // epoch is advanced. If the advance fails (e.g. the stats feature
    // is compiled out), bail out with zeros so the formatter renders
    // them as `-`.
    if epoch::advance().is_err() {
        return JemallocSnapshot::default();
    }
    JemallocSnapshot {
        allocated: stats::allocated::read().unwrap_or(0) as u64,
        active: stats::active::read().unwrap_or(0) as u64,
        resident: stats::resident::read().unwrap_or(0) as u64,
        mapped: stats::mapped::read().unwrap_or(0) as u64,
        retained: stats::retained::read().unwrap_or(0) as u64,
    }
}

#[cfg(any(not(target_os = "linux"), target_env = "msvc"))]
pub fn read_jemalloc() -> JemallocSnapshot {
    JemallocSnapshot::default()
}
