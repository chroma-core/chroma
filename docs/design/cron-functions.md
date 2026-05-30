# Cron Support for Attached Functions

## Overview

Add time-based scheduling to Chroma's attached functions system. Today, functions are triggered by collection writes; this proposal adds cron-based triggers that run functions on a schedule regardless of write activity.

**Use cases:**
- Daily statistics rollups
- Periodic data quality checks
- Scheduled embedding refreshes
- Hourly aggregation jobs

## Current Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Write-Triggered Flow                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Collection Write → WAL → Compaction Scheduler → run_attached_fn() │
│                                                                     │
│  Trigger condition: completion_offset + min_records <= log_position│
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Key components:**
- `AttachedFunction` (rust/types/src/task.rs) — function metadata
- `WorkQueueManager` (rust/worker/src/work_queue/) — manages pending work, persists to parquet
- `fn_consumer` — pulls from work queue, executes functions

## Proposed Design

### Core Idea

Store a **min-heap of scheduled entries** directly in the work queue, alongside the existing `pending_work` queue. The heap contains one entry per cron function, always pointing to its next scheduled run time.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Cron-Triggered Flow                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  WorkQueue tick:                                                    │
│    1. Peek heap for entries where next_run_at <= now               │
│    2. Move ready entries to pending_work                           │
│    3. Update next_run_at from cron expression (stays in heap)      │
│    4. Persist atomically                                           │
│                                                                     │
│  fn_consumer pulls from pending_work (same as today)               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Work Queue, Not Sysdb Polling

| Approach | Pros | Cons |
|----------|------|------|
| Poll sysdb for due cron functions | Simple query | DB load, separate scheduler component, split state |
| Heap in work queue | Zero DB queries, reuses existing tick loop, atomic persistence | Rebuild heap on state loss |

The work queue already handles persistence, deduplication, and distributed coordination (ETag-based locking). Adding cron scheduling here keeps all execution state in one place.

## Data Structures

### New: ScheduledEntry

```rust
// rust/worker/src/work_queue/types.rs

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduledEntry {
    /// Next scheduled execution time
    pub next_run_at: SystemTime,
    /// The attached function to run
    pub fn_id: AttachedFunctionUuid,
    /// Input collection for the function
    pub input_coll_id: CollectionUuid,
    /// Cron expression (e.g., "0 0 * * *" for daily at midnight)
    pub cron_expression: String,
    /// True if a job is already in pending_work (prevents duplicates)
    pub pending: bool,
}
```

### Modified: QueueState

```rust
// rust/worker/src/work_queue/state.rs

pub struct QueueState {
    /// Existing: jobs ready to execute
    pub pending_work: VecDeque<WorkQueueRecord>,
    
    /// NEW: scheduled cron jobs, ordered by next_run_at
    /// Persisted as flat vec, heapified on load
    pub scheduled: Vec<ScheduledEntry>,
    
    pub current_etag: Option<String>,
    pub dirty: bool,
    pub next_insertion_order: u64,
}
```

### Modified: AttachedFunction

```rust
// rust/types/src/task.rs

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TriggerType {
    /// Triggered by collection writes (existing behavior)
    Write,
    /// Triggered on a cron schedule
    Cron,
}

pub struct AttachedFunction {
    // ... existing fields ...
    
    /// NEW: How the function is triggered
    pub trigger_type: TriggerType,
    
    /// NEW: Cron expression (only for TriggerType::Cron)
    pub cron_expression: Option<String>,
}
```

## Detailed Flow

### Creating a Cron Function

```
1. Client calls attach_function(..., trigger="cron", cron_schedule="0 0 * * *")

2. Frontend validates cron expression syntax

3. Sysdb creates AttachedFunction record:
   - trigger_type = Cron
   - cron_expression = "0 0 * * *"

4. Frontend sends AddScheduledEntryMessage to WorkQueueManager:
   - fn_id, input_coll_id, cron_expression
   - next_run_at = compute_next(cron_expression, now)

5. WorkQueueManager:
   - Adds ScheduledEntry to heap
   - Marks state dirty
   - Persists on next tick
```

### Tick Processing

```
WorkQueueManager periodic tick (existing, e.g., every 1s):

1. Get current time

2. While heap.peek().next_run_at <= now:
   entry = heap.peek_mut()
   
   if entry.pending:
       // Already in pending_work, skip
       continue
   
   // Create work record
   pending_work.push(WorkQueueRecord {
       fn_id: entry.fn_id,
       input_coll_id: entry.input_coll_id,
       completion_offset: -1,  // N/A for cron
       insertion_order: next_order++,
   })
   
   // Update for next occurrence
   entry.next_run_at = compute_next(entry.cron_expression, now)
   entry.pending = true
   
   // Re-heapify (entry moved to new position)
   heap.sift_down()

3. Persist if dirty
```

### Job Completion

```
fn_consumer completes a cron job:

1. Sends FinishWorkMessage { fn_id, ... }

2. WorkQueueManager:
   - Removes from pending_work (existing)
   - Finds ScheduledEntry by fn_id
   - Sets entry.pending = false
   - Persists
```

### Deleting a Cron Function

```
1. Client calls detach_function(name)

2. Sysdb deletes AttachedFunction record

3. Frontend sends RemoveScheduledEntryMessage { fn_id }

4. WorkQueueManager:
   - Removes from scheduled heap
   - Removes any pending work for fn_id
   - Persists
```

## Crash Recovery

| Scenario | State After Recovery | Behavior |
|----------|---------------------|----------|
| Crash before persist | Last persisted state | May re-trigger job (idempotent) |
| Crash after persist, job in pending_work | Job in pending_work | fn_consumer picks it up |
| State file lost entirely | Empty state | Rebuild heap from sysdb (see below) |

### Heap Rebuild from Sysdb

On startup, if state file is missing or corrupt:

```rust
async fn rebuild_scheduled_heap(&mut self) -> Result<(), WorkQueueError> {
    let cron_functions = self.sysdb
        .get_attached_functions_by_trigger_type(TriggerType::Cron)
        .await?;
    
    let now = SystemTime::now();
    for af in cron_functions {
        if let Some(cron_expr) = &af.cron_expression {
            self.state.scheduled.push(ScheduledEntry {
                next_run_at: compute_next(cron_expr, now),
                fn_id: af.id,
                input_coll_id: af.input_collection_id,
                cron_expression: cron_expr.clone(),
                pending: false,
            });
        }
    }
    
    self.heapify_scheduled();
    Ok(())
}
```

## API Changes

### Python Client

```python
from chromadb.api.functions import STATISTICS_FUNCTION

# Existing (write-triggered)
collection.attach_function(
    function=STATISTICS_FUNCTION,
    name="my_stats",
    output_collection="stats_output",
)

# NEW (cron-triggered)
collection.attach_function(
    function=STATISTICS_FUNCTION,
    name="daily_stats",
    output_collection="stats_output",
    trigger="cron",
    cron_schedule="0 0 * * *",  # daily at midnight UTC
)
```

### REST API

```
POST /api/v2/tenants/{tenant}/databases/{database}/collections/{collection}/functions

{
  "function": "statistics",
  "name": "daily_stats",
  "output_collection": "stats_output",
  "trigger": "cron",                    // NEW: "write" (default) or "cron"
  "cron_schedule": "0 0 * * *"          // NEW: required if trigger="cron"
}
```

### Cron Expression Format

Standard 5-field cron syntax:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, Sun-Sat)
│ │ │ │ │
* * * * *
```

Recommended crate: `cron` (https://crates.io/crates/cron) for parsing and next-time computation.

## Files to Modify

| Layer | File | Changes |
|-------|------|---------|
| Types | `rust/types/src/task.rs` | Add `TriggerType`, `cron_expression` to `AttachedFunction` |
| Proto | `idl/chromadb/proto/coordinator.proto` | Add fields to `AttachedFunction` message |
| Sysdb | `rust/rust-sysdb/src/spanner.rs` | Add columns, update queries |
| Sysdb | `rust/spanner-migrations/` | New migration for schema changes |
| Work Queue | `rust/worker/src/work_queue/types.rs` | Add `ScheduledEntry` |
| Work Queue | `rust/worker/src/work_queue/state.rs` | Add `scheduled` vec, parquet serialization |
| Work Queue | `rust/worker/src/work_queue/work_queue_manager.rs` | Tick processing, heap management |
| Frontend | `rust/frontend/src/` | Validation, route handlers |
| Python | `chromadb/api/models/Collection.py` | `attach_function()` params |
| Python | `chromadb/api/types.py` | Type definitions |

## Sizing

**Heap memory:**
- 1 entry per cron function
- ~100 bytes per entry
- 1,000 cron functions ≈ 100 KB
- 10,000 cron functions ≈ 1 MB

This is negligible. The heap size is bounded by the number of cron functions, not by the number of occurrences.

**Persistence overhead:**
- Additional parquet column group in work queue state file
- Minimal impact — same persistence frequency, slightly larger file

## Rollout Plan

### Phase 1: Infrastructure
1. Add `TriggerType` and `cron_expression` to types
2. Schema migration for sysdb
3. Update proto definitions
4. Add `ScheduledEntry` and heap to work queue

### Phase 2: Work Queue Integration
1. Implement tick processing for scheduled entries
2. Add `pending` flag handling on job completion
3. Add heap rebuild from sysdb on startup
4. Integration tests

### Phase 3: API Surface
1. Frontend validation and routes
2. Python client updates
3. Documentation

### Phase 4: Observability
1. Metrics: scheduled_entries_count, cron_jobs_triggered, cron_jobs_completed
2. Logging: job scheduling, execution, errors
3. Alerting: jobs not running on schedule, heap rebuild events

## Open Questions

1. **Timezone handling** — Store cron expressions as UTC? Allow per-function timezone?

2. **Backfill on missed runs** — If the system was down during a scheduled time, should it run immediately on recovery, or skip to the next occurrence?

3. **Minimum interval** — Should we enforce a minimum cron interval (e.g., no more frequent than every 5 minutes) to prevent abuse?

4. **Manual trigger** — Add a `run_now()` API for cron functions to allow ad-hoc execution?

---

*Authors: [Team]*  
*Status: Draft*  
*Last Updated: 2026-05-30*
