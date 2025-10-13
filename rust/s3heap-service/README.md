# s3heap-service

The s3heap-service integrates with the task manager to trigger tasks at no-faster than a particular
cadence, with reasonable guarantees that writing data will cause a task to run.

This document lays refines the design of the heap-tender and heap service until it can be
implemented safely.

## Abstract:  A heap and a sysdb.

At the most abstract level, we have a heap and the sysdb.  An item is either in the heap or not in
the heap.  For the sysdb, an item is not in the sysdb, in the sysdb and should be scheduled, or in
the sysdb and waiting for writes to trigger the next scheduled run.

That gives this chart

| Heap State | Sysdb State |
|------------|-------------|
| Not in heap | Not in sysdb |
| Not in heap | In sysdb, should be scheduled |
| Not in heap | In sysdb, waiting for writes |
| In heap | Not in sysdb |
| In heap | In sysdb, should be scheduled |
| In heap | In sysdb, waiting for writes |

More abstractly, view it like this:

                    | On Heap    | Not On Heap |
--------------------|------------|-------------|
Not in sysdb        | A_1        | A_2         |
In sysdb, scheduled | B_1        | B_2         |
In sysdb, waiting   | C_1        | C_2         |

When viewed like this, we can establish rules for state transitions in our system.  Each operation
operates on either the sysdb or the heap, never both because there is no transactionality between S3
and databases.  Thus, we can reason that we can jump to any row within the same column, or to
another column within the same row.

## State space diagram

                     From
|     |      | A_1  | A_2  | B_1  | B_2  | C_1  | C_2  |
|-----|------|------|------|------|------|------|------|
|     | A_1  | -    | IMP1 | YES1 | X    | YES1 | X    |
|     | A_2  | GC1  | -    | X    | GC2  | X    | YES1 |
| To  | B_1  | IMP2 | X    |-     | NEW2 | YES3 | X    |
|     | B_2  | X    | NEW1 | IMP3 | -    | X    | YES3 |
|     | C_1  | IMP2 | X    | YES2 | X    | -    | IMP4 |
|     | C_2  | X    | NO1  | X    | YES2 | IMP3 | -    |

GC1:  Item gets a perpetual "is-done" from the sysdb and transitions to A_2.
GC2:  Garbage collection.

NEW1:  Create a new task in the sysdb.
NEW2:  Finish the new operation by primiing the task and putting it on the heap.

YES1:  Task gets deleted from sysdb.
YES2:  This implies that we move from scheduled to waiting while the task is on heap.  This happens
       when a job completes and reads all data from the log.
YES3:  There was a write, the heap needed to schedule, so it picked a time and updated sysdb.

NO1:  This implies that the state transitioned from being not-in-sysdb to in-sysdb.   A new task
      will always run straight away, so it should not be put into waiting state.

IMP1:  The item is not on heap or in the database.  First transition is to B_2 or C_2.
IMP2:  Task UUIDs are not re-used.  Starting from A_1 implies the task was created and then put on
       the heap and subsequently removed from sysdb.  There should be no means by which it reappears
       in the sysdb.  Therefore this path is impossible.
IMP3:  We never take something off the heap until the sysdb is updated to reflect the job being
       done.  Therefore we don't take this transition.
IMP4:  We don't add something to the heap until it has been scheduled.

X:  Impossible.
