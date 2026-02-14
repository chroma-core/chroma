# s3heap-service

The s3heap-service integrates with the function manager to trigger functions at no faster than a
particular cadence, with reasonable guarantees that writing data will cause a function to run.

This document refines the design of the heap-tender and heap service until it can be implemented
safely.

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

And then one must take into account whether there's a function template.

More abstractly, view it like this:

                         |                     | On Heap    | Not On Heap |
-------------------------|---------------------|------------|-------------|
Has no function template | Not in sysdb        | A_1        | A_2         |
                         | In sysdb, scheduled | B_1        | B_2         |
                         | In sysdb, waiting   | C_1        | C_2         |
-------------------------|---------------------|------------|-------------|
Has function template    | Not in sysdb        | D_1        | D_2         |
                         | In sysdb, scheduled | E_1        | E_2         |
                         | In sysdb, waiting   | F_1        | F_2         |

When viewed like this, we can establish rules for state transitions in our system.  Each operation
operates on either the sysdb or the heap, never both because there is no transactionality between S3
and databases.  Thus, we can reason that we can jump to any row within the same column, or to
another column within the same row.

## State space diagram

Note that there are six base cases.  Reasoning through all 36 cases and getting them right will be
difficult.  Instead, we aim to exploit symmetry:  If there is a function template and something is
in sysdb, it is as if there is no function template.  As before, we can mark as trivially impossible
anything that changes along two axes simultaneously.  Anything listed as INVX is invariant X and is
prohibited by the invariant.

                     From
|     |      | A_1  | A_2  | B_1  | B_2  | C_1  | C_2  | D_1  | D_2  | E_1  | E_2  | F_1  | F_2  |
|-----|------|------|------|------|------|------|------|------|------|------|------|------|------|
|     | A_1  | -    | INV2 | DEL1 | X    | DEL1 | X    | TT2  | X    | X    | X    | X    | X    |
|     | A_2  | STOP | -    | X    | GC   | X    | DEL1 | X    | TT2  | X    | X    | X    | X    |
| To  | B_1  | INV1 | X    | -    | R1   | R1   | X    | X    | X    | T2   | X    | X    | X    |
|     | B_2  | X    | ADD1 | INV4 | -    | X    | WT1  | X    | X    | X    | T2   | X    | X    |
|     | C_1  | INV1 | X    | DO1  | X    | -    | INV4 | X    | X    | X    | X    | T2   | X    |
|     | C_2  | X    | INV3 | X    | DO2  | INV4 | -    | X    | X    | X    | X    | X    | T2   |
|     |------|------|------|------|------|------|------|------|------|------|------|------|------|
|     | D_1  | TT1  | X    | X    | X    | X    | X    | -    | INV2 | INV6 | X    | INV6 | X    |
|     | D_2  | X    | TT1  | X    | X    | X    | X    | INV5 | -    | X    | INV6 | X    | INV6 |
|     | E_1  | X    | X    | TT1  | X    | X    | X    | X    | X    | -    | R1   | WT1  | X    |
|     | E_2  | X    | X    | X    | TT1  | X    | X    | X    | TT3  | INV4 | -    | X    | WT1  |
|     | F_1  | X    | X    | X    | X    | TT1  | X    | TT3  | X    | DO1  | X    | -    | X    |
|     | F_2  | X    | X    | X    | X    | X    | TT1  | X    | HOLE2| X    | DO2  | INV4 | -    |

- -:  Identity function.  Always permitted.
- X:  The transition hops rows, columns, or column families in the 2x6 table.
- STOP:  Transition to the quiescent state.
- TT1:  Add function template.
- TT2:  Task template deleted.
- TT3:  Task template instantiated.
- ADD1:  Attach function.
- DEL1:  Delete function.
- DO1:  The function ran once and now is waiting for more log records.
- DO2:  Same as DO1, but technically not possible to happen.
- WT1:  Write triggered-state change.
- GC:  Garbage collection kicks in.
- INV1:  Task UUIDs are not reused.  Therefore the function lifetime has the progression not used -> used -> never used again.
- INV2:  A function will only be added to the heap after it has been witnessed to exist as a template or sysdb entry.  By INV1 if it is on heap and no longer witnessed it will never be used again.  Therefore it cannot resurrect to add to the heap.
- INV3:  A function is always added in a non-waiting state.  This is necessary to guarantee that functions don't get dropped.  It is either existing and on the heap or quiescent and waiting for additional writes.  The latter should never be the starting condition.
- INV4:  A two-phase commit with the heap makes it possible to transition the schedule to keep the function scheduled, commit the heap change, and then commit the change to sysdb.  Therefore the signal will never leave the heap as long as the sysdb has a scheduled function.
- INV5:  By INV2 the function template was witnessed in sysdb before the function was added to the heap.  By INV1, this means the function was deleted.  An impossibility arises.
- INV6:  A function cannot be deleted if it descends a template.
- R1:  Corollary to INV4:  On start, any outstanding 2PC is reconciled and converged to push the function to the heap.

Holes to overcomb/Unsurities:
- HOLE2:  What would compel a process to instantiate a function template if not in heap?
