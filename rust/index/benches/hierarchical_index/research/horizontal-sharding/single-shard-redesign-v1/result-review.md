# Independent result interpretation review

The existing independent reviewer reads the four raw results, summary and counter meanings after the bounded comparison. The reviewer agrees that variable relative timing and unequal work do not establish parity or a stable regression. All four runs preserve exact membership and satisfy the configured size limits.

The reviewer identifies two adverse details that the report includes: insertion consumes 1.35–1.38 CPU-seconds in the candidate versus 0.58–0.77 in the reference, and the process peak observed through balancing is 576–591 decimal MB versus 460–464 MB. The report distinguishes the cumulative process high-water mark from current resident memory and isolated phase allocation.

Deferred transfer-group retries are not unique-vector counts or machine messages. Scheduler condition waits overlap across workers; summing them does not measure added wall time. Zero lazy-storage load counters describe fresh resident construction, not absence of cache activity or all decoding. An initial dependency from one oversized leaf is real, but these receipts lack per-job critical-path traces that would assign the complete delay to it.

Root incorporates these qualifications in [the report](REPORT.md). The review is read-only; no additional benchmark or implementation change follows. Reviewer: `/root/recovery_review`.
