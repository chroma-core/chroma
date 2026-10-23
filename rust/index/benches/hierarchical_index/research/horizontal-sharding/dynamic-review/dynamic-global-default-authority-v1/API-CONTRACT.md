# Local construction API

A caller starts from an ordinary saved root and streams dense new vector IDs into actual owner actors. Storage chunking and balancing are separate decisions; a matched driver must use identical balance boundaries and settings for its reference.

- `AuthoritativeTree::open_dynamic(provider, roots, metric, config, existing_rows, owner_slots, limits)` loads the saved tree and actual owner state. It admits full-precision navigation and neighbor analysis, one replica, and uniform write policy. The initial saved data belongs to owner zero; other actors start empty.
- `register_dynamic_batch(&[(id, vector)])` saves one bounded immutable vector batch and registers its rows without forcing a balance cut. It rejects the entire malformed ID/dimension/nonfinite/count envelope before writing that batch. A later actor or storage error poisons the instance; there is no retry or rollback contract.
- `balance_dynamic_current()` applies the reference leaf/internal split, merge and root-edit rules to current state. It preserves the reference depth and 100-round stop and records pending work when that stop is reached.
- `add_dynamic_batch(rows, balance_each_row)` is a convenience interface. `true` balances after every row; `false` balances after that storage batch. It is not interchangeable with deferred registration when a reference balances less frequently.
- `configure_dynamic(config)` changes supported structural settings between complete operations. Tests use this to switch a scaled structural-coverage fixture to the unmodified defaults; it is not a production configuration service.
- `move_branch(leaf, owner)` transfers actual owned postings/current records and durable vector references. Optional `limits.move_above` performs placement checks at storage-batch boundaries. Placement leaves node IDs, topology, centroids, vectors and current versions unchanged.
- `save()` writes actual owner postings/versions and complete structural roots. The result remains a global saved index; per-owner durable saved roots and query-runner wiring are not implemented.

Tracing is off by default. `construction_accounting()` reports root-fork counts, retained-event validity, event counts, configured limits and remaining cost categories. These are logical operation counters, not physical I/O, RSS or latency fractions. Local actor threads and the shared provider do not establish a multi-process or cross-host treatment.

Ordinary runtime construction does not need a test tape. Paired tests explicitly enable strict replay of the reference's split decisions and outer round order. Test-only reference hooks preserve production code behavior and do not introduce a seed or quality guarantee for independent real-data builds.
