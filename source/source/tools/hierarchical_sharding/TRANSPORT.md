# Independent shard reads

Each logical shard stores its own hierarchical index. A coordinator sends every query to every shard and merges the returned distances. This is an all-shard baseline; it does not route queries to spatial subtrees or integrate with a production write-ahead log.

Global IDs remain stable when rows are appended. With a fixed count of logical shards, shard `i` owns IDs `i + local_id * shard_count`. Place the same logical shards across different numbers of hosts when measuring scaling. Changing the logical shard count requires rebuilding the dataset partition.

Build each shard independently with the Rust worker on the host that owns its checkpoint directory. Use its immutable `manifest-N.json` when publishing and serving generation `N`. Publish only after all independent build jobs succeed:

```sh
python3 -m tools.hierarchical_sharding.publish \
  --shard /checkpoints/0/manifest-1.json=http://host-a:8080 \
  --shard /checkpoints/1/manifest-1.json=http://host-b:8080 \
  --total-vector-count 1000000 --output /snapshots/current.json
```

The publisher validates that all shards share a generation, dimension, and metric. It checks that each shard owns a distinct residue and that their counts cover one dense global ID prefix. It replaces the global manifest atomically. Publishing identical content is idempotent; changing the content of an existing generation is rejected. Use a single publisher for the global pointer. Retain old immutable generation manifests and checkpoint files while readers use them.

Start one persistent reader per shard. The adapter defaults to loopback. Set an explicit private interface for requests across hosts:

```sh
python3 -m tools.hierarchical_sharding.server \
  --manifest /checkpoints/0/manifest-1.json \
  --checkpoint-dir /checkpoints/0 \
  --reader-binary /path/to/hierarchical_spann_shard \
  --host 10.0.0.10 --port 8080 --timeout 10
```

The adapter exposes only `POST /query`; it does not accept build commands or arbitrary remote commands. Its plain HTTP interface has no authentication or encryption and belongs on a controlled private network. The reader receives the pinned generation on startup and on every query. Restart the adapter to serve a different generation.

Call `tools.hierarchical_sharding.query_all(manifest, query, k, timeout=10, max_workers=16)` from the coordinator. Each request contains the generation, shard ID, dimension, metric, vector, and requested result count. Every shard must return the complete local result count, matching generation and ownership, finite distances, and unique IDs. Any timeout, unreachable shard, stale generation, malformed response, or incomplete result fails the whole query. Merge order is ascending distance and then global ID. This computes the global top results from the approximate shard candidates; recall still depends on each shard's search. Optional physical-byte and scored-vector counters are aggregated only when every shard supplies them.

The coordinator bounds simultaneous requests and sets a per-request socket timeout. The adapter bounds active request threads and serializes access to its persistent reader. A reader response timeout kills the reader to prevent a late response from being used for the next request. Restart a failed reader adapter before retrying. The coordinator enforces a whole-query wait deadline and cancels queued work on expiry. Already running HTTP requests finish in background under their socket timeouts.

Run transport tests with `python3 -m unittest tools.hierarchical_sharding.test_transport`. They exercise global top-k merging, deterministic ties, stale generations, duplicate ownership and result IDs, missing results, failed shards, and immutable publication.

Run the real checkpoint integration smoke with `python3 -m tools.hierarchical_sharding.smoke_http --reader-binary /path/to/hierarchical_spann_shard`. It builds two shards, verifies exact global IDs and distances over HTTP, appends a second generation, and checks that the old reader endpoints remain pinned. Every build has a twenty-second timeout.
