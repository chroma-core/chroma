# Explicit saved identity-map bridge

Both native construction treatments stream the dense global IDs `0..rows` into a generation-local `store/id-map-1.u64le` file. Each ID uses eight little-endian bytes. A 64 KiB write buffer avoids a complete in-memory map. The writer hashes the exact bytes it writes, flushes and syncs the file, and verifies its complete stored length and hash through the existing ID-map reader before creating either original manifest.

The original create-new saved manifest contains the actual `id_mapping` descriptor with `path`, `count`, `bytes`, and `sha256`. The outer construction result reports the same digest and file fields alongside its existing dense identity semantics. Diagnostic consumers use these original manifests directly; no downstream manifest rewrite is required or permitted by this bridge.

At two million rows the map occupies exactly 16,000,000 bytes in each treatment. The readback adds a separately acknowledged 16,000,000-byte logical verification read per treatment. These figures exclude filesystem metadata and do not claim physical write traffic or performance. The current `audit_seconds` interval includes the saved census, map creation and map verification; it is not a pure census duration.

The fresh local test builds both treatments from the retained 98-row source. It runs the actual saved-ground-truth command on three independently brute-force-ranked L2 queries with ten expected IDs each, verifies complete unique coverage of their 21-ID union, and compares leaf-count and weighted-row oracles against independent exhaustive subset calculations. It also reopens both outputs through the ordinary reader and checks exact rerank result parity. Original saved-manifest hashes remain unchanged throughout those consumer operations.

Accepted V2 source archives, outputs, manifests, tests and review remain intact. The bridge uses a distinct V3 source/archive, binary, output directory and test receipt. It establishes local integration correctness only, not ANN quality, two-million-row performance, Linux execution or cross-host ownership.
