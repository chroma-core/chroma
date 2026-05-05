# notion_cli

A local CLI + daemon that dumps a Notion workspace via the undocumented
`/api/v3` endpoints, then keeps that on-disk dump synced incrementally.
Single static Rust binary under `rust/` -- `login`, `probe`, `discover`,
`dump`, `grab`, `sync`, `sync-install`, `sync-uninstall`. See `--help`
on the binary for usage:

```sh
cd rust && cargo build --release
./target/release/notion-internal-dump --help
./target/release/notion-internal-dump login        # capture token_v2
./target/release/notion-internal-dump grab         # one-shot full dump
./target/release/notion-internal-dump sync         # incremental
```

Output layout (default `notion-internal-dump/`):

```
sidebar.jsonl                            current top-level containers
discovery.jsonl                          per-page state baseline (id, title,
                                         parent_id, parent_table,
                                         last_edited_time, ...)
dump.summary.jsonl                       append-only history of exports
dump.changelog.jsonl                     append-only per-file change log
                                         (the downstream-consumer contract)
exports/<slug>__<id>/                    one dir per container
    export.zip                           raw Notion ZIP
    unzipped/                            extracted markdown + assets
_state/file-hashes/<container_id>.json   per-file SHA256 leaves +
                                         per-container Merkle root
_state/last_run.json                     latest sync_run_id, started/ended,
                                         workspace Merkle root
.tombstones/<id>_<ts>/                   removed-then-archived containers
```

## Design notes

### Why everything is Rust now

Earlier versions kept `login` + `probe` in Python on the assumption that
nothing in the Rust ecosystem matched `browser_cookie3` for cross-browser
cookie extraction. That assumption is stale: by 2026 the
[`rookie`](https://docs.rs/rookie) crate (`0.5.6+`) covers
Chrome/Edge/Brave/Chromium/Opera/Opera GX/Vivaldi/Arc/Zen/LibreWolf/Cachy
+ Firefox + Safari on Win/macOS/Linux, with the same macOS-Keychain /
Linux-NSS / Windows-DPAPI handling, and
[`chromiumoxide`](https://docs.rs/chromiumoxide) (`0.9+`) is a
production-grade async CDP client that subsumes our prior
`subprocess.Popen + manual JSON-RPC` Python loop. Together they replaced
~470 LoC of Python cookie-store enumeration with ~50 LoC of orchestration
and ~280 LoC of CDP polling with ~150 LoC. End result: single static
binary, no Python dependency on the user's machine, faster cold start
(no `python3` boot per-login), one set of error paths to maintain.

The escape hatches:

- `login --paste` — paste `token_v2` from DevTools (covers headless
  boxes / weird display setups).
- `login --chrome` — skip the cookie-store scan, jump straight to the
  managed-Chromium CDP flow (useful when the user wants the picker UI).
- `login --cookie-file <path>` — point `rookie::any_browser` at one
  specific cookie database. Catches the long tail (Atlas, Sidekick,
  Comet, Dia, Wavebox, Yandex, Tor, snap/flatpak Firefox containers)
  that rookie's named-browser list doesn't enumerate by default. The
  CLI prints a per-OS hint table when the umbrella scan misses
  everything.

We deliberately don't ship a browser binary in the CLI distribution.
The older `login-playwright` escape hatch (which downloaded ~150MB of
Chromium on first use) was removed: if no Chromium-family browser is
installed at all, `login` bails with platform-specific install
instructions and points users at `--paste` for the manual route.

### Why `--full` is still required even with `--subtree-export`

`--subtree-export` re-exports only the lowest-common-ancestor of dirty
pages inside a single container and stitches it into the existing
`unzipped/` tree. Two real failure modes that only a full container
re-export resolves:

1. **Cross-subtree page-mention link drift.** Notion's markdown export
   embeds page-to-page mentions by *filename*: `[Sync](Sync%20<uuid>.md)`.
   When you rename "Sync" → "Synchronization Architecture", the LCA
   subtree refreshes its own `.md` (filename now contains the new title),
   but every sibling page outside the subtree that linked to Sync still
   has the OLD filename in its href. Those links are broken until the next
   full export rewrites every sibling's markdown.

2. **Top-level database CSV staleness.** Inline databases get exported
   as CSVs at the container root (e.g.
   `Engineering Hub 616a75e3...csv`). Subtree mode never touches the
   container root, so adding/editing a row inside a subtree page leaves
   the root CSV stale.

Synced blocks and database relation columns are the same idea applied to
other Notion features. Pattern: subtree mode for low-latency incrementals
(e.g. every 15 min), `dump --full` weekly to absorb drift.

URL expiry is **not** a concern with the default `--include-files
everything`: Notion downloads every asset to disk and rewrites markdown
to local relative paths (`![diagram](Some%20Page/diagram.png)`). No
`file.notion.so` URLs land on disk. Expiry only matters in `no_files`
mode, which we don't recommend.

### Why a Merkle tree (vs a plain hashtable of file → SHA256)?

Today's runtime needs are satisfied by a plain hashtable. The leaf set
(`leaves: BTreeMap<rel_path, FileLeaf>` in
`rust/src/sync/merkle.rs`) does 100% of the change-detection work. The
container-level Merkle root is currently used for two things:

- A single fingerprint to fast-skip "did anything change?" before walking
  the leaves to emit changelog entries.
- A workspace-level manifest version printed at the end of every run
  (`workspace_merkle_root=0x...`).

Both could be replaced with `sha256(sorted_concat(leaves))` and the
codebase would be functionally equivalent. None of the standard
Merkle-tree affordances (inclusion proofs, exclusion proofs, O(log N)
incremental updates, verifiable streaming sync) are exercised on the
local-daemon-talking-to-itself path.

**Why keep it then?** The intended deployment is multiple daemon
instances (sharded across users / workspaces) feeding the same Chroma
backend. That gets us into territory where the Merkle properties earn
their keep:

- **Sharded reconciliation.** Two daemons covering overlapping pages
  (e.g. two collaborators sync the same teamspace) need to agree on
  which version the server is holding without uploading their full leaf
  sets. Server says "I'm at root R for container C; what's yours?" If
  the roots match, zero data movement.

- **Verifiable partial sync.** When a daemon's local manifest diverges
  from the server's, the client can send only the changed leaves plus
  their Merkle paths so the server can verify atomically that the
  partial update lands on the expected new root. Plain
  `sha256(concat)` can't do this.

- **Inclusion proofs for audit / dedup.** "Prove this page is part of
  the manifest version the server is currently serving" without
  shipping the other ~1500 leaves. Useful for cross-tenant dedup
  (Q4 of the original design discussion: dedup key
  `(page_id, last_edited_time)` as logical identity, with a Merkle
  inclusion proof tying any specific leaf to a global manifest root the
  server already trusts).

- **Composability.** The workspace root is `Merkle(per-container
  roots)` so we can answer "is *this one container* part of *that*
  workspace manifest version?" without touching other containers.
  Important when shards land independently and the server reconciles
  asynchronously.

So: the Merkle structure is overhead today (~50 lines, one extra crate
in `Cargo.toml`) and a load-bearing primitive once we ship the
multi-client server protocol. It's cheap to keep and disruptive to
add back later (every existing client's persisted state would need to
be re-derived after a schema change), so we hold it.

If we ever decide *not* to ship that protocol, removal is mechanical:
swap `merkle_root_hex` for a `summary_hash_hex` field, drop `rs_merkle`
from `Cargo.toml`, regenerate state on next run.
