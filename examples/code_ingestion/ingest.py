"""Ingest a codebase into Chroma with incremental updates.

Usage:
    python ingest.py /path/to/repo --collection code --watch

On first run, all matching files are chunked and upserted.  On subsequent runs,
only files with changed content (detected via hash) trigger updates.  In
``--watch`` mode, the script monitors the filesystem and auto-reindexes with
a configurable debounce.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import threading
import time
from pathlib import Path

import chromadb

from chunker import Chunk, chunk_file

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("code_ingestion")


# ---------------------------------------------------------------------------
# Ingestion engine
# ---------------------------------------------------------------------------

class CodebaseIndexer:
    def __init__(
        self,
        client: chromadb.ClientAPI,
        collection_name: str,
        root: Path,
        extensions: set[str] | None = None,
        ignore: set[str] | None = None,
    ):
        self.client = client
        self.collection = client.get_or_create_collection(collection_name)
        self.root = root.resolve()
        self.extensions = extensions or {".py", ".md", ".yaml", ".yml", ".json", ".ts", ".js"}
        self.ignore = ignore or {".git", "__pycache__", "node_modules", ".venv", "venv", "build", "dist"}

    def _is_relevant(self, path: Path) -> bool:
        if path.suffix not in self.extensions:
            return False
        try:
            rel = path.relative_to(self.root)
        except ValueError:
            return False
        return not any(part in self.ignore for part in rel.parts)

    def _file_hash(self, path: Path) -> str:
        """Fast hash for change detection."""
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]

    def _collect_files(self) -> list[Path]:
        return [p for p in self.root.rglob("*") if self._is_relevant(p)]

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------

    def full_index(self) -> None:
        """One-shot full index (or rebuild)."""
        files = self._collect_files()
        logger.info("Indexing %d files from %s", len(files), self.root)

        for path in files:
            self._index_file(path)

        self._prune_stale(files)
        logger.info("Index complete. Collection size: %d", self.collection.count())

    def incremental_index(self) -> None:
        """Only re-index files whose content has changed since last run."""
        files = self._collect_files()
        logger.info("Incremental scan of %d files", len(files))

        for path in files:
            current_hash = self._file_hash(path)
            existing = self.collection.get(
                where={"file_path": str(path)},
                include=["metadatas"],
            )
            hashes = {m.get("file_hash") for m in (existing["metadatas"] or [])}
            if current_hash not in hashes:
                # Delete old chunks for this file, then re-index
                self.collection.delete(where={"file_path": str(path)})
                self._index_file(path)

        self._prune_stale(files)
        logger.info("Incremental update complete. Collection size: %d", self.collection.count())

    def _index_file(self, path: Path) -> None:
        chunks = chunk_file(path)
        if not chunks:
            return

        ids = [c.id for c in chunks]
        documents = [c.content for c in chunks]
        metadatas = [
            {
                "file_path": c.file_path,
                "chunk_type": c.chunk_type.name,
                "start_line": c.start_line,
                "end_line": c.end_line,
                "file_hash": self._file_hash(path),
                **c.metadata,
            }
            for c in chunks
        ]

        self.collection.upsert(
            ids=ids,
            documents=documents,
            metadatas=metadatas,
        )
        logger.info("  + %s (%d chunks)", path.relative_to(self.root), len(chunks))

    def _prune_stale(self, current_files: list[Path]) -> None:
        """Remove chunks from files that no longer exist or were renamed."""
        current_set = {str(p) for p in current_files}
        # Chroma doesn't support listing unique metadata values directly,
        # so we fetch a representative sample.  For large collections you
        # may prefer a secondary SQLite index of file_path -> hash.
        all_ids = self.collection.get()["ids"]
        if not all_ids:
            return

        # Batch fetch metadata to find stale file_paths
        batch_size = 1000
        stale_paths: set[str] = set()

        for i in range(0, len(all_ids), batch_size):
            batch_ids = all_ids[i : i + batch_size]
            result = self.collection.get(ids=batch_ids, include=["metadatas"])
            for meta in result["metadatas"] or []:
                fp = meta.get("file_path")
                if fp and fp not in current_set:
                    stale_paths.add(fp)

        for fp in stale_paths:
            logger.info("  - pruning stale: %s", fp)
            self.collection.delete(where={"file_path": fp})


# ---------------------------------------------------------------------------
# File watcher (optional dependency: watchdog)
# ---------------------------------------------------------------------------

def watch_and_reindex(indexer: CodebaseIndexer, debounce: float = 2.0) -> None:
    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError as exc:
        raise SystemExit("Watch mode requires watchdog.  Install: pip install watchdog") from exc

    timer: threading.Timer | None = None
    lock = threading.Lock()

    def _do_reindex() -> None:
        indexer.incremental_index()

    def _schedule() -> None:
        nonlocal timer
        with lock:
            if timer is not None:
                timer.cancel()
            timer = threading.Timer(debounce, _do_reindex)
            timer.daemon = True
            timer.start()

    class _Handler(FileSystemEventHandler):
        def _relevant(self, path: str) -> bool:
            p = Path(path)
            if p.suffix not in indexer.extensions:
                return False
            try:
                rel = p.relative_to(indexer.root)
            except ValueError:
                return False
            return not any(part in indexer.ignore for part in rel.parts)

        def on_created(self, event):
            if not event.is_directory and self._relevant(event.src_path):
                _schedule()

        def on_modified(self, event):
            if not event.is_directory and self._relevant(event.src_path):
                _schedule()

        def on_deleted(self, event):
            if not event.is_directory and self._relevant(event.src_path):
                _schedule()

        def on_moved(self, event):
            if event.is_directory:
                return
            if self._relevant(event.src_path) or self._relevant(event.dest_path):
                _schedule()

    observer = Observer()
    observer.schedule(_Handler(), str(indexer.root), recursive=True)
    observer.start()

    logger.info("Watching %s (debounce=%.1fs).  Press Ctrl+C to stop.", indexer.root, debounce)
    try:
        while observer.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()
        with lock:
            if timer is not None:
                timer.cancel()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest a codebase into Chroma")
    parser.add_argument("root", type=Path, help="Project root to index")
    parser.add_argument("--db-path", default="./chroma_code_db", help="Chroma persist directory")
    parser.add_argument("--collection", default="code", help="Collection name")
    parser.add_argument("--watch", action="store_true", help="Watch for changes and auto-reindex")
    parser.add_argument("--extensions", default=".py,.md,.yaml,.yml,.json", help="Comma-separated extensions")
    parser.add_argument("--ignore", default=".git,__pycache__,node_modules,venv,.venv", help="Comma-separated ignore dirs")
    args = parser.parse_args(argv)

    if not args.root.exists():
        print(f"Path not found: {args.root}", file=sys.stderr)
        return 1

    client = chromadb.PersistentClient(path=args.db_path)
    indexer = CodebaseIndexer(
        client=client,
        collection_name=args.collection,
        root=args.root,
        extensions=set(args.extensions.split(",")),
        ignore=set(args.ignore.split(",")),
    )

    # First run — full index
    indexer.full_index()

    if args.watch:
        watch_and_reindex(indexer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
