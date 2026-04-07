#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import random
import signal
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
import pyarrow.parquet as pq
from tqdm.auto import tqdm

from wikipedia_cohere_dataset import REPO_ID, ensure_shard_cached, list_dataset_shards

DEFAULT_COLLECTION = "100m_quantized_spann"
DEFAULT_TARGET_COUNT = 100_000_000
DEFAULT_CHUNK_SIZE = 1_000_000
DEFAULT_UPLOAD_BATCH_SIZE = 300
DEFAULT_READ_BATCH_SIZE = 2_000
DEFAULT_THREADS = 10
DEFAULT_QUEUE_DEPTH = 40
DEFAULT_CLOUD_HOST = "api.trychroma.com"
DEFAULT_CLOUD_PORT = 443
DEFAULT_HTTP_TIMEOUT_SECS = 30.0
DEFAULT_PROGRESS_SAVE_INTERVAL_VECTORS = 10_000
DEFAULT_MAX_WRITE_REQUESTS_PER_MINUTE = (
    463.0  # 100M vectors in 12hours. I've seen it do 30k/hr, 500/min
)
DEFAULT_MIN_WRITE_REQUESTS_PER_MINUTE = 30.0
DEFAULT_RATE_LIMIT_BACKOFF_FACTOR = 0.5
DEFAULT_RATE_LIMIT_RECOVERY_FACTOR = 1.05
DEFAULT_RATE_LIMIT_RECOVERY_SUCCESSES = 20

# Progress file lives next to this script so cwd and repo layout do not matter.
_SCRIPT_DIR = Path(__file__).resolve().parent


def default_progress_file_for_collection(collection_name: str) -> Path:
    safe_name = "".join(
        ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in collection_name
    )
    return _SCRIPT_DIR / f"{safe_name}.progress.json"


@dataclass
class Cursor:
    shard_index: int = 0
    row_offset_in_shard: int = 0


@dataclass
class UploadTask:
    chunk_index: int
    start_id: int
    count: int
    ids: List[str]
    embeddings: List[List[float]]


def _merge_intervals(
    intervals: List[Tuple[int, int]],
) -> List[Tuple[int, int]]:
    if not intervals:
        return []
    intervals = sorted(intervals)
    merged: List[List[int]] = [[intervals[0][0], intervals[0][1]]]
    for start, end in intervals[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(a, b) for a, b in merged]


def _contiguous_prefix_end(intervals: Sequence[Tuple[int, int]]) -> int:
    """Largest W such that [0, W) is covered by merged intervals."""
    merged = _merge_intervals(list(intervals))
    w = 0
    for start, end in merged:
        if start > w:
            break
        w = max(w, end)
    return w


def cursor_for_global_row(global_row: int, row_counts: Sequence[int]) -> Cursor:
    """Map global row index (0-based along sorted shards) to shard cursor."""
    cum = 0
    for i, n in enumerate(row_counts):
        n = int(n)
        if global_row < cum + n:
            return Cursor(i, global_row - cum)
        cum += n
    raise RuntimeError(f"global_row {global_row} is past total dataset rows ({cum})")


class ShardRowCounter:
    """Lazy per-shard row counts for mapping global row index -> (shard, offset).

    Avoids scanning all 2707 shards at startup: extends the prefix only as far as the
    current watermark needs. A full cache file (from a prior run or pre-download) loads
    instantly.
    """

    def __init__(self, shards: Sequence[str], repo_id: str, cache_path: Path) -> None:
        self.shards = list(shards)
        self.repo_id = repo_id
        self.cache_path = cache_path
        self._counts: List[int] = []
        self._lock = threading.Lock()

    def load_cache(self) -> None:
        if not self.cache_path.exists():
            return
        try:
            data = json.loads(self.cache_path.read_text())
            if data.get("repo_id") != self.repo_id or data.get("shards") != self.shards:
                return
            self._counts = [int(x) for x in data["row_counts"]]
            if len(self._counts) > len(self.shards):
                self._counts = []
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self._counts = []

    def ensure_through_global_row(self, global_row: int) -> None:
        """Ensure we know enough shard sizes that global row index ``global_row`` is covered."""
        with self._lock:
            while sum(self._counts) <= global_row and len(self._counts) < len(
                self.shards
            ):
                idx = len(self._counts)
                path = ensure_shard_cached(self.repo_id, self.shards[idx])
                n = int(pq.ParquetFile(path).metadata.num_rows)
                self._counts.append(n)
                self._persist_unlocked()
            total = sum(self._counts)
            if global_row >= total and len(self._counts) >= len(self.shards):
                raise RuntimeError(
                    f"global_row {global_row} is past total dataset rows ({total})"
                )

    @property
    def counts(self) -> List[int]:
        return self._counts

    def _persist_unlocked(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "repo_id": self.repo_id,
            "shards": self.shards,
            "row_counts": self._counts,
            "complete": len(self._counts) == len(self.shards),
        }
        self.cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def open_shard_row_counter(
    shards: Sequence[str],
    repo_id: str,
    cache_path: Path,
) -> ShardRowCounter:
    counter = ShardRowCounter(shards, repo_id, cache_path)
    counter.load_cache()
    if counter.counts and len(counter.counts) == len(shards):
        logging.info(
            "Loaded full shard row-count cache (%s shards) from %s",
            len(shards),
            cache_path,
        )
    elif counter.counts:
        logging.info(
            "Loaded partial shard row-count cache (%s / %s shards); will extend lazily.",
            len(counter.counts),
            len(shards),
        )
    else:
        logging.info(
            "No shard row-count cache; will fetch parquet metadata lazily as ingest "
            "progresses (cache: %s).",
            cache_path,
        )
    return counter


class ProgressTracker:
    def __init__(
        self,
        path: Path,
        collection: str,
        target_count: int,
        chunk_size: int,
        repo_id: str,
        shards: Sequence[str],
    ) -> None:
        self.path = path
        self.collection = collection
        self.target_count = target_count
        self.chunk_size = chunk_size
        self.repo_id = repo_id
        self.shards = list(shards)
        self.lock = threading.Lock()
        self.committed_rows = 0
        self.cursor = Cursor()
        self.current_chunk_index = 0
        self._row_counter: Optional[ShardRowCounter] = None
        self._intervals: List[Tuple[int, int]] = []
        self._last_saved_watermark = 0
        self._last_save_time = 0.0

    def set_row_counter(self, counter: ShardRowCounter) -> None:
        self._row_counter = counter

    def load(self) -> None:
        if not self.path.exists():
            self.save()
            return

        data = json.loads(self.path.read_text())
        if data.get("repo_id") != self.repo_id:
            raise ValueError(
                f"Progress file repo_id mismatch: {data.get('repo_id')} != {self.repo_id}"
            )
        if data.get("collection") != self.collection:
            raise ValueError(
                f"Progress file collection mismatch: {data.get('collection')} != {self.collection}"
            )
        if data.get("chunk_size") != self.chunk_size:
            raise ValueError(
                f"Progress file chunk_size mismatch: {data.get('chunk_size')} != {self.chunk_size}"
            )
        self.committed_rows = int(data.get("committed_rows", 0))
        self.cursor = Cursor(**data.get("cursor", {}))
        self.current_chunk_index = self.committed_rows // self.chunk_size
        # Rows [0, committed_rows) are treated as already on the server.
        self._intervals = [(0, self.committed_rows)] if self.committed_rows > 0 else []
        self._last_saved_watermark = self.committed_rows
        self._last_save_time = time.time()

        saved_shards = data.get("shards")
        if saved_shards and saved_shards != self.shards:
            logging.warning(
                "Shard list changed since last run; continuing with latest listing."
            )

    def _payload(self) -> Dict[str, Any]:
        return {
            "repo_id": self.repo_id,
            "collection": self.collection,
            "target_count": self.target_count,
            "chunk_size": self.chunk_size,
            "committed_rows": self.committed_rows,
            "cursor": asdict(self.cursor),
            "shards": self.shards,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def save(self) -> None:
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        tmp_path.write_text(json.dumps(self._payload(), indent=2, sort_keys=True))
        tmp_path.replace(self.path)

    def _save_unlocked(self) -> None:
        self.save()

    def report_upload_done(
        self,
        start_id: int,
        count: int,
        *,
        debounce_vectors: int,
        flush_time_secs: float,
    ) -> None:
        """Record a successful upsert; advance contiguous watermark and maybe persist."""
        end = start_id + count
        with self.lock:
            self._intervals.append((start_id, end))
            self._intervals = _merge_intervals(self._intervals)
            w = _contiguous_prefix_end(self._intervals)
            self.committed_rows = w
            self.current_chunk_index = w // self.chunk_size

        if self._row_counter is not None:
            self._row_counter.ensure_through_global_row(w)

        with self.lock:
            w2 = self.committed_rows
            if self._row_counter is not None:
                self._row_counter.ensure_through_global_row(w2)
                self.cursor = cursor_for_global_row(w2, self._row_counter.counts)
            now = time.time()
            should_save = (
                w2 - self._last_saved_watermark >= debounce_vectors
                or now - self._last_save_time >= flush_time_secs
            )
            if should_save:
                self._save_unlocked()
                self._last_saved_watermark = w2
                self._last_save_time = now

    def force_save(self) -> None:
        """Write progress to disk immediately (e.g. chunk boundary or shutdown)."""
        with self.lock:
            self._save_unlocked()
            self._last_saved_watermark = self.committed_rows
            self._last_save_time = time.time()

    def recompute_cursor_from_watermark(self) -> None:
        """Align cursor with committed_rows (call after set_row_counter)."""
        if self._row_counter is None:
            return
        with self.lock:
            self._row_counter.ensure_through_global_row(self.committed_rows)
            self.cursor = cursor_for_global_row(
                self.committed_rows, self._row_counter.counts
            )


class ChunkStatus:
    def __init__(self, chunk_index: int, target_rows: int) -> None:
        self.chunk_index = chunk_index
        self.target_rows = target_rows
        self.completed_rows = 0
        self.condition = threading.Condition()

    def mark_completed(self, count: int) -> None:
        with self.condition:
            self.completed_rows += count
            self.condition.notify_all()

    def is_done(self) -> bool:
        with self.condition:
            return self.completed_rows >= self.target_rows


class FailureState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.error: Optional[BaseException] = None

    def set(self, error: BaseException) -> None:
        with self.lock:
            if self.error is None:
                self.error = error

    def get(self) -> Optional[BaseException]:
        with self.lock:
            return self.error


class WriteRateLimiter:
    """Shared adaptive limiter across upload threads.

    Chroma Cloud documents concurrent write and batch-size quotas, but does not
    publish a requests-per-minute limit. This limiter keeps the average request
    start rate under a configurable ceiling and adapts downward when Chroma
    responds with write backpressure.
    """

    def __init__(
        self,
        requests_per_minute: float,
        min_requests_per_minute: float = DEFAULT_MIN_WRITE_REQUESTS_PER_MINUTE,
        backoff_factor: float = DEFAULT_RATE_LIMIT_BACKOFF_FACTOR,
        recovery_factor: float = DEFAULT_RATE_LIMIT_RECOVERY_FACTOR,
        recovery_successes: int = DEFAULT_RATE_LIMIT_RECOVERY_SUCCESSES,
    ) -> None:
        self.requests_per_minute = requests_per_minute
        self.current_requests_per_minute = requests_per_minute
        self.min_requests_per_minute = min(min_requests_per_minute, requests_per_minute)
        self.backoff_factor = backoff_factor
        self.recovery_factor = recovery_factor
        self.recovery_successes = recovery_successes
        self._interval_secs = 60.0 / requests_per_minute
        self._next_allowed_at = time.monotonic()
        self._lock = threading.Lock()
        self._success_streak = 0

    def _set_rate_unlocked(self, requests_per_minute: float) -> None:
        self.current_requests_per_minute = requests_per_minute
        self._interval_secs = 60.0 / requests_per_minute
        self._next_allowed_at = (
            max(self._next_allowed_at, time.monotonic()) + self._interval_secs
        )

    def acquire(
        self,
        stop_event: threading.Event,
        progress_bars: Optional["ProgressBars"] = None,
    ) -> None:
        while True:
            if stop_event.is_set():
                raise KeyboardInterrupt()

            with self._lock:
                now = time.monotonic()
                wait_secs = self._next_allowed_at - now
                if wait_secs <= 0:
                    self._next_allowed_at = (
                        max(self._next_allowed_at, now) + self._interval_secs
                    )
                    return

            if progress_bars is not None and wait_secs >= 1.0:
                progress_bars.set_upload_status(
                    f"throttling for {wait_secs:.1f}s ({self.current_requests_per_minute:.0f} req/min)"
                )
            time.sleep(min(wait_secs, 0.25))

    def report_rate_limited(
        self,
        *,
        is_compaction_backpressure: bool,
        progress_bars: Optional["ProgressBars"] = None,
    ) -> float:
        with self._lock:
            factor = self.backoff_factor if is_compaction_backpressure else 0.8
            new_rate = max(
                self.min_requests_per_minute,
                self.current_requests_per_minute * factor,
            )
            self._success_streak = 0
            if new_rate != self.current_requests_per_minute:
                self._set_rate_unlocked(new_rate)
            if progress_bars is not None:
                progress_bars.set_upload_status(
                    f"adaptive rate {self.current_requests_per_minute:.0f} req/min"
                )
            return self.current_requests_per_minute

    def report_success(self, progress_bars: Optional["ProgressBars"] = None) -> float:
        with self._lock:
            if self.current_requests_per_minute >= self.requests_per_minute:
                self._success_streak = 0
                return self.current_requests_per_minute

            self._success_streak += 1
            if self._success_streak < self.recovery_successes:
                return self.current_requests_per_minute

            self._success_streak = 0
            new_rate = min(
                self.requests_per_minute,
                self.current_requests_per_minute * self.recovery_factor,
            )
            if new_rate != self.current_requests_per_minute:
                self.current_requests_per_minute = new_rate
                self._interval_secs = 60.0 / new_rate
            if progress_bars is not None:
                progress_bars.set_upload_status(
                    f"recovering to {self.current_requests_per_minute:.0f} req/min"
                )
            return self.current_requests_per_minute


class ProgressBars:
    def __init__(
        self,
        total_vectors: int,
        initial_vectors: int,
        *,
        download_desc: str,
        upload_desc: str,
    ) -> None:
        self.lock = threading.Lock()
        self._download_status = ""
        self._upload_status = ""
        self._last_download_status_at = 0.0
        self._last_upload_status_at = 0.0
        self.download = tqdm(
            total=total_vectors,
            initial=initial_vectors,
            desc=download_desc,
            unit="vec",
            dynamic_ncols=True,
        )
        self.upload = tqdm(
            total=total_vectors,
            initial=initial_vectors,
            desc=upload_desc,
            unit="vec",
            dynamic_ncols=True,
        )

    def advance_download(self, count: int) -> None:
        with self.lock:
            self.download.update(count)

    def advance_upload(self, count: int) -> None:
        with self.lock:
            self.upload.update(count)

    def set_download_status(self, status: str, min_interval_secs: float = 0.5) -> None:
        with self.lock:
            now = time.monotonic()
            if (
                status == self._download_status
                or now - self._last_download_status_at < min_interval_secs
            ):
                return
            self._download_status = status
            self._last_download_status_at = now
            self.download.set_postfix_str(status)

    def set_upload_status(self, status: str, min_interval_secs: float = 1.0) -> None:
        with self.lock:
            now = time.monotonic()
            if (
                status == self._upload_status
                or now - self._last_upload_status_at < min_interval_secs
            ):
                return
            self._upload_status = status
            self._last_upload_status_at = now
            self.upload.set_postfix_str(status)

    def close(self) -> None:
        with self.lock:
            self.download.close()
            self.upload.close()


class ChromaCloudCollection:
    def __init__(
        self,
        api_key: str,
        tenant: str,
        database: str,
        collection_name: str,
        cloud_host: str,
        cloud_port: int,
        enable_ssl: bool,
        http_timeout_secs: float,
    ) -> None:
        scheme = "https" if enable_ssl else "http"
        self.base_url = f"{scheme}://{cloud_host}:{cloud_port}/api/v2"
        self.tenant = tenant
        self.database = database
        self.collection_name = collection_name
        self.http = httpx.Client(
            headers={
                "Content-Type": "application/json",
                "X-Chroma-Token": api_key,
                "User-Agent": "chroma-ingest-script/1.0",
            },
            timeout=httpx.Timeout(http_timeout_secs),
        )
        self.collection_id = self._fetch_collection_id()

    def _fetch_collection_id(self) -> str:
        for attempt in range(10):
            try:
                logging.info(
                    "Looking up collection %s in tenant=%s database=%s",
                    self.collection_name,
                    self.tenant,
                    self.database,
                )
                response = self.http.get(
                    f"{self.base_url}/tenants/{self.tenant}/databases/{self.database}/collections/{self.collection_name}"
                )
                response.raise_for_status()
                payload = response.json()
                collection_id = payload.get("id")
                if not collection_id:
                    raise RuntimeError(
                        f"Collection lookup for {self.collection_name} did not return an id."
                    )
                return collection_id
            except BaseException as exc:
                if not is_transient(exc) or attempt == 9:
                    raise
                logging.warning(
                    "Retrying collection lookup for %s after error: %s",
                    self.collection_name,
                    exc,
                )
                backoff_sleep(attempt)
        raise RuntimeError(
            f"Failed to resolve collection id for {self.collection_name}"
        )

    def upsert(self, ids: List[str], embeddings: List[List[float]]) -> None:
        response = self.http.post(
            f"{self.base_url}/tenants/{self.tenant}/databases/{self.database}/collections/{self.collection_id}/upsert",
            json={"ids": ids, "embeddings": embeddings},
        )
        if response.is_error:
            detail = response.text
            if len(detail) > 8000:
                detail = detail[:8000] + "..."
            logging.error(
                "Chroma upsert failed: HTTP %s for %s records (first id=%s): %s",
                response.status_code,
                len(ids),
                ids[0] if ids else "(none)",
                detail,
            )
        response.raise_for_status()

    def close(self) -> None:
        self.http.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest 100M unique Wikipedia embeddings into Chroma Cloud."
    )
    parser.add_argument("--api-key", default=os.environ.get("CHROMA_API_KEY"))
    parser.add_argument("--tenant", default=os.environ.get("CHROMA_TENANT"))
    parser.add_argument("--database", default=os.environ.get("CHROMA_DATABASE"))
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--target-count", type=int, default=DEFAULT_TARGET_COUNT)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument(
        "--upload-batch-size", type=int, default=DEFAULT_UPLOAD_BATCH_SIZE
    )
    parser.add_argument("--read-batch-size", type=int, default=DEFAULT_READ_BATCH_SIZE)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--queue-depth", type=int, default=DEFAULT_QUEUE_DEPTH)
    parser.add_argument("--cloud-host", default=DEFAULT_CLOUD_HOST)
    parser.add_argument("--cloud-port", type=int, default=DEFAULT_CLOUD_PORT)
    parser.add_argument("--disable-ssl", action="store_true")
    parser.add_argument(
        "--http-timeout-secs",
        type=float,
        default=DEFAULT_HTTP_TIMEOUT_SECS,
        help="Per-request timeout for Chroma Cloud HTTP calls.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate dataset iteration and collection lookup without uploading.",
    )
    parser.add_argument(
        "--dry-run-vectors",
        type=int,
        default=10_000,
        help="Number of vectors to scan in dry-run mode.",
    )
    parser.add_argument(
        "--skip-chroma-check",
        action="store_true",
        help="Skip Chroma Cloud collection lookup during dry-run validation.",
    )
    parser.add_argument(
        "--progress-file",
        default=None,
    )
    parser.add_argument(
        "--progress-save-interval-vectors",
        type=int,
        default=DEFAULT_PROGRESS_SAVE_INTERVAL_VECTORS,
        help=(
            "Persist after the contiguous upload watermark advances by at least this many "
            "rows (capped duplicate work on restart; default 10000)."
        ),
    )
    parser.add_argument(
        "--progress-flush-time-secs",
        type=float,
        default=120.0,
        help="Also persist progress at least this often even if the interval is not reached.",
    )
    parser.add_argument(
        "--max-write-requests-per-minute",
        type=float,
        default=DEFAULT_MAX_WRITE_REQUESTS_PER_MINUTE,
        help=(
            "Client-side global write pacing across all upload threads. "
            "Use 0 to disable. Default: 463 req/min."
        ),
    )
    parser.add_argument(
        "--min-write-requests-per-minute",
        type=float,
        default=DEFAULT_MIN_WRITE_REQUESTS_PER_MINUTE,
        help="Lower bound for adaptive write pacing. Default: 30 req/min.",
    )
    parser.add_argument(
        "--id-prefix",
        default="wiki-",
        help="Prefix for generated auto-incrementing record ids.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()
    if args.progress_file is None:
        args.progress_file = str(default_progress_file_for_collection(args.collection))
    return args


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("huggingface_hub").setLevel(logging.WARNING)


def require_args(args: argparse.Namespace) -> None:
    missing = []
    needs_chroma = not (args.dry_run and args.skip_chroma_check)
    if needs_chroma and not args.api_key:
        missing.append("api_key / CHROMA_API_KEY")
    if needs_chroma and not args.tenant:
        missing.append("tenant / CHROMA_TENANT")
    if needs_chroma and not args.database:
        missing.append("database / CHROMA_DATABASE")
    if missing:
        raise ValueError(f"Missing required credentials: {', '.join(missing)}")


def is_rate_limited(exc: BaseException) -> bool:
    message = str(exc).lower()
    status_code = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    if status_code is None and response is not None:
        status_code = getattr(response, "status_code", None)
    return bool(
        status_code == 429
        or "429" in message
        or "rate limit" in message
        or "too many requests" in message
    )


def is_transient(exc: BaseException) -> bool:
    if is_rate_limited(exc):
        return True
    if isinstance(exc, (httpx.TransportError, httpx.TimeoutException)):
        return True
    status_code = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    if status_code is None and response is not None:
        status_code = getattr(response, "status_code", None)
    return status_code in {408, 500, 502, 503, 504}


def is_compaction_backpressure(exc: BaseException) -> bool:
    return "log needs compaction" in str(exc).lower()


def backoff_sleep(attempt: int, minimum: float = 1.0, maximum: float = 60.0) -> float:
    delay = min(maximum, minimum * (2**attempt))
    delay += random.uniform(0.0, min(1.0, delay / 4.0))
    time.sleep(delay)
    return delay


def interruptible_sleep(stop_event: threading.Event, delay_secs: float) -> None:
    end = time.monotonic() + delay_secs
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        remaining = end - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.25))


def create_collection(args: argparse.Namespace) -> ChromaCloudCollection:
    logging.info(
        "Creating Chroma Cloud client for %s/%s/%s",
        args.tenant,
        args.database,
        args.collection,
    )
    return ChromaCloudCollection(
        api_key=args.api_key,
        tenant=args.tenant,
        database=args.database,
        collection_name=args.collection,
        cloud_host=args.cloud_host,
        cloud_port=args.cloud_port,
        enable_ssl=not args.disable_ssl,
        http_timeout_secs=args.http_timeout_secs,
    )


def upload_with_backoff(
    collection: Any,
    task: UploadTask,
    stop_event: threading.Event,
    rate_limiter: Optional[WriteRateLimiter],
    progress_bars: ProgressBars,
    max_attempts: int = 12,
) -> None:
    transient_attempt = 0
    rate_limit_attempt = 0
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        try:
            if rate_limiter is not None:
                rate_limiter.acquire(stop_event, progress_bars)
            collection.upsert(ids=task.ids, embeddings=task.embeddings)
            if rate_limiter is not None:
                rate_limiter.report_success(progress_bars)
            return
        except BaseException as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            if not is_transient(exc):
                raise

            if is_rate_limited(exc):
                rate_limit_attempt += 1
                transient_attempt = 0
                compaction_backpressure = is_compaction_backpressure(exc)
                minimum = 5.0 if compaction_backpressure else 2.0
                maximum = 600.0 if compaction_backpressure else 240.0
                adaptive_rate = None
                if rate_limiter is not None:
                    adaptive_rate = rate_limiter.report_rate_limited(
                        is_compaction_backpressure=compaction_backpressure,
                        progress_bars=progress_bars,
                    )
                delay = min(maximum, minimum * (2 ** min(rate_limit_attempt - 1, 10)))
                delay += random.uniform(0.0, min(1.0, delay / 4.0))
                progress_bars.set_upload_status(
                    f"429 backoff {delay:.1f}s at {adaptive_rate:.0f} req/min for ids {task.start_id}-{task.start_id + task.count - 1}"
                    if adaptive_rate is not None
                    else f"429 backoff {delay:.1f}s for ids {task.start_id}-{task.start_id + task.count - 1}"
                )
                logging.warning(
                    "Rate limited on id range %s..%s; retrying in %.1fs (attempt %s, adaptive rate %.1f req/min): %s",
                    task.start_id,
                    task.start_id + task.count - 1,
                    delay,
                    rate_limit_attempt,
                    adaptive_rate if adaptive_rate is not None else -1.0,
                    exc,
                )
                interruptible_sleep(stop_event, delay)
                continue

            if transient_attempt >= max_attempts:
                raise

            transient_attempt += 1
            delay = min(60.0, 1.0 * (2 ** min(transient_attempt - 1, 10)))
            delay += random.uniform(0.0, min(1.0, delay / 4.0))
            logging.warning(
                "Retrying transient upload error for id range %s..%s in %.1fs (attempt %s/%s): %s",
                task.start_id,
                task.start_id + task.count - 1,
                delay,
                transient_attempt,
                max_attempts,
                exc,
            )
            interruptible_sleep(stop_event, delay)


def make_ids(prefix: str, start_id: int, count: int) -> List[str]:
    return [f"{prefix}{value}" for value in range(start_id, start_id + count)]


def worker_loop(
    args: argparse.Namespace,
    task_queue: "queue.Queue[Optional[UploadTask]]",
    chunk_statuses: Dict[int, ChunkStatus],
    stop_event: threading.Event,
    failures: FailureState,
    progress_bars: ProgressBars,
    progress: "ProgressTracker",
    rate_limiter: Optional[WriteRateLimiter],
) -> None:
    collection = create_collection(args)
    progress_bars.set_upload_status("starting workers")

    while True:
        task = task_queue.get()
        try:
            if task is None:
                return
            upload_with_backoff(
                collection,
                task,
                stop_event,
                rate_limiter,
                progress_bars,
            )
            progress.report_upload_done(
                task.start_id,
                task.count,
                debounce_vectors=args.progress_save_interval_vectors,
                flush_time_secs=args.progress_flush_time_secs,
            )
            chunk_statuses[task.chunk_index].mark_completed(task.count)
            progress_bars.advance_upload(task.count)
        except BaseException as exc:
            failures.set(exc)
            stop_event.set()
            logging.exception("Worker failed")
            return
        finally:
            task_queue.task_done()


def enqueue_chunk(
    args: argparse.Namespace,
    shards: Sequence[str],
    chunk_index: int,
    chunk_start_row: int,
    cursor: Cursor,
    rows_needed: int,
    task_queue: "queue.Queue[Optional[UploadTask]]",
    stop_event: threading.Event,
    progress_bars: ProgressBars,
) -> Cursor:
    rows_remaining = rows_needed
    next_id = chunk_start_row
    current_shard_index = cursor.shard_index
    current_offset = cursor.row_offset_in_shard

    while rows_remaining > 0:
        if stop_event.is_set():
            raise RuntimeError("Stopping before finishing current chunk.")
        if current_shard_index >= len(shards):
            raise RuntimeError(
                "Ran out of dataset shards before reaching target count."
            )

        shard_name = shards[current_shard_index]
        progress_bars.set_download_status(
            f"chunk {chunk_index} shard {current_shard_index + 1}/{len(shards)} {shard_name}"
        )
        local_path = ensure_shard_cached(REPO_ID, shard_name)
        parquet = pq.ParquetFile(local_path)
        rows_in_shard = parquet.metadata.num_rows
        row_in_shard = 0

        for record_batch in parquet.iter_batches(
            columns=["emb"],
            batch_size=args.read_batch_size,
        ):
            if row_in_shard >= rows_in_shard or rows_remaining == 0:
                break

            embeddings = record_batch.column(0).to_pylist()
            batch_row_start = row_in_shard
            batch_row_end = batch_row_start + len(embeddings)
            row_in_shard = batch_row_end

            if batch_row_end <= current_offset:
                continue

            if current_offset > batch_row_start:
                slice_start = current_offset - batch_row_start
                embeddings = embeddings[slice_start:]
                batch_row_start = current_offset

            offset = 0
            while offset < len(embeddings) and rows_remaining > 0:
                batch_count = min(
                    args.upload_batch_size,
                    rows_remaining,
                    len(embeddings) - offset,
                )
                payload = embeddings[offset : offset + batch_count]
                task_queue.put(
                    UploadTask(
                        chunk_index=chunk_index,
                        start_id=next_id,
                        count=batch_count,
                        ids=make_ids(args.id_prefix, next_id, batch_count),
                        embeddings=payload,
                    )
                )
                progress_bars.advance_download(batch_count)
                next_id += batch_count
                rows_remaining -= batch_count
                offset += batch_count
                current_offset = batch_row_start + offset

                if current_offset == rows_in_shard:
                    if rows_remaining == 0:
                        return Cursor(current_shard_index + 1, 0)
                    current_shard_index += 1
                    current_offset = 0
                    break

            if rows_remaining == 0:
                return Cursor(current_shard_index, current_offset)

            if current_offset == 0:
                break

        if current_offset == 0:
            continue

        if current_offset >= rows_in_shard:
            current_shard_index += 1
            current_offset = 0
            continue

        raise RuntimeError(
            f"Stopped mid-shard without finishing chunk for {shard_name} at row {current_offset}."
        )

    return Cursor(current_shard_index, current_offset)


def validate_shard_iteration(
    args: argparse.Namespace, shards: Sequence[str], cursor: Cursor
) -> None:
    if cursor.shard_index >= len(shards):
        raise RuntimeError("Progress cursor is past the end of the shard list.")
    if cursor.row_offset_in_shard < 0:
        raise RuntimeError("Progress cursor row offset is negative.")


def install_signal_handlers(stop_event: Optional[threading.Event] = None) -> None:
    def _handler(signum: int, _frame: Any) -> None:
        logging.warning("Received signal %s; exiting.", signum)
        if stop_event is not None:
            stop_event.set()
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def run_dry_run(
    args: argparse.Namespace, shards: Sequence[str], cursor: Cursor
) -> None:
    progress_bars = ProgressBars(
        total_vectors=args.dry_run_vectors,
        initial_vectors=0,
        download_desc="Dry-run scan",
        upload_desc="Dry-run upload",
    )
    progress_bars.upload.close()
    collection: Optional[ChromaCloudCollection] = None
    if args.skip_chroma_check:
        progress_bars.set_download_status("skipping Chroma check")
    else:
        progress_bars.set_download_status("checking Chroma collection")
        collection = create_collection(args)
    scanned = 0
    current_shard_index = cursor.shard_index
    current_offset = cursor.row_offset_in_shard
    first_embedding_dim: Optional[int] = None
    shards_visited = 0

    try:
        if collection is not None:
            logging.info(
                "Dry run: collection_id=%s target_scan=%s starting_cursor=%s:%s",
                collection.collection_id,
                args.dry_run_vectors,
                current_shard_index,
                current_offset,
            )
        else:
            logging.info(
                "Dry run without Chroma check: target_scan=%s starting_cursor=%s:%s",
                args.dry_run_vectors,
                current_shard_index,
                current_offset,
            )

        while scanned < args.dry_run_vectors and current_shard_index < len(shards):
            shard_name = shards[current_shard_index]
            progress_bars.set_download_status(
                f"shard {current_shard_index + 1}/{len(shards)} {shard_name}"
            )
            local_path = ensure_shard_cached(REPO_ID, shard_name)
            parquet = pq.ParquetFile(local_path)
            row_in_shard = 0

            for record_batch in parquet.iter_batches(
                columns=["emb"],
                batch_size=args.read_batch_size,
            ):
                embeddings = record_batch.column(0).to_pylist()
                batch_row_start = row_in_shard
                batch_row_end = batch_row_start + len(embeddings)
                row_in_shard = batch_row_end

                if batch_row_end <= current_offset:
                    continue

                if current_offset > batch_row_start:
                    embeddings = embeddings[current_offset - batch_row_start :]

                for embedding in embeddings:
                    if embedding is None:
                        raise RuntimeError(
                            f"Encountered null embedding in shard {shard_name}."
                        )
                    if first_embedding_dim is None:
                        first_embedding_dim = len(embedding)
                    elif len(embedding) != first_embedding_dim:
                        raise RuntimeError(
                            f"Inconsistent embedding dimension in shard {shard_name}: "
                            f"{len(embedding)} != {first_embedding_dim}"
                        )
                    scanned += 1
                    progress_bars.advance_download(1)
                    if scanned >= args.dry_run_vectors:
                        break

                if scanned >= args.dry_run_vectors:
                    break

            shards_visited += 1
            logging.info("Dry run scanned shard %s cumulative=%s", shard_name, scanned)
            current_shard_index += 1
            current_offset = 0

        logging.info(
            "Dry run complete: scanned=%s dimension=%s shards_visited=%s",
            scanned,
            first_embedding_dim,
            shards_visited,
        )
    finally:
        progress_bars.close()
        if collection is not None:
            collection.close()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    require_args(args)
    install_signal_handlers()

    shards = list_dataset_shards(REPO_ID)
    progress = ProgressTracker(
        path=Path(args.progress_file),
        collection=args.collection,
        target_count=args.target_count,
        chunk_size=args.chunk_size,
        repo_id=REPO_ID,
        shards=shards,
    )
    progress.load()
    shard_row_cache = Path(args.progress_file).with_suffix(".shard_row_counts.json")
    if not args.dry_run:
        progress.set_row_counter(
            open_shard_row_counter(shards, REPO_ID, shard_row_cache)
        )
        progress.recompute_cursor_from_watermark()

    validate_shard_iteration(args, shards, progress.cursor)

    if progress.committed_rows >= args.target_count:
        logging.info(
            "Target already reached according to progress file: %s rows committed.",
            progress.committed_rows,
        )
        return

    if args.dry_run:
        try:
            run_dry_run(args, shards, progress.cursor)
        except KeyboardInterrupt:
            logging.warning("Interrupted by user.")
        return

    task_queue: "queue.Queue[Optional[UploadTask]]" = queue.Queue(
        maxsize=args.queue_depth
    )
    stop_event = threading.Event()
    failures = FailureState()
    install_signal_handlers(stop_event)
    progress_bars = ProgressBars(
        total_vectors=args.target_count,
        initial_vectors=progress.committed_rows,
        download_desc="Download/read",
        upload_desc="Upload",
    )
    rate_limiter = (
        WriteRateLimiter(
            args.max_write_requests_per_minute,
            min_requests_per_minute=args.min_write_requests_per_minute,
        )
        if args.max_write_requests_per_minute > 0
        else None
    )
    if rate_limiter is not None:
        logging.info(
            "Client-side adaptive write pacing enabled at %.1f requests/minute (min %.1f).",
            args.max_write_requests_per_minute,
            args.min_write_requests_per_minute,
        )
    else:
        logging.info("Client-side write pacing disabled.")

    chunk_statuses: Dict[int, ChunkStatus] = {}
    workers = [
        threading.Thread(
            target=worker_loop,
            name=f"uploader-{idx}",
            args=(
                args,
                task_queue,
                chunk_statuses,
                stop_event,
                failures,
                progress_bars,
                progress,
                rate_limiter,
            ),
            daemon=True,
        )
        for idx in range(args.threads)
    ]
    for worker in workers:
        worker.start()

    cursor = Cursor(progress.cursor.shard_index, progress.cursor.row_offset_in_shard)

    logging.info(
        "Starting ingest from row %s into collection %s using %s shards.",
        progress.committed_rows,
        args.collection,
        len(shards),
    )

    interrupted_by_user = False
    try:
        while True:
            committed_rows = progress.committed_rows
            if committed_rows >= args.target_count or stop_event.is_set():
                break
            chunk_base = (committed_rows // args.chunk_size) * args.chunk_size
            offset_in_chunk = committed_rows - chunk_base
            space_in_chunk = args.chunk_size - offset_in_chunk
            rows_in_chunk = min(space_in_chunk, args.target_count - committed_rows)
            chunk_index = committed_rows // args.chunk_size
            chunk_status = ChunkStatus(
                chunk_index=chunk_index, target_rows=rows_in_chunk
            )
            chunk_statuses[chunk_index] = chunk_status

            logging.info(
                "Queueing chunk %s: rows %s..%s (%s rows in this segment)",
                chunk_index,
                committed_rows,
                committed_rows + rows_in_chunk - 1,
                rows_in_chunk,
            )
            enqueue_chunk(
                args=args,
                shards=shards,
                chunk_index=chunk_index,
                chunk_start_row=committed_rows,
                cursor=cursor,
                rows_needed=rows_in_chunk,
                task_queue=task_queue,
                stop_event=stop_event,
                progress_bars=progress_bars,
            )
            logging.info("Waiting for chunk %s uploads to finish.", chunk_index)
            while not chunk_status.is_done():
                failure = failures.get()
                if failure is not None:
                    raise failure
                if stop_event.is_set():
                    raise RuntimeError("Stopped before current chunk finished.")
                time.sleep(1.0)
            committed_rows = progress.committed_rows
            cursor = Cursor(
                progress.cursor.shard_index, progress.cursor.row_offset_in_shard
            )
            progress.force_save()
            logging.info(
                "Finished segment; progress saved. %s / %s rows. cursor=%s:%s",
                committed_rows,
                args.target_count,
                progress.cursor.shard_index,
                progress.cursor.row_offset_in_shard,
            )
            del chunk_statuses[chunk_index]

            failure = failures.get()
            if failure is not None:
                raise failure

        if stop_event.is_set():
            failure = failures.get()
            if failure is not None:
                raise failure
            logging.warning("Stopped before reaching target.")
    except KeyboardInterrupt:
        interrupted_by_user = True
        logging.warning("Interrupted by user.")
    finally:
        stop_event.set()
        progress.force_save()
        if not interrupted_by_user:
            for _ in workers:
                task_queue.put(None)
            task_queue.join()
            for worker in workers:
                worker.join(timeout=5.0)
        else:
            logging.warning("Skipping worker drain due to user interrupt.")
        progress.force_save()
        progress_bars.close()

    logging.info("Ingest finished at %s committed rows.", progress.committed_rows)


if __name__ == "__main__":
    main()
