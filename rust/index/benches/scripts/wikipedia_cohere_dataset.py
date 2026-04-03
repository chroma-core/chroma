"""Shared Hugging Face helpers for Cohere Wikipedia embedding parquet shards."""

from __future__ import annotations

import logging
import random
import time
from pathlib import Path
from typing import List

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import HfHubHTTPError, LocalEntryNotFoundError

REPO_ID = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"


def list_dataset_shards(repo_id: str) -> List[str]:
    """List sorted parquet shard paths (e.g. ``ab/0000.parquet``) in the dataset repo."""
    logging.info("Listing dataset shards from %s", repo_id)
    api = HfApi()
    files = api.list_repo_files(repo_id=repo_id, repo_type="dataset")
    shards = [
        path
        for path in files
        if path.endswith(".parquet")
        and path.count("/") == 1
        and Path(path).name.split(".")[0].isdigit()
    ]
    shards.sort()
    if not shards:
        raise RuntimeError(f"No parquet shards found in dataset {repo_id}")
    logging.info("Found %s parquet shards.", len(shards))
    return shards


def _hf_transient(exc: BaseException) -> bool:
    if isinstance(exc, HfHubHTTPError):
        status = getattr(exc, "response", None)
        code = getattr(status, "status_code", None) if status is not None else None
        if code in {408, 429, 500, 502, 503, 504}:
            return True
        msg = str(exc).lower()
        return "429" in msg or "rate limit" in msg or "too many requests" in msg
    return False


def _backoff_sleep(attempt: int, minimum: float = 1.0, maximum: float = 60.0) -> None:
    delay = min(maximum, minimum * (2**attempt))
    delay += random.uniform(0.0, min(1.0, delay / 4.0))
    time.sleep(delay)


def ensure_shard_cached(
    repo_id: str,
    filename: str,
    *,
    max_attempts: int = 10,
    log_cache_hits: bool = False,
) -> str:
    """
    Return the local path to a shard parquet file.

    Uses the Hugging Face cache only if present; otherwise downloads with retries.
    """
    try:
        path = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
            local_files_only=True,
        )
        if log_cache_hits:
            logging.debug("Using cached shard %s -> %s", filename, path)
        return path
    except LocalEntryNotFoundError:
        pass

    for attempt in range(max_attempts):
        try:
            logging.info("Downloading shard from Hugging Face: %s", filename)
            return hf_hub_download(
                repo_id=repo_id,
                filename=filename,
                repo_type="dataset",
            )
        except HfHubHTTPError as exc:
            if not _hf_transient(exc) or attempt == max_attempts - 1:
                raise
            logging.warning(
                "Retrying shard download for %s after error: %s", filename, exc
            )
            _backoff_sleep(attempt)

    raise RuntimeError(f"Failed to download shard {filename}")
