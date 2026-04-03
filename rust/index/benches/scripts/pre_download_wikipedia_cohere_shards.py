#!/usr/bin/env python3
"""Pre-download all Hugging Face parquet shards for the Cohere Wikipedia dataset."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from tqdm.auto import tqdm

from wikipedia_cohere_dataset import (
    REPO_ID,
    ensure_shard_cached,
    list_dataset_shards,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download every parquet shard for the CohereLabs Wikipedia "
            "multilingual embedding dataset into the Hugging Face cache. "
            "Safe to re-run; already-cached files are skipped quickly."
        )
    )
    p.add_argument(
        "--repo-id",
        default=REPO_ID,
        help=f"Hugging Face dataset repo (default: {REPO_ID})",
    )
    p.add_argument(
        "--max-shards",
        type=int,
        default=None,
        help="If set, only download the first N shards (after sorting). For testing.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("huggingface_hub").setLevel(logging.WARNING)

    if not os.environ.get("HF_TOKEN") and not os.environ.get("HUGGING_FACE_HUB_TOKEN"):
        logging.warning(
            "HF_TOKEN is not set; Hugging Face may apply lower rate limits."
        )

    shards = list_dataset_shards(args.repo_id)
    if args.max_shards is not None:
        shards = shards[: args.max_shards]

    failed: list[str] = []
    for name in tqdm(shards, desc="Shards", unit="file"):
        try:
            ensure_shard_cached(args.repo_id, name, log_cache_hits=False)
        except BaseException as exc:
            logging.exception("Failed on shard %s: %s", name, exc)
            failed.append(name)

    if failed:
        logging.error("Finished with %s failed shards.", len(failed))
        sys.exit(1)
    logging.info("All %s shards are available in the local HF cache.", len(shards))


if __name__ == "__main__":
    main()
