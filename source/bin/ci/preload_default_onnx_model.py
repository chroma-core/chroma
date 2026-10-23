#!/usr/bin/env python3
"""Preload Chroma's default ONNX embedding model for CI."""

from __future__ import annotations

import hashlib
import os
import tarfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional


MODEL_NAME = "all-MiniLM-L6-v2"
DOWNLOAD_PATH = Path.home() / ".cache" / "chroma" / "onnx_models" / MODEL_NAME
EXTRACTED_FOLDER_NAME = "onnx"
ARCHIVE_FILENAME = "onnx.tar.gz"
MODEL_DOWNLOAD_URL = (
    "https://chroma-onnx-models.s3.amazonaws.com/all-MiniLM-L6-v2/onnx.tar.gz"
)
MODEL_SHA256 = "913d7300ceae3b2dbc2c50d1de4baacab4be7b9380491c27fab7418616a16ec3"
MARKER_FILENAME = ".chroma_model_sha256"
EXPECTED_ONNX_FILES = (
    "config.json",
    "model.onnx",
    "special_tokens_map.json",
    "tokenizer_config.json",
    "tokenizer.json",
    "vocab.txt",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def expected_files_exist(root: Path) -> bool:
    extracted = root / EXTRACTED_FOLDER_NAME
    return all((extracted / filename).exists() for filename in EXPECTED_ONNX_FILES)


def cached_model_is_current(root: Path) -> bool:
    marker = root / MARKER_FILENAME
    return (
        expected_files_exist(root)
        and marker.exists()
        and marker.read_text(encoding="utf-8").strip() == MODEL_SHA256
    )


def download_archive(destination: Path) -> None:
    request = urllib.request.Request(
        MODEL_DOWNLOAD_URL,
        headers={"User-Agent": "chroma-ci-default-onnx-preload"},
    )
    last_error: Optional[BaseException] = None

    for attempt in range(1, 4):
        tmp_path: Optional[Path] = None
        try:
            print(
                f"Downloading {MODEL_NAME} archive to {destination} "
                f"(attempt {attempt}/3)"
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                content_length = response.headers.get("content-length")
                total_bytes = int(content_length) if content_length is not None else 0
                downloaded = 0
                next_progress = 5 * 1024 * 1024

                with NamedTemporaryFile(
                    dir=destination.parent,
                    prefix=f"{ARCHIVE_FILENAME}.",
                    suffix=".tmp",
                    delete=False,
                ) as tmp:
                    tmp_path = Path(tmp.name)
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        tmp.write(chunk)
                        downloaded += len(chunk)
                        if downloaded >= next_progress:
                            if total_bytes:
                                print(
                                    f"Downloaded {downloaded}/{total_bytes} bytes "
                                    f"for {MODEL_NAME}"
                                )
                            else:
                                print(f"Downloaded {downloaded} bytes for {MODEL_NAME}")
                            next_progress += 5 * 1024 * 1024

            actual_sha256 = sha256(tmp_path)
            if actual_sha256 != MODEL_SHA256:
                raise RuntimeError(
                    f"Downloaded archive SHA256 mismatch: got {actual_sha256}, "
                    f"expected {MODEL_SHA256}"
                )

            tmp_path.replace(destination)
            return
        except (OSError, RuntimeError, urllib.error.URLError) as exc:
            last_error = exc
            if tmp_path is not None and tmp_path.exists():
                tmp_path.unlink()
            if attempt < 3:
                sleep_seconds = attempt * 2
                print(
                    f"Download failed for {MODEL_NAME}: {exc}. "
                    f"Retrying in {sleep_seconds}s."
                )
                time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to download {MODEL_NAME}") from last_error


def safe_extract(archive: Path, destination: Path) -> None:
    destination_resolved = destination.resolve()
    with tarfile.open(archive, mode="r:gz") as tar:
        members = tar.getmembers()
        for member in members:
            target = (destination / member.name).resolve()
            try:
                target.relative_to(destination_resolved)
            except ValueError as exc:
                raise RuntimeError(
                    f"Refusing to extract {member.name} outside {destination}"
                ) from exc

            if member.issym() or member.islnk():
                link_target = (target.parent / member.linkname).resolve()
                try:
                    link_target.relative_to(destination_resolved)
                except ValueError as exc:
                    raise RuntimeError(
                        f"Refusing to extract link {member.name} outside {destination}"
                    ) from exc

        tar.extractall(path=destination, members=members)


def main() -> None:
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)

    if cached_model_is_current(DOWNLOAD_PATH):
        print(f"{MODEL_NAME} already preloaded at {DOWNLOAD_PATH}")
        return

    archive = DOWNLOAD_PATH / ARCHIVE_FILENAME
    if archive.exists():
        actual_sha256 = sha256(archive)
        if actual_sha256 != MODEL_SHA256:
            print(f"Removing stale {MODEL_NAME} archive with SHA256 {actual_sha256}")
            archive.unlink()

    if not archive.exists():
        download_archive(archive)

    print(f"Extracting {archive} into {DOWNLOAD_PATH}")
    safe_extract(archive, DOWNLOAD_PATH)

    if not expected_files_exist(DOWNLOAD_PATH):
        missing = [
            filename
            for filename in EXPECTED_ONNX_FILES
            if not (DOWNLOAD_PATH / EXTRACTED_FOLDER_NAME / filename).exists()
        ]
        raise RuntimeError(
            f"Extracted {MODEL_NAME} archive is missing expected files: {missing}"
        )

    marker = DOWNLOAD_PATH / MARKER_FILENAME
    marker.write_text(f"{MODEL_SHA256}{os.linesep}", encoding="utf-8")
    print(f"{MODEL_NAME} preloaded at {DOWNLOAD_PATH}")


if __name__ == "__main__":
    main()
