#!/usr/bin/env python3
"""Check storage and WAL dependencies without workspace feature unification.

Run with `python3 rust/wal3/tests/check_standalone_dependencies.py`.
The temporary consumers resolve independently of Chroma's workspace lockfile.
"""

import argparse
import json
import pathlib
import subprocess
import tempfile


def check_graph(graph, profile):
    packages = {package["id"]: package for package in graph["packages"]}
    found = set()
    errors = []
    for node in graph["resolve"]["nodes"]:
        package = packages[node["id"]]
        name = package["name"]
        features = set(node["features"])
        found.add(name)
        if name in ("aws-sdk-s3", "aws-sigv4") and "sigv4a" in features:
            errors.append(f"{name}: optional SigV4a signing is enabled")
        if name == "aws-sdk-s3" and "default-https-client" not in features:
            errors.append("S3's modern HTTPS connector is missing")
        if name == "aws-config" and not {
            "sso", "credentials-process", "default-https-client"
        }.issubset(features):
            errors.append("AWS credential providers or modern HTTPS connector are missing")
        if name == "parquet" and profile == "wal3":
            if "zstd" in features:
                errors.append("standalone WAL enables Parquet's optional zstd codec")
            if not {"arrow", "async", "snap"}.issubset(features):
                errors.append("WAL's Arrow, async, or Snappy support is missing")
        if name == "crypto-bigint":
            version = package["version"].split(".")
            if not all(part.isdecimal() for part in version) or tuple(
                map(int, version)
            ) < (0, 7, 6):
                errors.append(f"affected crypto-bigint {package['version']} is present")
        if name in ("zstd", "zstd-safe"):
            errors.append(f"unexpected {name} dependency in standalone {profile}")
    required = {"aws-sdk-s3", "aws-config", "aws-sigv4"}
    if profile == "wal3":
        required.add("parquet")
    if not required.issubset(found):
        errors.append(f"expected dependencies missing: {sorted(required - found)}")
    if errors:
        raise ValueError("; ".join(errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[3]
    )
    parser.add_argument("--profile", choices=["storage", "wal3", "all"], default="all")
    args = parser.parse_args()
    profiles = ["storage", "wal3"] if args.profile == "all" else [args.profile]
    for profile in profiles:
        package = "chroma-storage" if profile == "storage" else "wal3"
        with tempfile.TemporaryDirectory(prefix="chroma-dependency-consumer-") as directory:
            consumer = pathlib.Path(directory)
            (consumer / "src").mkdir()
            (consumer / "src/lib.rs").write_text("")
            dependency = json.dumps(str(args.root.resolve() / "rust" / profile))
            (consumer / "Cargo.toml").write_text(
                '[package]\nname="dependency-consumer"\nversion="0.0.0"\nedition="2021"\n'
                '[workspace]\n[dependencies]\n'
                + package + "={path=" + dependency + "}\n"
            )
            result = subprocess.run(
                ["cargo", "metadata", "--format-version", "1"],
                cwd=consumer, check=True, capture_output=True, text=True,
            )
            check_graph(json.loads(result.stdout), profile)
        print(f"{profile}: regional S3, modern HTTPS, and expected codecs verified", flush=True)


if __name__ == "__main__":
    main()
