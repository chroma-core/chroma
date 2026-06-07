#!/usr/bin/env python3
"""Check that requirements.txt is in sync with pyproject.toml dependencies.

Usage:
    python bin/ci/check-requirements-sync.py          # checks root
    python bin/ci/check-requirements-sync.py clients/python  # checks thin client
"""

import os
import re
import sys
import tomllib


def parse_pyproject_deps(pyproject_path):
    """Extract dependency specs from pyproject.toml [project] dependencies."""
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    raw_deps = data["project"].get("dependencies", [])
    deps = {}
    for dep in raw_deps:
        dep = dep.strip()
        # Remove environment markers after ;
        dep = dep.split(";")[0].strip()
        # Parse name + version spec; handle extras like [standard] and complex constraints
        m = re.match(r"^([\w\-_.\[\]]+)\s*([>=<!~]+\s*\S+(?:\s*,\s*[>=<!]+\s*\S+)*)?", dep)
        if m:
            name = m.group(1).lower()
            spec = m.group(2)
            spec = "".join(spec.split()) if spec else ""
            deps[name] = spec
    return deps


def parse_requirements(req_path):
    """Extract dependency specs from a requirements.txt file."""
    deps = {}
    with open(req_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("--"):
                continue
            # Remove environment markers
            line = line.split(";")[0].strip()
            m = re.match(r"^([\w\-_.\[\]]+)\s*([>=<!~]+\s*\S+(?:\s*,\s*[>=<!]+\s*\S+)*)?", line)
            if m:
                name = m.group(1).lower()
                spec = m.group(2)
                spec = "".join(spec.split()) if spec else ""
                deps[name] = spec
    return deps

def main():
    if len(sys.argv) > 1:
        subdir = sys.argv[1].strip("/")
    else:
        subdir = ""

    repo_root = os.path.join(os.path.dirname(__file__), "..", "..")
    pyproject = os.path.join(repo_root, subdir, "pyproject.toml")
    req_txt = os.path.join(repo_root, subdir, "requirements.txt")

    if not os.path.exists(pyproject):
        print(f"No pyproject.toml found at {pyproject}")
        sys.exit(0)
    if not os.path.exists(req_txt):
        print(f"No requirements.txt found at {req_txt}")
        sys.exit(0)

    pyproject_deps = parse_pyproject_deps(pyproject)
    req_deps = parse_requirements(req_txt)

    # Build-time only deps in pyproject.toml that don't need to be in requirements.txt
    build_only = {"build", "setuptools", "setuptools-scm", "wheel", "maturin"}

    errors = []

    # Check each pyproject dep is in requirements.txt (unless build-only)
    for name, spec in pyproject_deps.items():
        if name in build_only:
            continue
        if name not in req_deps:
            errors.append(f"Missing: {name}{spec or ''} is in pyproject.toml but not in requirements.txt")
        elif req_deps[name] != spec:
            errors.append(
                f"Version mismatch: {name}: pyproject.toml has {spec or '(any)'}, "
                f"requirements.txt has {req_deps[name] or '(any)'}"
            )

    # Check each requirements.txt entry is in pyproject.toml
    for name, spec in req_deps.items():
        if name not in pyproject_deps:
            errors.append(f"Extra: {name}{spec or ''} is in requirements.txt but not in pyproject.toml")

    if errors:
        print(f"ERROR: Dependency drift detected in {subdir or 'root'}:" if subdir else "ERROR: Dependency drift detected:")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)
    else:
        label = subdir if subdir else "root"
        print(f"OK: {label} requirements.txt is in sync with pyproject.toml")


if __name__ == "__main__":
    main()
