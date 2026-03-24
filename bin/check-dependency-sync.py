#!/usr/bin/env python3
import tomllib
import sys
import re


def normalize(dep: str) -> str:
    dep = dep.split(";")[0].strip()
    return re.split(r"[><=!\s\[]", dep)[0].strip().lower()


with open("pyproject.toml", "rb") as f:
    data = tomllib.load(f)

pyproject_deps = set(
    normalize(dep) for dep in data.get("project", {}).get("dependencies", [])
)

with open("requirements.txt", "r") as f:
    requirements_deps = set(
        normalize(line) for line in f if line.strip() and not line.startswith("#")
    )

missing_from_requirements = pyproject_deps - requirements_deps
missing_from_pyproject = requirements_deps - pyproject_deps

has_error = False

if missing_from_requirements:
    print("In pyproject.toml but missing from requirements.txt:")
    print("\n".join(sorted(missing_from_requirements)))
    has_error = True

if missing_from_pyproject:
    print("In requirements.txt but missing from pyproject.toml:")
    print("\n".join(sorted(missing_from_pyproject)))
    has_error = True

if has_error:
    sys.exit(1)

print("All runtime dependencies are in sync.")
