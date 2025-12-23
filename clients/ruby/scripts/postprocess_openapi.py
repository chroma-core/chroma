#!/usr/bin/env python3

import pathlib
import shutil
import sys


def remove_generated_artifacts(root: pathlib.Path) -> None:
    entries = (
        ".gitlab-ci.yml",
        ".travis.yml",
        "git_push.sh",
        ".openapi-generator",
        ".openapi-generator-ignore",
        ".gitignore",
        ".rspec",
        ".rubocop.yml",
        "Gemfile",
        "Rakefile",
        "README.md",
        "chromadb.gemspec",
        "docs",
        "spec",
    )
    for entry in entries:
        path = root / entry
        if not path.exists():
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def override_hash_map(models_dir: pathlib.Path) -> None:
    hash_map_path = models_dir / "hash_map.rb"
    if not hash_map_path.exists():
        return
    hash_map_path.write_text(
        "# frozen_string_literal: true\n\nmodule Chroma\n  module Openapi\n    class HashMap < Hash\n    end\n  end\nend\n",
        encoding="utf-8",
    )


def main(output_dir: str) -> None:
    root = pathlib.Path(output_dir)
    models_dir = root / "lib" / "chromadb" / "openapi" / "models"
    if not models_dir.exists():
        return
    override_hash_map(models_dir)
    remove_generated_artifacts(root)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: postprocess_openapi.py <openapi_output_dir>")
    main(sys.argv[1])
