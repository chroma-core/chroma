#!/usr/bin/env bash

# Define the paths to the existing and new toml files
existing_toml="pyproject.toml"
thin_client_toml="clients/python/pyproject.toml"

# Define the path to the thin client flag script
is_thin_client_py="clients/python/is_thin_client.py"
is_thin_client_target="chromadb/is_thin_client.py"

# Define the path to the existing readme and new readme for packaging
existing_readme="README.md"
thin_client_readme="clients/python/README.md"

# Stage the existing toml file
staged_toml="staged_pyproject.toml"
mv "$existing_toml" "$staged_toml"

# Stage the existing readme file
staged_readme="staged_README.md"
mv "$existing_readme" "$staged_readme"

function cleanup {
  # Teardown: Remove the new toml file and put the old one back
  rm "$existing_toml"
  mv "$staged_toml" "$existing_toml"

  rm "$is_thin_client_target"

  # Teardown: Remove the new readme file and put the old one back
  rm "$existing_readme"
  mv "$staged_readme" "$existing_readme"
}

trap cleanup EXIT

# Copy the new toml file in place
cp "$thin_client_toml" "$existing_toml"

# Copy the thin client flag script in place
cp "$is_thin_client_py" "$is_thin_client_target"

# Copy the new readme file in place
cp "$thin_client_readme" "$existing_readme"

python -m build
