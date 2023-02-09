#!/bin/bash

# Verify PIP tarball
tarball=$(readlink -f $1)
if [ -f "$tarball" ]; then
    echo "Testing PIP package from tarball: $tarball"
else
    echo "Could not find PIP package: $tarball"
fi

# Create temporary project dir
dir=$(mktemp -d)

echo "Building python project dir at $dir ..."

cd $dir

python3 -m venv venv

source venv/bin/activate

pip install $tarball

python -c "import chromadb; api = chromadb.Client(); print(api.heartbeat())"
