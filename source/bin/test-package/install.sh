#!/bin/bash

pip_install_from_tarball() {
    local tarball=$(readlink -f $1)
    if [ -f "$tarball" ]; then
        echo "Testing PIP package from tarball: $tarball"
    else
        echo "Could not find PIP package: $tarball"
        return 1
    fi

    # Create temporary project dir
    local dir=$(mktemp -d)

    echo "Building python project dir at $dir ..."

    cd $dir

    python3 -m venv venv

    source venv/bin/activate

    pip install $tarball
}
