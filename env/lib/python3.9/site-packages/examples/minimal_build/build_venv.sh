#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

#----------------------------------------------------------------------
# Change this to whatever makes sense for your system

WORKDIR=${WORKDIR:-$HOME}
MINICONDA=$WORKDIR/miniconda-for-arrow
LIBRARY_INSTALL_DIR=$WORKDIR/local-libs
CPP_BUILD_DIR=$WORKDIR/arrow-cpp-build
ARROW_ROOT=/arrow
export ARROW_HOME=$WORKDIR/dist
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH

python3 -m venv $WORKDIR/venv
source $WORKDIR/venv/bin/activate

git config --global --add safe.directory $ARROW_ROOT

pip install -r $ARROW_ROOT/python/requirements-build.txt
pip install wheel

#----------------------------------------------------------------------
# Build C++ library

mkdir -p $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=DEBUG \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DCMAKE_UNITY_BUILD=ON \
      -DARROW_BUILD_STATIC=OFF \
      -DARROW_COMPUTE=ON \
      -DARROW_CSV=ON \
      -DARROW_FILESYSTEM=ON \
      -DARROW_JSON=ON \
      $ARROW_ROOT/cpp

ninja install

popd

#----------------------------------------------------------------------
# Build and test Python library
pushd $ARROW_ROOT/python

rm -rf build/  # remove any pesky preexisting build directory

export CMAKE_PREFIX_PATH=${ARROW_HOME}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}
export PYARROW_BUILD_TYPE=Debug
export PYARROW_CMAKE_GENERATOR=Ninja

# Use the same command that we use on python_build.sh
python -m pip install --no-deps --no-build-isolation -vv .

popd

pip install -r $ARROW_ROOT/python/requirements-test.txt

pytest -vv -r s ${PYTEST_ARGS} --pyargs pyarrow
