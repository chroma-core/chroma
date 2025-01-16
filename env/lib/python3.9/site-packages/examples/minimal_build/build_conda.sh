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

set -e

#----------------------------------------------------------------------
# Change this to whatever makes sense for your system

HOME=
MINICONDA=$HOME/miniconda-for-arrow
LIBRARY_INSTALL_DIR=$HOME/local-libs
CPP_BUILD_DIR=$HOME/arrow-cpp-build
ARROW_ROOT=/arrow
PYTHON=3.10

git config --global --add safe.directory $ARROW_ROOT

#----------------------------------------------------------------------
# Run these only once

function setup_miniconda() {
  MINICONDA_URL="https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh"
  wget -O miniconda.sh $MINICONDA_URL
  bash miniconda.sh -b -p $MINICONDA
  rm -f miniconda.sh
  LOCAL_PATH=$PATH
  export PATH="$MINICONDA/bin:$PATH"

  mamba info -a

  conda config --set show_channel_urls True
  conda config --show channels

  mamba create -y -n pyarrow-$PYTHON \
        --file arrow/ci/conda_env_unix.txt \
        --file arrow/ci/conda_env_cpp.txt \
        --file arrow/ci/conda_env_python.txt \
        compilers \
        python=$PYTHON \
        pandas

  export PATH=$LOCAL_PATH
}

setup_miniconda

#----------------------------------------------------------------------
# Activate mamba in bash and activate mamba environment

. $MINICONDA/etc/profile.d/conda.sh
conda activate pyarrow-$PYTHON
export ARROW_HOME=$CONDA_PREFIX

#----------------------------------------------------------------------
# Build C++ library

mkdir -p $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=DEBUG \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DCMAKE_UNITY_BUILD=ON \
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

pytest -vv -r s ${PYTEST_ARGS} --pyargs pyarrow
