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

message(STATUS "Using vcpkg to find dependencies")

# ----------------------------------------------------------------------
# Define macros

# macro to list subdirectories (non-recursive)
macro(list_subdirs SUBDIRS DIR)
  file(GLOB children_
       RELATIVE ${DIR}
       ${DIR}/*)
  set(subdirs_ "")
  foreach(child_ ${children_})
    if(IS_DIRECTORY "${DIR}/${child_}")
      list(APPEND subdirs_ ${child_})
    endif()
  endforeach()
  set("${SUBDIRS}" ${subdirs_})
  unset(children_)
  unset(subdirs_)
endmacro()

# ----------------------------------------------------------------------
# Get VCPKG_ROOT

if(DEFINED CMAKE_TOOLCHAIN_FILE)
  # Get it from the CMake variable CMAKE_TOOLCHAIN_FILE
  get_filename_component(_VCPKG_DOT_CMAKE "${CMAKE_TOOLCHAIN_FILE}" NAME)
  if(EXISTS "${CMAKE_TOOLCHAIN_FILE}" AND _VCPKG_DOT_CMAKE STREQUAL "vcpkg.cmake")
    get_filename_component(_VCPKG_BUILDSYSTEMS_DIR "${CMAKE_TOOLCHAIN_FILE}" DIRECTORY)
    get_filename_component(VCPKG_ROOT "${_VCPKG_BUILDSYSTEMS_DIR}/../.." ABSOLUTE)
  else()
    message(FATAL_ERROR "vcpkg toolchain file not found at path specified in -DCMAKE_TOOLCHAIN_FILE"
    )
  endif()
else()
  if(DEFINED VCPKG_ROOT)
    # Get it from the CMake variable VCPKG_ROOT
    find_program(_VCPKG_BIN vcpkg
                 PATHS "${VCPKG_ROOT}"
                 NO_DEFAULT_PATH)
    if(NOT _VCPKG_BIN)
      message(FATAL_ERROR "vcpkg not found in directory specified in -DVCPKG_ROOT")
    endif()
  elseif(DEFINED ENV{VCPKG_ROOT})
    # Get it from the environment variable VCPKG_ROOT
    set(VCPKG_ROOT $ENV{VCPKG_ROOT})
    find_program(_VCPKG_BIN vcpkg
                 PATHS "${VCPKG_ROOT}"
                 NO_DEFAULT_PATH)
    if(NOT _VCPKG_BIN)
      message(FATAL_ERROR "vcpkg not found in directory in environment variable VCPKG_ROOT"
      )
    endif()
  else()
    # Get it from the file vcpkg.path.txt
    find_program(_VCPKG_BIN vcpkg)
    if(_VCPKG_BIN)
      get_filename_component(_VCPKG_REAL_BIN "${_VCPKG_BIN}" REALPATH)
      get_filename_component(VCPKG_ROOT "${_VCPKG_REAL_BIN}" DIRECTORY)
    else()
      if(CMAKE_HOST_WIN32)
        set(_VCPKG_PATH_TXT "$ENV{LOCALAPPDATA}/vcpkg/vcpkg.path.txt")
      else()
        set(_VCPKG_PATH_TXT "$ENV{HOME}/.vcpkg/vcpkg.path.txt")
      endif()
      if(EXISTS "${_VCPKG_PATH_TXT}")
        file(READ "${_VCPKG_PATH_TXT}" VCPKG_ROOT)
      else()
        message(FATAL_ERROR "vcpkg not found. Install vcpkg if not installed, "
                            "then run vcpkg integrate install or set environment variable VCPKG_ROOT."
        )
      endif()
      find_program(_VCPKG_BIN vcpkg
                   PATHS "${VCPKG_ROOT}"
                   NO_DEFAULT_PATH)
      if(NOT _VCPKG_BIN)
        message(FATAL_ERROR "vcpkg not found. Re-run vcpkg integrate install "
                            "or set environment variable VCPKG_ROOT.")
      endif()
    endif()
  endif()
  set(CMAKE_TOOLCHAIN_FILE
      "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
      CACHE FILEPATH "Path to vcpkg CMake toolchain file")
endif()
message(STATUS "Using CMAKE_TOOLCHAIN_FILE: ${CMAKE_TOOLCHAIN_FILE}")
message(STATUS "Using VCPKG_ROOT: ${VCPKG_ROOT}")

# ----------------------------------------------------------------------
# Get VCPKG_TARGET_TRIPLET

if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
  set(VCPKG_TARGET_TRIPLET "$ENV{VCPKG_DEFAULT_TRIPLET}")
endif()
# Explicitly set manifest mode on if it is not set and vcpkg.json exists
if(NOT DEFINED VCPKG_MANIFEST_MODE AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg.json")
  set(VCPKG_MANIFEST_MODE
      ON
      CACHE BOOL "Use vcpkg.json manifest")
  message(STATUS "vcpkg.json manifest found. Using VCPKG_MANIFEST_MODE: ON")
endif()
# vcpkg can install packages in three different places
set(_INST_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/vcpkg_installed") # try here first
set(_INST_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg_installed") # try here second
set(_INST_VCPKG_ROOT "${VCPKG_ROOT}/installed")
# Iterate over the places
foreach(_INST_DIR ${_INST_BUILD_DIR} ${_INST_SOURCE_DIR} ${_INST_VCPKG_ROOT} "notfound")
  if(_INST_DIR STREQUAL "notfound")
    message(FATAL_ERROR "vcpkg installed libraries directory not found. "
                        "Install packages with vcpkg before executing cmake.")
  elseif(NOT EXISTS "${_INST_DIR}")
    continue()
  elseif((_INST_DIR STREQUAL _INST_BUILD_DIR OR _INST_DIR STREQUAL _INST_SOURCE_DIR)
         AND NOT VCPKG_MANIFEST_MODE)
    # Do not look for packages in the build or source dirs if manifest mode is off
    message(STATUS "Skipped looking for installed packages in ${_INST_DIR} "
                   "because -DVCPKG_MANIFEST_MODE=OFF")
    continue()
  else()
    message(STATUS "Looking for installed packages in ${_INST_DIR}")
  endif()
  if(DEFINED VCPKG_TARGET_TRIPLET)
    # Check if a subdirectory named VCPKG_TARGET_TRIPLET
    # exists in the vcpkg installed directory
    if(EXISTS "${_INST_DIR}/${VCPKG_TARGET_TRIPLET}")
      set(_VCPKG_INSTALLED_DIR "${_INST_DIR}")
      break()
    endif()
  else()
    # Infer VCPKG_TARGET_TRIPLET from the name of the
    # subdirectory in the vcpkg installed directory
    list_subdirs(_VCPKG_TRIPLET_SUBDIRS "${_INST_DIR}")
    list(REMOVE_ITEM _VCPKG_TRIPLET_SUBDIRS "vcpkg")
    list(LENGTH _VCPKG_TRIPLET_SUBDIRS _NUM_VCPKG_TRIPLET_SUBDIRS)
    if(_NUM_VCPKG_TRIPLET_SUBDIRS EQUAL 1)
      list(GET _VCPKG_TRIPLET_SUBDIRS 0 VCPKG_TARGET_TRIPLET)
      set(_VCPKG_INSTALLED_DIR "${_INST_DIR}")
      break()
    endif()
  endif()
endforeach()
if(NOT DEFINED VCPKG_TARGET_TRIPLET)
  message(FATAL_ERROR "Could not infer VCPKG_TARGET_TRIPLET. "
                      "Specify triplet with -DVCPKG_TARGET_TRIPLET.")
elseif(NOT DEFINED _VCPKG_INSTALLED_DIR)
  message(FATAL_ERROR "Could not find installed vcpkg packages for triplet ${VCPKG_TARGET_TRIPLET}. "
                      "Install packages with vcpkg before executing cmake.")
endif()

set(VCPKG_TARGET_TRIPLET
    "${VCPKG_TARGET_TRIPLET}"
    CACHE STRING "vcpkg triplet for the target environment")

if(NOT DEFINED VCPKG_BUILD_TYPE)
  set(VCPKG_BUILD_TYPE
      "${LOWERCASE_BUILD_TYPE}"
      CACHE STRING "vcpkg build type (release|debug)")
endif()

if(NOT DEFINED VCPKG_LIBRARY_LINKAGE)
  if(ARROW_DEPENDENCY_USE_SHARED)
    set(VCPKG_LIBRARY_LINKAGE "dynamic")
  else()
    set(VCPKG_LIBRARY_LINKAGE "static")
  endif()
  set(VCPKG_LIBRARY_LINKAGE
      "${VCPKG_LIBRARY_LINKAGE}"
      CACHE STRING "vcpkg preferred library linkage (static|dynamic)")
endif()

message(STATUS "Using vcpkg installed libraries directory: ${_VCPKG_INSTALLED_DIR}")
message(STATUS "Using VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
message(STATUS "Using VCPKG_BUILD_TYPE: ${VCPKG_BUILD_TYPE}")
message(STATUS "Using VCPKG_LIBRARY_LINKAGE: ${VCPKG_LIBRARY_LINKAGE}")

set(ARROW_VCPKG_PREFIX
    "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}"
    CACHE PATH "Path to target triplet subdirectory in vcpkg installed directory")

set(ARROW_VCPKG
    ON
    CACHE BOOL "Use vcpkg for dependencies")

set(ARROW_DEPENDENCY_SOURCE
    "SYSTEM"
    CACHE STRING "The specified value VCPKG is implemented internally as SYSTEM" FORCE)

set(BOOST_ROOT
    "${ARROW_VCPKG_PREFIX}"
    CACHE STRING "")
set(BOOST_INCLUDEDIR
    "${ARROW_VCPKG_PREFIX}/include/boost"
    CACHE STRING "")
set(BOOST_LIBRARYDIR
    "${ARROW_VCPKG_PREFIX}/lib"
    CACHE STRING "")
set(OPENSSL_INCLUDE_DIR
    "${ARROW_VCPKG_PREFIX}/include"
    CACHE STRING "")
set(OPENSSL_LIBRARIES
    "${ARROW_VCPKG_PREFIX}/lib"
    CACHE STRING "")
set(OPENSSL_ROOT_DIR
    "${ARROW_VCPKG_PREFIX}"
    CACHE STRING "")
set(Thrift_ROOT
    "${ARROW_VCPKG_PREFIX}/lib"
    CACHE STRING "")
set(ZSTD_INCLUDE_DIR
    "${ARROW_VCPKG_PREFIX}/include"
    CACHE STRING "")
set(ZSTD_ROOT
    "${ARROW_VCPKG_PREFIX}"
    CACHE STRING "")
set(BROTLI_ROOT
    "${ARROW_VCPKG_PREFIX}"
    CACHE STRING "")
set(LZ4_ROOT
    "${ARROW_VCPKG_PREFIX}"
    CACHE STRING "")

if(CMAKE_HOST_WIN32)
  set(LZ4_MSVC_LIB_PREFIX
      ""
      CACHE STRING "")
  set(LZ4_MSVC_STATIC_LIB_SUFFIX
      ""
      CACHE STRING "")
  set(ZSTD_MSVC_LIB_PREFIX
      ""
      CACHE STRING "")
  set(ZSTD_MSVC_STATIC_LIB_SUFFIX
      ""
      CACHE STRING "")
endif()
