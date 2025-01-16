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

if(zstdAlt_FOUND)
  return()
endif()

set(find_package_args)
if(zstdAlt_FIND_VERSION)
  list(APPEND find_package_args ${zstdAlt_FIND_VERSION})
endif()
if(zstdAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(zstd ${find_package_args})
if(zstd_FOUND)
  set(zstdAlt_FOUND TRUE)
  return()
endif()

if(MSVC AND NOT DEFINED ZSTD_MSVC_LIB_PREFIX)
  set(ZSTD_MSVC_LIB_PREFIX "lib")
endif()
set(ZSTD_LIB_NAME_BASE "${ZSTD_MSVC_LIB_PREFIX}zstd")

if(ARROW_ZSTD_USE_SHARED)
  set(ZSTD_LIB_NAMES)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(APPEND
         ZSTD_LIB_NAMES
         "${CMAKE_IMPORT_LIBRARY_PREFIX}${ZSTD_LIB_NAME_BASE}${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    )
  endif()
  list(APPEND ZSTD_LIB_NAMES
       "${CMAKE_SHARED_LIBRARY_PREFIX}${ZSTD_LIB_NAME_BASE}${CMAKE_SHARED_LIBRARY_SUFFIX}"
  )
else()
  if(MSVC AND NOT DEFINED ZSTD_MSVC_STATIC_LIB_SUFFIX)
    set(ZSTD_MSVC_STATIC_LIB_SUFFIX "_static")
  endif()
  set(ZSTD_STATIC_LIB_SUFFIX
      "${ZSTD_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(ZSTD_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}${ZSTD_LIB_NAME_BASE}${ZSTD_STATIC_LIB_SUFFIX}")
endif()

# First, find via if specified ZSTD_ROOT
if(ZSTD_ROOT)
  message(STATUS "Using ZSTD_ROOT: ${ZSTD_ROOT}")
  find_library(ZSTD_LIB
               NAMES ${ZSTD_LIB_NAMES}
               PATHS ${ZSTD_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(ZSTD_INCLUDE_DIR
            NAMES zstd.h
            PATHS ${ZSTD_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})

else()
  # Second, find via pkg_check_modules
  find_package(PkgConfig QUIET)
  pkg_check_modules(ZSTD_PC libzstd)
  if(ZSTD_PC_FOUND)
    set(zstdAlt_VERSION "${ZSTD_PC_VERSION}")
    set(ZSTD_INCLUDE_DIR "${ZSTD_PC_INCLUDEDIR}")

    list(APPEND ZSTD_PC_LIBRARY_DIRS "${ZSTD_PC_LIBDIR}")
    find_library(ZSTD_LIB
                 NAMES ${ZSTD_LIB_NAMES}
                 PATHS ${ZSTD_PC_LIBRARY_DIRS}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  else()
    # Third, check all other CMake paths
    find_library(ZSTD_LIB
                 NAMES ${ZSTD_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_path(ZSTD_INCLUDE_DIR
              NAMES zstd.h
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
endif()

if("${zstdAlt_VERSION}" STREQUAL "" AND ZSTD_INCLUDE_DIR)
  file(READ "${ZSTD_INCLUDE_DIR}/zstd.h" ZSTD_H_CONTENT)
  string(REGEX MATCH "#define ZSTD_VERSION_MAJOR +([0-9]+)" ZSTD_VERSION_MAJOR_DEFINITION
               "${ZSTD_H_CONTENT}")
  string(REGEX REPLACE "^.+ ([0-9]+)$" "\\1" ZSTD_VERSION_MAJOR
                       "${ZSTD_VERSION_MAJOR_DEFINITION}")
  string(REGEX MATCH "#define ZSTD_VERSION_MINOR +([0-9]+)" ZSTD_VERSION_MINOR_DEFINITION
               "${ZSTD_H_CONTENT}")
  string(REGEX REPLACE "^.+ ([0-9]+)$" "\\1" ZSTD_VERSION_MINOR
                       "${ZSTD_VERSION_MINOR_DEFINITION}")
  string(REGEX MATCH "#define ZSTD_VERSION_RELEASE +([0-9]+)"
               ZSTD_VERSION_RELEASE_DEFINITION "${ZSTD_H_CONTENT}")
  string(REGEX REPLACE "^.+ ([0-9]+)$" "\\1" ZSTD_VERSION_RELEASE
                       "${ZSTD_VERSION_RELEASE_DEFINITION}")
  if("${ZSTD_VERSION_MAJOR}" STREQUAL ""
     OR "${ZSTD_VERSION_MINOR}" STREQUAL ""
     OR "${ZSTD_VERSION_RELEASE}" STREQUAL "")
    set(zstdAlt_VERSION "0.0.0")
  else()
    set(zstdAlt_VERSION
        "${ZSTD_VERSION_MAJOR}.${ZSTD_VERSION_MINOR}.${ZSTD_VERSION_RELEASE}")
  endif()
endif()

find_package_handle_standard_args(
  zstdAlt
  REQUIRED_VARS ZSTD_LIB ZSTD_INCLUDE_DIR
  VERSION_VAR zstdAlt_VERSION)

if(zstdAlt_FOUND)
  if(ARROW_ZSTD_USE_SHARED)
    set(zstd_TARGET zstd::libzstd_shared)
    add_library(${zstd_TARGET} SHARED IMPORTED)
  else()
    set(zstd_TARGET zstd::libzstd_static)
    add_library(${zstd_TARGET} STATIC IMPORTED)
  endif()
  set_target_properties(${zstd_TARGET}
                        PROPERTIES IMPORTED_LOCATION "${ZSTD_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}")
  message(STATUS "Zstandard library: ${ZSTD_LIB}")
  message(STATUS "Zstandard include directory: ${ZSTD_INCLUDE_DIR}")
endif()
