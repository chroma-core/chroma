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

if(gflagsAlt_FOUND)
  return()
endif()

set(find_package_args)
if(gflagsAlt_FIND_VERSION)
  list(APPEND find_package_args ${gflagsAlt_FIND_VERSION})
endif()
if(gflagsAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(gflags ${find_package_args})
if(gflags_FOUND)
  set(gflagsAlt_FOUND TRUE)
  return()
endif()

# TODO: Support version detection.

if(gflags_ROOT)
  find_library(gflags_LIB
               NAMES gflags
               PATHS ${gflags_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(GFLAGS_INCLUDE_DIR
            NAMES gflags/gflags.h
            PATHS ${gflags_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_library(gflags_LIB NAMES gflags)
  find_path(GFLAGS_INCLUDE_DIR
            NAMES gflags/gflags.h
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(gflagsAlt REQUIRED_VARS gflags_LIB GFLAGS_INCLUDE_DIR)

if(gflagsAlt_FOUND)
  add_library(gflags::gflags UNKNOWN IMPORTED)
  set_target_properties(gflags::gflags
                        PROPERTIES IMPORTED_LOCATION "${gflags_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GFLAGS_INCLUDE_DIR}")
  set(GFLAGS_LIBRARIES gflags::gflags)
endif()
