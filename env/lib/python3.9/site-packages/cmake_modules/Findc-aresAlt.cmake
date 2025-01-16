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

if(c-aresAlt_FOUND)
  return()
endif()

set(find_package_args)
if(c-aresAlt_FIND_VERSION)
  list(APPEND find_package_args ${c-aresAlt_FIND_VERSION})
endif()
if(c-aresAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(c-ares ${find_package_args})
if(c-ares_FOUND)
  set(c-aresAlt_FOUND TRUE)
  return()
endif()

find_package(PkgConfig QUIET)
pkg_check_modules(c-ares_PC libcares)
if(c-ares_PC_FOUND)
  set(c-ares_INCLUDE_DIR "${c-ares_PC_INCLUDEDIR}")

  list(APPEND c-ares_PC_LIBRARY_DIRS "${c-ares_PC_LIBDIR}")
  find_library(c-ares_LIB cares
               PATHS ${c-ares_PC_LIBRARY_DIRS}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
elseif(c-ares_ROOT)
  find_library(c-ares_LIB
               NAMES cares
                     "${CMAKE_SHARED_LIBRARY_PREFIX}cares${CMAKE_SHARED_LIBRARY_SUFFIX}"
               PATHS ${c-ares_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(c-ares_INCLUDE_DIR
            NAMES ares.h
            PATHS ${c-ares_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_library(c-ares_LIB
               NAMES cares
                     "${CMAKE_SHARED_LIBRARY_PREFIX}cares${CMAKE_SHARED_LIBRARY_SUFFIX}"
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  find_path(c-ares_INCLUDE_DIR
            NAMES ares.h
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(c-aresAlt REQUIRED_VARS c-ares_LIB c-ares_INCLUDE_DIR)

if(c-aresAlt_FOUND)
  if(NOT TARGET c-ares::cares)
    add_library(c-ares::cares UNKNOWN IMPORTED)
    set_target_properties(c-ares::cares
                          PROPERTIES IMPORTED_LOCATION "${c-ares_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${c-ares_INCLUDE_DIR}")
  endif()
endif()
