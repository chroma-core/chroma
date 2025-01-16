# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tries to find jemalloc headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(jemallocAlt)
#
# This module defines
#  jemalloc::jemalloc, target to use jemalloc

if(jemallocAlt_FOUND)
  return()
endif()

if(ARROW_PACKAGE_KIND STREQUAL "conan")
  set(find_package_args "")
  if(jemallocAlt_FIND_VERSION)
    list(APPEND find_package_args ${jemallocAlt_FIND_VERSION})
  endif()
  if(jemallocAlt_FIND_QUIETLY)
    list(APPEND find_package_args QUIET)
  endif()
  if(jemallocAlt_FIND_REQUIRED)
    list(APPEND find_package_args REQUIRED)
  endif()
  find_package(jemallocAlt NAMES jemalloc ${find_package_args})
  set(jemalloc_FOUND ${jemallocAlt_FOUND})
  if(jemallocAlt_FOUND)
    return()
  endif()
endif()

if(ARROW_JEMALLOC_USE_SHARED)
  set(jemallocAlt_LIB_NAMES)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(APPEND jemallocAlt_LIB_NAMES
         "${CMAKE_IMPORT_LIBRARY_PREFIX}jemalloc${CMAKE_IMPORT_LIBRARY_SUFFIX}")
  endif()
  list(APPEND jemallocAlt_LIB_NAMES
       "${CMAKE_SHARED_LIBRARY_PREFIX}jemalloc${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
  set(jemallocAlt_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif()

if(jemalloc_ROOT)
  find_library(jemallocAlt_LIB
               NAMES ${jemallocAlt_LIB_NAMES}
               PATHS ${jemalloc_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(jemallocAlt_INCLUDE_DIR
            NAMES jemalloc/jemalloc.h
            PATHS ${jemalloc_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})

else()
  find_package(PkgConfig QUIET)
  pkg_check_modules(jemallocAlt_PC jemalloc)
  if(jemallocAlt_PC_FOUND)
    set(jemallocAlt_INCLUDE_DIR "${jemallocAlt_PC_INCLUDEDIR}")
    list(APPEND jemallocAlt_PC_LIBRARY_DIRS "${jemallocAlt_PC_LIBDIR}")
    find_library(jemallocAlt_LIB
                 NAMES ${jemallocAlt_LIB_NAMES}
                 PATHS ${jemallocAlt_PC_LIBRARY_DIRS}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  else()
    find_library(jemallocAlt_LIB
                 NAMES ${jemallocAlt_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_path(jemallocAlt_INCLUDE_DIR
              NAMES jemalloc/jemalloc.h
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
endif()

find_package_handle_standard_args(jemallocAlt REQUIRED_VARS jemallocAlt_LIB
                                                            jemallocAlt_INCLUDE_DIR)
set(jemalloc_FOUND ${jemallocAlt_FOUND})
if(jemallocAlt_FOUND)
  if(NOT TARGET jemalloc::jemalloc)
    if(ARROW_JEMALLOC_USE_SHARED)
      add_library(jemalloc::jemalloc SHARED IMPORTED)
    else()
      add_library(jemalloc::jemalloc STATIC IMPORTED)
    endif()
    set_target_properties(jemalloc::jemalloc
                          PROPERTIES IMPORTED_LOCATION "${jemallocAlt_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${jemallocAlt_INCLUDE_DIR}")
  endif()
endif()
