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

if(re2Alt_FOUND)
  return()
endif()

set(find_package_args)
if(re2Alt_FIND_VERSION)
  list(APPEND find_package_args ${re2Alt_FIND_VERSION})
endif()
if(re2Alt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(re2 ${find_package_args})
if(re2_FOUND)
  set(re2Alt_FOUND TRUE)
  return()
endif()

if(re2_ROOT)
  find_library(RE2_LIB
               NAMES re2_static
                     re2
                     "${CMAKE_STATIC_LIBRARY_PREFIX}re2${RE2_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
                     "${CMAKE_SHARED_LIBRARY_PREFIX}re2${CMAKE_SHARED_LIBRARY_SUFFIX}"
               PATHS ${re2_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(RE2_INCLUDE_DIR
            NAMES re2/re2.h
            PATHS ${re2_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_package(PkgConfig QUIET)
  pkg_check_modules(RE2_PC re2)
  if(RE2_PC_FOUND)
    set(RE2_INCLUDE_DIR "${RE2_PC_INCLUDEDIR}")

    list(APPEND RE2_PC_LIBRARY_DIRS "${RE2_PC_LIBDIR}")
    find_library(RE2_LIB re2
                 PATHS ${RE2_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)

    # On Fedora, the reported prefix is wrong. As users likely run into this,
    # workaround.
    # https://bugzilla.redhat.com/show_bug.cgi?id=1652589
    if(UNIX
       AND NOT APPLE
       AND NOT RE2_LIB)
      if(RE2_PC_PREFIX STREQUAL "/usr/local")
        find_library(RE2_LIB re2)
      endif()
    endif()
  else()
    find_library(RE2_LIB
                 NAMES re2_static
                       re2
                       "${CMAKE_STATIC_LIBRARY_PREFIX}re2${RE2_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
                       "${CMAKE_SHARED_LIBRARY_PREFIX}re2${CMAKE_SHARED_LIBRARY_SUFFIX}"
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_path(RE2_INCLUDE_DIR
              NAMES re2/re2.h
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
endif()

find_package_handle_standard_args(re2Alt REQUIRED_VARS RE2_LIB RE2_INCLUDE_DIR)

if(re2Alt_FOUND)
  if(NOT TARGET re2::re2)
    add_library(re2::re2 UNKNOWN IMPORTED)
    set_target_properties(re2::re2
                          PROPERTIES IMPORTED_LOCATION "${RE2_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${RE2_INCLUDE_DIR}")
  endif()
endif()
