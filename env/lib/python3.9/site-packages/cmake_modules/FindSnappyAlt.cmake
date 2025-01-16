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

if(SnappyAlt_FOUND)
  return()
endif()

set(find_package_args)
if(SnappyAlt_FIND_VERSION)
  list(APPEND find_package_args ${SnappyAlt_FIND_VERSION})
endif()
if(SnappyAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(Snappy ${find_package_args})
if(Snappy_FOUND)
  if(ARROW_SNAPPY_USE_SHARED)
    set(Snappy_TARGET Snappy::snappy)
    set(SnappyAlt_FOUND TRUE)
    return()
  else()
    if(TARGET Snappy::snappy-static)
      # The official SnappyTargets.cmake uses Snappy::snappy-static for
      # static version.
      set(Snappy_TARGET Snappy::snappy-static)
      set(SnappyAlt_FOUND TRUE)
      return()
    else()
      # The Conan's Snappy package always uses Snappy::snappy and it's
      # an INTERFACE_LIBRARY.
      get_target_property(Snappy Snappy::snappy TYPE)
      if(Snappy_TYPE STREQUAL "STATIC_LIBRARY" OR Snappy_TYPE STREQUAL
                                                  "INTERFACE_LIBRARY")
        set(Snappy_TARGET Snappy::snappy)
        set(SnappyAlt_FOUND TRUE)
        return()
      endif()
    endif()
  endif()
endif()

if(ARROW_SNAPPY_USE_SHARED)
  set(SNAPPY_LIB_NAMES)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(APPEND SNAPPY_LIB_NAMES
         "${CMAKE_IMPORT_LIBRARY_PREFIX}snappy${CMAKE_IMPORT_LIBRARY_SUFFIX}")
  endif()
  list(APPEND SNAPPY_LIB_NAMES
       "${CMAKE_SHARED_LIBRARY_PREFIX}snappy${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
  set(SNAPPY_STATIC_LIB_NAME_BASE "snappy")
  if(MSVC)
    set(SNAPPY_STATIC_LIB_NAME_BASE
        "${SNAPPY_STATIC_LIB_NAME_BASE}${SNAPPY_MSVC_STATIC_LIB_SUFFIX}")
  endif()
  set(SNAPPY_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_STATIC_LIB_NAME_BASE}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
endif()

if(Snappy_ROOT)
  find_library(Snappy_LIB
               NAMES ${SNAPPY_LIB_NAMES}
               PATHS ${Snappy_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(Snappy_INCLUDE_DIR
            NAMES snappy.h
            PATHS ${Snappy_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_library(Snappy_LIB NAMES ${SNAPPY_LIB_NAMES})
  find_path(Snappy_INCLUDE_DIR
            NAMES snappy.h
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(SnappyAlt REQUIRED_VARS Snappy_LIB Snappy_INCLUDE_DIR)

if(SnappyAlt_FOUND)
  if(ARROW_SNAPPY_USE_SHARED)
    set(Snappy_TARGET Snappy::snappy)
    set(Snappy_TARGET_TYPE SHARED)
  else()
    set(Snappy_TARGET Snappy::snappy-static)
    set(Snappy_TARGET_TYPE STATIC)
  endif()
  add_library(${Snappy_TARGET} ${Snappy_TARGET_TYPE} IMPORTED)
  set_target_properties(${Snappy_TARGET}
                        PROPERTIES IMPORTED_LOCATION "${Snappy_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${Snappy_INCLUDE_DIR}")
endif()
