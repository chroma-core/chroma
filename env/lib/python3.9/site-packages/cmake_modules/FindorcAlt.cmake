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

if(orcAlt_FOUND)
  return()
endif()

set(find_package_args)
if(orcAlt_FIND_VERSION)
  list(APPEND find_package_args ${orcAlt_FIND_VERSION})
endif()
if(orcAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(orc ${find_package_args})
if(orc_FOUND)
  set(orcAlt_FOUND TRUE)
  set(orcAlt_VERSION ${orc_VERSION})
  return()
endif()

if(ORC_ROOT)
  find_library(ORC_STATIC_LIB
               NAMES orc
               PATHS ${ORC_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  find_path(ORC_INCLUDE_DIR
            NAMES orc/orc-config.hh
            PATHS ${ORC_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_library(ORC_STATIC_LIB
               NAMES orc
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  find_path(ORC_INCLUDE_DIR
            NAMES orc/orc-config.hh
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
endif()
if(ORC_INCLUDE_DIR)
  file(READ "${ORC_INCLUDE_DIR}/orc/orc-config.hh" ORC_CONFIG_HH_CONTENT)
  string(REGEX MATCH "#define ORC_VERSION \"[0-9.]+\"" ORC_VERSION_DEFINITION
               "${ORC_CONFIG_HH_CONTENT}")
  string(REGEX MATCH "[0-9.]+" ORC_VERSION "${ORC_VERSION_DEFINITION}")
endif()

find_package_handle_standard_args(
  orcAlt
  REQUIRED_VARS ORC_STATIC_LIB ORC_INCLUDE_DIR
  VERSION_VAR ORC_VERSION)

if(orcAlt_FOUND)
  if(NOT TARGET orc::orc)
    add_library(orc::orc STATIC IMPORTED)
    set_target_properties(orc::orc
                          PROPERTIES IMPORTED_LOCATION "${ORC_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ORC_INCLUDE_DIR}")
  endif()
  set(orcAlt_VERSION ${ORC_VERSION})
endif()
