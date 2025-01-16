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

# Once done this will define
# - FindSQLite3Alt
#
# This module will set the following variables if found:
#  SQLite3_INCLUDE_DIRS  - SQLite3 include dir.
#  SQLite3_LIBRARIES     - List of libraries when using SQLite3.
#  SQLite3_FOUND         - True if SQLite3 found.
#
# Usage of this module as follows:
# find_package(SQLite3Alt)

if(FindSQLite3Alt_FOUND)
  return()
endif()

find_path(SQLite3_INCLUDE_DIR sqlite3.h)
find_library(SQLite3_LIBRARY NAMES sqlite3)

# handle the QUIETLY and REQUIRED arguments and set SQLite3_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SQLite3Alt REQUIRED_VARS SQLite3_LIBRARY
                                                           SQLite3_INCLUDE_DIR)

mark_as_advanced(SQLite3_LIBRARY SQLite3_INCLUDE_DIR)

if(SQLite3Alt_FOUND)
  set(SQLite3_INCLUDE_DIRS ${SQLite3_INCLUDE_DIR})
  set(SQLite3_LIBRARIES ${SQLite3_LIBRARY})
endif()
