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

if(librados_FOUND)
  return()
endif()

find_path(LIBRADOS_INCLUDE_DIR rados/librados.hpp)

find_library(LIBRADOS_LIBRARY NAMES rados)

mark_as_advanced(LIBRADOS_LIBRARY LIBRADOS_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(librados DEFAULT_MSG LIBRADOS_LIBRARY
                                  LIBRADOS_INCLUDE_DIR)

if(librados_FOUND)
  add_library(librados::rados UNKNOWN IMPORTED)
  set_target_properties(librados::rados
                        PROPERTIES IMPORTED_LOCATION "${LIBRADOS_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${LIBRADOS_INCLUDE_DIR}")
endif()
