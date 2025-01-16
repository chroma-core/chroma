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

if(Azure_FOUND)
  return()
endif()

set(find_package_args)
list(APPEND find_package_args CONFIG)
if(Azure_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()

if(Azure_FIND_REQUIRED)
  list(APPEND find_package_args REQUIRED)
endif()

find_package(azure-core-cpp ${find_package_args})
find_package(azure-identity-cpp ${find_package_args})
find_package(azure-storage-blobs-cpp ${find_package_args})
find_package(azure-storage-common-cpp ${find_package_args})
find_package(azure-storage-files-datalake-cpp ${find_package_args})

find_package_handle_standard_args(
  Azure
  REQUIRED_VARS azure-core-cpp_FOUND
                azure-identity-cpp_FOUND
                azure-storage-blobs-cpp_FOUND
                azure-storage-common-cpp_FOUND
                azure-storage-files-datalake-cpp_FOUND
  VERSION_VAR azure-core-cpp_VERSION)
