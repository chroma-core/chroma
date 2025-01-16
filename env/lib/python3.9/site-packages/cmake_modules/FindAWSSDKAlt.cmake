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

set(find_package_args)
if(AWSSDKAlt_FIND_VERSION)
  list(APPEND find_package_args ${AWSSDKAlt_FIND_VERSION})
endif()
if(AWSSDKAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
# See https://aws.amazon.com/blogs/developer/developer-experience-of-the-aws-sdk-for-c-now-simplified-by-cmake/
# Workaround to force AWS CMake configuration to look for shared libraries
if(DEFINED ENV{CONDA_PREFIX})
  if(DEFINED BUILD_SHARED_LIBS)
    set(BUILD_SHARED_LIBS_WAS_SET TRUE)
    set(BUILD_SHARED_LIBS_KEEP ${BUILD_SHARED_LIBS})
  else()
    set(BUILD_SHARED_LIBS_WAS_SET FALSE)
  endif()
  set(BUILD_SHARED_LIBS ON)
endif()
find_package(AWSSDK ${find_package_args}
             COMPONENTS config
                        s3
                        transfer
                        identity-management
                        sts)
# Restore previous value of BUILD_SHARED_LIBS
if(DEFINED ENV{CONDA_PREFIX})
  if(BUILD_SHARED_LIBS_WAS_SET)
    set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_KEEP})
  else()
    unset(BUILD_SHARED_LIBS)
  endif()
endif()
set(AWSSDKAlt_FOUND ${AWSSDK_FOUND})
