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

if(ARROW_PROTOBUF_USE_SHARED)
  set(Protobuf_USE_STATIC_LIBS OFF)
else()
  set(Protobuf_USE_STATIC_LIBS ON)
endif()

set(find_package_args)
if(ProtobufAlt_FIND_VERSION)
  list(APPEND find_package_args ${ProtobufAlt_FIND_VERSION})
endif()
if(ProtobufAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(protobuf CONFIG ${find_package_args})
set(ProtobufAlt_FOUND ${protobuf_FOUND})
if(ProtobufAlt_FOUND)
  if(Protobuf_PROTOC_EXECUTABLE)
    # work around https://github.com/protocolbuffers/protobuf/issues/14576
    set_target_properties(protobuf::protoc PROPERTIES IMPORTED_LOCATION_RELEASE
                                                      "${Protobuf_PROTOC_EXECUTABLE}")
  endif()
  set(ProtobufAlt_VERSION ${protobuf_VERSION})
  set(ProtobufAlt_VERSION_MAJOR ${protobuf_VERSION_MAJOR})
  set(ProtobufAlt_VERSION_MINOR ${protobuf_VERSION_MINOR})
  set(ProtobufAlt_VERSION_PATCH ${protobuf_VERSION_PATCH})
  set(ProtobufAlt_VERSION_TWEEK ${protobuf_VERSION_TWEEK})
else()
  find_package(Protobuf ${find_package_args})
  set(ProtobufAlt_FOUND ${Protobuf_FOUND})
  if(ProtobufAlt_FOUND)
    set(ProtobufAlt_VERSION ${Protobuf_VERSION})
    set(ProtobufAlt_VERSION_MAJOR ${Protobuf_VERSION_MAJOR})
    set(ProtobufAlt_VERSION_MINOR ${Protobuf_VERSION_MINOR})
    set(ProtobufAlt_VERSION_PATCH ${Protobuf_VERSION_PATCH})
    set(ProtobufAlt_VERSION_TWEEK ${Protobuf_VERSION_TWEEK})
  endif()
endif()
