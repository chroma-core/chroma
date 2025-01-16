#
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

if(gRPCAlt_FOUND)
  return()
endif()

set(find_package_args)
if(gRPCAlt_FIND_VERSION)
  list(APPEND find_package_args ${gRPCAlt_FIND_VERSION})
endif()
if(gRPCAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(gRPC ${find_package_args})
if(gRPC_FOUND)
  set(gRPCAlt_FOUND TRUE)
  return()
endif()

find_package(PkgConfig QUIET)
pkg_check_modules(GRPCPP_PC grpc++)
if(GRPCPP_PC_FOUND)
  set(gRPCAlt_VERSION "${GRPCPP_PC_VERSION}")
  set(GRPCPP_INCLUDE_DIRECTORIES ${GRPCPP_PC_INCLUDEDIR})
  # gRPC's pkg-config file neglects to specify pthreads.
  find_package(Threads REQUIRED)
  if(ARROW_GRPC_USE_SHARED)
    set(GRPCPP_LINK_LIBRARIES ${GRPCPP_PC_LINK_LIBRARIES})
    set(GRPCPP_LINK_OPTIONS ${GRPCPP_PC_LDFLAGS_OTHER})
    set(GRPCPP_COMPILE_OPTIONS ${GRPCPP_PC_CFLAGS_OTHER})
  else()
    set(GRPCPP_LINK_LIBRARIES)
    foreach(GRPCPP_LIBRARY_NAME ${GRPCPP_PC_STATIC_LIBRARIES})
      find_library(GRPCPP_LIBRARY_${GRPCPP_LIBRARY_NAME}
                   NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}${GRPCPP_LIBRARY_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
                   HINTS ${GRPCPP_PC_STATIC_LIBRARY_DIRS})
      list(APPEND GRPCPP_LINK_LIBRARIES "${GRPCPP_LIBRARY_${GRPCPP_LIBRARY_NAME}}")
    endforeach()
    set(GRPCPP_LINK_OPTIONS ${GRPCPP_PC_STATIC_LDFLAGS_OTHER})
    set(GRPCPP_COMPILE_OPTIONS ${GRPCPP_PC_STATIC_CFLAGS_OTHER})
  endif()
  list(APPEND GRPCPP_LINK_LIBRARIES Threads::Threads)
  list(GET GRPCPP_LINK_LIBRARIES 0 GRPCPP_IMPORTED_LOCATION)
  list(REMOVE_AT GRPCPP_LINK_LIBRARIES 0)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
               HINTS ${GRPCPP_PC_PREFIX}
               NO_DEFAULT_PATH
               PATH_SUFFIXES "bin")
endif()
set(gRPCAlt_FIND_PACKAGE_ARGS gRPCAlt REQUIRED_VARS GRPCPP_IMPORTED_LOCATION
                              GRPC_CPP_PLUGIN)
if(gRPCAlt_VERSION)
  list(APPEND gRPCAlt_FIND_PACKAGE_ARGS VERSION_VAR gRPCAlt_VERSION)
endif()
find_package_handle_standard_args(${gRPCAlt_FIND_PACKAGE_ARGS})

if(gRPCAlt_FOUND)
  # gRPC does not expose the reflection library via pkg-config, but it should be alongside the main library
  get_filename_component(GRPCPP_IMPORTED_DIRECTORY ${GRPCPP_IMPORTED_LOCATION} DIRECTORY)
  if(ARROW_GRPC_USE_SHARED)
    set(GRPCPP_REFLECTION_LIB_NAME
        "${CMAKE_SHARED_LIBRARY_PREFIX}grpc++_reflection${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(GRPCPP_REFLECTION_LIB_NAME
        "${CMAKE_STATIC_LIBRARY_PREFIX}grpc++_reflection${CMAKE_STATIC_LIBRARY_SUFFIX}")
  endif()
  find_library(GRPCPP_REFLECTION_IMPORTED_LOCATION
               NAMES grpc++_reflection ${GRPCPP_REFLECTION_LIB_NAME}
               PATHS ${GRPCPP_IMPORTED_DIRECTORY}
               NO_DEFAULT_PATH)

  add_library(gRPC::grpc++ UNKNOWN IMPORTED)
  set_target_properties(gRPC::grpc++
                        PROPERTIES IMPORTED_LOCATION "${GRPCPP_IMPORTED_LOCATION}"
                                   INTERFACE_COMPILE_OPTIONS "${GRPCPP_COMPILE_OPTIONS}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${GRPCPP_INCLUDE_DIRECTORIES}"
                                   INTERFACE_LINK_LIBRARIES "${GRPCPP_LINK_LIBRARIES}"
                                   INTERFACE_LINK_OPTIONS "${GRPCPP_LINK_OPTIONS}")

  add_library(gRPC::grpc++_reflection UNKNOWN IMPORTED)
  set_target_properties(gRPC::grpc++_reflection
                        PROPERTIES IMPORTED_LOCATION
                                   "${GRPCPP_REFLECTION_IMPORTED_LOCATION}"
                                   INTERFACE_LINK_LIBRARIES gRPC::grpc++)

  add_executable(gRPC::grpc_cpp_plugin IMPORTED)
  set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES IMPORTED_LOCATION
                                                         ${GRPC_CPP_PLUGIN})
endif()
