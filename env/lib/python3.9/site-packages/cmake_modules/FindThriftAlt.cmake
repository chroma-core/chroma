# Copyright 2012 Cloudera Inc.
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

# - Find Thrift (a cross platform RPC lib/tool)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Thrift_ROOT - When set, this path is inspected instead of standard library
#                locations as the root of the Thrift installation.
#                The environment variable THRIFT_HOME overrides this variable.
#
# This module defines
#  Thrift_FOUND, whether Thrift is found or not
#  Thrift_COMPILER_FOUND, whether Thrift compiler is found or not
#
#  thrift::thrift, a library target to use Thrift
#  thrift::compiler, a executable target to use Thrift compiler

if(ThriftAlt_FOUND)
  return()
endif()

# There are some problems in ThriftConfig.cmake provided by MSYS2 and
# conda on Windows:
#
#   * https://github.com/conda-forge/thrift-cpp-feedstock/issues/68
#   * https://github.com/msys2/MINGW-packages/issues/6619#issuecomment-649728718
#
# We can remove the following "if(NOT WIN32)" condition once the
# followings are fixed and a new version that includes these fixes is
# published by MSYS2 and conda:
#
#   * https://github.com/apache/thrift/pull/2725
#   * https://github.com/apache/thrift/pull/2726
#   * https://github.com/conda-forge/thrift-cpp-feedstock/issues/68
if(NOT WIN32)
  set(find_package_args "")
  if(ThriftAlt_FIND_VERSION)
    list(APPEND find_package_args ${ThriftAlt_FIND_VERSION})
  endif()
  if(ThriftAlt_FIND_QUIETLY)
    list(APPEND find_package_args QUIET)
  endif()
  find_package(Thrift ${find_package_args})
  if(Thrift_FOUND)
    set(ThriftAlt_FOUND TRUE)
    add_executable(thrift::compiler IMPORTED)
    set_target_properties(thrift::compiler PROPERTIES IMPORTED_LOCATION
                                                      "${THRIFT_COMPILER}")
    return()
  endif()
endif()

function(extract_thrift_version)
  if(ThriftAlt_INCLUDE_DIR)
    file(READ "${ThriftAlt_INCLUDE_DIR}/thrift/config.h" THRIFT_CONFIG_H_CONTENT)
    string(REGEX MATCH "#define PACKAGE_VERSION \"[0-9.]+\"" THRIFT_VERSION_DEFINITION
                 "${THRIFT_CONFIG_H_CONTENT}")
    string(REGEX MATCH "[0-9.]+" ThriftAlt_VERSION "${THRIFT_VERSION_DEFINITION}")
    set(ThriftAlt_VERSION
        "${ThriftAlt_VERSION}"
        PARENT_SCOPE)
  else()
    set(ThriftAlt_VERSION
        ""
        PARENT_SCOPE)
  endif()
endfunction()

if(MSVC_TOOLCHAIN AND NOT DEFINED THRIFT_MSVC_LIB_SUFFIX)
  if(NOT ARROW_THRIFT_USE_SHARED)
    if(ARROW_USE_STATIC_CRT)
      if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
        set(THRIFT_MSVC_LIB_SUFFIX "mtd")
      else()
        set(THRIFT_MSVC_LIB_SUFFIX "mt")
      endif()
    else()
      if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
        set(THRIFT_MSVC_LIB_SUFFIX "mdd")
      else()
        set(THRIFT_MSVC_LIB_SUFFIX "md")
      endif()
    endif()
  endif()
endif()
set(ThriftAlt_LIB_NAME_BASE "thrift${THRIFT_MSVC_LIB_SUFFIX}")

if(ARROW_THRIFT_USE_SHARED)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    set(ThriftAlt_LIB_NAME
        "${CMAKE_IMPORT_LIBRARY_PREFIX}${ThriftAlt_LIB_NAME_BASE}${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    )
  else()
    set(ThriftAlt_LIB_NAME
        "${CMAKE_SHARED_LIBRARY_PREFIX}${ThriftAlt_LIB_NAME_BASE}${CMAKE_SHARED_LIBRARY_SUFFIX}"
    )
  endif()
else()
  set(ThriftAlt_LIB_NAME
      "${CMAKE_STATIC_LIBRARY_PREFIX}${ThriftAlt_LIB_NAME_BASE}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
endif()

if(Thrift_ROOT)
  find_library(ThriftAlt_LIB
               NAMES ${ThriftAlt_LIB_NAME}
               PATHS ${Thrift_ROOT}
               PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
  find_path(ThriftAlt_INCLUDE_DIR thrift/Thrift.h
            PATHS ${Thrift_ROOT}
            PATH_SUFFIXES "include")
  find_program(THRIFT_COMPILER thrift
               PATHS ${Thrift_ROOT}
               PATH_SUFFIXES "bin")
  extract_thrift_version()
else()
  # THRIFT-4760: The pkgconfig files are currently only installed when using autotools.
  # Starting with 0.13, they are also installed for the CMake-based installations of Thrift.
  find_package(PkgConfig QUIET)
  pkg_check_modules(THRIFT_PC thrift)
  if(THRIFT_PC_FOUND)
    set(ThriftAlt_INCLUDE_DIR "${THRIFT_PC_INCLUDEDIR}")

    list(APPEND THRIFT_PC_LIBRARY_DIRS "${THRIFT_PC_LIBDIR}")

    find_library(ThriftAlt_LIB
                 NAMES ${ThriftAlt_LIB_NAME}
                 PATHS ${THRIFT_PC_LIBRARY_DIRS}
                 NO_DEFAULT_PATH)
    find_program(THRIFT_COMPILER thrift
                 HINTS ${THRIFT_PC_PREFIX}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES "bin")
    set(ThriftAlt_VERSION ${THRIFT_PC_VERSION})
  else()
    find_library(ThriftAlt_LIB
                 NAMES ${ThriftAlt_LIB_NAME}
                 PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
    find_path(ThriftAlt_INCLUDE_DIR thrift/Thrift.h PATH_SUFFIXES "include")
    find_program(THRIFT_COMPILER thrift PATH_SUFFIXES "bin")
    extract_thrift_version()
  endif()
endif()

if(THRIFT_COMPILER)
  set(Thrift_COMPILER_FOUND TRUE)
else()
  set(Thrift_COMPILER_FOUND FALSE)
endif()

find_package_handle_standard_args(
  ThriftAlt
  REQUIRED_VARS ThriftAlt_LIB ThriftAlt_INCLUDE_DIR
  VERSION_VAR ThriftAlt_VERSION
  HANDLE_COMPONENTS)

if(ThriftAlt_FOUND)
  set(Thrift_VERSION ${ThriftAlt_VERSION})
  set(ThriftAlt_IMPORTED_PROPERTY_NAME IMPORTED_LOCATION)
  # Reuse partially defined thrift::thrift by ThriftConfig.cmake.
  if(NOT TARGET thrift::thrift)
    if(ARROW_THRIFT_USE_SHARED)
      add_library(thrift::thrift SHARED IMPORTED)
      if(CMAKE_IMPORT_LIBRARY_SUFFIX)
        set(ThriftAlt_IMPORTED_PROPERTY_NAME IMPORTED_IMPLIB)
      endif()
    else()
      add_library(thrift::thrift STATIC IMPORTED)
    endif()
  endif()
  set_target_properties(thrift::thrift
                        PROPERTIES ${ThriftAlt_IMPORTED_PROPERTY_NAME} "${ThriftAlt_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${ThriftAlt_INCLUDE_DIR}")
  if(WIN32 AND NOT MSVC_TOOLCHAIN)
    # We don't need this for Visual C++ because Thrift uses
    # "#pragma comment(lib, "Ws2_32.lib")" in
    # thrift/windows/config.h for Visual C++.
    set_target_properties(thrift::thrift PROPERTIES INTERFACE_LINK_LIBRARIES "ws2_32")
  endif()

  if(Thrift_COMPILER_FOUND)
    add_executable(thrift::compiler IMPORTED)
    set_target_properties(thrift::compiler PROPERTIES IMPORTED_LOCATION
                                                      "${THRIFT_COMPILER}")
  endif()
endif()
