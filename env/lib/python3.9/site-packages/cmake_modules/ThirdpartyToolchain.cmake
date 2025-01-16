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

include(ProcessorCount)
processorcount(NPROC)

# Accumulate all bundled targets and we will splice them together later as
# libarrow_bundled_dependencies.a so that third party libraries have something
# usable to create statically-linked builds with some BUNDLED dependencies,
# including allocators like jemalloc and mimalloc
set(ARROW_BUNDLED_STATIC_LIBS)

# Accumulate all system dependencies to provide suitable static link
# parameters to the third party libraries.
set(ARROW_SYSTEM_DEPENDENCIES)
set(ARROW_FLIGHT_SYSTEM_DEPENDENCIES)
set(ARROW_TESTING_SYSTEM_DEPENDENCIES)
set(PARQUET_SYSTEM_DEPENDENCIES)

# ----------------------------------------------------------------------
# Toolchain linkage options

set(ARROW_RE2_LINKAGE
    "static"
    CACHE STRING "How to link the re2 library. static|shared (default static)")

# ----------------------------------------------------------------------
# Resolve the dependencies

set(ARROW_THIRDPARTY_DEPENDENCIES
    absl
    AWSSDK
    Azure
    benchmark
    Boost
    Brotli
    BZip2
    c-ares
    gflags
    glog
    google_cloud_cpp_storage
    gRPC
    GTest
    jemalloc
    LLVM
    lz4
    nlohmann_json
    opentelemetry-cpp
    orc
    re2
    Protobuf
    RapidJSON
    Snappy
    Substrait
    Thrift
    ucx
    utf8proc
    xsimd
    ZLIB
    zstd)

# For backward compatibility. We use "BOOST_SOURCE" if "Boost_SOURCE"
# isn't specified and "BOOST_SOURCE" is specified.
# We renamed "BOOST" dependency name to "Boost" in 3.0.0 because
# upstreams (CMake and Boost) use "Boost" not "BOOST" as package name.
if("${Boost_SOURCE}" STREQUAL "" AND NOT "${BOOST_SOURCE}" STREQUAL "")
  set(Boost_SOURCE ${BOOST_SOURCE})
endif()

# For backward compatibility. We use "RE2_SOURCE" if "re2_SOURCE"
# isn't specified and "RE2_SOURCE" is specified.
# We renamed "RE2" dependency name to "re2" in 3.0.0 because
# upstream uses "re2" not "RE2" as package name.
if("${re2_SOURCE}" STREQUAL "" AND NOT "${RE2_SOURCE}" STREQUAL "")
  set(re2_SOURCE ${RE2_SOURCE})
endif()

# For backward compatibility. We use "ORC_SOURCE" if "orc_SOURCE"
# isn't specified and "ORC_SOURCE" is specified.
# We renamed "ORC" dependency name to "orc" in 15.0.0 because
# upstream uses "orc" not "ORC" as package name.
if("${orc_SOURCE}" STREQUAL "" AND NOT "${ORC_SOURCE}" STREQUAL "")
  set(orc_SOURCE ${ORC_SOURCE})
endif()

# For backward compatibility. We use "RE2_ROOT" if "re2_ROOT"
# isn't specified and "RE2_ROOT" is specified.
if("${re2_ROOT}" STREQUAL "" AND NOT "${RE2_ROOT}" STREQUAL "")
  set(re2_ROOT ${RE2_ROOT})
endif()

# For backward compatibility. We use "Lz4_SOURCE" if "lz4_SOURCE"
# isn't specified and "lz4_SOURCE" is specified.
# We renamed "Lz4" dependency name to "lz4" in 9.0.0 because
# upstream uses "lz4" not "Lz4" as package name.
if("${lz4_SOURCE}" STREQUAL "" AND NOT "${Lz4_SOURCE}" STREQUAL "")
  set(lz4_SOURCE ${Lz4_SOURCE})
endif()

# For backward compatibility. We use "GLOG_SOURCE" if "glog_SOURCE"
# isn't specified and "GLOG_SOURCE" is specified.
# We renamed "GLOG" dependency name to "glog" in 16.0.0 because
# upstream uses "glog" not "GLOG" as package name.
if("${glog_SOURCE}" STREQUAL "" AND NOT "${GLOG_SOURCE}" STREQUAL "")
  set(glog_SOURCE ${GLOG_SOURCE})
endif()

# For backward compatibility. We use bundled jemalloc by default.
if("${jemalloc_SOURCE}" STREQUAL "")
  set(jemalloc_SOURCE "BUNDLED")
endif()

message(STATUS "Using ${ARROW_DEPENDENCY_SOURCE} approach to find dependencies")

if(ARROW_DEPENDENCY_SOURCE STREQUAL "CONDA")
  if(MSVC)
    set(ARROW_PACKAGE_PREFIX "$ENV{CONDA_PREFIX}/Library")
  else()
    set(ARROW_PACKAGE_PREFIX $ENV{CONDA_PREFIX})
  endif()
  set(ARROW_ACTUAL_DEPENDENCY_SOURCE "SYSTEM")
  # GoogleTest provided by conda can't be used on macOS because it's
  # built with C++14. So we accept auto fallback only for GoogleTest.
  if("${GTest_SOURCE}" STREQUAL "")
    set(GTest_SOURCE "AUTO")
  endif()
  message(STATUS "Using CONDA_PREFIX for ARROW_PACKAGE_PREFIX: ${ARROW_PACKAGE_PREFIX}")
else()
  set(ARROW_ACTUAL_DEPENDENCY_SOURCE "${ARROW_DEPENDENCY_SOURCE}")
endif()

if(ARROW_PACKAGE_PREFIX)
  message(STATUS "Setting (unset) dependency *_ROOT variables: ${ARROW_PACKAGE_PREFIX}")
  set(ENV{PKG_CONFIG_PATH} "${ARROW_PACKAGE_PREFIX}/lib/pkgconfig/")

  if(NOT ENV{BOOST_ROOT})
    set(ENV{BOOST_ROOT} ${ARROW_PACKAGE_PREFIX})
  endif()
  if(NOT ENV{Boost_ROOT})
    set(ENV{Boost_ROOT} ${ARROW_PACKAGE_PREFIX})
  endif()
  if(NOT DEFINED OPENSSL_ROOT_DIR)
    set(OPENSSL_ROOT_DIR ${ARROW_PACKAGE_PREFIX})
  endif()
endif()

# For each dependency, set dependency source to global default, if unset
foreach(DEPENDENCY ${ARROW_THIRDPARTY_DEPENDENCIES})
  if("${${DEPENDENCY}_SOURCE}" STREQUAL "")
    set(${DEPENDENCY}_SOURCE ${ARROW_ACTUAL_DEPENDENCY_SOURCE})
    # If no ROOT was supplied and we have a global prefix, use it
    if(NOT ${DEPENDENCY}_ROOT AND ARROW_PACKAGE_PREFIX)
      set(${DEPENDENCY}_ROOT ${ARROW_PACKAGE_PREFIX})
    endif()
  endif()
endforeach()

macro(build_dependency DEPENDENCY_NAME)
  if("${DEPENDENCY_NAME}" STREQUAL "absl")
    build_absl()
  elseif("${DEPENDENCY_NAME}" STREQUAL "AWSSDK")
    build_awssdk()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Azure")
    build_azure_sdk()
  elseif("${DEPENDENCY_NAME}" STREQUAL "benchmark")
    build_benchmark()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Boost")
    build_boost()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Brotli")
    build_brotli()
  elseif("${DEPENDENCY_NAME}" STREQUAL "BZip2")
    build_bzip2()
  elseif("${DEPENDENCY_NAME}" STREQUAL "c-ares")
    build_cares()
  elseif("${DEPENDENCY_NAME}" STREQUAL "gflags")
    build_gflags()
  elseif("${DEPENDENCY_NAME}" STREQUAL "glog")
    build_glog()
  elseif("${DEPENDENCY_NAME}" STREQUAL "google_cloud_cpp_storage")
    build_google_cloud_cpp_storage()
  elseif("${DEPENDENCY_NAME}" STREQUAL "gRPC")
    build_grpc()
  elseif("${DEPENDENCY_NAME}" STREQUAL "GTest")
    build_gtest()
  elseif("${DEPENDENCY_NAME}" STREQUAL "jemalloc")
    build_jemalloc()
  elseif("${DEPENDENCY_NAME}" STREQUAL "lz4")
    build_lz4()
  elseif("${DEPENDENCY_NAME}" STREQUAL "nlohmann_json")
    build_nlohmann_json()
  elseif("${DEPENDENCY_NAME}" STREQUAL "opentelemetry-cpp")
    build_opentelemetry()
  elseif("${DEPENDENCY_NAME}" STREQUAL "orc")
    build_orc()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Protobuf")
    build_protobuf()
  elseif("${DEPENDENCY_NAME}" STREQUAL "RapidJSON")
    build_rapidjson()
  elseif("${DEPENDENCY_NAME}" STREQUAL "re2")
    build_re2()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Snappy")
    build_snappy()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Substrait")
    build_substrait()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Thrift")
    build_thrift()
  elseif("${DEPENDENCY_NAME}" STREQUAL "ucx")
    build_ucx()
  elseif("${DEPENDENCY_NAME}" STREQUAL "utf8proc")
    build_utf8proc()
  elseif("${DEPENDENCY_NAME}" STREQUAL "xsimd")
    build_xsimd()
  elseif("${DEPENDENCY_NAME}" STREQUAL "ZLIB")
    build_zlib()
  elseif("${DEPENDENCY_NAME}" STREQUAL "zstd")
    build_zstd()
  else()
    message(FATAL_ERROR "Unknown thirdparty dependency to build: ${DEPENDENCY_NAME}")
  endif()
endmacro()

function(provide_cmake_module MODULE_NAME ARROW_CMAKE_PACKAGE_NAME)
  set(module "${CMAKE_SOURCE_DIR}/cmake_modules/${MODULE_NAME}.cmake")
  if(EXISTS "${module}")
    message(STATUS "Providing CMake module for ${MODULE_NAME} as part of ${ARROW_CMAKE_PACKAGE_NAME} CMake package"
    )
    install(FILES "${module}"
            DESTINATION "${ARROW_CMAKE_DIR}/${ARROW_CMAKE_PACKAGE_NAME}")
  endif()
endfunction()

# Find modules are needed by the consumer in case of a static build, or if the
# linkage is PUBLIC or INTERFACE.
function(provide_find_module PACKAGE_NAME ARROW_CMAKE_PACKAGE_NAME)
  provide_cmake_module("Find${PACKAGE_NAME}" ${ARROW_CMAKE_PACKAGE_NAME})
endfunction()

macro(resolve_dependency DEPENDENCY_NAME)
  set(options)
  set(one_value_args
      ARROW_CMAKE_PACKAGE_NAME
      ARROW_PC_PACKAGE_NAME
      FORCE_ANY_NEWER_VERSION
      HAVE_ALT
      IS_RUNTIME_DEPENDENCY
      REQUIRED_VERSION
      USE_CONFIG)
  set(multi_value_args COMPONENTS PC_PACKAGE_NAMES)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()
  if("${ARG_IS_RUNTIME_DEPENDENCY}" STREQUAL "")
    set(ARG_IS_RUNTIME_DEPENDENCY TRUE)
  endif()

  if(ARG_HAVE_ALT)
    set(PACKAGE_NAME "${DEPENDENCY_NAME}Alt")
  else()
    set(PACKAGE_NAME ${DEPENDENCY_NAME})
  endif()
  set(FIND_PACKAGE_ARGUMENTS ${PACKAGE_NAME})
  if(ARG_REQUIRED_VERSION AND NOT ARG_FORCE_ANY_NEWER_VERSION)
    list(APPEND FIND_PACKAGE_ARGUMENTS ${ARG_REQUIRED_VERSION})
  endif()
  if(ARG_USE_CONFIG)
    list(APPEND FIND_PACKAGE_ARGUMENTS CONFIG)
  endif()
  if(ARG_COMPONENTS)
    list(APPEND FIND_PACKAGE_ARGUMENTS COMPONENTS ${ARG_COMPONENTS})
  endif()
  if(${DEPENDENCY_NAME}_SOURCE STREQUAL "AUTO")
    find_package(${FIND_PACKAGE_ARGUMENTS})
    set(COMPATIBLE ${${PACKAGE_NAME}_FOUND})
    if(COMPATIBLE
       AND ARG_FORCE_ANY_NEWER_VERSION
       AND ARG_REQUIRED_VERSION)
      if(${${PACKAGE_NAME}_VERSION} VERSION_LESS ${ARG_REQUIRED_VERSION})
        message(DEBUG "Couldn't find ${DEPENDENCY_NAME} >= ${ARG_REQUIRED_VERSION}")
        set(COMPATIBLE FALSE)
      endif()
    endif()
    if(COMPATIBLE)
      set(${DEPENDENCY_NAME}_SOURCE "SYSTEM")
    else()
      build_dependency(${DEPENDENCY_NAME})
      set(${DEPENDENCY_NAME}_SOURCE "BUNDLED")
    endif()
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "BUNDLED")
    build_dependency(${DEPENDENCY_NAME})
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM")
    find_package(${FIND_PACKAGE_ARGUMENTS} REQUIRED)
    if(ARG_FORCE_ANY_NEWER_VERSION AND ARG_REQUIRED_VERSION)
      if(${${PACKAGE_NAME}_VERSION} VERSION_LESS ${ARG_REQUIRED_VERSION})
        message(FATAL_ERROR "Couldn't find ${DEPENDENCY_NAME} >= ${ARG_REQUIRED_VERSION}")
      endif()
    endif()
  endif()
  if(${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM" AND ARG_IS_RUNTIME_DEPENDENCY)
    if(NOT ARG_ARROW_CMAKE_PACKAGE_NAME)
      set(ARG_ARROW_CMAKE_PACKAGE_NAME "Arrow")
    endif()
    # ArrowFlight -> _Arrow_Flight
    string(REGEX REPLACE "([A-Z])" "_\\1" ARG_ARROW_CMAKE_PACKAGE_NAME_SNAKE
                         ${ARG_ARROW_CMAKE_PACKAGE_NAME})
    # _Arrow_Flight -> Arrow_Flight
    string(SUBSTRING ${ARG_ARROW_CMAKE_PACKAGE_NAME_SNAKE} 1 -1
                     ARG_ARROW_CMAKE_PACKAGE_NAME_SNAKE)
    # Arrow_Flight -> ARROW_FLIGHT
    string(TOUPPER ${ARG_ARROW_CMAKE_PACKAGE_NAME_SNAKE}
                   ARG_ARROW_CMAKE_PACKAGE_NAME_UPPER_SNAKE)
    provide_find_module(${PACKAGE_NAME} ${ARG_ARROW_CMAKE_PACKAGE_NAME})
    list(APPEND ${ARG_ARROW_CMAKE_PACKAGE_NAME_UPPER_SNAKE}_SYSTEM_DEPENDENCIES
         ${PACKAGE_NAME})
    if(NOT ARG_ARROW_PC_PACKAGE_NAME)
      set(ARG_ARROW_PC_PACKAGE_NAME "arrow")
    endif()
    # arrow-flight -> arrow_flight
    string(REPLACE "-" "_" ARG_ARROW_PC_PACKAGE_NAME_SNAKE ${ARG_ARROW_PC_PACKAGE_NAME})
    # arrow_flight -> ARROW_FLIGHT
    string(TOUPPER ${ARG_ARROW_PC_PACKAGE_NAME_SNAKE}
                   ARG_ARROW_PC_PACKAGE_NAME_UPPER_SNAKE)
    if(ARROW_BUILD_STATIC)
      find_package(PkgConfig QUIET)
      foreach(ARG_PC_PACKAGE_NAME ${ARG_PC_PACKAGE_NAMES})
        pkg_check_modules(${ARG_PC_PACKAGE_NAME}_PC
                          ${ARG_PC_PACKAGE_NAME}
                          NO_CMAKE_PATH
                          NO_CMAKE_ENVIRONMENT_PATH
                          QUIET)
        set(RESOLVE_DEPENDENCY_PC_PACKAGE
            "pkg-config package for ${ARG_PC_PACKAGE_NAME} ")
        string(APPEND RESOLVE_DEPENDENCY_PC_PACKAGE
               "that is used by ${ARG_ARROW_PC_PACKAGE_NAME} for static link")
        if(${${ARG_PC_PACKAGE_NAME}_PC_FOUND})
          message(STATUS "Using ${RESOLVE_DEPENDENCY_PC_PACKAGE}")
          string(APPEND ${ARG_ARROW_PC_PACKAGE_NAME_UPPER_SNAKE}_PC_REQUIRES_PRIVATE
                 " ${ARG_PC_PACKAGE_NAME}")
        else()
          message(STATUS "${RESOLVE_DEPENDENCY_PC_PACKAGE} isn't found")
        endif()
      endforeach()
    endif()
  endif()
endmacro()

# ----------------------------------------------------------------------
# Thirdparty versions, environment variables, source URLs

set(THIRDPARTY_DIR "${arrow_SOURCE_DIR}/thirdparty")

add_library(arrow::flatbuffers INTERFACE IMPORTED)
target_include_directories(arrow::flatbuffers
                           INTERFACE "${THIRDPARTY_DIR}/flatbuffers/include")

# ----------------------------------------------------------------------
# Some EP's require other EP's

if(PARQUET_REQUIRE_ENCRYPTION)
  set(ARROW_JSON ON)
endif()

if(ARROW_WITH_OPENTELEMETRY)
  set(ARROW_WITH_NLOHMANN_JSON ON)
  set(ARROW_WITH_PROTOBUF ON)
endif()

if(ARROW_THRIFT)
  set(ARROW_WITH_ZLIB ON)
endif()

if(ARROW_PARQUET)
  set(ARROW_WITH_THRIFT ON)
endif()

if(ARROW_FLIGHT)
  set(ARROW_WITH_GRPC ON)
endif()

if(ARROW_WITH_GRPC)
  set(ARROW_WITH_RE2 ON)
  set(ARROW_WITH_ZLIB ON)
endif()

if(ARROW_GCS)
  set(ARROW_WITH_GOOGLE_CLOUD_CPP ON)
  set(ARROW_WITH_NLOHMANN_JSON ON)
  set(ARROW_WITH_ZLIB ON)
endif()

if(ARROW_AZURE)
  set(ARROW_WITH_AZURE_SDK ON)
endif()

if(ARROW_JSON)
  set(ARROW_WITH_RAPIDJSON ON)
endif()

if(ARROW_ORC OR ARROW_FLIGHT)
  set(ARROW_WITH_PROTOBUF ON)
endif()

if(ARROW_SUBSTRAIT)
  set(ARROW_WITH_PROTOBUF ON)
endif()

if(ARROW_S3)
  set(ARROW_WITH_ZLIB ON)
endif()

if((NOT ARROW_COMPUTE) AND (NOT ARROW_GANDIVA))
  set(ARROW_WITH_UTF8PROC OFF)
endif()

if((NOT ARROW_COMPUTE)
   AND (NOT ARROW_GANDIVA)
   AND (NOT ARROW_WITH_GRPC))
  set(ARROW_WITH_RE2 OFF)
endif()

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds, which also can be used to configure
# offline builds
# Note: We should not use the Apache dist server for build dependencies

macro(set_urls URLS)
  set(${URLS} ${ARGN})
endmacro()

# Read toolchain versions from cpp/thirdparty/versions.txt
file(STRINGS "${THIRDPARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach(_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
  # Exclude comments
  if(NOT ((_VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_VERSION=")
          OR (_VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_CHECKSUM=")))
    continue()
  endif()

  string(REGEX MATCH "^[^=]*" _VARIABLE_NAME ${_VERSION_ENTRY})
  string(REPLACE "${_VARIABLE_NAME}=" "" _VARIABLE_VALUE ${_VERSION_ENTRY})

  # Skip blank or malformed lines
  if(_VARIABLE_VALUE STREQUAL "")
    continue()
  endif()

  # For debugging
  message(STATUS "${_VARIABLE_NAME}: ${_VARIABLE_VALUE}")

  set(${_VARIABLE_NAME} ${_VARIABLE_VALUE})
endforeach()

set(THIRDPARTY_MIRROR_URL "https://apache.jfrog.io/artifactory/arrow/thirdparty/7.0.0")

if(DEFINED ENV{ARROW_ABSL_URL})
  set(ABSL_SOURCE_URL "$ENV{ARROW_ABSL_URL}")
else()
  set_urls(ABSL_SOURCE_URL
           "https://github.com/abseil/abseil-cpp/archive/${ARROW_ABSL_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_AUTH_URL})
  set(AWS_C_AUTH_SOURCE_URL "$ENV{ARROW_AWS_C_AUTH_URL}")
else()
  set_urls(AWS_C_AUTH_SOURCE_URL
           "https://github.com/awslabs/aws-c-auth/archive/${ARROW_AWS_C_AUTH_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_CAL_URL})
  set(AWS_C_CAL_SOURCE_URL "$ENV{ARROW_AWS_C_CAL_URL}")
else()
  set_urls(AWS_C_CAL_SOURCE_URL
           "https://github.com/awslabs/aws-c-cal/archive/${ARROW_AWS_C_CAL_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_COMMON_URL})
  set(AWS_C_COMMON_SOURCE_URL "$ENV{ARROW_AWS_C_COMMON_URL}")
else()
  set_urls(AWS_C_COMMON_SOURCE_URL
           "https://github.com/awslabs/aws-c-common/archive/${ARROW_AWS_C_COMMON_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_COMPRESSION_URL})
  set(AWS_C_COMPRESSION_SOURCE_URL "$ENV{ARROW_AWS_C_COMPRESSION_URL}")
else()
  set_urls(AWS_C_COMPRESSION_SOURCE_URL
           "https://github.com/awslabs/aws-c-compression/archive/${ARROW_AWS_C_COMPRESSION_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_EVENT_STREAM_URL})
  set(AWS_C_EVENT_STREAM_SOURCE_URL "$ENV{ARROW_AWS_C_EVENT_STREAM_URL}")
else()
  set_urls(AWS_C_EVENT_STREAM_SOURCE_URL
           "https://github.com/awslabs/aws-c-event-stream/archive/${ARROW_AWS_C_EVENT_STREAM_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_HTTP_URL})
  set(AWS_C_HTTP_SOURCE_URL "$ENV{ARROW_AWS_C_HTTP_URL}")
else()
  set_urls(AWS_C_HTTP_SOURCE_URL
           "https://github.com/awslabs/aws-c-http/archive/${ARROW_AWS_C_HTTP_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_IO_URL})
  set(AWS_C_IO_SOURCE_URL "$ENV{ARROW_AWS_C_IO_URL}")
else()
  set_urls(AWS_C_IO_SOURCE_URL
           "https://github.com/awslabs/aws-c-io/archive/${ARROW_AWS_C_IO_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_MQTT_URL})
  set(AWS_C_MQTT_SOURCE_URL "$ENV{ARROW_AWS_C_MQTT_URL}")
else()
  set_urls(AWS_C_MQTT_SOURCE_URL
           "https://github.com/awslabs/aws-c-mqtt/archive/${ARROW_AWS_C_MQTT_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_S3_URL})
  set(AWS_C_S3_SOURCE_URL "$ENV{ARROW_AWS_C_S3_URL}")
else()
  set_urls(AWS_C_S3_SOURCE_URL
           "https://github.com/awslabs/aws-c-s3/archive/${ARROW_AWS_C_S3_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_C_SDKUTILS_URL})
  set(AWS_C_SDKUTILS_SOURCE_URL "$ENV{ARROW_AWS_C_SDKUTILS_URL}")
else()
  set_urls(AWS_C_SDKUTILS_SOURCE_URL
           "https://github.com/awslabs/aws-c-sdkutils/archive/${ARROW_AWS_C_SDKUTILS_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_CHECKSUMS_URL})
  set(AWS_CHECKSUMS_SOURCE_URL "$ENV{ARROW_AWS_CHECKSUMS_URL}")
else()
  set_urls(AWS_CHECKSUMS_SOURCE_URL
           "https://github.com/awslabs/aws-checksums/archive/${ARROW_AWS_CHECKSUMS_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_CRT_CPP_URL})
  set(AWS_CRT_CPP_SOURCE_URL "$ENV{ARROW_AWS_CRT_CPP_URL}")
else()
  set_urls(AWS_CRT_CPP_SOURCE_URL
           "https://github.com/awslabs/aws-crt-cpp/archive/${ARROW_AWS_CRT_CPP_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWS_LC_URL})
  set(AWS_LC_SOURCE_URL "$ENV{ARROW_AWS_LC_URL}")
else()
  set_urls(AWS_LC_SOURCE_URL
           "https://github.com/awslabs/aws-lc/archive/${ARROW_AWS_LC_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_AWSSDK_URL})
  set(AWSSDK_SOURCE_URL "$ENV{ARROW_AWSSDK_URL}")
else()
  set_urls(AWSSDK_SOURCE_URL
           "https://github.com/aws/aws-sdk-cpp/archive/${ARROW_AWSSDK_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/aws-sdk-cpp-${ARROW_AWSSDK_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_AZURE_SDK_URL})
  set(ARROW_AZURE_SDK_URL "$ENV{ARROW_AZURE_SDK_URL}")
else()
  set_urls(ARROW_AZURE_SDK_URL
           "https://github.com/Azure/azure-sdk-for-cpp/archive/${ARROW_AZURE_SDK_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_BOOST_URL})
  set(BOOST_SOURCE_URL "$ENV{ARROW_BOOST_URL}")
else()
  string(REPLACE "." "_" ARROW_BOOST_BUILD_VERSION_UNDERSCORES
                 ${ARROW_BOOST_BUILD_VERSION})
  set_urls(BOOST_SOURCE_URL
           # These are trimmed boost bundles we maintain.
           # See cpp/build-support/trim-boost.sh
           # FIXME(ARROW-6407) automate uploading this archive to ensure it reflects
           # our currently used packages and doesn't fall out of sync with
           # ${ARROW_BOOST_BUILD_VERSION_UNDERSCORES}
           "${THIRDPARTY_MIRROR_URL}/boost_${ARROW_BOOST_BUILD_VERSION_UNDERSCORES}.tar.gz"
           "https://boostorg.jfrog.io/artifactory/main/release/${ARROW_BOOST_BUILD_VERSION}/source/boost_${ARROW_BOOST_BUILD_VERSION_UNDERSCORES}.tar.gz"
           "https://sourceforge.net/projects/boost/files/boost/${ARROW_BOOST_BUILD_VERSION}/boost_${ARROW_BOOST_BUILD_VERSION_UNDERSCORES}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_BROTLI_URL})
  set(BROTLI_SOURCE_URL "$ENV{ARROW_BROTLI_URL}")
else()
  set_urls(BROTLI_SOURCE_URL
           "https://github.com/google/brotli/archive/${ARROW_BROTLI_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/brotli-${ARROW_BROTLI_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_BZIP2_URL})
  set(ARROW_BZIP2_SOURCE_URL "$ENV{ARROW_BZIP2_URL}")
else()
  set_urls(ARROW_BZIP2_SOURCE_URL
           "https://sourceware.org/pub/bzip2/bzip2-${ARROW_BZIP2_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/bzip2-${ARROW_BZIP2_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_CARES_URL})
  set(CARES_SOURCE_URL "$ENV{ARROW_CARES_URL}")
else()
  string(REPLACE "." "_" ARROW_CARES_BUILD_VERSION_UNDERSCORES
                 ${ARROW_CARES_BUILD_VERSION})
  set_urls(CARES_SOURCE_URL
           "https://github.com/c-ares/c-ares/releases/download/cares-${ARROW_CARES_BUILD_VERSION_UNDERSCORES}/c-ares-${ARROW_CARES_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/cares-${ARROW_CARES_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_CRC32C_URL})
  set(CRC32C_SOURCE_URL "$ENV{ARROW_CRC32C_URL}")
else()
  set_urls(CRC32C_SOURCE_URL
           "https://github.com/google/crc32c/archive/${ARROW_CRC32C_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_GBENCHMARK_URL})
  set(GBENCHMARK_SOURCE_URL "$ENV{ARROW_GBENCHMARK_URL}")
else()
  set_urls(GBENCHMARK_SOURCE_URL
           "https://github.com/google/benchmark/archive/${ARROW_GBENCHMARK_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/gbenchmark-${ARROW_GBENCHMARK_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GFLAGS_URL})
  set(GFLAGS_SOURCE_URL "$ENV{ARROW_GFLAGS_URL}")
else()
  set_urls(GFLAGS_SOURCE_URL
           "https://github.com/gflags/gflags/archive/${ARROW_GFLAGS_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/gflags-${ARROW_GFLAGS_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GLOG_URL})
  set(GLOG_SOURCE_URL "$ENV{ARROW_GLOG_URL}")
else()
  set_urls(GLOG_SOURCE_URL
           "https://github.com/google/glog/archive/${ARROW_GLOG_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/glog-${ARROW_GLOG_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GOOGLE_CLOUD_CPP_URL})
  set(google_cloud_cpp_storage_SOURCE_URL "$ENV{ARROW_GOOGLE_CLOUD_CPP_URL}")
else()
  set_urls(google_cloud_cpp_storage_SOURCE_URL
           "https://github.com/googleapis/google-cloud-cpp/archive/${ARROW_GOOGLE_CLOUD_CPP_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/google-cloud-cpp-${ARROW_GOOGLE_CLOUD_CPP_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_GRPC_URL})
  set(GRPC_SOURCE_URL "$ENV{ARROW_GRPC_URL}")
else()
  set_urls(GRPC_SOURCE_URL
           "https://github.com/grpc/grpc/archive/${ARROW_GRPC_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/grpc-${ARROW_GRPC_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GTEST_URL})
  set(GTEST_SOURCE_URL "$ENV{ARROW_GTEST_URL}")
else()
  set_urls(GTEST_SOURCE_URL
           "https://github.com/google/googletest/archive/release-${ARROW_GTEST_BUILD_VERSION}.tar.gz"
           "https://chromium.googlesource.com/external/github.com/google/googletest/+archive/release-${ARROW_GTEST_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/gtest-${ARROW_GTEST_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_JEMALLOC_URL})
  set(JEMALLOC_SOURCE_URL "$ENV{ARROW_JEMALLOC_URL}")
else()
  set_urls(JEMALLOC_SOURCE_URL
           "https://github.com/jemalloc/jemalloc/releases/download/${ARROW_JEMALLOC_BUILD_VERSION}/jemalloc-${ARROW_JEMALLOC_BUILD_VERSION}.tar.bz2"
           "${THIRDPARTY_MIRROR_URL}/jemalloc-${ARROW_JEMALLOC_BUILD_VERSION}.tar.bz2")
endif()

if(DEFINED ENV{ARROW_MIMALLOC_URL})
  set(MIMALLOC_SOURCE_URL "$ENV{ARROW_MIMALLOC_URL}")
else()
  set_urls(MIMALLOC_SOURCE_URL
           "https://github.com/microsoft/mimalloc/archive/${ARROW_MIMALLOC_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/mimalloc-${ARROW_MIMALLOC_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_NLOHMANN_JSON_URL})
  set(NLOHMANN_JSON_SOURCE_URL "$ENV{ARROW_NLOHMANN_JSON_URL}")
else()
  set_urls(NLOHMANN_JSON_SOURCE_URL
           "https://github.com/nlohmann/json/archive/${ARROW_NLOHMANN_JSON_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_LZ4_URL})
  set(LZ4_SOURCE_URL "$ENV{ARROW_LZ4_URL}")
else()
  set_urls(LZ4_SOURCE_URL
           "https://github.com/lz4/lz4/archive/${ARROW_LZ4_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/lz4-${ARROW_LZ4_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_ORC_URL})
  set(ORC_SOURCE_URL "$ENV{ARROW_ORC_URL}")
else()
  set_urls(ORC_SOURCE_URL
           "https://www.apache.org/dyn/closer.cgi?action=download&filename=/orc/orc-${ARROW_ORC_BUILD_VERSION}/orc-${ARROW_ORC_BUILD_VERSION}.tar.gz"
           "https://downloads.apache.org/orc/orc-${ARROW_ORC_BUILD_VERSION}/orc-${ARROW_ORC_BUILD_VERSION}.tar.gz"
           "https://github.com/apache/orc/archive/rel/release-${ARROW_ORC_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_OPENTELEMETRY_URL})
  set(OPENTELEMETRY_SOURCE_URL "$ENV{ARROW_OPENTELEMETRY_URL}")
else()
  # TODO: add mirror
  set_urls(OPENTELEMETRY_SOURCE_URL
           "https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/${ARROW_OPENTELEMETRY_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_OPENTELEMETRY_PROTO_URL})
  set(OPENTELEMETRY_PROTO_SOURCE_URL "$ENV{ARROW_OPENTELEMETRY_PROTO_URL}")
else()
  # TODO: add mirror
  # N.B. upstream pins to particular commits, not tags
  set_urls(OPENTELEMETRY_PROTO_SOURCE_URL
           "https://github.com/open-telemetry/opentelemetry-proto/archive/${ARROW_OPENTELEMETRY_PROTO_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_PROTOBUF_URL})
  set(PROTOBUF_SOURCE_URL "$ENV{ARROW_PROTOBUF_URL}")
else()
  string(SUBSTRING ${ARROW_PROTOBUF_BUILD_VERSION} 1 -1
                   ARROW_PROTOBUF_STRIPPED_BUILD_VERSION)
  # strip the leading `v`
  set_urls(PROTOBUF_SOURCE_URL
           "https://github.com/protocolbuffers/protobuf/releases/download/${ARROW_PROTOBUF_BUILD_VERSION}/protobuf-all-${ARROW_PROTOBUF_STRIPPED_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/protobuf-${ARROW_PROTOBUF_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_RE2_URL})
  set(RE2_SOURCE_URL "$ENV{ARROW_RE2_URL}")
else()
  set_urls(RE2_SOURCE_URL
           "https://github.com/google/re2/archive/${ARROW_RE2_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/re2-${ARROW_RE2_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_RAPIDJSON_URL})
  set(RAPIDJSON_SOURCE_URL "$ENV{ARROW_RAPIDJSON_URL}")
else()
  set_urls(RAPIDJSON_SOURCE_URL
           "https://github.com/miloyip/rapidjson/archive/${ARROW_RAPIDJSON_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/rapidjson-${ARROW_RAPIDJSON_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_S2N_TLS_URL})
  set(S2N_TLS_SOURCE_URL "$ENV{ARROW_S2N_TLS_URL}")
else()
  set_urls(S2N_TLS_SOURCE_URL
           "https://github.com/aws/s2n-tls/archive/${ARROW_S2N_TLS_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_SNAPPY_URL})
  set(SNAPPY_SOURCE_URL "$ENV{ARROW_SNAPPY_URL}")
else()
  set_urls(SNAPPY_SOURCE_URL
           "https://github.com/google/snappy/archive/${ARROW_SNAPPY_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/snappy-${ARROW_SNAPPY_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_SUBSTRAIT_URL})
  set(SUBSTRAIT_SOURCE_URL "$ENV{ARROW_SUBSTRAIT_URL}")
else()
  set_urls(SUBSTRAIT_SOURCE_URL
           "https://github.com/substrait-io/substrait/archive/${ARROW_SUBSTRAIT_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_THRIFT_URL})
  set(THRIFT_SOURCE_URL "$ENV{ARROW_THRIFT_URL}")
else()
  set_urls(THRIFT_SOURCE_URL
           "https://www.apache.org/dyn/closer.cgi?action=download&filename=/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://downloads.apache.org/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://apache.claz.org/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://apache.cs.utah.edu/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://apache.mirrors.lucidnetworks.net/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://apache.osuosl.org/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://ftp.wayne.edu/apache/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://mirror.olnevhost.net/pub/apache/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://mirrors.gigenet.com/apache/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://mirrors.koehn.com/apache/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://mirrors.ocf.berkeley.edu/apache/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://mirrors.sonic.net/apache/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "https://us.mirrors.quenda.co/apache/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_UCX_URL})
  set(ARROW_UCX_SOURCE_URL "$ENV{ARROW_UCX_URL}")
else()
  set_urls(ARROW_UCX_SOURCE_URL
           "https://github.com/openucx/ucx/archive/v${ARROW_UCX_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_UTF8PROC_URL})
  set(ARROW_UTF8PROC_SOURCE_URL "$ENV{ARROW_UTF8PROC_URL}")
else()
  set_urls(ARROW_UTF8PROC_SOURCE_URL
           "https://github.com/JuliaStrings/utf8proc/archive/${ARROW_UTF8PROC_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_XSIMD_URL})
  set(XSIMD_SOURCE_URL "$ENV{ARROW_XSIMD_URL}")
else()
  set_urls(XSIMD_SOURCE_URL
           "https://github.com/xtensor-stack/xsimd/archive/${ARROW_XSIMD_BUILD_VERSION}.tar.gz"
  )
endif()

if(DEFINED ENV{ARROW_ZLIB_URL})
  set(ZLIB_SOURCE_URL "$ENV{ARROW_ZLIB_URL}")
else()
  set_urls(ZLIB_SOURCE_URL
           "https://zlib.net/fossils/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_ZSTD_URL})
  set(ZSTD_SOURCE_URL "$ENV{ARROW_ZSTD_URL}")
else()
  set_urls(ZSTD_SOURCE_URL
           "https://github.com/facebook/zstd/releases/download/v${ARROW_ZSTD_BUILD_VERSION}/zstd-${ARROW_ZSTD_BUILD_VERSION}.tar.gz"
  )
endif()

# ----------------------------------------------------------------------
# ExternalProject options

set(EP_LIST_SEPARATOR "|")
set(EP_COMMON_OPTIONS LIST_SEPARATOR ${EP_LIST_SEPARATOR})

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS}")
if(NOT MSVC_TOOLCHAIN)
  # Set -fPIC on all external projects
  string(APPEND EP_CXX_FLAGS " -fPIC")
  string(APPEND EP_C_FLAGS " -fPIC")
endif()

# We pass MSVC runtime related options via
# CMAKE_${LANG}_FLAGS_${CONFIG} explicitly because external projects
# may not require CMake 3.15 or later. If an external project doesn't
# require CMake 3.15 or later, CMAKE_MSVC_RUNTIME_LIBRARY is ignored.
# If CMAKE_MSVC_RUNTIME_LIBRARY is ignored, an external project may
# use different MSVC runtime. For example, Apache Arrow C++ uses /MTd
# (multi threaded debug) but an external project uses /MT (multi
# threaded release). It causes an link error.
foreach(CONFIG DEBUG MINSIZEREL RELEASE RELWITHDEBINFO)
  set(EP_CXX_FLAGS_${CONFIG} "${CMAKE_CXX_FLAGS_${CONFIG}}")
  set(EP_C_FLAGS_${CONFIG} "${CMAKE_C_FLAGS_${CONFIG}}")
  if(CONFIG STREQUAL DEBUG)
    set(EP_MSVC_RUNTIME_LIBRARY MultiThreadedDebugDLL)
  else()
    set(EP_MSVC_RUNTIME_LIBRARY MultiThreadedDLL)
  endif()
  string(APPEND EP_CXX_FLAGS_${CONFIG}
         " ${CMAKE_CXX_COMPILE_OPTIONS_MSVC_RUNTIME_LIBRARY_${EP_MSVC_RUNTIME_LIBRARY}}")
  string(APPEND EP_C_FLAGS_${CONFIG}
         " ${CMAKE_C_COMPILE_OPTIONS_MSVC_RUNTIME_LIBRARY_${EP_MSVC_RUNTIME_LIBRARY}}")
endforeach()
if(MSVC_TOOLCHAIN)
  string(REPLACE "/WX" "" EP_CXX_FLAGS_DEBUG "${EP_CXX_FLAGS_DEBUG}")
  string(REPLACE "/WX" "" EP_C_FLAGS_DEBUG "${EP_C_FLAGS_DEBUG}")
else()
  string(APPEND EP_CXX_FLAGS_DEBUG " -Wno-error")
  string(APPEND EP_C_FLAGS_DEBUG " -Wno-error")
endif()

# CC/CXX environment variables are captured on the first invocation of the
# builder (e.g make or ninja) instead of when CMake is invoked into to build
# directory. This leads to issues if the variables are exported in a subshell
# and the invocation of make/ninja is in distinct subshell without the same
# environment (CC/CXX).
set(EP_C_COMPILER "${CMAKE_C_COMPILER}")
if(NOT CMAKE_VERSION VERSION_LESS 3.19)
  if(CMAKE_C_COMPILER_ARG1)
    separate_arguments(EP_C_COMPILER_ARGS NATIVE_COMMAND "${CMAKE_C_COMPILER_ARG1}")
    list(APPEND EP_C_COMPILER ${EP_C_COMPILER_ARGS})
  endif()
  string(REPLACE ";" ${EP_LIST_SEPARATOR} EP_C_COMPILER "${EP_C_COMPILER}")
endif()
set(EP_CXX_COMPILER "${CMAKE_CXX_COMPILER}")
if(NOT CMAKE_VERSION VERSION_LESS 3.19)
  if(CMAKE_CXX_COMPILER_ARG1)
    separate_arguments(EP_CXX_COMPILER_ARGS NATIVE_COMMAND "${CMAKE_CXX_COMPILER_ARG1}")
    list(APPEND EP_CXX_COMPILER ${EP_CXX_COMPILER_ARGS})
  endif()
  string(REPLACE ";" ${EP_LIST_SEPARATOR} EP_CXX_COMPILER "${EP_CXX_COMPILER}")
endif()
set(EP_COMMON_TOOLCHAIN "-DCMAKE_C_COMPILER=${EP_C_COMPILER}"
                        "-DCMAKE_CXX_COMPILER=${EP_CXX_COMPILER}")

if(CMAKE_AR)
  # Ensure using absolute path.
  find_program(EP_CMAKE_AR ${CMAKE_AR} REQUIRED)
  list(APPEND EP_COMMON_TOOLCHAIN -DCMAKE_AR=${EP_CMAKE_AR})
endif()

# RANLIB isn't used for MSVC
if(NOT MSVC)
  if(CMAKE_RANLIB)
    # Ensure using absolute path.
    find_program(EP_CMAKE_RANLIB ${CMAKE_RANLIB} REQUIRED)
    list(APPEND EP_COMMON_TOOLCHAIN -DCMAKE_RANLIB=${EP_CMAKE_RANLIB})
  endif()
endif()

# External projects are still able to override the following declarations.
# cmake command line will favor the last defined variable when a duplicate is
# encountered. This requires that `EP_COMMON_CMAKE_ARGS` is always the first
# argument.
set(EP_COMMON_CMAKE_ARGS
    ${EP_COMMON_TOOLCHAIN}
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_TESTING=OFF
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
    -DCMAKE_CXX_FLAGS_DEBUG=${EP_CXX_FLAGS_DEBUG}
    -DCMAKE_CXX_FLAGS_MISIZEREL=${EP_CXX_FLAGS_MINSIZEREL}
    -DCMAKE_CXX_FLAGS_RELEASE=${EP_CXX_FLAGS_RELEASE}
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${EP_CXX_FLAGS_RELWITHDEBINFO}
    -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
    -DCMAKE_C_FLAGS=${EP_C_FLAGS}
    -DCMAKE_C_FLAGS_DEBUG=${EP_C_FLAGS_DEBUG}
    -DCMAKE_C_FLAGS_MISIZEREL=${EP_C_FLAGS_MINSIZEREL}
    -DCMAKE_C_FLAGS_RELEASE=${EP_C_FLAGS_RELEASE}
    -DCMAKE_C_FLAGS_RELWITHDEBINFO=${EP_C_FLAGS_RELWITHDEBINFO}
    -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=${CMAKE_EXPORT_NO_PACKAGE_REGISTRY}
    -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=${CMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY}
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_OSX_SYSROOT=${CMAKE_OSX_SYSROOT}
    -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE})

# if building with a toolchain file, pass that through
if(CMAKE_TOOLCHAIN_FILE)
  list(APPEND EP_COMMON_CMAKE_ARGS -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
endif()

# and crosscompiling emulator (for try_run() )
if(CMAKE_CROSSCOMPILING_EMULATOR)
  string(REPLACE ";" ${EP_LIST_SEPARATOR} EP_CMAKE_CROSSCOMPILING_EMULATOR
                 "${CMAKE_CROSSCOMPILING_EMULATOR}")
  list(APPEND EP_COMMON_CMAKE_ARGS
       -DCMAKE_CROSSCOMPILING_EMULATOR=${EP_CMAKE_CROSSCOMPILING_EMULATOR})
endif()

if(CMAKE_PROJECT_INCLUDE)
  list(APPEND EP_COMMON_CMAKE_ARGS -DCMAKE_PROJECT_INCLUDE=${CMAKE_PROJECT_INCLUDE})
endif()

# Enable s/ccache if set by parent.
if(CMAKE_C_COMPILER_LAUNCHER AND CMAKE_CXX_COMPILER_LAUNCHER)
  list(APPEND EP_COMMON_CMAKE_ARGS
       -DCMAKE_C_COMPILER_LAUNCHER=${CMAKE_C_COMPILER_LAUNCHER}
       -DCMAKE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER})
endif()

if(NOT ARROW_VERBOSE_THIRDPARTY_BUILD)
  list(APPEND
       EP_COMMON_OPTIONS
       LOG_CONFIGURE
       1
       LOG_BUILD
       1
       LOG_INSTALL
       1
       LOG_DOWNLOAD
       1
       LOG_OUTPUT_ON_FAILURE
       1)
  set(Boost_DEBUG FALSE)
else()
  set(Boost_DEBUG TRUE)
endif()

# Ensure that a default make is set
if("${MAKE}" STREQUAL "")
  if(NOT MSVC)
    find_program(MAKE make)
  endif()
endif()

# Args for external projects using make.
set(MAKE_BUILD_ARGS "-j${NPROC}")

include(FetchContent)
set(FC_DECLARE_COMMON_OPTIONS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
  list(APPEND FC_DECLARE_COMMON_OPTIONS EXCLUDE_FROM_ALL TRUE)
endif()

macro(prepare_fetchcontent)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_STATIC_LIBS ON)
  set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY TRUE)
  set(CMAKE_MACOSX_RPATH ${ARROW_INSTALL_NAME_RPATH})
  if(MSVC)
    string(REPLACE "/WX" "" CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")
    string(REPLACE "/WX" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
  else()
    string(APPEND CMAKE_C_FLAGS_DEBUG " -Wno-error")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " -Wno-error")
  endif()
endmacro()

# ----------------------------------------------------------------------
# Find pthreads

if(ARROW_ENABLE_THREADING)
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
endif()

# ----------------------------------------------------------------------
# Add Boost dependencies (code adapted from Apache Kudu)

macro(build_boost)
  set(BOOST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/boost_ep-prefix/src/boost_ep")

  # This is needed by the thrift_ep build
  set(BOOST_ROOT ${BOOST_PREFIX})
  set(Boost_INCLUDE_DIR "${BOOST_PREFIX}")

  if(ARROW_BOOST_REQUIRE_LIBRARY)
    set(BOOST_LIB_DIR "${BOOST_PREFIX}/stage/lib")
    set(BOOST_BUILD_LINK "static")
    if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
      set(BOOST_BUILD_VARIANT "debug")
    else()
      set(BOOST_BUILD_VARIANT "release")
    endif()
    if(MSVC)
      set(BOOST_CONFIGURE_COMMAND ".\\\\bootstrap.bat")
    else()
      set(BOOST_CONFIGURE_COMMAND "./bootstrap.sh")
    endif()

    set(BOOST_BUILD_WITH_LIBRARIES "filesystem" "system")
    string(REPLACE ";" "," BOOST_CONFIGURE_LIBRARIES "${BOOST_BUILD_WITH_LIBRARIES}")
    list(APPEND BOOST_CONFIGURE_COMMAND "--prefix=${BOOST_PREFIX}"
         "--with-libraries=${BOOST_CONFIGURE_LIBRARIES}")
    set(BOOST_BUILD_COMMAND "./b2" "-j${NPROC}" "link=${BOOST_BUILD_LINK}"
                            "variant=${BOOST_BUILD_VARIANT}")
    if(MSVC)
      string(REGEX REPLACE "([0-9])$" ".\\1" BOOST_TOOLSET_MSVC_VERSION
                           ${MSVC_TOOLSET_VERSION})
      list(APPEND BOOST_BUILD_COMMAND "toolset=msvc-${BOOST_TOOLSET_MSVC_VERSION}")
      set(BOOST_BUILD_WITH_LIBRARIES_MSVC)
      foreach(_BOOST_LIB ${BOOST_BUILD_WITH_LIBRARIES})
        list(APPEND BOOST_BUILD_WITH_LIBRARIES_MSVC "--with-${_BOOST_LIB}")
      endforeach()
      list(APPEND BOOST_BUILD_COMMAND ${BOOST_BUILD_WITH_LIBRARIES_MSVC})
    else()
      list(APPEND BOOST_BUILD_COMMAND "cxxflags=-fPIC")
    endif()

    if(MSVC)
      string(REGEX
             REPLACE "^([0-9]+)\\.([0-9]+)\\.[0-9]+$" "\\1_\\2"
                     ARROW_BOOST_BUILD_VERSION_NO_MICRO_UNDERSCORE
                     ${ARROW_BOOST_BUILD_VERSION})
      set(BOOST_LIBRARY_SUFFIX "-vc${MSVC_TOOLSET_VERSION}-mt")
      if(BOOST_BUILD_VARIANT STREQUAL "debug")
        set(BOOST_LIBRARY_SUFFIX "${BOOST_LIBRARY_SUFFIX}-gd")
      endif()
      set(BOOST_LIBRARY_SUFFIX
          "${BOOST_LIBRARY_SUFFIX}-x64-${ARROW_BOOST_BUILD_VERSION_NO_MICRO_UNDERSCORE}")
    else()
      set(BOOST_LIBRARY_SUFFIX "")
    endif()
    set(BOOST_STATIC_SYSTEM_LIBRARY
        "${BOOST_LIB_DIR}/libboost_system${BOOST_LIBRARY_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(BOOST_STATIC_FILESYSTEM_LIBRARY
        "${BOOST_LIB_DIR}/libboost_filesystem${BOOST_LIBRARY_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(BOOST_SYSTEM_LIBRARY boost_system_static)
    set(BOOST_FILESYSTEM_LIBRARY boost_filesystem_static)
    set(BOOST_BUILD_PRODUCTS ${BOOST_STATIC_SYSTEM_LIBRARY}
                             ${BOOST_STATIC_FILESYSTEM_LIBRARY})

    add_thirdparty_lib(Boost::system
                       STATIC
                       "${BOOST_STATIC_SYSTEM_LIBRARY}"
                       INCLUDE_DIRECTORIES
                       "${Boost_INCLUDE_DIR}")
    add_thirdparty_lib(Boost::filesystem
                       STATIC
                       "${BOOST_STATIC_FILESYSTEM_LIBRARY}"
                       INCLUDE_DIRECTORIES
                       "${Boost_INCLUDE_DIR}")

    externalproject_add(boost_ep
                        ${EP_COMMON_OPTIONS}
                        URL ${BOOST_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_BOOST_BUILD_SHA256_CHECKSUM}"
                        BUILD_BYPRODUCTS ${BOOST_BUILD_PRODUCTS}
                        BUILD_IN_SOURCE 1
                        CONFIGURE_COMMAND ${BOOST_CONFIGURE_COMMAND}
                        BUILD_COMMAND ${BOOST_BUILD_COMMAND}
                        INSTALL_COMMAND "")
    add_dependencies(Boost::system boost_ep)
    add_dependencies(Boost::filesystem boost_ep)
  else()
    externalproject_add(boost_ep
                        ${EP_COMMON_OPTIONS}
                        BUILD_COMMAND ""
                        CONFIGURE_COMMAND ""
                        INSTALL_COMMAND ""
                        URL ${BOOST_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_BOOST_BUILD_SHA256_CHECKSUM}")
  endif()
  add_library(Boost::headers INTERFACE IMPORTED)
  target_include_directories(Boost::headers INTERFACE "${Boost_INCLUDE_DIR}")
  add_dependencies(Boost::headers boost_ep)
  # If Boost is found but one of system or filesystem components aren't found,
  # Boost::disable_autolinking and Boost::dynamic_linking are already defined.
  if(NOT TARGET Boost::disable_autolinking)
    add_library(Boost::disable_autolinking INTERFACE IMPORTED)
    if(WIN32)
      target_compile_definitions(Boost::disable_autolinking INTERFACE "BOOST_ALL_NO_LIB")
    endif()
  endif()
  if(NOT TARGET Boost::dynamic_linking)
    # This doesn't add BOOST_ALL_DYN_LINK because bundled Boost is a static library.
    add_library(Boost::dynamic_linking INTERFACE IMPORTED)
  endif()
  set(BOOST_VENDORED TRUE)
endmacro()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER
                                              15)
  # GH-34094 Older versions of Boost use the deprecated std::unary_function in
  # boost/container_hash/hash.hpp and support for that was removed in clang 16
  set(ARROW_BOOST_REQUIRED_VERSION "1.81")
elseif(ARROW_BUILD_TESTS)
  set(ARROW_BOOST_REQUIRED_VERSION "1.64")
else()
  set(ARROW_BOOST_REQUIRED_VERSION "1.58")
endif()

set(Boost_USE_MULTITHREADED ON)
if(MSVC AND ARROW_USE_STATIC_CRT)
  set(Boost_USE_STATIC_RUNTIME ON)
endif()
set(Boost_ADDITIONAL_VERSIONS
    "1.84.0"
    "1.84"
    "1.83.0"
    "1.83"
    "1.82.0"
    "1.82"
    "1.81.0"
    "1.81"
    "1.80.0"
    "1.80"
    "1.79.0"
    "1.79"
    "1.78.0"
    "1.78"
    "1.77.0"
    "1.77"
    "1.76.0"
    "1.76"
    "1.75.0"
    "1.75"
    "1.74.0"
    "1.74"
    "1.73.0"
    "1.73"
    "1.72.0"
    "1.72"
    "1.71.0"
    "1.71"
    "1.70.0"
    "1.70"
    "1.69.0"
    "1.69"
    "1.68.0"
    "1.68"
    "1.67.0"
    "1.67"
    "1.66.0"
    "1.66"
    "1.65.0"
    "1.65"
    "1.64.0"
    "1.64"
    "1.63.0"
    "1.63"
    "1.62.0"
    "1.61"
    "1.61.0"
    "1.62"
    "1.60.0"
    "1.60")

# Compilers that don't support int128_t have a compile-time
# (header-only) dependency on Boost for int128_t.
if(ARROW_USE_UBSAN)
  # NOTE: Avoid native int128_t on clang with UBSan as it produces linker errors
  # (such as "undefined reference to '__muloti4'")
  set(ARROW_USE_NATIVE_INT128 FALSE)
else()
  include(CheckCXXSymbolExists)
  check_cxx_symbol_exists("_M_ARM64" "" WIN32_ARM64_TARGET)
  if(WIN32_ARM64_TARGET AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # NOTE: For clang/win-arm64, native int128_t produce linker errors
    set(ARROW_USE_NATIVE_INT128 FALSE)
  else()
    check_cxx_symbol_exists("__SIZEOF_INT128__" "" ARROW_USE_NATIVE_INT128)
  endif()
endif()

# - Gandiva has a compile-time (header-only) dependency on Boost, not runtime.
# - Tests need Boost at runtime.
# - S3FS and Flight benchmarks need Boost at runtime.
if(ARROW_BUILD_INTEGRATION
   OR ARROW_BUILD_TESTS
   OR (ARROW_FLIGHT AND (ARROW_TESTING OR ARROW_BUILD_BENCHMARKS))
   OR (ARROW_S3 AND ARROW_BUILD_BENCHMARKS))
  set(ARROW_USE_BOOST TRUE)
  set(ARROW_BOOST_REQUIRE_LIBRARY TRUE)
elseif(ARROW_GANDIVA
       OR ARROW_WITH_THRIFT
       OR (NOT ARROW_USE_NATIVE_INT128))
  set(ARROW_USE_BOOST TRUE)
  set(ARROW_BOOST_REQUIRE_LIBRARY FALSE)
else()
  set(ARROW_USE_BOOST FALSE)
endif()

if(ARROW_USE_BOOST)
  if(ARROW_BOOST_USE_SHARED)
    # Find shared Boost libraries.
    set(Boost_USE_STATIC_LIBS OFF)
    set(BUILD_SHARED_LIBS_KEEP ${BUILD_SHARED_LIBS})
    set(BUILD_SHARED_LIBS ON)
  else()
    # Find static boost headers and libs
    set(Boost_USE_STATIC_LIBS ON)
  endif()
  if(ARROW_BOOST_REQUIRE_LIBRARY)
    set(ARROW_BOOST_COMPONENTS system filesystem)
  else()
    set(ARROW_BOOST_COMPONENTS)
  endif()
  resolve_dependency(Boost
                     REQUIRED_VERSION
                     ${ARROW_BOOST_REQUIRED_VERSION}
                     COMPONENTS
                     ${ARROW_BOOST_COMPONENTS}
                     IS_RUNTIME_DEPENDENCY
                     # libarrow.so doesn't depend on libboost*.
                     FALSE)
  if(ARROW_BOOST_USE_SHARED)
    set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_KEEP})
    unset(BUILD_SHARED_LIBS_KEEP)
  endif()

  foreach(BOOST_LIBRARY Boost::headers Boost::filesystem Boost::system)
    if(NOT TARGET ${BOOST_LIBRARY})
      continue()
    endif()
    target_link_libraries(${BOOST_LIBRARY} INTERFACE Boost::disable_autolinking)
    if(ARROW_BOOST_USE_SHARED)
      target_link_libraries(${BOOST_LIBRARY} INTERFACE Boost::dynamic_linking)
    endif()
  endforeach()

  if(WIN32 AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # boost/process/detail/windows/handle_workaround.hpp doesn't work
    # without BOOST_USE_WINDOWS_H with MinGW because MinGW doesn't
    # provide __kernel_entry without winternl.h.
    #
    # See also:
    # https://github.com/boostorg/process/blob/develop/include/boost/process/detail/windows/handle_workaround.hpp
    target_compile_definitions(Boost::headers INTERFACE "BOOST_USE_WINDOWS_H=1")
  endif()

  message(STATUS "Boost include dir: ${Boost_INCLUDE_DIRS}")
endif()

# ----------------------------------------------------------------------
# cURL

macro(find_curl)
  if(NOT TARGET CURL::libcurl)
    find_package(CURL REQUIRED)
    list(APPEND ARROW_SYSTEM_DEPENDENCIES CURL)
  endif()
endmacro()

# ----------------------------------------------------------------------
# Snappy

macro(build_snappy)
  message(STATUS "Building snappy from source")
  set(SNAPPY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/snappy_ep/src/snappy_ep-install")
  set(SNAPPY_STATIC_LIB_NAME snappy)
  set(SNAPPY_STATIC_LIB
      "${SNAPPY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  set(SNAPPY_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS} -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF
      "-DCMAKE_INSTALL_PREFIX=${SNAPPY_PREFIX}")
  # Snappy unconditionally enables -Werror when building with clang this can lead
  # to build failures by way of new compiler warnings. This adds a flag to disable
  # Werror to the very end of the invocation to override the snappy internal setting.
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    foreach(CONFIG DEBUG MINSIZEREL RELEASE RELWITHDEBINFO)
      list(APPEND
           SNAPPY_CMAKE_ARGS
           "-DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS_${CONFIG}} -Wno-error"
      )
    endforeach()
  endif()

  if(APPLE AND CMAKE_HOST_SYSTEM_VERSION VERSION_LESS 20)
    # On macOS 10.13 we need to explicitly add <functional> to avoid a missing include error
    # This can be removed once CRAN no longer checks on macOS 10.13
    find_program(PATCH patch REQUIRED)
    set(SNAPPY_PATCH_COMMAND ${PATCH} -p1 -i ${CMAKE_CURRENT_LIST_DIR}/snappy.diff)
  else()
    set(SNAPPY_PATCH_COMMAND)
  endif()

  if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    # ignore linker flag errors, as Snappy sets
    # -Werror -Wall, and Emscripten doesn't support -soname
    list(APPEND SNAPPY_CMAKE_ARGS
         "-DCMAKE_SHARED_LINKER_FLAGS=${CMAKE_SHARED_LINKER_FLAGS}"
         "-Wno-error=linkflags")
  endif()

  externalproject_add(snappy_ep
                      ${EP_COMMON_OPTIONS}
                      BUILD_IN_SOURCE 1
                      INSTALL_DIR ${SNAPPY_PREFIX}
                      URL ${SNAPPY_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_SNAPPY_BUILD_SHA256_CHECKSUM}"
                      PATCH_COMMAND ${SNAPPY_PATCH_COMMAND}
                      CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")

  file(MAKE_DIRECTORY "${SNAPPY_PREFIX}/include")

  set(Snappy_TARGET Snappy::snappy-static)
  add_library(${Snappy_TARGET} STATIC IMPORTED)
  set_target_properties(${Snappy_TARGET} PROPERTIES IMPORTED_LOCATION
                                                    "${SNAPPY_STATIC_LIB}")
  target_include_directories(${Snappy_TARGET} BEFORE INTERFACE "${SNAPPY_PREFIX}/include")
  add_dependencies(${Snappy_TARGET} snappy_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS ${Snappy_TARGET})
endmacro()

if(ARROW_WITH_SNAPPY)
  resolve_dependency(Snappy
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     snappy)
  if(${Snappy_SOURCE} STREQUAL "SYSTEM"
     AND NOT snappy_PC_FOUND
     AND ARROW_BUILD_STATIC)
    get_target_property(SNAPPY_TYPE ${Snappy_TARGET} TYPE)
    if(NOT SNAPPY_TYPE STREQUAL "INTERFACE_LIBRARY")
      string(APPEND ARROW_PC_LIBS_PRIVATE " $<TARGET_FILE:${Snappy_TARGET}>")
    endif()
  endif()
endif()

# ----------------------------------------------------------------------
# Brotli

macro(build_brotli)
  message(STATUS "Building brotli from source")
  set(BROTLI_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/brotli_ep/src/brotli_ep-install")
  set(BROTLI_INCLUDE_DIR "${BROTLI_PREFIX}/include")
  set(BROTLI_LIB_DIR "${BROTLI_PREFIX}/lib")
  set(BROTLI_STATIC_LIBRARY_ENC
      "${BROTLI_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(BROTLI_STATIC_LIBRARY_DEC
      "${BROTLI_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(BROTLI_STATIC_LIBRARY_COMMON
      "${BROTLI_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(BROTLI_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${BROTLI_PREFIX}")

  set(BROTLI_EP_OPTIONS)
  if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    # "cmake install" is disabled for Brotli on Emscripten, so the
    # default INSTALL_COMMAND fails. We need to disable the default
    # INSTALL_COMMAND.
    list(APPEND
         BROTLI_EP_OPTIONS
         INSTALL_COMMAND
         ${CMAKE_COMMAND}
         -E
         true)

    set(BROTLI_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/brotli_ep-prefix/src/brotli_ep-build)
    set(BROTLI_BUILD_LIBS
        "${BROTLI_BUILD_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${BROTLI_BUILD_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${BROTLI_BUILD_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  endif()

  externalproject_add(brotli_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${BROTLI_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_BROTLI_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS "${BROTLI_STATIC_LIBRARY_ENC}"
                                       "${BROTLI_STATIC_LIBRARY_DEC}"
                                       "${BROTLI_STATIC_LIBRARY_COMMON}"
                                       ${BROTLI_BUILD_BYPRODUCTS}
                      CMAKE_ARGS ${BROTLI_CMAKE_ARGS}
                      STEP_TARGETS headers_copy ${BROTLI_EP_OPTIONS})

  if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    # Copy the libraries to our install directory manually.
    set(BROTLI_BUILD_INCLUDE_DIR
        ${CMAKE_CURRENT_BINARY_DIR}/brotli_ep-prefix/src/brotli_ep/c/include/brotli)
    add_custom_command(TARGET brotli_ep
                       POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy_if_different
                               ${BROTLI_BUILD_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}*${CMAKE_STATIC_LIBRARY_SUFFIX}
                               ${BROTLI_LIB_DIR}
                       COMMAND ${CMAKE_COMMAND} -E copy_directory
                               ${BROTLI_BUILD_INCLUDE_DIR} ${BROTLI_INCLUDE_DIR}/brotli)
  endif()

  file(MAKE_DIRECTORY "${BROTLI_INCLUDE_DIR}")

  add_library(Brotli::brotlicommon STATIC IMPORTED)
  set_target_properties(Brotli::brotlicommon PROPERTIES IMPORTED_LOCATION
                                                        "${BROTLI_STATIC_LIBRARY_COMMON}")
  target_include_directories(Brotli::brotlicommon BEFORE
                             INTERFACE "${BROTLI_INCLUDE_DIR}")
  add_dependencies(Brotli::brotlicommon brotli_ep)

  add_library(Brotli::brotlienc STATIC IMPORTED)
  set_target_properties(Brotli::brotlienc PROPERTIES IMPORTED_LOCATION
                                                     "${BROTLI_STATIC_LIBRARY_ENC}")
  target_include_directories(Brotli::brotlienc BEFORE INTERFACE "${BROTLI_INCLUDE_DIR}")
  add_dependencies(Brotli::brotlienc brotli_ep)

  add_library(Brotli::brotlidec STATIC IMPORTED)
  set_target_properties(Brotli::brotlidec PROPERTIES IMPORTED_LOCATION
                                                     "${BROTLI_STATIC_LIBRARY_DEC}")
  target_include_directories(Brotli::brotlidec BEFORE INTERFACE "${BROTLI_INCLUDE_DIR}")
  add_dependencies(Brotli::brotlidec brotli_ep)

  list(APPEND
       ARROW_BUNDLED_STATIC_LIBS
       Brotli::brotlicommon
       Brotli::brotlienc
       Brotli::brotlidec)
endmacro()

if(ARROW_WITH_BROTLI)
  resolve_dependency(Brotli
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     libbrotlidec
                     libbrotlienc)
  # Order is important for static linking
  set(ARROW_BROTLI_LIBS Brotli::brotlienc Brotli::brotlidec Brotli::brotlicommon)
endif()

if(PARQUET_REQUIRE_ENCRYPTION AND NOT ARROW_PARQUET)
  set(PARQUET_REQUIRE_ENCRYPTION OFF)
endif()
set(ARROW_OPENSSL_REQUIRED_VERSION "1.0.2")
set(ARROW_USE_OPENSSL OFF)
if(PARQUET_REQUIRE_ENCRYPTION
   OR ARROW_AZURE
   OR ARROW_FLIGHT
   OR ARROW_GANDIVA
   OR ARROW_GCS
   OR ARROW_S3)
  set(OpenSSL_SOURCE "SYSTEM")
  resolve_dependency(OpenSSL
                     HAVE_ALT
                     TRUE
                     REQUIRED_VERSION
                     ${ARROW_OPENSSL_REQUIRED_VERSION})
  set(ARROW_USE_OPENSSL ON)
  set(ARROW_OPENSSL_LIBS OpenSSL::Crypto OpenSSL::SSL)
endif()

if(ARROW_USE_OPENSSL)
  message(STATUS "Found OpenSSL Crypto Library: ${OPENSSL_CRYPTO_LIBRARY}")
  message(STATUS "Building with OpenSSL (Version: ${OPENSSL_VERSION}) support")
else()
  message(STATUS "Building without OpenSSL support. Minimum OpenSSL version ${ARROW_OPENSSL_REQUIRED_VERSION} required."
  )
endif()

# ----------------------------------------------------------------------
# GLOG

macro(build_glog)
  message(STATUS "Building glog from source")
  set(GLOG_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/glog_ep-prefix/src/glog_ep")
  set(GLOG_INCLUDE_DIR "${GLOG_BUILD_DIR}/include")
  if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    set(GLOG_LIB_SUFFIX "d")
  else()
    set(GLOG_LIB_SUFFIX "")
  endif()
  set(GLOG_STATIC_LIB
      "${GLOG_BUILD_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}glog${GLOG_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GLOG_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")
  set(GLOG_CMAKE_C_FLAGS "${EP_C_FLAGS}")
  if(CMAKE_THREAD_LIBS_INIT)
    string(APPEND GLOG_CMAKE_CXX_FLAGS " ${CMAKE_THREAD_LIBS_INIT}")
    string(APPEND GLOG_CMAKE_C_FLAGS " ${CMAKE_THREAD_LIBS_INIT}")
  endif()

  if(APPLE)
    # If we don't set this flag, the binary built with 10.13 cannot be used in 10.12.
    string(APPEND GLOG_CMAKE_CXX_FLAGS " -mmacosx-version-min=10.9")
  endif()

  set(GLOG_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${GLOG_BUILD_DIR}"
      -DWITH_GFLAGS=OFF
      -DCMAKE_CXX_FLAGS=${GLOG_CMAKE_CXX_FLAGS}
      -DCMAKE_C_FLAGS=${GLOG_CMAKE_C_FLAGS})
  externalproject_add(glog_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${GLOG_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_GLOG_BUILD_SHA256_CHECKSUM}"
                      BUILD_IN_SOURCE 1
                      BUILD_BYPRODUCTS "${GLOG_STATIC_LIB}"
                      CMAKE_ARGS ${GLOG_CMAKE_ARGS})

  file(MAKE_DIRECTORY "${GLOG_INCLUDE_DIR}")

  add_library(glog::glog STATIC IMPORTED)
  set_target_properties(glog::glog PROPERTIES IMPORTED_LOCATION "${GLOG_STATIC_LIB}")
  target_include_directories(glog::glog BEFORE INTERFACE "${GLOG_INCLUDE_DIR}")
  add_dependencies(glog::glog glog_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS glog::glog)
endmacro()

if(ARROW_USE_GLOG)
  resolve_dependency(glog
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     libglog)
endif()

# ----------------------------------------------------------------------
# gflags

if(ARROW_BUILD_TESTS
   OR ARROW_BUILD_BENCHMARKS
   OR ARROW_BUILD_INTEGRATION
   OR ARROW_USE_GLOG
   OR ARROW_WITH_GRPC)
  set(ARROW_NEED_GFLAGS 1)
else()
  set(ARROW_NEED_GFLAGS 0)
endif()

macro(build_gflags)
  message(STATUS "Building gflags from source")

  set(GFLAGS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gflags_ep-prefix/src/gflags_ep")
  set(GFLAGS_INCLUDE_DIR "${GFLAGS_PREFIX}/include")
  if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    set(GFLAGS_LIB_SUFFIX "_debug")
  else()
    set(GFLAGS_LIB_SUFFIX "")
  endif()
  if(MSVC)
    set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/gflags_static${GFLAGS_LIB_SUFFIX}.lib")
  else()
    set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/libgflags${GFLAGS_LIB_SUFFIX}.a")
  endif()
  set(GFLAGS_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${GFLAGS_PREFIX}"
      -DBUILD_PACKAGING=OFF
      -DBUILD_CONFIG_TESTS=OFF
      -DINSTALL_HEADERS=ON)

  file(MAKE_DIRECTORY "${GFLAGS_INCLUDE_DIR}")
  externalproject_add(gflags_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${GFLAGS_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_GFLAGS_BUILD_SHA256_CHECKSUM}"
                      BUILD_IN_SOURCE 1
                      BUILD_BYPRODUCTS "${GFLAGS_STATIC_LIB}"
                      CMAKE_ARGS ${GFLAGS_CMAKE_ARGS})

  add_thirdparty_lib(gflags::gflags_static STATIC ${GFLAGS_STATIC_LIB})
  add_dependencies(gflags::gflags_static gflags_ep)
  set(GFLAGS_LIBRARY gflags::gflags_static)
  set_target_properties(${GFLAGS_LIBRARY} PROPERTIES INTERFACE_COMPILE_DEFINITIONS
                                                     "GFLAGS_IS_A_DLL=0")
  target_include_directories(${GFLAGS_LIBRARY} BEFORE INTERFACE "${GFLAGS_INCLUDE_DIR}")
  if(MSVC)
    set_target_properties(${GFLAGS_LIBRARY} PROPERTIES INTERFACE_LINK_LIBRARIES
                                                       "shlwapi.lib")
  endif()
  set(GFLAGS_LIBRARIES ${GFLAGS_LIBRARY})

  set(GFLAGS_VENDORED TRUE)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS gflags::gflags_static)
endmacro()

if(ARROW_NEED_GFLAGS)
  set(ARROW_GFLAGS_REQUIRED_VERSION "2.1.0")
  resolve_dependency(gflags
                     HAVE_ALT
                     TRUE
                     REQUIRED_VERSION
                     ${ARROW_GFLAGS_REQUIRED_VERSION}
                     IS_RUNTIME_DEPENDENCY
                     FALSE)

  if(NOT TARGET ${GFLAGS_LIBRARIES})
    if(TARGET gflags::gflags_shared)
      set(GFLAGS_LIBRARIES gflags::gflags_shared)
    elseif(TARGET gflags-shared)
      set(GFLAGS_LIBRARIES gflags-shared)
    elseif(TARGET gflags_shared)
      set(GFLAGS_LIBRARIES gflags_shared)
    endif()
  endif()
endif()

# ----------------------------------------------------------------------
# Thrift

macro(build_thrift)
  message(STATUS "Building Apache Thrift from source")
  set(THRIFT_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/thrift_ep-install")
  set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
  set(THRIFT_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${THRIFT_PREFIX}"
      "-DCMAKE_INSTALL_RPATH=${THRIFT_PREFIX}/lib"
      # Work around https://gitlab.kitware.com/cmake/cmake/issues/18865
      -DBoost_NO_BOOST_CMAKE=ON
      -DBUILD_COMPILER=OFF
      -DBUILD_EXAMPLES=OFF
      -DBUILD_TUTORIALS=OFF
      -DCMAKE_DEBUG_POSTFIX=
      -DWITH_AS3=OFF
      -DWITH_CPP=ON
      -DWITH_C_GLIB=OFF
      -DWITH_JAVA=OFF
      -DWITH_JAVASCRIPT=OFF
      -DWITH_LIBEVENT=OFF
      -DWITH_NODEJS=OFF
      -DWITH_PYTHON=OFF
      -DWITH_QT5=OFF
      -DWITH_ZLIB=OFF)

  # Thrift also uses boost. Forward important boost settings if there were ones passed.
  if(DEFINED BOOST_ROOT)
    list(APPEND THRIFT_CMAKE_ARGS "-DBOOST_ROOT=${BOOST_ROOT}")
  endif()
  if(DEFINED Boost_INCLUDE_DIR)
    list(APPEND THRIFT_CMAKE_ARGS "-DBoost_INCLUDE_DIR=${Boost_INCLUDE_DIR}")
  endif()
  if(DEFINED Boost_NAMESPACE)
    list(APPEND THRIFT_CMAKE_ARGS "-DBoost_NAMESPACE=${Boost_NAMESPACE}")
  endif()

  if(MSVC)
    if(ARROW_USE_STATIC_CRT)
      set(THRIFT_LIB_SUFFIX "mt")
      list(APPEND THRIFT_CMAKE_ARGS "-DWITH_MT=ON")
    else()
      set(THRIFT_LIB_SUFFIX "md")
      list(APPEND THRIFT_CMAKE_ARGS "-DWITH_MT=OFF")
    endif()
    set(THRIFT_LIB
        "${THRIFT_PREFIX}/bin/${CMAKE_IMPORT_LIBRARY_PREFIX}thrift${THRIFT_LIB_SUFFIX}${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    )
  else()
    set(THRIFT_LIB
        "${THRIFT_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  endif()

  if(BOOST_VENDORED)
    set(THRIFT_DEPENDENCIES ${THRIFT_DEPENDENCIES} boost_ep)
  endif()

  externalproject_add(thrift_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${THRIFT_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_THRIFT_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS "${THRIFT_LIB}"
                      CMAKE_ARGS ${THRIFT_CMAKE_ARGS}
                      DEPENDS ${THRIFT_DEPENDENCIES})

  add_library(thrift::thrift STATIC IMPORTED)
  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${THRIFT_INCLUDE_DIR}")
  set_target_properties(thrift::thrift PROPERTIES IMPORTED_LOCATION "${THRIFT_LIB}")
  target_include_directories(thrift::thrift BEFORE INTERFACE "${THRIFT_INCLUDE_DIR}")
  if(ARROW_USE_BOOST)
    target_link_libraries(thrift::thrift INTERFACE Boost::headers)
  endif()
  add_dependencies(thrift::thrift thrift_ep)
  set(Thrift_VERSION ${ARROW_THRIFT_BUILD_VERSION})
  set(THRIFT_VENDORED TRUE)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS thrift::thrift)
endmacro()

if(ARROW_WITH_THRIFT)
  # Thrift C++ code generated by 0.13 requires 0.11 or greater
  resolve_dependency(Thrift
                     ARROW_CMAKE_PACKAGE_NAME
                     Parquet
                     ARROW_PC_PACKAGE_NAME
                     parquet
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     thrift
                     REQUIRED_VERSION
                     0.11.0)

  string(REPLACE "." ";" Thrift_VERSION_LIST ${Thrift_VERSION})
  list(GET Thrift_VERSION_LIST 0 Thrift_VERSION_MAJOR)
  list(GET Thrift_VERSION_LIST 1 Thrift_VERSION_MINOR)
  list(GET Thrift_VERSION_LIST 2 Thrift_VERSION_PATCH)
endif()

# ----------------------------------------------------------------------
# Protocol Buffers (required for ORC, Flight and Substrait libraries)

macro(build_protobuf)
  message(STATUS "Building Protocol Buffers from source")
  set(PROTOBUF_VENDORED TRUE)
  set(PROTOBUF_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-install")
  set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
  # This flag is based on what the user initially requested but if
  # we've fallen back to building protobuf we always build it statically
  # so we need to reset the flag so that we can link against it correctly
  # later.
  set(Protobuf_USE_STATIC_LIBS ON)
  # Newer protobuf releases always have a lib prefix independent from CMAKE_STATIC_LIBRARY_PREFIX
  set(PROTOBUF_STATIC_LIB
      "${PROTOBUF_PREFIX}/lib/libprotobuf${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(PROTOC_STATIC_LIB "${PROTOBUF_PREFIX}/lib/libprotoc${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(Protobuf_PROTOC_LIBRARY "${PROTOC_STATIC_LIB}")
  set(PROTOBUF_COMPILER "${PROTOBUF_PREFIX}/bin/protoc")

  # Strip lto flags (which may be added by dh_auto_configure)
  # See https://github.com/protocolbuffers/protobuf/issues/7092
  set(PROTOBUF_C_FLAGS ${EP_C_FLAGS})
  set(PROTOBUF_CXX_FLAGS ${EP_CXX_FLAGS})
  string(REPLACE "-flto=auto" "" PROTOBUF_C_FLAGS "${PROTOBUF_C_FLAGS}")
  string(REPLACE "-ffat-lto-objects" "" PROTOBUF_C_FLAGS "${PROTOBUF_C_FLAGS}")
  string(REPLACE "-flto=auto" "" PROTOBUF_CXX_FLAGS "${PROTOBUF_CXX_FLAGS}")
  string(REPLACE "-ffat-lto-objects" "" PROTOBUF_CXX_FLAGS "${PROTOBUF_CXX_FLAGS}")
  set(PROTOBUF_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_CXX_FLAGS=${PROTOBUF_CXX_FLAGS}"
      "-DCMAKE_C_FLAGS=${PROTOBUF_C_FLAGS}"
      "-DCMAKE_INSTALL_PREFIX=${PROTOBUF_PREFIX}"
      -Dprotobuf_BUILD_TESTS=OFF
      -Dprotobuf_DEBUG_POSTFIX=)
  if(MSVC AND NOT ARROW_USE_STATIC_CRT)
    list(APPEND PROTOBUF_CMAKE_ARGS "-Dprotobuf_MSVC_STATIC_RUNTIME=OFF")
  endif()
  if(ZLIB_ROOT)
    list(APPEND PROTOBUF_CMAKE_ARGS "-DZLIB_ROOT=${ZLIB_ROOT}")
  endif()
  set(PROTOBUF_EXTERNAL_PROJECT_ADD_ARGS CMAKE_ARGS ${PROTOBUF_CMAKE_ARGS} SOURCE_SUBDIR
                                         "cmake")

  externalproject_add(protobuf_ep
                      ${EP_COMMON_OPTIONS} ${PROTOBUF_EXTERNAL_PROJECT_ADD_ARGS}
                      BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOBUF_COMPILER}"
                      BUILD_IN_SOURCE 1
                      URL ${PROTOBUF_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_PROTOBUF_BUILD_SHA256_CHECKSUM}")

  file(MAKE_DIRECTORY "${PROTOBUF_INCLUDE_DIR}")
  # For compatibility of CMake's FindProtobuf.cmake.
  set(Protobuf_INCLUDE_DIRS "${PROTOBUF_INCLUDE_DIR}")

  add_library(arrow::protobuf::libprotobuf STATIC IMPORTED)
  set_target_properties(arrow::protobuf::libprotobuf PROPERTIES IMPORTED_LOCATION
                                                                "${PROTOBUF_STATIC_LIB}")
  target_include_directories(arrow::protobuf::libprotobuf BEFORE
                             INTERFACE "${PROTOBUF_INCLUDE_DIR}")
  add_library(arrow::protobuf::libprotoc STATIC IMPORTED)
  set_target_properties(arrow::protobuf::libprotoc PROPERTIES IMPORTED_LOCATION
                                                              "${PROTOC_STATIC_LIB}")
  target_include_directories(arrow::protobuf::libprotoc BEFORE
                             INTERFACE "${PROTOBUF_INCLUDE_DIR}")
  add_executable(arrow::protobuf::protoc IMPORTED)
  set_target_properties(arrow::protobuf::protoc PROPERTIES IMPORTED_LOCATION
                                                           "${PROTOBUF_COMPILER}")

  add_dependencies(arrow::protobuf::libprotobuf protobuf_ep)
  add_dependencies(arrow::protobuf::protoc protobuf_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS arrow::protobuf::libprotobuf)

  if(CMAKE_CROSSCOMPILING)
    # If we are cross compiling, we need to build protoc for the host
    # system also, as it is used when building Arrow
    # We do this by calling CMake as a child process
    # with CXXFLAGS / CFLAGS and CMake flags cleared.
    set(PROTOBUF_HOST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep_host-install")
    set(PROTOBUF_HOST_COMPILER "${PROTOBUF_HOST_PREFIX}/bin/protoc")

    set(PROTOBUF_HOST_CMAKE_ARGS
        "-DCMAKE_CXX_FLAGS="
        "-DCMAKE_C_FLAGS="
        "-DCMAKE_INSTALL_PREFIX=${PROTOBUF_HOST_PREFIX}"
        -Dprotobuf_BUILD_TESTS=OFF
        -Dprotobuf_DEBUG_POSTFIX=)

    externalproject_add(protobuf_ep_host
                        ${EP_COMMON_OPTIONS}
                        CMAKE_ARGS ${PROTOBUF_HOST_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${PROTOBUF_HOST_COMPILER}"
                        BUILD_IN_SOURCE 1
                        URL ${PROTOBUF_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_PROTOBUF_BUILD_SHA256_CHECKSUM}")

    add_executable(arrow::protobuf::host_protoc IMPORTED)
    set_target_properties(arrow::protobuf::host_protoc
                          PROPERTIES IMPORTED_LOCATION "${PROTOBUF_HOST_COMPILER}")

    add_dependencies(arrow::protobuf::host_protoc protobuf_ep_host)
  endif()
endmacro()

if(ARROW_WITH_PROTOBUF)
  if(ARROW_FLIGHT_SQL)
    # Flight SQL uses proto3 optionals, which require 3.12 or later.
    # 3.12.0-3.14.0: need --experimental_allow_proto3_optional
    # 3.15.0-: don't need --experimental_allow_proto3_optional
    set(ARROW_PROTOBUF_REQUIRED_VERSION "3.12.0")
  elseif(ARROW_SUBSTRAIT)
    # Substrait protobuf files use proto3 syntax
    set(ARROW_PROTOBUF_REQUIRED_VERSION "3.0.0")
  else()
    set(ARROW_PROTOBUF_REQUIRED_VERSION "2.6.1")
  endif()
  if(ARROW_ORC
     OR ARROW_SUBSTRAIT
     OR ARROW_WITH_OPENTELEMETRY)
    set(ARROW_PROTOBUF_ARROW_CMAKE_PACKAGE_NAME "Arrow")
    set(ARROW_PROTOBUF_ARROW_PC_PACKAGE_NAME "arrow")
  elseif(ARROW_FLIGHT)
    set(ARROW_PROTOBUF_ARROW_CMAKE_PACKAGE_NAME "ArrowFlight")
    set(ARROW_PROTOBUF_ARROW_PC_PACKAGE_NAME "arrow-flight")
  else()
    message(FATAL_ERROR "ARROW_WITH_PROTOBUF must be propagated in the build tooling installation."
                        " Please extend the mappings of ARROW_PROTOBUF_ARROW_CMAKE_PACKAGE_NAME and"
                        " ARROW_PROTOBUF_ARROW_PC_PACKAGE_NAME for newly introduced dependencies on"
                        " protobuf.")
  endif()
  # We need to use FORCE_ANY_NEWER_VERSION here to accept Protobuf
  # newer version such as 23.4. If we don't use it, 23.4 is processed
  # as an incompatible version with 3.12.0 with protobuf-config.cmake
  # provided by Protobuf. Because protobuf-config-version.cmake
  # requires the same major version. In the example, "23" for 23.4 and
  # "3" for 3.12.0 are different. So 23.4 is rejected with 3.12.0. If
  # we use FORCE_ANY_NEWER_VERSION here, we can bypass the check and
  # use 23.4.
  resolve_dependency(Protobuf
                     ARROW_CMAKE_PACKAGE_NAME
                     ${ARROW_PROTOBUF_ARROW_CMAKE_PACKAGE_NAME}
                     ARROW_PC_PACKAGE_NAME
                     ${ARROW_PROTOBUF_ARROW_PC_PACKAGE_NAME}
                     FORCE_ANY_NEWER_VERSION
                     TRUE
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     protobuf
                     REQUIRED_VERSION
                     ${ARROW_PROTOBUF_REQUIRED_VERSION})

  if(NOT Protobuf_USE_STATIC_LIBS AND MSVC_TOOLCHAIN)
    add_definitions(-DPROTOBUF_USE_DLLS)
  endif()

  if(TARGET arrow::protobuf::libprotobuf)
    set(ARROW_PROTOBUF_LIBPROTOBUF arrow::protobuf::libprotobuf)
  else()
    set(ARROW_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
  endif()
  if(TARGET arrow::protobuf::libprotoc)
    set(ARROW_PROTOBUF_LIBPROTOC arrow::protobuf::libprotoc)
  else()
    set(ARROW_PROTOBUF_LIBPROTOC protobuf::libprotoc)
  endif()
  if(TARGET arrow::protobuf::host_protoc)
    # make sure host protoc is used for compiling protobuf files
    # during build of e.g. orc
    set(ARROW_PROTOBUF_PROTOC arrow::protobuf::host_protoc)
  elseif(TARGET arrow::protobuf::protoc)
    set(ARROW_PROTOBUF_PROTOC arrow::protobuf::protoc)
  else()
    if(NOT TARGET protobuf::protoc)
      add_executable(protobuf::protoc IMPORTED)
      set_target_properties(protobuf::protoc PROPERTIES IMPORTED_LOCATION
                                                        "${PROTOBUF_PROTOC_EXECUTABLE}")
    endif()
    set(ARROW_PROTOBUF_PROTOC protobuf::protoc)
  endif()

  # Log protobuf paths as we often see issues with mixed sources for
  # the libraries and protoc.
  get_target_property(PROTOBUF_PROTOC_EXECUTABLE ${ARROW_PROTOBUF_PROTOC}
                      IMPORTED_LOCATION_RELEASE)
  if(NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_target_property(PROTOBUF_PROTOC_EXECUTABLE ${ARROW_PROTOBUF_PROTOC}
                        IMPORTED_LOCATION)
  endif()
  message(STATUS "Found protoc: ${PROTOBUF_PROTOC_EXECUTABLE}")
  get_target_property(PROTOBUF_TYPE ${ARROW_PROTOBUF_LIBPROTOBUF} TYPE)
  if(NOT STREQUAL "INTERFACE_LIBRARY")
    # Protobuf_PROTOC_LIBRARY is set by all versions of FindProtobuf.cmake
    message(STATUS "Found libprotoc: ${Protobuf_PROTOC_LIBRARY}")
    get_target_property(PROTOBUF_LIBRARY ${ARROW_PROTOBUF_LIBPROTOBUF} IMPORTED_LOCATION)
    message(STATUS "Found libprotobuf: ${PROTOBUF_LIBRARY}")
    message(STATUS "Found protobuf headers: ${PROTOBUF_INCLUDE_DIR}")
  endif()
endif()

# ----------------------------------------------------------------------
# Substrait (required by compute engine)

macro(build_substrait)
  message(STATUS "Building Substrait from source")

  # Note: not all protos in Substrait actually matter to plan
  # consumption. No need to build the ones we don't need.
  set(SUBSTRAIT_PROTOS
      algebra
      extended_expression
      extensions/extensions
      plan
      type)
  set(ARROW_SUBSTRAIT_PROTOS extension_rels)
  set(ARROW_SUBSTRAIT_PROTOS_DIR "${CMAKE_SOURCE_DIR}/proto")

  externalproject_add(substrait_ep
                      ${EP_COMMON_OPTIONS}
                      CONFIGURE_COMMAND ""
                      BUILD_COMMAND ""
                      INSTALL_COMMAND ""
                      URL ${SUBSTRAIT_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_SUBSTRAIT_BUILD_SHA256_CHECKSUM}")

  externalproject_get_property(substrait_ep SOURCE_DIR)
  set(SUBSTRAIT_LOCAL_DIR ${SOURCE_DIR})

  set(SUBSTRAIT_CPP_DIR "${CMAKE_CURRENT_BINARY_DIR}/substrait_ep-generated")
  file(MAKE_DIRECTORY ${SUBSTRAIT_CPP_DIR})

  set(SUBSTRAIT_SUPPRESSED_FLAGS)
  if(MSVC)
    # Protobuf generated files trigger some spurious warnings on MSVC.

    # Implicit conversion from uint64_t to uint32_t:
    list(APPEND SUBSTRAIT_SUPPRESSED_FLAGS "/wd4244")

    # Missing dll-interface:
    list(APPEND SUBSTRAIT_SUPPRESSED_FLAGS "/wd4251")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                        "Clang")
    # Protobuf generated files trigger some errors on CLANG TSAN builds
    list(APPEND SUBSTRAIT_SUPPRESSED_FLAGS "-Wno-error=shorten-64-to-32")
  endif()

  set(SUBSTRAIT_SOURCES)
  set(SUBSTRAIT_PROTO_GEN_ALL)
  foreach(SUBSTRAIT_PROTO ${SUBSTRAIT_PROTOS})
    set(SUBSTRAIT_PROTO_GEN "${SUBSTRAIT_CPP_DIR}/substrait/${SUBSTRAIT_PROTO}.pb")

    foreach(EXT h cc)
      set_source_files_properties("${SUBSTRAIT_PROTO_GEN}.${EXT}"
                                  PROPERTIES COMPILE_OPTIONS
                                             "${SUBSTRAIT_SUPPRESSED_FLAGS}"
                                             GENERATED TRUE
                                             SKIP_UNITY_BUILD_INCLUSION TRUE)
      list(APPEND SUBSTRAIT_PROTO_GEN_ALL "${SUBSTRAIT_PROTO_GEN}.${EXT}")
    endforeach()
    add_custom_command(OUTPUT "${SUBSTRAIT_PROTO_GEN}.cc" "${SUBSTRAIT_PROTO_GEN}.h"
                       COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${SUBSTRAIT_LOCAL_DIR}/proto"
                               "--cpp_out=${SUBSTRAIT_CPP_DIR}"
                               "${SUBSTRAIT_LOCAL_DIR}/proto/substrait/${SUBSTRAIT_PROTO}.proto"
                       DEPENDS ${PROTO_DEPENDS} substrait_ep)

    list(APPEND SUBSTRAIT_SOURCES "${SUBSTRAIT_PROTO_GEN}.cc")
  endforeach()
  foreach(ARROW_SUBSTRAIT_PROTO ${ARROW_SUBSTRAIT_PROTOS})
    set(ARROW_SUBSTRAIT_PROTO_GEN
        "${SUBSTRAIT_CPP_DIR}/substrait/${ARROW_SUBSTRAIT_PROTO}.pb")
    foreach(EXT h cc)
      set_source_files_properties("${ARROW_SUBSTRAIT_PROTO_GEN}.${EXT}"
                                  PROPERTIES COMPILE_OPTIONS
                                             "${SUBSTRAIT_SUPPRESSED_FLAGS}"
                                             GENERATED TRUE
                                             SKIP_UNITY_BUILD_INCLUSION TRUE)
      list(APPEND SUBSTRAIT_PROTO_GEN_ALL "${ARROW_SUBSTRAIT_PROTO_GEN}.${EXT}")
    endforeach()
    add_custom_command(OUTPUT "${ARROW_SUBSTRAIT_PROTO_GEN}.cc"
                              "${ARROW_SUBSTRAIT_PROTO_GEN}.h"
                       COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${SUBSTRAIT_LOCAL_DIR}/proto"
                               "-I${ARROW_SUBSTRAIT_PROTOS_DIR}"
                               "--cpp_out=${SUBSTRAIT_CPP_DIR}"
                               "${ARROW_SUBSTRAIT_PROTOS_DIR}/substrait/${ARROW_SUBSTRAIT_PROTO}.proto"
                       DEPENDS ${PROTO_DEPENDS} substrait_ep)

    list(APPEND SUBSTRAIT_SOURCES "${ARROW_SUBSTRAIT_PROTO_GEN}.cc")
  endforeach()

  add_custom_target(substrait_gen ALL DEPENDS ${SUBSTRAIT_PROTO_GEN_ALL})

  set(SUBSTRAIT_INCLUDES ${SUBSTRAIT_CPP_DIR} ${PROTOBUF_INCLUDE_DIR})

  add_library(substrait STATIC ${SUBSTRAIT_SOURCES})
  set_target_properties(substrait PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_include_directories(substrait PUBLIC ${SUBSTRAIT_INCLUDES})
  target_link_libraries(substrait PUBLIC ${ARROW_PROTOBUF_LIBPROTOBUF})
  add_dependencies(substrait substrait_gen)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS substrait)
endmacro()

if(ARROW_SUBSTRAIT)
  # Currently, we can only build Substrait from source.
  set(Substrait_SOURCE "BUNDLED")
  resolve_dependency(Substrait)
endif()

# ----------------------------------------------------------------------
# jemalloc - Unix-only high-performance allocator

macro(build_jemalloc)
  # Our build of jemalloc is specially prefixed so that it will not
  # conflict with the default allocator as well as other jemalloc
  # installations.

  message(STATUS "Building jemalloc from source")

  set(ARROW_JEMALLOC_USE_SHARED OFF)
  set(JEMALLOC_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/jemalloc_ep/dist/")
  set(JEMALLOC_LIB_DIR "${JEMALLOC_PREFIX}/lib")
  set(JEMALLOC_STATIC_LIB
      "${JEMALLOC_LIB_DIR}/libjemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(JEMALLOC_CONFIGURE_COMMAND ./configure "AR=${CMAKE_AR}" "CC=${CMAKE_C_COMPILER}")
  if(CMAKE_OSX_SYSROOT)
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "SDKROOT=${CMAKE_OSX_SYSROOT}")
  endif()
  if(DEFINED ARROW_JEMALLOC_LG_PAGE)
    # Used for arm64 manylinux wheels in order to make the wheel work on both
    # 4k and 64k page arm64 systems.
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "--with-lg-page=${ARROW_JEMALLOC_LG_PAGE}")
  endif()
  list(APPEND
       JEMALLOC_CONFIGURE_COMMAND
       "--prefix=${JEMALLOC_PREFIX}"
       "--libdir=${JEMALLOC_LIB_DIR}"
       "--with-jemalloc-prefix=je_arrow_"
       "--with-private-namespace=je_arrow_private_"
       "--without-export"
       "--disable-shared"
       # Don't override operator new()
       "--disable-cxx"
       "--disable-libdl"
       # See https://github.com/jemalloc/jemalloc/issues/1237
       "--disable-initial-exec-tls"
       ${EP_LOG_OPTIONS})
  if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    # Enable jemalloc debug checks when Arrow itself has debugging enabled
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "--enable-debug")
  endif()

  set(JEMALLOC_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})

  if(CMAKE_OSX_SYSROOT)
    list(APPEND JEMALLOC_BUILD_COMMAND "SDKROOT=${CMAKE_OSX_SYSROOT}")
  endif()

  externalproject_add(jemalloc_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${JEMALLOC_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_JEMALLOC_BUILD_SHA256_CHECKSUM}"
                      PATCH_COMMAND touch doc/jemalloc.3 doc/jemalloc.html
                                    # The prefix "je_arrow_" must be kept in sync with the value in memory_pool.cc
                      CONFIGURE_COMMAND ${JEMALLOC_CONFIGURE_COMMAND}
                      BUILD_IN_SOURCE 1
                      BUILD_COMMAND ${JEMALLOC_BUILD_COMMAND}
                      BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}"
                      INSTALL_COMMAND ${MAKE} -j1 install)

  # Don't use the include directory directly so that we can point to a path
  # that is unique to our codebase.
  set(JEMALLOC_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/")
  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${JEMALLOC_INCLUDE_DIR}")
  add_library(jemalloc::jemalloc STATIC IMPORTED)
  set_target_properties(jemalloc::jemalloc PROPERTIES IMPORTED_LOCATION
                                                      "${JEMALLOC_STATIC_LIB}")
  target_link_libraries(jemalloc::jemalloc INTERFACE Threads::Threads)
  target_include_directories(jemalloc::jemalloc BEFORE
                             INTERFACE "${JEMALLOC_INCLUDE_DIR}")
  add_dependencies(jemalloc::jemalloc jemalloc_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS jemalloc::jemalloc)

  set(jemalloc_VENDORED TRUE)
  # For config.h.cmake
  set(ARROW_JEMALLOC_VENDORED ${jemalloc_VENDORED})
endmacro()

if(ARROW_JEMALLOC)
  if(NOT ARROW_ENABLE_THREADING)
    message(FATAL_ERROR "Can't use jemalloc with ARROW_ENABLE_THREADING=OFF")
  endif()
  resolve_dependency(jemalloc HAVE_ALT TRUE)
endif()

# ----------------------------------------------------------------------
# mimalloc - Cross-platform high-performance allocator, from Microsoft

if(ARROW_MIMALLOC)
  if(NOT ARROW_ENABLE_THREADING)
    message(FATAL_ERROR "Can't use mimalloc with ARROW_ENABLE_THREADING=OFF")
  endif()

  message(STATUS "Building (vendored) mimalloc from source")
  # We only use a vendored mimalloc as we want to control its build options.

  set(MIMALLOC_LIB_BASE_NAME "mimalloc")
  if(WIN32)
    set(MIMALLOC_LIB_BASE_NAME "${MIMALLOC_LIB_BASE_NAME}-static")
  endif()
  if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    set(MIMALLOC_LIB_BASE_NAME "${MIMALLOC_LIB_BASE_NAME}-${LOWERCASE_BUILD_TYPE}")
  endif()

  set(MIMALLOC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/mimalloc_ep/src/mimalloc_ep")
  set(MIMALLOC_INCLUDE_DIR "${MIMALLOC_PREFIX}/include/mimalloc-2.0")
  set(MIMALLOC_STATIC_LIB
      "${MIMALLOC_PREFIX}/lib/mimalloc-2.0/${CMAKE_STATIC_LIBRARY_PREFIX}${MIMALLOC_LIB_BASE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  set(MIMALLOC_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${MIMALLOC_PREFIX}"
      -DMI_OVERRIDE=OFF
      -DMI_LOCAL_DYNAMIC_TLS=ON
      -DMI_BUILD_OBJECT=OFF
      -DMI_BUILD_SHARED=OFF
      -DMI_BUILD_TESTS=OFF)

  externalproject_add(mimalloc_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${MIMALLOC_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_MIMALLOC_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${MIMALLOC_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${MIMALLOC_STATIC_LIB}")

  file(MAKE_DIRECTORY ${MIMALLOC_INCLUDE_DIR})

  add_library(mimalloc::mimalloc STATIC IMPORTED)
  set_target_properties(mimalloc::mimalloc PROPERTIES IMPORTED_LOCATION
                                                      "${MIMALLOC_STATIC_LIB}")
  target_include_directories(mimalloc::mimalloc BEFORE
                             INTERFACE "${MIMALLOC_INCLUDE_DIR}")
  target_link_libraries(mimalloc::mimalloc INTERFACE Threads::Threads)
  if(WIN32)
    target_link_libraries(mimalloc::mimalloc INTERFACE "bcrypt.lib" "psapi.lib")
  endif()
  add_dependencies(mimalloc::mimalloc mimalloc_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS mimalloc::mimalloc)

  set(mimalloc_VENDORED TRUE)
endif()

# ----------------------------------------------------------------------
# Google gtest

function(build_gtest)
  message(STATUS "Building gtest from source")
  set(GTEST_VENDORED TRUE)
  fetchcontent_declare(googletest
                       # We should not specify "EXCLUDE_FROM_ALL TRUE" here.
                       # Because we install GTest with custom path.
                       # ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${GTEST_SOURCE_URL}
                       URL_HASH "SHA256=${ARROW_GTEST_BUILD_SHA256_CHECKSUM}")
  prepare_fetchcontent()
  if(APPLE)
    string(APPEND CMAKE_CXX_FLAGS " -Wno-unused-value" " -Wno-ignored-attributes")
  endif()
  # If we're building static libs for Emscripten, we need to build *everything* as
  # static libs.
  if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    set(BUILD_SHARED_LIBS OFF)
    set(BUILD_STATIC_LIBS ON)
  else()
    set(BUILD_SHARED_LIBS ON)
    set(BUILD_STATIC_LIBS OFF)
  endif()
  # We need to use "cache" variable to override the default
  # INSTALL_GTEST option by this value. See also:
  # https://cmake.org/cmake/help/latest/policy/CMP0077.html
  set(INSTALL_GTEST
      OFF
      CACHE "BOOL"
            "Enable installation of googletest. (Projects embedding googletest may want to turn this OFF.)"
            FORCE)
  string(APPEND CMAKE_INSTALL_INCLUDEDIR "/arrow-gtest")
  fetchcontent_makeavailable(googletest)
  foreach(target gmock gmock_main gtest gtest_main)
    set_target_properties(${target}
                          PROPERTIES OUTPUT_NAME "arrow_${target}"
                                     PDB_NAME "arrow_${target}"
                                     PDB_NAME_DEBUG "arrow_${target}d"
                                     COMPILE_PDB_NAME "arrow_${target}"
                                     COMPILE_PDB_NAME_DEBUG "arrow_${target}d"
                                     RUNTIME_OUTPUT_DIRECTORY
                                     "${BUILD_OUTPUT_ROOT_DIRECTORY}"
                                     LIBRARY_OUTPUT_DIRECTORY
                                     "${BUILD_OUTPUT_ROOT_DIRECTORY}"
                                     ARCHIVE_OUTPUT_DIRECTORY
                                     "${BUILD_OUTPUT_ROOT_DIRECTORY}"
                                     PDB_OUTPUT_DIRECTORY
                                     "${BUILD_OUTPUT_ROOT_DIRECTORY}")
  endforeach()
  install(DIRECTORY "${googletest_SOURCE_DIR}/googlemock/include/"
                    "${googletest_SOURCE_DIR}/googletest/include/"
          DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  install(TARGETS gmock gmock_main gtest gtest_main
          EXPORT arrow_testing_targets
          RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
          ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
          LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
  if(MSVC)
    install(FILES $<TARGET_PDB_FILE:gmock> $<TARGET_PDB_FILE:gmock_main>
                  $<TARGET_PDB_FILE:gtest> $<TARGET_PDB_FILE:gtest_main>
            DESTINATION "${CMAKE_INSTALL_BINDIR}"
            OPTIONAL)
  endif()
  add_library(arrow::GTest::gmock ALIAS gmock)
  add_library(arrow::GTest::gmock_main ALIAS gmock_main)
  add_library(arrow::GTest::gtest ALIAS gtest)
  add_library(arrow::GTest::gtest_main ALIAS gtest_main)
endfunction()

if(ARROW_TESTING)
  set(GTestAlt_NEED_CXX_STANDARD_CHECK TRUE)
  resolve_dependency(GTest
                     ARROW_CMAKE_PACKAGE_NAME
                     ArrowTesting
                     HAVE_ALT
                     TRUE
                     REQUIRED_VERSION
                     1.10.0)

  if(GTest_SOURCE STREQUAL "SYSTEM")
    find_package(PkgConfig QUIET)
    pkg_check_modules(gtest_PC
                      gtest
                      NO_CMAKE_PATH
                      NO_CMAKE_ENVIRONMENT_PATH
                      QUIET)
    if(gtest_PC_FOUND)
      string(APPEND ARROW_TESTING_PC_REQUIRES " gtest")
    else()
      string(APPEND ARROW_TESTING_PC_CFLAGS " -I$<JOIN:")
      string(APPEND ARROW_TESTING_PC_CFLAGS
             "$<TARGET_PROPERTY:GTest::gtest,INTERFACE_INCLUDE_DIRECTORIES>")
      string(APPEND ARROW_TESTING_PC_CFLAGS ",-I>")

      string(APPEND ARROW_TESTING_PC_LIBS " $<TARGET_FILE:GTest::gtest>")
    endif()
    set(ARROW_GTEST_GMOCK GTest::gmock)
    set(ARROW_GTEST_GTEST GTest::gtest)
    set(ARROW_GTEST_GTEST_MAIN GTest::gtest_main)
  else()
    string(APPEND ARROW_TESTING_PC_CFLAGS " -I\${includedir}/arrow-gtest")
    string(APPEND ARROW_TESTING_PC_LIBS " -larrow_gtest")
    set(ARROW_GTEST_GMOCK arrow::GTest::gmock)
    set(ARROW_GTEST_GTEST arrow::GTest::gtest)
    set(ARROW_GTEST_GTEST_MAIN arrow::GTest::gtest_main)
  endif()
endif()

macro(build_benchmark)
  message(STATUS "Building benchmark from source")

  set(GBENCHMARK_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")
  if(APPLE AND (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID
                                                               STREQUAL "Clang"))
    string(APPEND GBENCHMARK_CMAKE_CXX_FLAGS " -stdlib=libc++")
  endif()

  set(GBENCHMARK_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
  set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
  set(GBENCHMARK_STATIC_LIB
      "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GBENCHMARK_MAIN_STATIC_LIB
      "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GBENCHMARK_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${GBENCHMARK_PREFIX}"
      -DBENCHMARK_ENABLE_TESTING=OFF -DCMAKE_CXX_FLAGS=${GBENCHMARK_CMAKE_CXX_FLAGS})
  if(APPLE)
    set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DBENCHMARK_USE_LIBCXX=ON")
  endif()

  externalproject_add(gbenchmark_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${GBENCHMARK_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_GBENCHMARK_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
                                       "${GBENCHMARK_MAIN_STATIC_LIB}"
                      CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${GBENCHMARK_INCLUDE_DIR}")

  add_library(benchmark::benchmark STATIC IMPORTED)
  set_target_properties(benchmark::benchmark PROPERTIES IMPORTED_LOCATION
                                                        "${GBENCHMARK_STATIC_LIB}")
  target_include_directories(benchmark::benchmark BEFORE
                             INTERFACE "${GBENCHMARK_INCLUDE_DIR}")
  target_compile_definitions(benchmark::benchmark INTERFACE "BENCHMARK_STATIC_DEFINE")

  add_library(benchmark::benchmark_main STATIC IMPORTED)
  set_target_properties(benchmark::benchmark_main
                        PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_MAIN_STATIC_LIB}")
  target_include_directories(benchmark::benchmark_main BEFORE
                             INTERFACE "${GBENCHMARK_INCLUDE_DIR}")
  target_link_libraries(benchmark::benchmark_main INTERFACE benchmark::benchmark)

  add_dependencies(benchmark::benchmark gbenchmark_ep)
  add_dependencies(benchmark::benchmark_main gbenchmark_ep)
endmacro()

if(ARROW_BUILD_BENCHMARKS)
  set(BENCHMARK_REQUIRED_VERSION 1.6.1)
  resolve_dependency(benchmark
                     REQUIRED_VERSION
                     ${BENCHMARK_REQUIRED_VERSION}
                     IS_RUNTIME_DEPENDENCY
                     FALSE)
endif()

macro(build_rapidjson)
  message(STATUS "Building RapidJSON from source")
  set(RAPIDJSON_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/rapidjson_ep/src/rapidjson_ep-install")
  set(RAPIDJSON_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      -DRAPIDJSON_BUILD_DOC=OFF
      -DRAPIDJSON_BUILD_EXAMPLES=OFF
      -DRAPIDJSON_BUILD_TESTS=OFF
      "-DCMAKE_INSTALL_PREFIX=${RAPIDJSON_PREFIX}")

  externalproject_add(rapidjson_ep
                      ${EP_COMMON_OPTIONS}
                      PREFIX "${CMAKE_BINARY_DIR}"
                      URL ${RAPIDJSON_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_RAPIDJSON_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${RAPIDJSON_CMAKE_ARGS})

  set(RAPIDJSON_INCLUDE_DIR "${RAPIDJSON_PREFIX}/include")
  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${RAPIDJSON_INCLUDE_DIR}")

  add_library(RapidJSON INTERFACE IMPORTED)
  target_include_directories(RapidJSON INTERFACE "${RAPIDJSON_INCLUDE_DIR}")
  add_dependencies(RapidJSON rapidjson_ep)

  set(RAPIDJSON_VENDORED TRUE)
endmacro()

if(ARROW_WITH_RAPIDJSON)
  set(ARROW_RAPIDJSON_REQUIRED_VERSION "1.1.0")
  resolve_dependency(RapidJSON
                     HAVE_ALT
                     TRUE
                     REQUIRED_VERSION
                     ${ARROW_RAPIDJSON_REQUIRED_VERSION}
                     IS_RUNTIME_DEPENDENCY
                     FALSE)
endif()

macro(build_xsimd)
  message(STATUS "Building xsimd from source")
  set(XSIMD_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/xsimd_ep/src/xsimd_ep-install")
  set(XSIMD_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${XSIMD_PREFIX}")

  externalproject_add(xsimd_ep
                      ${EP_COMMON_OPTIONS}
                      PREFIX "${CMAKE_BINARY_DIR}"
                      URL ${XSIMD_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_XSIMD_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${XSIMD_CMAKE_ARGS})

  set(XSIMD_INCLUDE_DIR "${XSIMD_PREFIX}/include")
  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${XSIMD_INCLUDE_DIR}")

  add_library(arrow::xsimd INTERFACE IMPORTED)
  target_include_directories(arrow::xsimd INTERFACE "${XSIMD_INCLUDE_DIR}")
  add_dependencies(arrow::xsimd xsimd_ep)

  set(XSIMD_VENDORED TRUE)
endmacro()

if((NOT ARROW_SIMD_LEVEL STREQUAL "NONE") OR (NOT ARROW_RUNTIME_SIMD_LEVEL STREQUAL "NONE"
                                             ))
  set(ARROW_USE_XSIMD TRUE)
else()
  set(ARROW_USE_XSIMD FALSE)
endif()

if(ARROW_USE_XSIMD)
  resolve_dependency(xsimd
                     FORCE_ANY_NEWER_VERSION
                     TRUE
                     IS_RUNTIME_DEPENDENCY
                     FALSE
                     REQUIRED_VERSION
                     "8.1.0")

  if(xsimd_SOURCE STREQUAL "BUNDLED")
    set(ARROW_XSIMD arrow::xsimd)
  else()
    message(STATUS "xsimd found. Headers: ${xsimd_INCLUDE_DIRS}")
    set(ARROW_XSIMD xsimd)
  endif()
endif()

macro(build_zlib)
  message(STATUS "Building ZLIB from source")

  # ensure zlib is built with -fpic
  # and make sure that the build finds the version in Emscripten ports
  # - n.b. the actual linking happens because -sUSE_ZLIB=1 is
  # set in the compiler variables, but cmake expects
  # it to exist at configuration time if we aren't building it as
  # bundled. We need to do this for all packages
  # not just zlib as some depend on zlib, but we don't rebuild
  # if it exists already
  if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    # build zlib using Emscripten ports
    if(NOT EXISTS ${EMSCRIPTEN_SYSROOT}/lib/wasm32-emscripten/pic/libz.a)
      execute_process(COMMAND embuilder --pic --force build zlib)
    endif()
    add_library(ZLIB::ZLIB STATIC IMPORTED)
    set_property(TARGET ZLIB::ZLIB
                 PROPERTY IMPORTED_LOCATION
                          "${EMSCRIPTEN_SYSROOT}/lib/wasm32-emscripten/pic/libz.a")
    target_include_directories(ZLIB::ZLIB INTERFACE "${EMSCRIPTEN_SYSROOT}/include")
    list(APPEND ARROW_BUNDLED_STATIC_LIBS ZLIB::ZLIB)
  else()
    set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep/src/zlib_ep-install")
    if(MSVC)
      if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
        set(ZLIB_STATIC_LIB_NAME zlibstaticd.lib)
      else()
        set(ZLIB_STATIC_LIB_NAME zlibstatic.lib)
      endif()
    else()
      set(ZLIB_STATIC_LIB_NAME libz.a)
    endif()
    set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}")
    set(ZLIB_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}")

    externalproject_add(zlib_ep
                        ${EP_COMMON_OPTIONS}
                        URL ${ZLIB_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_ZLIB_BUILD_SHA256_CHECKSUM}"
                        BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
                        CMAKE_ARGS ${ZLIB_CMAKE_ARGS})

    file(MAKE_DIRECTORY "${ZLIB_PREFIX}/include")

    add_library(ZLIB::ZLIB STATIC IMPORTED)
    set(ZLIB_LIBRARIES ${ZLIB_STATIC_LIB})
    set(ZLIB_INCLUDE_DIRS "${ZLIB_PREFIX}/include")
    set_target_properties(ZLIB::ZLIB PROPERTIES IMPORTED_LOCATION ${ZLIB_LIBRARIES})
    target_include_directories(ZLIB::ZLIB BEFORE INTERFACE "${ZLIB_INCLUDE_DIRS}")

    add_dependencies(ZLIB::ZLIB zlib_ep)
    list(APPEND ARROW_BUNDLED_STATIC_LIBS ZLIB::ZLIB)
  endif()

  set(ZLIB_VENDORED TRUE)
endmacro()

if(ARROW_WITH_ZLIB)
  resolve_dependency(ZLIB PC_PACKAGE_NAMES zlib)
endif()

macro(build_lz4)
  message(STATUS "Building LZ4 from source")

  set(LZ4_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-install")

  set(LZ4_STATIC_LIB
      "${LZ4_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX}")

  set(LZ4_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
                     -DLZ4_BUILD_CLI=OFF -DLZ4_BUILD_LEGACY_LZ4C=OFF)

  # We need to copy the header in lib to directory outside of the build
  externalproject_add(lz4_ep
                      ${EP_COMMON_OPTIONS}
                      CMAKE_ARGS ${LZ4_CMAKE_ARGS}
                      SOURCE_SUBDIR "build/cmake"
                      INSTALL_DIR ${LZ4_PREFIX}
                      URL ${LZ4_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_LZ4_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS ${LZ4_STATIC_LIB})

  file(MAKE_DIRECTORY "${LZ4_PREFIX}/include")
  add_library(LZ4::lz4 STATIC IMPORTED)
  set_target_properties(LZ4::lz4 PROPERTIES IMPORTED_LOCATION "${LZ4_STATIC_LIB}")
  target_include_directories(LZ4::lz4 BEFORE INTERFACE "${LZ4_PREFIX}/include")
  add_dependencies(LZ4::lz4 lz4_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS LZ4::lz4)
endmacro()

if(ARROW_WITH_LZ4)
  resolve_dependency(lz4
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     liblz4)
endif()

macro(build_zstd)
  message(STATUS "Building Zstandard from source")

  set(ZSTD_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-install")

  set(ZSTD_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${ZSTD_PREFIX}"
      -DZSTD_BUILD_PROGRAMS=OFF
      -DZSTD_BUILD_SHARED=OFF
      -DZSTD_BUILD_STATIC=ON
      -DZSTD_MULTITHREAD_SUPPORT=OFF)

  if(MSVC)
    set(ZSTD_STATIC_LIB "${ZSTD_PREFIX}/lib/zstd_static.lib")
    if(ARROW_USE_STATIC_CRT)
      list(APPEND ZSTD_CMAKE_ARGS "-DZSTD_USE_STATIC_RUNTIME=ON")
    endif()
  else()
    set(ZSTD_STATIC_LIB "${ZSTD_PREFIX}/lib/libzstd.a")
  endif()

  externalproject_add(zstd_ep
                      ${EP_COMMON_OPTIONS}
                      CMAKE_ARGS ${ZSTD_CMAKE_ARGS}
                      SOURCE_SUBDIR "build/cmake"
                      INSTALL_DIR ${ZSTD_PREFIX}
                      URL ${ZSTD_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_ZSTD_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS "${ZSTD_STATIC_LIB}")

  file(MAKE_DIRECTORY "${ZSTD_PREFIX}/include")

  add_library(zstd::libzstd_static STATIC IMPORTED)
  set_target_properties(zstd::libzstd_static PROPERTIES IMPORTED_LOCATION
                                                        "${ZSTD_STATIC_LIB}")
  target_include_directories(zstd::libzstd_static BEFORE
                             INTERFACE "${ZSTD_PREFIX}/include")

  add_dependencies(zstd::libzstd_static zstd_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS zstd::libzstd_static)

  set(ZSTD_VENDORED TRUE)
endmacro()

if(ARROW_WITH_ZSTD)
  # ARROW-13384: ZSTD_minCLevel was added in v1.4.0, required by ARROW-13091
  resolve_dependency(zstd
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     libzstd
                     REQUIRED_VERSION
                     1.4.0)

  if(ZSTD_VENDORED)
    set(ARROW_ZSTD_LIBZSTD zstd::libzstd_static)
  else()
    if(ARROW_ZSTD_USE_SHARED)
      set(ARROW_ZSTD_LIBZSTD zstd::libzstd_shared)
    else()
      set(ARROW_ZSTD_LIBZSTD zstd::libzstd_static)
    endif()
    if(NOT TARGET ${ARROW_ZSTD_LIBZSTD})
      message(FATAL_ERROR "Zstandard target doesn't exist: ${ARROW_ZSTD_LIBZSTD}")
    endif()
    message(STATUS "Found Zstandard: ${ARROW_ZSTD_LIBZSTD}")
  endif()
endif()

# ----------------------------------------------------------------------
# RE2 (required for Gandiva)

macro(build_re2)
  message(STATUS "Building RE2 from source")
  set(RE2_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/re2_ep-install")
  set(RE2_STATIC_LIB
      "${RE2_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}re2${CMAKE_STATIC_LIBRARY_SUFFIX}")

  set(RE2_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${RE2_PREFIX}")

  externalproject_add(re2_ep
                      ${EP_COMMON_OPTIONS}
                      INSTALL_DIR ${RE2_PREFIX}
                      URL ${RE2_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_RE2_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${RE2_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${RE2_STATIC_LIB}")

  file(MAKE_DIRECTORY "${RE2_PREFIX}/include")
  add_library(re2::re2 STATIC IMPORTED)
  set_target_properties(re2::re2 PROPERTIES IMPORTED_LOCATION "${RE2_STATIC_LIB}")
  target_include_directories(re2::re2 BEFORE INTERFACE "${RE2_PREFIX}/include")

  add_dependencies(re2::re2 re2_ep)
  set(RE2_VENDORED TRUE)
  # Set values so that FindRE2 finds this too
  set(RE2_LIB ${RE2_STATIC_LIB})
  set(RE2_INCLUDE_DIR "${RE2_PREFIX}/include")

  list(APPEND ARROW_BUNDLED_STATIC_LIBS re2::re2)
endmacro()

if(ARROW_WITH_RE2)
  resolve_dependency(re2
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     re2)
endif()

macro(build_bzip2)
  message(STATUS "Building BZip2 from source")
  set(BZIP2_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/bzip2_ep-install")
  set(BZIP2_STATIC_LIB
      "${BZIP2_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}bz2${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  set(BZIP2_EXTRA_ARGS "CC=${CMAKE_C_COMPILER}" "CFLAGS=${EP_C_FLAGS}")

  if(CMAKE_OSX_SYSROOT)
    list(APPEND BZIP2_EXTRA_ARGS "SDKROOT=${CMAKE_OSX_SYSROOT}")
  endif()

  if(CMAKE_AR)
    list(APPEND BZIP2_EXTRA_ARGS AR=${CMAKE_AR})
  endif()

  if(CMAKE_RANLIB)
    list(APPEND BZIP2_EXTRA_ARGS RANLIB=${CMAKE_RANLIB})
  endif()

  externalproject_add(bzip2_ep
                      ${EP_COMMON_OPTIONS}
                      CONFIGURE_COMMAND ""
                      BUILD_IN_SOURCE 1
                      BUILD_COMMAND ${MAKE} libbz2.a ${MAKE_BUILD_ARGS}
                                    ${BZIP2_EXTRA_ARGS}
                      INSTALL_COMMAND ${MAKE} install -j1 PREFIX=${BZIP2_PREFIX}
                                      ${BZIP2_EXTRA_ARGS}
                      INSTALL_DIR ${BZIP2_PREFIX}
                      URL ${ARROW_BZIP2_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_BZIP2_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS "${BZIP2_STATIC_LIB}")

  file(MAKE_DIRECTORY "${BZIP2_PREFIX}/include")
  add_library(BZip2::BZip2 STATIC IMPORTED)
  set_target_properties(BZip2::BZip2 PROPERTIES IMPORTED_LOCATION "${BZIP2_STATIC_LIB}")
  target_include_directories(BZip2::BZip2 BEFORE INTERFACE "${BZIP2_PREFIX}/include")
  set(BZIP2_INCLUDE_DIR "${BZIP2_PREFIX}/include")

  add_dependencies(BZip2::BZip2 bzip2_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS BZip2::BZip2)
endmacro()

if(ARROW_WITH_BZ2)
  resolve_dependency(BZip2 PC_PACKAGE_NAMES bzip2)

  if(${BZip2_SOURCE} STREQUAL "SYSTEM"
     AND NOT bzip2_PC_FOUND
     AND ARROW_BUILD_STATIC)
    get_target_property(BZIP2_TYPE BZip2::BZip2 TYPE)
    if(BZIP2_TYPE STREQUAL "INTERFACE_LIBRARY")
      # Conan
      string(APPEND ARROW_PC_LIBS_PRIVATE
             " $<TARGET_FILE:CONAN_LIB::bzip2_bz2_$<CONFIG>>")
    else()
      string(APPEND ARROW_PC_LIBS_PRIVATE " $<TARGET_FILE:BZip2::BZip2>")
    endif()
  endif()
endif()

macro(build_utf8proc)
  message(STATUS "Building utf8proc from source")
  set(UTF8PROC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/utf8proc_ep-install")
  if(MSVC)
    set(UTF8PROC_STATIC_LIB "${UTF8PROC_PREFIX}/lib/utf8proc_static.lib")
  else()
    set(UTF8PROC_STATIC_LIB
        "${UTF8PROC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}utf8proc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  endif()

  set(UTF8PROC_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS}
                          "-DCMAKE_INSTALL_PREFIX=${UTF8PROC_PREFIX}")

  externalproject_add(utf8proc_ep
                      ${EP_COMMON_OPTIONS}
                      CMAKE_ARGS ${UTF8PROC_CMAKE_ARGS}
                      INSTALL_DIR ${UTF8PROC_PREFIX}
                      URL ${ARROW_UTF8PROC_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_UTF8PROC_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS "${UTF8PROC_STATIC_LIB}")

  file(MAKE_DIRECTORY "${UTF8PROC_PREFIX}/include")
  add_library(utf8proc::utf8proc STATIC IMPORTED)
  set_target_properties(utf8proc::utf8proc
                        PROPERTIES IMPORTED_LOCATION "${UTF8PROC_STATIC_LIB}"
                                   INTERFACE_COMPILE_DEFINITIONS "UTF8PROC_STATIC")
  target_include_directories(utf8proc::utf8proc BEFORE
                             INTERFACE "${UTF8PROC_PREFIX}/include")

  add_dependencies(utf8proc::utf8proc utf8proc_ep)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS utf8proc::utf8proc)
endmacro()

if(ARROW_WITH_UTF8PROC)
  set(utf8proc_resolve_dependency_args utf8proc PC_PACKAGE_NAMES libutf8proc)
  if(NOT VCPKG_TOOLCHAIN)
    # utf8proc in vcpkg doesn't provide version information:
    # https://github.com/microsoft/vcpkg/issues/39176
    list(APPEND utf8proc_resolve_dependency_args REQUIRED_VERSION "2.2.0")
  endif()
  resolve_dependency(${utf8proc_resolve_dependency_args})
endif()

macro(build_cares)
  message(STATUS "Building c-ares from source")
  set(CARES_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/cares_ep-install")
  set(CARES_INCLUDE_DIR "${CARES_PREFIX}/include")

  # If you set -DCARES_SHARED=ON then the build system names the library
  # libcares_static.a
  set(CARES_STATIC_LIB
      "${CARES_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}cares${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  set(CARES_CMAKE_ARGS "${EP_COMMON_CMAKE_ARGS}" "-DCMAKE_INSTALL_PREFIX=${CARES_PREFIX}"
                       -DCARES_SHARED=OFF -DCARES_STATIC=ON)

  externalproject_add(cares_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${CARES_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_CARES_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${CARES_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${CARES_STATIC_LIB}")

  file(MAKE_DIRECTORY ${CARES_INCLUDE_DIR})

  add_library(c-ares::cares STATIC IMPORTED)
  set_target_properties(c-ares::cares PROPERTIES IMPORTED_LOCATION "${CARES_STATIC_LIB}")
  target_include_directories(c-ares::cares BEFORE INTERFACE "${CARES_INCLUDE_DIR}")
  add_dependencies(c-ares::cares cares_ep)

  if(APPLE)
    # libresolv must be linked from c-ares version 1.16.1
    find_library(LIBRESOLV_LIBRARY NAMES resolv libresolv REQUIRED)
    set_target_properties(c-ares::cares PROPERTIES INTERFACE_LINK_LIBRARIES
                                                   "${LIBRESOLV_LIBRARY}")
  endif()

  set(CARES_VENDORED TRUE)

  list(APPEND ARROW_BUNDLED_STATIC_LIBS c-ares::cares)
endmacro()

# ----------------------------------------------------------------------
# Dependencies for Arrow Flight RPC

macro(ensure_absl)
  if(NOT absl_FOUND)
    if(${absl_SOURCE} STREQUAL "AUTO")
      # We can't use resolve_dependency(absl 20211102) to use Abseil
      # 20211102 or later because Abseil's CMake package uses "EXACT"
      # version match strategy. Our CMake configuration will work with
      # Abseil LTS 20211102 or later. So we want to accept Abseil LTS
      # 20211102 or later. We need to update
      # ARROW_ABSL_REQUIRED_LTS_VERSIONS list when new Abseil LTS is
      # released.
      set(ARROW_ABSL_REQUIRED_LTS_VERSIONS 20230125 20220623 20211102)
      foreach(_VERSION ${ARROW_ABSL_REQUIRED_LTS_VERSIONS})
        find_package(absl ${_VERSION})
        if(absl_FOUND)
          break()
        endif()
      endforeach()
      # If we can't find Abseil LTS 20211102 or later, we use bundled
      # Abseil.
      if(NOT absl_FOUND)
        set(absl_SOURCE "BUNDLED")
      endif()
    endif()
    resolve_dependency(absl)
  endif()
endmacro()

macro(build_absl)
  message(STATUS "Building Abseil-cpp from source")
  set(absl_FOUND TRUE)
  set(absl_VERSION ${ARROW_ABSL_BUILD_VERSION})
  set(ABSL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/absl_ep-install")
  set(ABSL_INCLUDE_DIR "${ABSL_PREFIX}/include")
  set(ABSL_CMAKE_ARGS "${EP_COMMON_CMAKE_ARGS}" -DABSL_RUN_TESTS=OFF
                      "-DCMAKE_INSTALL_PREFIX=${ABSL_PREFIX}")
  set(ABSL_BUILD_BYPRODUCTS)
  set(ABSL_LIBRARIES)

  # Abseil produces the following libraries, each is fairly small, but there
  # are (as you can see), many of them. We need to add the libraries first,
  # and then describe how they depend on each other. The list can be
  # refreshed using:
  #   ls -1 $PREFIX/lib/libabsl_*.a | sed -e 's/.*libabsl_//' -e 's/.a$//'
  set(_ABSL_LIBS
      bad_any_cast_impl
      bad_optional_access
      bad_variant_access
      base
      city
      civil_time
      cord
      cord_internal
      cordz_functions
      cordz_handle
      cordz_info
      cordz_sample_token
      debugging_internal
      demangle_internal
      examine_stack
      exponential_biased
      failure_signal_handler
      flags
      flags_commandlineflag
      flags_commandlineflag_internal
      flags_config
      flags_internal
      flags_marshalling
      flags_parse
      flags_private_handle_accessor
      flags_program_name
      flags_reflection
      flags_usage
      flags_usage_internal
      graphcycles_internal
      hash
      hashtablez_sampler
      int128
      leak_check
      leak_check_disable
      log_severity
      low_level_hash
      malloc_internal
      periodic_sampler
      random_distributions
      random_internal_distribution_test_util
      random_internal_platform
      random_internal_pool_urbg
      random_internal_randen
      random_internal_randen_hwaes
      random_internal_randen_hwaes_impl
      random_internal_randen_slow
      random_internal_seed_material
      random_seed_gen_exception
      random_seed_sequences
      raw_hash_set
      raw_logging_internal
      scoped_set_env
      spinlock_wait
      stacktrace
      status
      statusor
      str_format_internal
      strerror
      strings
      strings_internal
      symbolize
      synchronization
      throw_delegate
      time
      time_zone
      wyhash)
  # Abseil creates a number of header-only targets, which are needed to resolve dependencies.
  # The list can be refreshed using:
  #   comm -13 <(ls -l $PREFIX/lib/libabsl_*.a | sed -e 's/.*libabsl_//' -e 's/.a$//' | sort -u) \
  #            <(ls -1 $PREFIX/lib/pkgconfig/absl_*.pc | sed -e 's/.*absl_//' -e 's/.pc$//' | sort -u)
  set(_ABSL_INTERFACE_LIBS
      algorithm
      algorithm_container
      any
      atomic_hook
      bad_any_cast
      base_internal
      bind_front
      bits
      btree
      cleanup
      cleanup_internal
      compare
      compressed_tuple
      config
      container_common
      container_memory
      cordz_statistics
      cordz_update_scope
      cordz_update_tracker
      core_headers
      counting_allocator
      debugging
      dynamic_annotations
      endian
      errno_saver
      fast_type_id
      fixed_array
      flags_path_util
      flat_hash_map
      flat_hash_set
      function_ref
      hash_function_defaults
      hash_policy_traits
      hashtable_debug
      hashtable_debug_hooks
      have_sse
      inlined_vector
      inlined_vector_internal
      kernel_timeout_internal
      layout
      memory
      meta
      node_hash_map
      node_hash_policy
      node_hash_set
      numeric
      numeric_representation
      optional
      pretty_function
      random_bit_gen_ref
      random_internal_distribution_caller
      random_internal_fast_uniform_bits
      random_internal_fastmath
      random_internal_generate_real
      random_internal_iostream_state_saver
      random_internal_mock_helpers
      random_internal_nonsecure_base
      random_internal_pcg_engine
      random_internal_randen_engine
      random_internal_salted_seed_seq
      random_internal_traits
      random_internal_uniform_helper
      random_internal_wide_multiply
      random_random
      raw_hash_map
      sample_recorder
      span
      str_format
      type_traits
      utility
      variant)

  foreach(_ABSL_LIB ${_ABSL_LIBS})
    set(_ABSL_STATIC_LIBRARY
        "${ABSL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}absl_${_ABSL_LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    add_library(absl::${_ABSL_LIB} STATIC IMPORTED)
    set_target_properties(absl::${_ABSL_LIB} PROPERTIES IMPORTED_LOCATION
                                                        ${_ABSL_STATIC_LIBRARY})
    target_include_directories(absl::${_ABSL_LIB} BEFORE INTERFACE "${ABSL_INCLUDE_DIR}")
    list(APPEND ABSL_BUILD_BYPRODUCTS ${_ABSL_STATIC_LIBRARY})
  endforeach()
  foreach(_ABSL_LIB ${_ABSL_INTERFACE_LIBS})
    add_library(absl::${_ABSL_LIB} INTERFACE IMPORTED)
    target_include_directories(absl::${_ABSL_LIB} BEFORE INTERFACE "${ABSL_INCLUDE_DIR}")
  endforeach()

  # Extracted the dependency information using the Abseil pkg-config files:
  #   grep Requires $PREFIX/lib/pkgconfig/absl_*.pc | \
  #   sed -e 's;.*/absl_;set_property(TARGET absl::;' \
  #       -e 's/.pc:Requires:/ PROPERTY INTERFACE_LINK_LIBRARIES /' \
  #       -E -e 's/ = 20[0-9]{6},?//g' \
  #       -e 's/absl_/absl::/g' \
  #       -e 's/$/)/'  | \
  #   grep -v 'INTERFACE_LINK_LIBRARIES[ ]*)'
  set_property(TARGET absl::algorithm PROPERTY INTERFACE_LINK_LIBRARIES absl::config)
  set_property(TARGET absl::algorithm_container
               PROPERTY INTERFACE_LINK_LIBRARIES absl::algorithm absl::core_headers
                        absl::meta)
  set_property(TARGET absl::any
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::bad_any_cast
                        absl::config
                        absl::core_headers
                        absl::fast_type_id
                        absl::type_traits
                        absl::utility)
  set_property(TARGET absl::atomic_hook PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                                 absl::core_headers)
  set_property(TARGET absl::bad_any_cast PROPERTY INTERFACE_LINK_LIBRARIES
                                                  absl::bad_any_cast_impl absl::config)
  set_property(TARGET absl::bad_any_cast_impl
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config absl::raw_logging_internal)
  set_property(TARGET absl::bad_optional_access
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config absl::raw_logging_internal)
  set_property(TARGET absl::bad_variant_access
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config absl::raw_logging_internal)
  set_property(TARGET absl::base
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::atomic_hook
                        absl::base_internal
                        absl::config
                        absl::core_headers
                        absl::dynamic_annotations
                        absl::log_severity
                        absl::raw_logging_internal
                        absl::spinlock_wait
                        absl::type_traits)
  set_property(TARGET absl::base_internal PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                                   absl::type_traits)
  set_property(TARGET absl::bind_front
               PROPERTY INTERFACE_LINK_LIBRARIES absl::base_internal
                        absl::compressed_tuple)
  set_property(TARGET absl::bits PROPERTY INTERFACE_LINK_LIBRARIES absl::core_headers)
  set_property(TARGET absl::btree
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::container_common
                        absl::compare
                        absl::compressed_tuple
                        absl::container_memory
                        absl::cord
                        absl::core_headers
                        absl::layout
                        absl::memory
                        absl::strings
                        absl::throw_delegate
                        absl::type_traits
                        absl::utility)
  set_property(TARGET absl::city PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                          absl::core_headers absl::endian)
  set_property(TARGET absl::cleanup
               PROPERTY INTERFACE_LINK_LIBRARIES absl::cleanup_internal absl::config
                        absl::core_headers)
  set_property(TARGET absl::cleanup_internal
               PROPERTY INTERFACE_LINK_LIBRARIES absl::base_internal absl::core_headers
                        absl::utility)
  set_property(TARGET absl::compare PROPERTY INTERFACE_LINK_LIBRARIES absl::core_headers
                                             absl::type_traits)
  set_property(TARGET absl::compressed_tuple PROPERTY INTERFACE_LINK_LIBRARIES
                                                      absl::utility)
  set_property(TARGET absl::container_common PROPERTY INTERFACE_LINK_LIBRARIES
                                                      absl::type_traits)
  set_property(TARGET absl::container_memory
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::memory
                        absl::type_traits
                        absl::utility)
  set_property(TARGET absl::cord
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::config
                        absl::cord_internal
                        absl::cordz_functions
                        absl::cordz_info
                        absl::cordz_update_scope
                        absl::cordz_update_tracker
                        absl::core_headers
                        absl::endian
                        absl::fixed_array
                        absl::function_ref
                        absl::inlined_vector
                        absl::optional
                        absl::raw_logging_internal
                        absl::strings
                        absl::type_traits)
  set_property(TARGET absl::cord_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base_internal
                        absl::compressed_tuple
                        absl::config
                        absl::core_headers
                        absl::endian
                        absl::inlined_vector
                        absl::layout
                        absl::raw_logging_internal
                        absl::strings
                        absl::throw_delegate
                        absl::type_traits)
  set_property(TARGET absl::cordz_functions
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::exponential_biased
                        absl::raw_logging_internal)
  set_property(TARGET absl::cordz_handle
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::config
                        absl::raw_logging_internal
                        absl::synchronization)
  set_property(TARGET absl::cordz_info
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::config
                        absl::cord_internal
                        absl::cordz_functions
                        absl::cordz_handle
                        absl::cordz_statistics
                        absl::cordz_update_tracker
                        absl::core_headers
                        absl::inlined_vector
                        absl::span
                        absl::raw_logging_internal
                        absl::stacktrace
                        absl::synchronization)
  set_property(TARGET absl::cordz_sample_token
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config absl::cordz_handle
                        absl::cordz_info)
  set_property(TARGET absl::cordz_statistics
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::cordz_update_tracker
                        absl::synchronization)
  set_property(TARGET absl::cordz_update_scope
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::cord_internal
                        absl::cordz_info
                        absl::cordz_update_tracker
                        absl::core_headers)
  set_property(TARGET absl::cordz_update_tracker PROPERTY INTERFACE_LINK_LIBRARIES
                                                          absl::config)
  set_property(TARGET absl::core_headers PROPERTY INTERFACE_LINK_LIBRARIES absl::config)
  set_property(TARGET absl::counting_allocator PROPERTY INTERFACE_LINK_LIBRARIES
                                                        absl::config)
  set_property(TARGET absl::debugging PROPERTY INTERFACE_LINK_LIBRARIES absl::stacktrace
                                               absl::leak_check)
  set_property(TARGET absl::debugging_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::core_headers
                        absl::config
                        absl::dynamic_annotations
                        absl::errno_saver
                        absl::raw_logging_internal)
  set_property(TARGET absl::demangle_internal PROPERTY INTERFACE_LINK_LIBRARIES
                                                       absl::base absl::core_headers)
  set_property(TARGET absl::dynamic_annotations PROPERTY INTERFACE_LINK_LIBRARIES
                                                         absl::config)
  set_property(TARGET absl::endian PROPERTY INTERFACE_LINK_LIBRARIES absl::base
                                            absl::config absl::core_headers)
  set_property(TARGET absl::errno_saver PROPERTY INTERFACE_LINK_LIBRARIES absl::config)
  set_property(TARGET absl::examine_stack
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::stacktrace
                        absl::symbolize
                        absl::config
                        absl::core_headers
                        absl::raw_logging_internal)
  set_property(TARGET absl::exponential_biased PROPERTY INTERFACE_LINK_LIBRARIES
                                                        absl::config absl::core_headers)
  set_property(TARGET absl::failure_signal_handler
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::examine_stack
                        absl::stacktrace
                        absl::base
                        absl::config
                        absl::core_headers
                        absl::errno_saver
                        absl::raw_logging_internal)
  set_property(TARGET absl::fast_type_id PROPERTY INTERFACE_LINK_LIBRARIES absl::config)
  set_property(TARGET absl::fixed_array
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::compressed_tuple
                        absl::algorithm
                        absl::config
                        absl::core_headers
                        absl::dynamic_annotations
                        absl::throw_delegate
                        absl::memory)
  set_property(TARGET absl::flags
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::flags_commandlineflag
                        absl::flags_config
                        absl::flags_internal
                        absl::flags_reflection
                        absl::base
                        absl::core_headers
                        absl::strings)
  set_property(TARGET absl::flags_commandlineflag
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::fast_type_id
                        absl::flags_commandlineflag_internal
                        absl::optional
                        absl::strings)
  set_property(TARGET absl::flags_commandlineflag_internal
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config absl::fast_type_id)
  set_property(TARGET absl::flags_config
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::flags_path_util
                        absl::flags_program_name
                        absl::core_headers
                        absl::strings
                        absl::synchronization)
  set_property(TARGET absl::flags_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::config
                        absl::flags_commandlineflag
                        absl::flags_commandlineflag_internal
                        absl::flags_config
                        absl::flags_marshalling
                        absl::synchronization
                        absl::meta
                        absl::utility)
  set_property(TARGET absl::flags_marshalling
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::log_severity
                        absl::strings
                        absl::str_format)
  set_property(TARGET absl::flags_parse
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::flags_config
                        absl::flags
                        absl::flags_commandlineflag
                        absl::flags_commandlineflag_internal
                        absl::flags_internal
                        absl::flags_private_handle_accessor
                        absl::flags_program_name
                        absl::flags_reflection
                        absl::flags_usage
                        absl::strings
                        absl::synchronization)
  set_property(TARGET absl::flags_path_util PROPERTY INTERFACE_LINK_LIBRARIES
                                                     absl::config absl::strings)
  set_property(TARGET absl::flags_private_handle_accessor
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::flags_commandlineflag
                        absl::flags_commandlineflag_internal
                        absl::strings)
  set_property(TARGET absl::flags_program_name
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::flags_path_util
                        absl::strings
                        absl::synchronization)
  set_property(TARGET absl::flags_reflection
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::flags_commandlineflag
                        absl::flags_private_handle_accessor
                        absl::flags_config
                        absl::strings
                        absl::synchronization
                        absl::flat_hash_map)
  set_property(TARGET absl::flags_usage
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::flags_usage_internal
                        absl::strings
                        absl::synchronization)
  set_property(TARGET absl::flags_usage_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::flags_config
                        absl::flags
                        absl::flags_commandlineflag
                        absl::flags_internal
                        absl::flags_path_util
                        absl::flags_private_handle_accessor
                        absl::flags_program_name
                        absl::flags_reflection
                        absl::flat_hash_map
                        absl::strings
                        absl::synchronization)
  set_property(TARGET absl::flat_hash_map
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::container_memory
                        absl::hash_function_defaults
                        absl::raw_hash_map
                        absl::algorithm_container
                        absl::memory)
  set_property(TARGET absl::flat_hash_set
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::container_memory
                        absl::hash_function_defaults
                        absl::raw_hash_set
                        absl::algorithm_container
                        absl::core_headers
                        absl::memory)
  set_property(TARGET absl::function_ref
               PROPERTY INTERFACE_LINK_LIBRARIES absl::base_internal absl::core_headers
                        absl::meta)
  set_property(TARGET absl::graphcycles_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::base_internal
                        absl::config
                        absl::core_headers
                        absl::malloc_internal
                        absl::raw_logging_internal)
  set_property(TARGET absl::hash
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::city
                        absl::config
                        absl::core_headers
                        absl::endian
                        absl::fixed_array
                        absl::meta
                        absl::int128
                        absl::strings
                        absl::optional
                        absl::variant
                        absl::utility
                        absl::low_level_hash)
  set_property(TARGET absl::hash_function_defaults
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::cord
                        absl::hash
                        absl::strings)
  set_property(TARGET absl::hash_policy_traits PROPERTY INTERFACE_LINK_LIBRARIES
                                                        absl::meta)
  set_property(TARGET absl::hashtable_debug PROPERTY INTERFACE_LINK_LIBRARIES
                                                     absl::hashtable_debug_hooks)
  set_property(TARGET absl::hashtable_debug_hooks PROPERTY INTERFACE_LINK_LIBRARIES
                                                           absl::config)
  set_property(TARGET absl::hashtablez_sampler
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::exponential_biased
                        absl::have_sse
                        absl::sample_recorder
                        absl::synchronization)
  set_property(TARGET absl::inlined_vector
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::algorithm
                        absl::core_headers
                        absl::inlined_vector_internal
                        absl::throw_delegate
                        absl::memory)
  set_property(TARGET absl::inlined_vector_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::compressed_tuple
                        absl::core_headers
                        absl::memory
                        absl::span
                        absl::type_traits)
  set_property(TARGET absl::int128 PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                            absl::core_headers absl::bits)
  set_property(TARGET absl::kernel_timeout_internal
               PROPERTY INTERFACE_LINK_LIBRARIES absl::core_headers
                        absl::raw_logging_internal absl::time)
  set_property(TARGET absl::layout
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::meta
                        absl::strings
                        absl::span
                        absl::utility)
  set_property(TARGET absl::leak_check PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                                absl::core_headers)
  set_property(TARGET absl::log_severity PROPERTY INTERFACE_LINK_LIBRARIES
                                                  absl::core_headers)
  set_property(TARGET absl::low_level_hash
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::bits
                        absl::config
                        absl::endian
                        absl::int128)
  set_property(TARGET absl::malloc_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::base_internal
                        absl::config
                        absl::core_headers
                        absl::dynamic_annotations
                        absl::raw_logging_internal)
  set_property(TARGET absl::memory PROPERTY INTERFACE_LINK_LIBRARIES absl::core_headers
                                            absl::meta)
  set_property(TARGET absl::meta PROPERTY INTERFACE_LINK_LIBRARIES absl::type_traits)
  set_property(TARGET absl::node_hash_map
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::container_memory
                        absl::hash_function_defaults
                        absl::node_hash_policy
                        absl::raw_hash_map
                        absl::algorithm_container
                        absl::memory)
  set_property(TARGET absl::node_hash_policy PROPERTY INTERFACE_LINK_LIBRARIES
                                                      absl::config)
  set_property(TARGET absl::node_hash_set
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::hash_function_defaults
                        absl::node_hash_policy
                        absl::raw_hash_set
                        absl::algorithm_container
                        absl::memory)
  set_property(TARGET absl::numeric PROPERTY INTERFACE_LINK_LIBRARIES absl::int128)
  set_property(TARGET absl::numeric_representation PROPERTY INTERFACE_LINK_LIBRARIES
                                                            absl::config)
  set_property(TARGET absl::optional
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::bad_optional_access
                        absl::base_internal
                        absl::config
                        absl::core_headers
                        absl::memory
                        absl::type_traits
                        absl::utility)
  set_property(TARGET absl::periodic_sampler
               PROPERTY INTERFACE_LINK_LIBRARIES absl::core_headers
                        absl::exponential_biased)
  set_property(TARGET absl::random_bit_gen_ref
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::core_headers
                        absl::random_internal_distribution_caller
                        absl::random_internal_fast_uniform_bits
                        absl::type_traits)
  set_property(TARGET absl::random_distributions
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base_internal
                        absl::config
                        absl::core_headers
                        absl::random_internal_generate_real
                        absl::random_internal_distribution_caller
                        absl::random_internal_fast_uniform_bits
                        absl::random_internal_fastmath
                        absl::random_internal_iostream_state_saver
                        absl::random_internal_traits
                        absl::random_internal_uniform_helper
                        absl::random_internal_wide_multiply
                        absl::strings
                        absl::type_traits)
  set_property(TARGET absl::random_internal_distribution_caller
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config absl::utility
                        absl::fast_type_id)
  set_property(TARGET absl::random_internal_distribution_test_util
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::raw_logging_internal
                        absl::strings
                        absl::str_format
                        absl::span)
  set_property(TARGET absl::random_internal_fast_uniform_bits
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config)
  set_property(TARGET absl::random_internal_fastmath PROPERTY INTERFACE_LINK_LIBRARIES
                                                              absl::bits)
  set_property(TARGET absl::random_internal_generate_real
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::bits
                        absl::random_internal_fastmath
                        absl::random_internal_traits
                        absl::type_traits)
  set_property(TARGET absl::random_internal_iostream_state_saver
               PROPERTY INTERFACE_LINK_LIBRARIES absl::int128 absl::type_traits)
  set_property(TARGET absl::random_internal_mock_helpers
               PROPERTY INTERFACE_LINK_LIBRARIES absl::fast_type_id absl::optional)
  set_property(TARGET absl::random_internal_nonsecure_base
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::core_headers
                        absl::optional
                        absl::random_internal_pool_urbg
                        absl::random_internal_salted_seed_seq
                        absl::random_internal_seed_material
                        absl::span
                        absl::type_traits)
  set_property(TARGET absl::random_internal_pcg_engine
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::int128
                        absl::random_internal_fastmath
                        absl::random_internal_iostream_state_saver
                        absl::type_traits)
  set_property(TARGET absl::random_internal_platform PROPERTY INTERFACE_LINK_LIBRARIES
                                                              absl::config)
  set_property(TARGET absl::random_internal_pool_urbg
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::config
                        absl::core_headers
                        absl::endian
                        absl::random_internal_randen
                        absl::random_internal_seed_material
                        absl::random_internal_traits
                        absl::random_seed_gen_exception
                        absl::raw_logging_internal
                        absl::span)
  set_property(TARGET absl::random_internal_randen
               PROPERTY INTERFACE_LINK_LIBRARIES absl::random_internal_platform
                        absl::random_internal_randen_hwaes
                        absl::random_internal_randen_slow)
  set_property(TARGET absl::random_internal_randen_engine
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::endian
                        absl::random_internal_iostream_state_saver
                        absl::random_internal_randen
                        absl::raw_logging_internal
                        absl::type_traits)
  set_property(TARGET absl::random_internal_randen_hwaes
               PROPERTY INTERFACE_LINK_LIBRARIES absl::random_internal_platform
                        absl::random_internal_randen_hwaes_impl absl::config)
  set_property(TARGET absl::random_internal_randen_hwaes_impl
               PROPERTY INTERFACE_LINK_LIBRARIES absl::random_internal_platform
                        absl::config)
  set_property(TARGET absl::random_internal_randen_slow
               PROPERTY INTERFACE_LINK_LIBRARIES absl::random_internal_platform
                        absl::config)
  set_property(TARGET absl::random_internal_salted_seed_seq
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::inlined_vector
                        absl::optional
                        absl::span
                        absl::random_internal_seed_material
                        absl::type_traits)
  set_property(TARGET absl::random_internal_seed_material
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::core_headers
                        absl::optional
                        absl::random_internal_fast_uniform_bits
                        absl::raw_logging_internal
                        absl::span
                        absl::strings)
  set_property(TARGET absl::random_internal_traits PROPERTY INTERFACE_LINK_LIBRARIES
                                                            absl::config)
  set_property(TARGET absl::random_internal_uniform_helper
               PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                        absl::random_internal_traits absl::type_traits)
  set_property(TARGET absl::random_internal_wide_multiply
               PROPERTY INTERFACE_LINK_LIBRARIES absl::bits absl::config absl::int128)
  set_property(TARGET absl::random_random
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::random_distributions
                        absl::random_internal_nonsecure_base
                        absl::random_internal_pcg_engine
                        absl::random_internal_pool_urbg
                        absl::random_internal_randen_engine
                        absl::random_seed_sequences)
  set_property(TARGET absl::random_seed_gen_exception PROPERTY INTERFACE_LINK_LIBRARIES
                                                               absl::config)
  set_property(TARGET absl::random_seed_sequences
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::inlined_vector
                        absl::random_internal_nonsecure_base
                        absl::random_internal_pool_urbg
                        absl::random_internal_salted_seed_seq
                        absl::random_internal_seed_material
                        absl::random_seed_gen_exception
                        absl::span)
  set_property(TARGET absl::raw_hash_map
               PROPERTY INTERFACE_LINK_LIBRARIES absl::container_memory
                        absl::raw_hash_set absl::throw_delegate)
  set_property(TARGET absl::raw_hash_set
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::bits
                        absl::compressed_tuple
                        absl::config
                        absl::container_common
                        absl::container_memory
                        absl::core_headers
                        absl::endian
                        absl::hash_policy_traits
                        absl::hashtable_debug_hooks
                        absl::have_sse
                        absl::memory
                        absl::meta
                        absl::optional
                        absl::utility
                        absl::hashtablez_sampler)
  set_property(TARGET absl::raw_logging_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::atomic_hook
                        absl::config
                        absl::core_headers
                        absl::log_severity)
  set_property(TARGET absl::sample_recorder PROPERTY INTERFACE_LINK_LIBRARIES absl::base
                                                     absl::synchronization)
  set_property(TARGET absl::scoped_set_env PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                                    absl::raw_logging_internal)
  set_property(TARGET absl::span
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::algorithm
                        absl::core_headers
                        absl::throw_delegate
                        absl::type_traits)
  set_property(TARGET absl::spinlock_wait
               PROPERTY INTERFACE_LINK_LIBRARIES absl::base_internal absl::core_headers
                        absl::errno_saver)
  set_property(TARGET absl::stacktrace
               PROPERTY INTERFACE_LINK_LIBRARIES absl::debugging_internal absl::config
                        absl::core_headers)
  set_property(TARGET absl::status
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::atomic_hook
                        absl::config
                        absl::core_headers
                        absl::function_ref
                        absl::raw_logging_internal
                        absl::inlined_vector
                        absl::stacktrace
                        absl::symbolize
                        absl::strings
                        absl::cord
                        absl::str_format
                        absl::optional)
  set_property(TARGET absl::statusor
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::status
                        absl::core_headers
                        absl::raw_logging_internal
                        absl::type_traits
                        absl::strings
                        absl::utility
                        absl::variant)
  set_property(TARGET absl::str_format PROPERTY INTERFACE_LINK_LIBRARIES
                                                absl::str_format_internal)
  set_property(TARGET absl::str_format_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::bits
                        absl::strings
                        absl::config
                        absl::core_headers
                        absl::numeric_representation
                        absl::type_traits
                        absl::int128
                        absl::span)
  set_property(TARGET absl::strerror PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                              absl::core_headers absl::errno_saver)
  set_property(TARGET absl::strings
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::strings_internal
                        absl::base
                        absl::bits
                        absl::config
                        absl::core_headers
                        absl::endian
                        absl::int128
                        absl::memory
                        absl::raw_logging_internal
                        absl::throw_delegate
                        absl::type_traits)
  set_property(TARGET absl::strings_internal
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::config
                        absl::core_headers
                        absl::endian
                        absl::raw_logging_internal
                        absl::type_traits)
  set_property(TARGET absl::symbolize
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::debugging_internal
                        absl::demangle_internal
                        absl::base
                        absl::config
                        absl::core_headers
                        absl::dynamic_annotations
                        absl::malloc_internal
                        absl::raw_logging_internal
                        absl::strings)
  set_property(TARGET absl::synchronization
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::graphcycles_internal
                        absl::kernel_timeout_internal
                        absl::atomic_hook
                        absl::base
                        absl::base_internal
                        absl::config
                        absl::core_headers
                        absl::dynamic_annotations
                        absl::malloc_internal
                        absl::raw_logging_internal
                        absl::stacktrace
                        absl::symbolize
                        absl::time)
  set_property(TARGET absl::throw_delegate PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                                    absl::raw_logging_internal)
  set_property(TARGET absl::time
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::base
                        absl::civil_time
                        absl::core_headers
                        absl::int128
                        absl::raw_logging_internal
                        absl::strings
                        absl::time_zone)
  set_property(TARGET absl::type_traits PROPERTY INTERFACE_LINK_LIBRARIES absl::config)
  set_property(TARGET absl::utility PROPERTY INTERFACE_LINK_LIBRARIES absl::base_internal
                                             absl::config absl::type_traits)
  set_property(TARGET absl::variant
               PROPERTY INTERFACE_LINK_LIBRARIES
                        absl::bad_variant_access
                        absl::base_internal
                        absl::config
                        absl::core_headers
                        absl::type_traits
                        absl::utility)
  set_property(TARGET absl::wyhash PROPERTY INTERFACE_LINK_LIBRARIES absl::config
                                            absl::endian absl::int128)

  if(APPLE)
    # This is due to upstream absl::cctz issue
    # https://github.com/abseil/abseil-cpp/issues/283
    find_library(CoreFoundation CoreFoundation)
    set_property(TARGET absl::time
                 APPEND
                 PROPERTY INTERFACE_LINK_LIBRARIES ${CoreFoundation})
  endif()

  externalproject_add(absl_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${ABSL_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_ABSL_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${ABSL_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${ABSL_BUILD_BYPRODUCTS})

  # Work around https://gitlab.kitware.com/cmake/cmake/issues/15052
  file(MAKE_DIRECTORY ${ABSL_INCLUDE_DIR})

  set(ABSL_VENDORED TRUE)
endmacro()

macro(build_grpc)
  resolve_dependency(c-ares
                     ARROW_CMAKE_PACKAGE_NAME
                     ArrowFlight
                     ARROW_PC_PACKAGE_NAME
                     arrow-flight
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     libcares)
  ensure_absl()

  message(STATUS "Building gRPC from source")

  set(GRPC_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/grpc_ep-prefix/src/grpc_ep-build")
  set(GRPC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/grpc_ep-install")
  set(GRPC_HOME "${GRPC_PREFIX}")
  set(GRPC_INCLUDE_DIR "${GRPC_PREFIX}/include")

  set(GRPC_STATIC_LIBRARY_GPR
      "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gpr${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GRPC_STATIC_LIBRARY_GRPC
      "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GRPC_STATIC_LIBRARY_GRPCPP
      "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc++${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GRPC_STATIC_LIBRARY_ADDRESS_SORTING
      "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}address_sorting${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GRPC_STATIC_LIBRARY_GRPCPP_REFLECTION
      "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc++_reflection${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GRPC_STATIC_LIBRARY_UPB
      "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}upb${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(GRPC_CPP_PLUGIN "${GRPC_PREFIX}/bin/grpc_cpp_plugin${CMAKE_EXECUTABLE_SUFFIX}")

  set(GRPC_CMAKE_PREFIX)

  add_custom_target(grpc_dependencies)

  if(ABSL_VENDORED)
    add_dependencies(grpc_dependencies absl_ep)
  endif()
  if(CARES_VENDORED)
    add_dependencies(grpc_dependencies cares_ep)
  endif()

  if(GFLAGS_VENDORED)
    add_dependencies(grpc_dependencies gflags_ep)
  endif()

  if(RE2_VENDORED)
    add_dependencies(grpc_dependencies re2_ep)
  endif()

  add_dependencies(grpc_dependencies ${ARROW_PROTOBUF_LIBPROTOBUF} c-ares::cares
                   ZLIB::ZLIB)

  get_target_property(GRPC_PROTOBUF_INCLUDE_DIR ${ARROW_PROTOBUF_LIBPROTOBUF}
                      INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(GRPC_PB_ROOT "${GRPC_PROTOBUF_INCLUDE_DIR}" DIRECTORY)
  get_target_property(GRPC_Protobuf_PROTOC_LIBRARY ${ARROW_PROTOBUF_LIBPROTOC}
                      IMPORTED_LOCATION)
  get_target_property(GRPC_CARES_INCLUDE_DIR c-ares::cares INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(GRPC_CARES_ROOT "${GRPC_CARES_INCLUDE_DIR}" DIRECTORY)
  get_target_property(GRPC_GFLAGS_INCLUDE_DIR ${GFLAGS_LIBRARIES}
                      INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(GRPC_GFLAGS_ROOT "${GRPC_GFLAGS_INCLUDE_DIR}" DIRECTORY)
  get_target_property(GRPC_RE2_INCLUDE_DIR re2::re2 INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(GRPC_RE2_ROOT "${GRPC_RE2_INCLUDE_DIR}" DIRECTORY)

  # Put Abseil, etc. first so that local directories are searched
  # before (what are likely) system directories
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${ABSL_PREFIX}")
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${GRPC_PB_ROOT}")
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${GRPC_GFLAGS_ROOT}")
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${GRPC_CARES_ROOT}")
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${GRPC_RE2_ROOT}")

  # ZLIB is never vendored
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${ZLIB_ROOT}")

  if(RAPIDJSON_VENDORED)
    add_dependencies(grpc_dependencies rapidjson_ep)
  endif()

  # Yuck, see https://stackoverflow.com/a/45433229/776560
  string(REPLACE ";" ${EP_LIST_SEPARATOR} GRPC_PREFIX_PATH_ALT_SEP "${GRPC_CMAKE_PREFIX}")

  set(GRPC_C_FLAGS "${EP_C_FLAGS}")
  set(GRPC_CXX_FLAGS "${EP_CXX_FLAGS}")
  if(NOT MSVC)
    # Negate warnings that gRPC cannot build under
    # See https://github.com/grpc/grpc/issues/29417
    string(APPEND
           GRPC_C_FLAGS
           " -Wno-attributes"
           " -Wno-format-security"
           " -Wno-unknown-warning-option")
    string(APPEND
           GRPC_CXX_FLAGS
           " -Wno-attributes"
           " -Wno-format-security"
           " -Wno-unknown-warning-option")
  endif()

  set(GRPC_CMAKE_ARGS
      "${EP_COMMON_CMAKE_ARGS}"
      "-DCMAKE_C_FLAGS=${GRPC_C_FLAGS}"
      "-DCMAKE_CXX_FLAGS=${GRPC_CXX_FLAGS}"
      -DCMAKE_INSTALL_PREFIX=${GRPC_PREFIX}
      -DCMAKE_PREFIX_PATH='${GRPC_PREFIX_PATH_ALT_SEP}'
      -DOPENSSL_CRYPTO_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}
      -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
      -DOPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY}
      -DgRPC_ABSL_PROVIDER=package
      -DgRPC_BUILD_CSHARP_EXT=OFF
      -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF
      -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF
      -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF
      -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF
      -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF
      -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF
      -DgRPC_BUILD_TESTS=OFF
      -DgRPC_CARES_PROVIDER=package
      -DgRPC_GFLAGS_PROVIDER=package
      -DgRPC_MSVC_STATIC_RUNTIME=${ARROW_USE_STATIC_CRT}
      -DgRPC_PROTOBUF_PROVIDER=package
      -DgRPC_RE2_PROVIDER=package
      -DgRPC_SSL_PROVIDER=package
      -DgRPC_ZLIB_PROVIDER=package)
  if(PROTOBUF_VENDORED)
    list(APPEND GRPC_CMAKE_ARGS -DgRPC_PROTOBUF_PACKAGE_TYPE=CONFIG)
  endif()

  # XXX the gRPC git checkout is huge and takes a long time
  # Ideally, we should be able to use the tarballs, but they don't contain
  # vendored dependencies such as c-ares...
  externalproject_add(grpc_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${GRPC_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_GRPC_BUILD_SHA256_CHECKSUM}"
                      BUILD_BYPRODUCTS ${GRPC_STATIC_LIBRARY_GPR}
                                       ${GRPC_STATIC_LIBRARY_GRPC}
                                       ${GRPC_STATIC_LIBRARY_GRPCPP}
                                       ${GRPC_STATIC_LIBRARY_ADDRESS_SORTING}
                                       ${GRPC_STATIC_LIBRARY_GRPCPP_REFLECTION}
                                       ${GRPC_STATIC_LIBRARY_UPB}
                                       ${GRPC_CPP_PLUGIN}
                      CMAKE_ARGS ${GRPC_CMAKE_ARGS}
                      DEPENDS ${grpc_dependencies})

  # Work around https://gitlab.kitware.com/cmake/cmake/issues/15052
  file(MAKE_DIRECTORY ${GRPC_INCLUDE_DIR})

  add_library(gRPC::upb STATIC IMPORTED)
  set_target_properties(gRPC::upb PROPERTIES IMPORTED_LOCATION
                                             "${GRPC_STATIC_LIBRARY_UPB}")
  target_include_directories(gRPC::upb BEFORE INTERFACE "${GRPC_INCLUDE_DIR}")

  set(GRPC_GPR_ABSL_LIBRARIES
      # We need a flattened list of Abseil libraries for the static linking case,
      # because our method for creating arrow_bundled_dependencies.a doesn't walk
      # the dependency tree of targets.
      #
      # This list should be updated when we change Abseil / gRPC versions. It can
      # be generated with:
      # pkg-config --libs --static grpc \
      #   | tr " " "\n" \
      #   | grep ^-labsl_ \
      #   | sed 's/^-labsl_/absl::/'
      absl::raw_hash_set
      absl::hashtablez_sampler
      absl::hash
      absl::city
      absl::low_level_hash
      absl::random_distributions
      absl::random_seed_sequences
      absl::random_internal_pool_urbg
      absl::random_internal_randen
      absl::random_internal_randen_hwaes
      absl::random_internal_randen_hwaes_impl
      absl::random_internal_randen_slow
      absl::random_internal_platform
      absl::random_internal_seed_material
      absl::random_seed_gen_exception
      absl::statusor
      absl::status
      absl::cord
      absl::cordz_info
      absl::cord_internal
      absl::cordz_functions
      absl::exponential_biased
      absl::cordz_handle
      absl::bad_optional_access
      absl::str_format_internal
      absl::synchronization
      absl::graphcycles_internal
      absl::stacktrace
      absl::symbolize
      absl::debugging_internal
      absl::demangle_internal
      absl::malloc_internal
      absl::time
      absl::civil_time
      absl::strings
      absl::strings_internal
      absl::base
      absl::spinlock_wait
      absl::int128
      absl::throw_delegate
      absl::time_zone
      absl::bad_variant_access
      absl::raw_logging_internal
      absl::log_severity)

  add_library(gRPC::gpr STATIC IMPORTED)
  set_target_properties(gRPC::gpr PROPERTIES IMPORTED_LOCATION
                                             "${GRPC_STATIC_LIBRARY_GPR}")
  target_link_libraries(gRPC::gpr INTERFACE ${GRPC_GPR_ABSL_LIBRARIES})
  target_include_directories(gRPC::gpr BEFORE INTERFACE "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::address_sorting STATIC IMPORTED)
  set_target_properties(gRPC::address_sorting
                        PROPERTIES IMPORTED_LOCATION
                                   "${GRPC_STATIC_LIBRARY_ADDRESS_SORTING}")
  target_include_directories(gRPC::address_sorting BEFORE INTERFACE "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc++_reflection STATIC IMPORTED)
  set_target_properties(gRPC::grpc++_reflection
                        PROPERTIES IMPORTED_LOCATION
                                   "${GRPC_STATIC_LIBRARY_GRPCPP_REFLECTION}")
  target_include_directories(gRPC::grpc++_reflection BEFORE
                             INTERFACE "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc STATIC IMPORTED)
  set_target_properties(gRPC::grpc PROPERTIES IMPORTED_LOCATION
                                              "${GRPC_STATIC_LIBRARY_GRPC}")
  target_link_libraries(gRPC::grpc
                        INTERFACE gRPC::gpr
                                  gRPC::upb
                                  gRPC::address_sorting
                                  re2::re2
                                  c-ares::cares
                                  ZLIB::ZLIB
                                  OpenSSL::SSL
                                  Threads::Threads)
  target_include_directories(gRPC::grpc BEFORE INTERFACE "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc++ STATIC IMPORTED)
  set_target_properties(gRPC::grpc++ PROPERTIES IMPORTED_LOCATION
                                                "${GRPC_STATIC_LIBRARY_GRPCPP}")
  target_link_libraries(gRPC::grpc++ INTERFACE gRPC::grpc ${ARROW_PROTOBUF_LIBPROTOBUF})
  target_include_directories(gRPC::grpc++ BEFORE INTERFACE "${GRPC_INCLUDE_DIR}")

  add_executable(gRPC::grpc_cpp_plugin IMPORTED)
  set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES IMPORTED_LOCATION
                                                         ${GRPC_CPP_PLUGIN})

  add_dependencies(grpc_ep grpc_dependencies)
  add_dependencies(gRPC::grpc++ grpc_ep)
  add_dependencies(gRPC::grpc_cpp_plugin grpc_ep)
  set(GRPC_VENDORED TRUE)

  # ar -M rejects with the "libgrpc++.a" filename because "+" is a line
  # continuation character in these scripts, so we have to create a copy of the
  # static lib that we will bundle later

  set(GRPC_STATIC_LIBRARY_GRPCPP_FOR_AR
      "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}grpcpp${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  add_custom_command(OUTPUT ${GRPC_STATIC_LIBRARY_GRPCPP_FOR_AR}
                     COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:gRPC::grpc++>
                             ${GRPC_STATIC_LIBRARY_GRPCPP_FOR_AR}
                     DEPENDS grpc_ep)
  add_library(gRPC::grpcpp_for_bundling STATIC IMPORTED)
  set_target_properties(gRPC::grpcpp_for_bundling
                        PROPERTIES IMPORTED_LOCATION
                                   "${GRPC_STATIC_LIBRARY_GRPCPP_FOR_AR}")

  set_source_files_properties("${GRPC_STATIC_LIBRARY_GRPCPP_FOR_AR}" PROPERTIES GENERATED
                                                                                TRUE)
  add_custom_target(grpc_copy_grpc++ ALL DEPENDS "${GRPC_STATIC_LIBRARY_GRPCPP_FOR_AR}")
  add_dependencies(gRPC::grpcpp_for_bundling grpc_copy_grpc++)

  list(APPEND
       ARROW_BUNDLED_STATIC_LIBS
       gRPC::address_sorting
       gRPC::gpr
       gRPC::grpc
       gRPC::grpcpp_for_bundling
       gRPC::upb)
  if(ABSL_VENDORED)
    list(APPEND ARROW_BUNDLED_STATIC_LIBS ${GRPC_GPR_ABSL_LIBRARIES})
  endif()
endmacro()

if(ARROW_WITH_GRPC)
  if(NOT ARROW_ENABLE_THREADING)
    message(FATAL_ERROR "Can't use gRPC with ARROW_ENABLE_THREADING=OFF")
  endif()

  set(ARROW_GRPC_REQUIRED_VERSION "1.30.0")
  if(NOT Protobuf_SOURCE STREQUAL gRPC_SOURCE)
    # ARROW-15495: Protobuf/gRPC must come from the same source
    message(STATUS "Forcing gRPC_SOURCE to Protobuf_SOURCE (${Protobuf_SOURCE})")
    set(gRPC_SOURCE "${Protobuf_SOURCE}")
  endif()
  resolve_dependency(gRPC
                     ARROW_CMAKE_PACKAGE_NAME
                     ArrowFlight
                     ARROW_PC_PACKAGE_NAME
                     arrow-flight
                     HAVE_ALT
                     TRUE
                     PC_PACKAGE_NAMES
                     grpc++
                     REQUIRED_VERSION
                     ${ARROW_GRPC_REQUIRED_VERSION})

  if(GRPC_VENDORED)
    # Remove "v" from "vX.Y.Z"
    string(SUBSTRING ${ARROW_GRPC_BUILD_VERSION} 1 -1 ARROW_GRPC_VERSION)
    # Examples need to link to static Arrow if we're using static gRPC
    set(ARROW_GRPC_USE_SHARED OFF)
  else()
    if(gRPCAlt_VERSION)
      set(ARROW_GRPC_VERSION ${gRPCAlt_VERSION})
    else()
      set(ARROW_GRPC_VERSION ${gRPC_VERSION})
    endif()
    if(ARROW_USE_ASAN)
      # Disable ASAN in system gRPC.
      add_library(gRPC::grpc_asan_suppressed INTERFACE IMPORTED)
      target_compile_definitions(gRPC::grpc_asan_suppressed
                                 INTERFACE "GRPC_ASAN_SUPPRESSED")
      target_link_libraries(gRPC::grpc++ INTERFACE gRPC::grpc_asan_suppressed)
    endif()
  endif()
endif()

# ----------------------------------------------------------------------
# GCS and dependencies

macro(build_crc32c_once)
  if(NOT TARGET crc32c_ep)
    message(STATUS "Building crc32c from source")
    # Build crc32c
    set(CRC32C_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/crc32c_ep-install")
    set(CRC32C_INCLUDE_DIR "${CRC32C_PREFIX}/include")
    set(CRC32C_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
        -DCRC32C_BUILD_TESTS=OFF
        -DCRC32C_BUILD_BENCHMARKS=OFF
        -DCRC32C_USE_GLOG=OFF)

    set(_CRC32C_STATIC_LIBRARY
        "${CRC32C_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}crc32c${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(CRC32C_BUILD_BYPRODUCTS ${_CRC32C_STATIC_LIBRARY})
    set(CRC32C_LIBRARIES crc32c)

    externalproject_add(crc32c_ep
                        ${EP_COMMON_OPTIONS}
                        INSTALL_DIR ${CRC32C_PREFIX}
                        URL ${CRC32C_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_CRC32C_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${CRC32C_CMAKE_ARGS}
                        BUILD_BYPRODUCTS ${CRC32C_BUILD_BYPRODUCTS})
    # Work around https://gitlab.kitware.com/cmake/cmake/issues/15052
    file(MAKE_DIRECTORY "${CRC32C_INCLUDE_DIR}")
    add_library(Crc32c::crc32c STATIC IMPORTED)
    set_target_properties(Crc32c::crc32c PROPERTIES IMPORTED_LOCATION
                                                    ${_CRC32C_STATIC_LIBRARY})
    target_include_directories(Crc32c::crc32c BEFORE INTERFACE "${CRC32C_INCLUDE_DIR}")
    add_dependencies(Crc32c::crc32c crc32c_ep)
    list(APPEND ARROW_BUNDLED_STATIC_LIBS Crc32c::crc32c)
  endif()
endmacro()

macro(build_nlohmann_json)
  message(STATUS "Building nlohmann-json from source")
  # "Build" nlohmann-json
  set(NLOHMANN_JSON_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/nlohmann_json_ep-install")
  set(NLOHMANN_JSON_INCLUDE_DIR "${NLOHMANN_JSON_PREFIX}/include")
  set(NLOHMANN_JSON_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
      # google-cloud-cpp requires JSON_MultipleHeaders=ON
      -DJSON_BuildTests=OFF -DJSON_MultipleHeaders=ON)

  set(NLOHMANN_JSON_BUILD_BYPRODUCTS ${NLOHMANN_JSON_PREFIX}/include/nlohmann/json.hpp)

  externalproject_add(nlohmann_json_ep
                      ${EP_COMMON_OPTIONS}
                      INSTALL_DIR ${NLOHMANN_JSON_PREFIX}
                      URL ${NLOHMANN_JSON_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_NLOHMANN_JSON_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${NLOHMANN_JSON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${NLOHMANN_JSON_BUILD_BYPRODUCTS})

  # Work around https://gitlab.kitware.com/cmake/cmake/issues/15052
  file(MAKE_DIRECTORY ${NLOHMANN_JSON_INCLUDE_DIR})

  add_library(nlohmann_json::nlohmann_json INTERFACE IMPORTED)
  target_include_directories(nlohmann_json::nlohmann_json BEFORE
                             INTERFACE "${NLOHMANN_JSON_INCLUDE_DIR}")
  add_dependencies(nlohmann_json::nlohmann_json nlohmann_json_ep)
endmacro()
if(ARROW_WITH_NLOHMANN_JSON)
  resolve_dependency(nlohmann_json)
  get_target_property(nlohmann_json_INCLUDE_DIR nlohmann_json::nlohmann_json
                      INTERFACE_INCLUDE_DIRECTORIES)
  message(STATUS "Found nlohmann_json headers: ${nlohmann_json_INCLUDE_DIR}")
endif()

macro(build_google_cloud_cpp_storage)
  message(STATUS "Building google-cloud-cpp from source")
  message(STATUS "Only building the google-cloud-cpp::storage component")

  # List of dependencies taken from https://github.com/googleapis/google-cloud-cpp/blob/main/doc/packaging.md
  ensure_absl()
  build_crc32c_once()

  # Curl is required on all platforms, but building it internally might also trip over S3's copy.
  # For now, force its inclusion from the underlying system or fail.
  find_curl()

  # Build google-cloud-cpp, with only storage_client

  # Inject vendored packages via CMAKE_PREFIX_PATH
  if(ABSL_VENDORED)
    list(APPEND GOOGLE_CLOUD_CPP_PREFIX_PATH_LIST ${ABSL_PREFIX})
  endif()
  if(ZLIB_VENDORED)
    list(APPEND GOOGLE_CLOUD_CPP_PREFIX_PATH_LIST ${ZLIB_PREFIX})
  endif()
  list(APPEND GOOGLE_CLOUD_CPP_PREFIX_PATH_LIST ${CRC32C_PREFIX})
  list(APPEND GOOGLE_CLOUD_CPP_PREFIX_PATH_LIST ${NLOHMANN_JSON_PREFIX})

  string(JOIN ${EP_LIST_SEPARATOR} GOOGLE_CLOUD_CPP_PREFIX_PATH
         ${GOOGLE_CLOUD_CPP_PREFIX_PATH_LIST})

  set(GOOGLE_CLOUD_CPP_INSTALL_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_ep-install")
  set(GOOGLE_CLOUD_CPP_INCLUDE_DIR "${GOOGLE_CLOUD_CPP_INSTALL_PREFIX}/include")
  set(GOOGLE_CLOUD_CPP_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
      -DCMAKE_INSTALL_RPATH=$ORIGIN
      -DCMAKE_PREFIX_PATH=${GOOGLE_CLOUD_CPP_PREFIX_PATH}
      # Compile only the storage library and its dependencies. To enable
      # other services (Spanner, Bigtable, etc.) add them (as a list) to this
      # parameter. Each has its own `google-cloud-cpp::*` library.
      -DGOOGLE_CLOUD_CPP_ENABLE=storage
      # We need this to build with OpenSSL 3.0.
      # See also: https://github.com/googleapis/google-cloud-cpp/issues/8544
      -DGOOGLE_CLOUD_CPP_ENABLE_WERROR=OFF
      -DGOOGLE_CLOUD_CPP_WITH_MOCKS=OFF
      -DOPENSSL_CRYPTO_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}
      -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
      -DOPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY})

  add_custom_target(google_cloud_cpp_dependencies)

  if(ABSL_VENDORED)
    add_dependencies(google_cloud_cpp_dependencies absl_ep)
  endif()
  if(ZLIB_VENDORED)
    add_dependencies(google_cloud_cpp_dependencies zlib_ep)
  endif()
  add_dependencies(google_cloud_cpp_dependencies crc32c_ep)
  add_dependencies(google_cloud_cpp_dependencies nlohmann_json::nlohmann_json)

  set(GOOGLE_CLOUD_CPP_STATIC_LIBRARY_STORAGE
      "${GOOGLE_CLOUD_CPP_INSTALL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}google_cloud_cpp_storage${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  set(GOOGLE_CLOUD_CPP_STATIC_LIBRARY_REST_INTERNAL
      "${GOOGLE_CLOUD_CPP_INSTALL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}google_cloud_cpp_rest_internal${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  set(GOOGLE_CLOUD_CPP_STATIC_LIBRARY_COMMON
      "${GOOGLE_CLOUD_CPP_INSTALL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}google_cloud_cpp_common${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  # Remove unused directories to save build directory storage.
  # 141MB -> 79MB
  set(GOOGLE_CLOUD_CPP_PATCH_COMMAND ${CMAKE_COMMAND} -E)
  if(CMAKE_VERSION VERSION_LESS 3.17)
    list(APPEND GOOGLE_CLOUD_CPP_PATCH_COMMAND remove_directory)
  else()
    list(APPEND GOOGLE_CLOUD_CPP_PATCH_COMMAND rm -rf)
  endif()
  list(APPEND GOOGLE_CLOUD_CPP_PATCH_COMMAND ci)

  externalproject_add(google_cloud_cpp_ep
                      ${EP_COMMON_OPTIONS}
                      INSTALL_DIR ${GOOGLE_CLOUD_CPP_INSTALL_PREFIX}
                      URL ${google_cloud_cpp_storage_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_GOOGLE_CLOUD_CPP_BUILD_SHA256_CHECKSUM}"
                      PATCH_COMMAND ${GOOGLE_CLOUD_CPP_PATCH_COMMAND}
                      CMAKE_ARGS ${GOOGLE_CLOUD_CPP_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${GOOGLE_CLOUD_CPP_STATIC_LIBRARY_STORAGE}
                                       ${GOOGLE_CLOUD_CPP_STATIC_LIBRARY_REST_INTERNAL}
                                       ${GOOGLE_CLOUD_CPP_STATIC_LIBRARY_COMMON}
                      DEPENDS google_cloud_cpp_dependencies)

  # Work around https://gitlab.kitware.com/cmake/cmake/issues/15052
  file(MAKE_DIRECTORY ${GOOGLE_CLOUD_CPP_INCLUDE_DIR})

  add_library(google-cloud-cpp::common STATIC IMPORTED)
  set_target_properties(google-cloud-cpp::common
                        PROPERTIES IMPORTED_LOCATION
                                   "${GOOGLE_CLOUD_CPP_STATIC_LIBRARY_COMMON}")
  target_include_directories(google-cloud-cpp::common BEFORE
                             INTERFACE "${GOOGLE_CLOUD_CPP_INCLUDE_DIR}")
  # Refer to https://github.com/googleapis/google-cloud-cpp/blob/main/google/cloud/google_cloud_cpp_common.cmake
  # (substitute `main` for the SHA of the version we use)
  # Version 1.39.0 is at a different place (they refactored after):
  # https://github.com/googleapis/google-cloud-cpp/blob/29e5af8ca9b26cec62106d189b50549f4dc1c598/google/cloud/CMakeLists.txt#L146-L155
  target_link_libraries(google-cloud-cpp::common
                        INTERFACE absl::base
                                  absl::cord
                                  absl::memory
                                  absl::optional
                                  absl::span
                                  absl::time
                                  absl::variant
                                  Threads::Threads
                                  OpenSSL::Crypto)

  add_library(google-cloud-cpp::rest-internal STATIC IMPORTED)
  set_target_properties(google-cloud-cpp::rest-internal
                        PROPERTIES IMPORTED_LOCATION
                                   "${GOOGLE_CLOUD_CPP_STATIC_LIBRARY_REST_INTERNAL}")
  target_include_directories(google-cloud-cpp::rest-internal BEFORE
                             INTERFACE "${GOOGLE_CLOUD_CPP_INCLUDE_DIR}")
  target_link_libraries(google-cloud-cpp::rest-internal
                        INTERFACE absl::span
                                  google-cloud-cpp::common
                                  CURL::libcurl
                                  nlohmann_json::nlohmann_json
                                  OpenSSL::SSL
                                  OpenSSL::Crypto)
  if(WIN32)
    target_link_libraries(google-cloud-cpp::rest-internal INTERFACE ws2_32)
  endif()

  add_library(google-cloud-cpp::storage STATIC IMPORTED)
  set_target_properties(google-cloud-cpp::storage
                        PROPERTIES IMPORTED_LOCATION
                                   "${GOOGLE_CLOUD_CPP_STATIC_LIBRARY_STORAGE}")
  target_include_directories(google-cloud-cpp::storage BEFORE
                             INTERFACE "${GOOGLE_CLOUD_CPP_INCLUDE_DIR}")
  # Update this from https://github.com/googleapis/google-cloud-cpp/blob/main/google/cloud/storage/google_cloud_cpp_storage.cmake
  target_link_libraries(google-cloud-cpp::storage
                        INTERFACE google-cloud-cpp::common
                                  google-cloud-cpp::rest-internal
                                  absl::memory
                                  absl::strings
                                  absl::str_format
                                  absl::time
                                  absl::variant
                                  nlohmann_json::nlohmann_json
                                  Crc32c::crc32c
                                  CURL::libcurl
                                  Threads::Threads
                                  OpenSSL::SSL
                                  OpenSSL::Crypto
                                  ZLIB::ZLIB)
  add_dependencies(google-cloud-cpp::storage google_cloud_cpp_ep)

  list(APPEND
       ARROW_BUNDLED_STATIC_LIBS
       google-cloud-cpp::storage
       google-cloud-cpp::rest-internal
       google-cloud-cpp::common)
  if(ABSL_VENDORED)
    # Figure out what absl libraries (not header-only) are required by the
    # google-cloud-cpp libraries above and add them to the bundled_dependencies
    #
    #   pkg-config --libs absl_memory absl_strings absl_str_format absl_time absl_variant absl_base absl_memory absl_optional absl_span absl_time absl_variant
    # (and then some regexing)
    list(APPEND
         ARROW_BUNDLED_STATIC_LIBS
         absl::bad_optional_access
         absl::bad_variant_access
         absl::base
         absl::civil_time
         absl::cord
         absl::cord_internal
         absl::cordz_functions
         absl::cordz_info
         absl::cordz_handle
         absl::debugging_internal
         absl::demangle_internal
         absl::exponential_biased
         absl::int128
         absl::log_severity
         absl::malloc_internal
         absl::raw_logging_internal
         absl::spinlock_wait
         absl::stacktrace
         absl::str_format_internal
         absl::strings
         absl::strings_internal
         absl::symbolize
         absl::synchronization
         absl::throw_delegate
         absl::time
         absl::time_zone)
  endif()
endmacro()

if(ARROW_WITH_GOOGLE_CLOUD_CPP)
  if(NOT ARROW_ENABLE_THREADING)
    message(FATAL_ERROR "Can't use Google Cloud Platform C++ Client Libraries with ARROW_ENABLE_THREADING=OFF"
    )
  endif()

  resolve_dependency(google_cloud_cpp_storage PC_PACKAGE_NAMES google_cloud_cpp_storage)
  get_target_property(google_cloud_cpp_storage_INCLUDE_DIR google-cloud-cpp::storage
                      INTERFACE_INCLUDE_DIRECTORIES)
  message(STATUS "Found google-cloud-cpp::storage headers: ${google_cloud_cpp_storage_INCLUDE_DIR}"
  )
endif()

#
# HDFS thirdparty setup

if(DEFINED ENV{HADOOP_HOME})
  set(HADOOP_HOME $ENV{HADOOP_HOME})
  if(NOT EXISTS "${HADOOP_HOME}/include/hdfs.h")
    message(STATUS "Did not find hdfs.h in expected location, using vendored one")
    set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
  endif()
else()
  set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
endif()

set(HDFS_H_PATH "${HADOOP_HOME}/include/hdfs.h")
if(NOT EXISTS ${HDFS_H_PATH})
  message(FATAL_ERROR "Did not find hdfs.h at ${HDFS_H_PATH}")
endif()
message(STATUS "Found hdfs.h at: ${HDFS_H_PATH}")

add_library(arrow::hadoop INTERFACE IMPORTED)
target_include_directories(arrow::hadoop INTERFACE "${HADOOP_HOME}/include")

# ----------------------------------------------------------------------
# Apache ORC

function(build_orc)
  message(STATUS "Building Apache ORC from source")

  if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.29)
    fetchcontent_declare(orc
                         ${FC_DECLARE_COMMON_OPTIONS}
                         URL ${ORC_SOURCE_URL}
                         URL_HASH "SHA256=${ARROW_ORC_BUILD_SHA256_CHECKSUM}")
    prepare_fetchcontent()

    set(CMAKE_UNITY_BUILD FALSE)

    set(ORC_PREFER_STATIC_LZ4
        OFF
        CACHE BOOL "" FORCE)
    get_target_property(LZ4_INCLUDE_DIR LZ4::lz4 INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(LZ4_ROOT "${LZ4_INCLUDE_DIR}" DIRECTORY)
    set(LZ4_HOME
        ${LZ4_ROOT}
        CACHE STRING "" FORCE)
    set(LZ4_LIBRARY
        LZ4::lz4
        CACHE STRING "" FORCE)

    set(ORC_PREFER_STATIC_PROTOBUF
        OFF
        CACHE BOOL "" FORCE)
    get_target_property(PROTOBUF_INCLUDE_DIR ${ARROW_PROTOBUF_LIBPROTOBUF}
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(Protobuf_ROOT "${PROTOBUF_INCLUDE_DIR}" DIRECTORY)
    set(PROTOBUF_HOME
        ${Protobuf_ROOT}
        CACHE STRING "" FORCE)
    # ORC uses this.
    target_include_directories(${ARROW_PROTOBUF_LIBPROTOC}
                               INTERFACE "${PROTOBUF_INCLUDE_DIR}")
    set(PROTOBUF_EXECUTABLE ${ARROW_PROTOBUF_PROTOC})
    set(PROTOBUF_LIBRARY ${ARROW_PROTOBUF_LIBPROTOBUF})
    set(PROTOC_LIBRARY ${ARROW_PROTOBUF_LIBPROTOC})

    set(ORC_PREFER_STATIC_SNAPPY
        OFF
        CACHE BOOL "" FORCE)
    get_target_property(SNAPPY_INCLUDE_DIR ${Snappy_TARGET} INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(Snappy_ROOT "${SNAPPY_INCLUDE_DIR}" DIRECTORY)
    set(SNAPPY_HOME
        ${Snappy_ROOT}
        CACHE STRING "" FORCE)
    set(SNAPPY_LIBRARY
        ${Snappy_TARGET}
        CACHE STRING "" FORCE)

    set(ORC_PREFER_STATIC_ZLIB
        OFF
        CACHE BOOL "" FORCE)
    get_target_property(ZLIB_INCLUDE_DIR ZLIB::ZLIB INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ZLIB_ROOT "${ZLIB_INCLUDE_DIR}" DIRECTORY)
    set(ZLIB_HOME
        ${ZLIB_ROOT}
        CACHE STRING "" FORCE)
    set(ZLIB_LIBRARY
        ZLIB::ZLIB
        CACHE STRING "" FORCE)

    set(ORC_PREFER_STATIC_ZSTD
        OFF
        CACHE BOOL "" FORCE)
    get_target_property(ZSTD_INCLUDE_DIR ${ARROW_ZSTD_LIBZSTD}
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ZSTD_ROOT "${ZSTD_INCLUDE_DIR}" DIRECTORY)
    set(ZSTD_HOME
        ${ZSTD_ROOT}
        CACHE STRING "" FORCE)
    set(ZSTD_LIBRARY ${ARROW_ZSTD_LIBZSTD})

    set(BUILD_CPP_TESTS
        OFF
        CACHE BOOL "" FORCE)
    set(BUILD_JAVA
        OFF
        CACHE BOOL "" FORCE)
    set(BUILD_LIBHDFSPP
        OFF
        CACHE BOOL "" FORCE)
    set(BUILD_TOOLS
        OFF
        CACHE BOOL "" FORCE)
    set(INSTALL_VENDORED_LIBS
        OFF
        CACHE BOOL "" FORCE)
    set(STOP_BUILD_ON_WARNING
        OFF
        CACHE BOOL "" FORCE)

    # We can remove this with ORC 2.0.2 or later.
    list(PREPEND CMAKE_MODULE_PATH
         ${CMAKE_CURRENT_BINARY_DIR}/_deps/orc-src/cmake_modules)

    fetchcontent_makeavailable(orc)

    add_library(orc::orc INTERFACE IMPORTED)
    target_link_libraries(orc::orc INTERFACE orc)
    target_include_directories(orc::orc INTERFACE "${orc_BINARY_DIR}/c++/include"
                                                  "${orc_SOURCE_DIR}/c++/include")

    list(APPEND ARROW_BUNDLED_STATIC_LIBS orc)
  else()
    set(ORC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/orc_ep-install")
    set(ORC_HOME "${ORC_PREFIX}")
    set(ORC_INCLUDE_DIR "${ORC_PREFIX}/include")
    set(ORC_STATIC_LIB
        "${ORC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}orc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )

    get_target_property(ORC_PROTOBUF_ROOT ${ARROW_PROTOBUF_LIBPROTOBUF}
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_PROTOBUF_ROOT "${ORC_PROTOBUF_ROOT}" DIRECTORY)

    get_target_property(ORC_SNAPPY_INCLUDE_DIR ${Snappy_TARGET}
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_SNAPPY_ROOT "${ORC_SNAPPY_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ORC_LZ4_ROOT LZ4::lz4 INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_LZ4_ROOT "${ORC_LZ4_ROOT}" DIRECTORY)

    get_target_property(ORC_ZSTD_ROOT ${ARROW_ZSTD_LIBZSTD} INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_ZSTD_ROOT "${ORC_ZSTD_ROOT}" DIRECTORY)

    set(ORC_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${ORC_PREFIX}"
        -DSTOP_BUILD_ON_WARNING=OFF
        -DBUILD_LIBHDFSPP=OFF
        -DBUILD_JAVA=OFF
        -DBUILD_TOOLS=OFF
        -DBUILD_CPP_TESTS=OFF
        -DINSTALL_VENDORED_LIBS=OFF
        "-DLZ4_HOME=${ORC_LZ4_ROOT}"
        "-DPROTOBUF_EXECUTABLE=$<TARGET_FILE:${ARROW_PROTOBUF_PROTOC}>"
        "-DPROTOBUF_HOME=${ORC_PROTOBUF_ROOT}"
        "-DPROTOBUF_INCLUDE_DIR=$<TARGET_PROPERTY:${ARROW_PROTOBUF_LIBPROTOBUF},INTERFACE_INCLUDE_DIRECTORIES>"
        "-DPROTOBUF_LIBRARY=$<TARGET_FILE:${ARROW_PROTOBUF_LIBPROTOBUF}>"
        "-DPROTOC_LIBRARY=$<TARGET_FILE:${ARROW_PROTOBUF_LIBPROTOC}>"
        "-DSNAPPY_HOME=${ORC_SNAPPY_ROOT}"
        "-DSNAPPY_LIBRARY=$<TARGET_FILE:${Snappy_TARGET}>"
        "-DLZ4_LIBRARY=$<TARGET_FILE:LZ4::lz4>"
        "-DLZ4_STATIC_LIB=$<TARGET_FILE:LZ4::lz4>"
        "-DLZ4_INCLUDE_DIR=${ORC_LZ4_ROOT}/include"
        "-DSNAPPY_INCLUDE_DIR=${ORC_SNAPPY_INCLUDE_DIR}"
        "-DZSTD_HOME=${ORC_ZSTD_ROOT}"
        "-DZSTD_INCLUDE_DIR=$<TARGET_PROPERTY:${ARROW_ZSTD_LIBZSTD},INTERFACE_INCLUDE_DIRECTORIES>"
        "-DZSTD_LIBRARY=$<TARGET_FILE:${ARROW_ZSTD_LIBZSTD}>")
    if(ZLIB_ROOT)
      set(ORC_CMAKE_ARGS ${ORC_CMAKE_ARGS} "-DZLIB_HOME=${ZLIB_ROOT}")
    endif()

    # Work around CMake bug
    file(MAKE_DIRECTORY ${ORC_INCLUDE_DIR})

    externalproject_add(orc_ep
                        ${EP_COMMON_OPTIONS}
                        URL ${ORC_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_ORC_BUILD_SHA256_CHECKSUM}"
                        BUILD_BYPRODUCTS ${ORC_STATIC_LIB}
                        CMAKE_ARGS ${ORC_CMAKE_ARGS}
                        DEPENDS ${ARROW_PROTOBUF_LIBPROTOBUF}
                                ${ARROW_PROTOBUF_PROTOC}
                                ${ARROW_ZSTD_LIBZSTD}
                                ${Snappy_TARGET}
                                LZ4::lz4
                                ZLIB::ZLIB)
    add_library(orc::orc STATIC IMPORTED)
    set_target_properties(orc::orc PROPERTIES IMPORTED_LOCATION "${ORC_STATIC_LIB}")
    target_include_directories(orc::orc BEFORE INTERFACE "${ORC_INCLUDE_DIR}")
    target_link_libraries(orc::orc INTERFACE LZ4::lz4 ZLIB::ZLIB ${ARROW_ZSTD_LIBZSTD}
                                             ${Snappy_TARGET})
    # Protobuf generated files may use ABSL_DCHECK*() and
    # absl::log_internal_check_op is needed for them.
    if(TARGET absl::log_internal_check_op)
      target_link_libraries(orc::orc INTERFACE absl::log_internal_check_op)
    endif()
    if(NOT MSVC)
      if(NOT APPLE AND ARROW_ENABLE_THREADING)
        target_link_libraries(orc::orc INTERFACE Threads::Threads)
      endif()
      target_link_libraries(orc::orc INTERFACE ${CMAKE_DL_LIBS})
    endif()
    target_link_libraries(orc::orc INTERFACE ${ARROW_PROTOBUF_LIBPROTOBUF})
    add_dependencies(orc::orc orc_ep)
    list(APPEND ARROW_BUNDLED_STATIC_LIBS orc::orc)
  endif()

  set(ORC_VENDORED
      TRUE
      PARENT_SCOPE)
  set(ARROW_BUNDLED_STATIC_LIBS
      ${ARROW_BUNDLED_STATIC_LIBS}
      PARENT_SCOPE)
endfunction()

if(ARROW_ORC)
  resolve_dependency(orc HAVE_ALT TRUE)
  if(ORC_VENDORED)
    set(ARROW_ORC_VERSION ${ARROW_ORC_BUILD_VERSION})
  else()
    target_link_libraries(orc::orc INTERFACE ${ARROW_PROTOBUF_LIBPROTOBUF})
    set(ARROW_ORC_VERSION ${orcAlt_VERSION})
    message(STATUS "Found ORC static library: ${ORC_STATIC_LIB}")
    message(STATUS "Found ORC headers: ${ORC_INCLUDE_DIR}")
  endif()
endif()

# ----------------------------------------------------------------------
# OpenTelemetry C++

macro(build_opentelemetry)
  message(STATUS "Building OpenTelemetry from source")
  if(Protobuf_VERSION VERSION_GREATER_EQUAL 3.22)
    message(FATAL_ERROR "GH-36013: Can't use bundled OpenTelemetry with Protobuf 3.22 or later. "
                        "Protobuf is version ${Protobuf_VERSION}")
  endif()

  set(OPENTELEMETRY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry_ep-install")
  set(OPENTELEMETRY_INCLUDE_DIR "${OPENTELEMETRY_PREFIX}/include")
  set(OPENTELEMETRY_STATIC_LIB
      "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(_OPENTELEMETRY_APIS api ext sdk)
  set(_OPENTELEMETRY_LIBS
      common
      http_client_curl
      logs
      ostream_log_record_exporter
      ostream_span_exporter
      otlp_http_client
      otlp_http_log_record_exporter
      otlp_http_exporter
      otlp_recordable
      proto
      resources
      trace
      version)
  set(OPENTELEMETRY_BUILD_BYPRODUCTS)
  set(OPENTELEMETRY_LIBRARIES)

  foreach(_OPENTELEMETRY_LIB ${_OPENTELEMETRY_APIS})
    add_library(opentelemetry-cpp::${_OPENTELEMETRY_LIB} INTERFACE IMPORTED)
    target_include_directories(opentelemetry-cpp::${_OPENTELEMETRY_LIB} BEFORE
                               INTERFACE "${OPENTELEMETRY_INCLUDE_DIR}")
  endforeach()
  foreach(_OPENTELEMETRY_LIB ${_OPENTELEMETRY_LIBS})
    # N.B. OTel targets and libraries don't follow any consistent naming scheme
    if(_OPENTELEMETRY_LIB STREQUAL "http_client_curl")
      set(_OPENTELEMETRY_STATIC_LIBRARY
          "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry_${_OPENTELEMETRY_LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    elseif(_OPENTELEMETRY_LIB STREQUAL "ostream_span_exporter")
      set(_OPENTELEMETRY_STATIC_LIBRARY
          "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry_exporter_ostream_span${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    elseif(_OPENTELEMETRY_LIB STREQUAL "otlp_http_client")
      set(_OPENTELEMETRY_STATIC_LIBRARY
          "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry_exporter_otlp_http_client${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    elseif(_OPENTELEMETRY_LIB STREQUAL "otlp_http_exporter")
      set(_OPENTELEMETRY_STATIC_LIBRARY
          "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry_exporter_otlp_http${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    elseif(_OPENTELEMETRY_LIB STREQUAL "otlp_http_log_record_exporter")
      set(_OPENTELEMETRY_STATIC_LIBRARY
          "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry_exporter_otlp_http_log${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    elseif(_OPENTELEMETRY_LIB STREQUAL "ostream_log_record_exporter")
      set(_OPENTELEMETRY_STATIC_LIBRARY
          "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry_exporter_ostream_logs${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    else()
      set(_OPENTELEMETRY_STATIC_LIBRARY
          "${OPENTELEMETRY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}opentelemetry_${_OPENTELEMETRY_LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    endif()
    add_library(opentelemetry-cpp::${_OPENTELEMETRY_LIB} STATIC IMPORTED)
    set_target_properties(opentelemetry-cpp::${_OPENTELEMETRY_LIB}
                          PROPERTIES IMPORTED_LOCATION ${_OPENTELEMETRY_STATIC_LIBRARY})
    list(APPEND OPENTELEMETRY_BUILD_BYPRODUCTS ${_OPENTELEMETRY_STATIC_LIBRARY})
    list(APPEND OPENTELEMETRY_LIBRARIES opentelemetry-cpp::${_OPENTELEMETRY_LIB})
  endforeach()

  set(OPENTELEMETRY_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${OPENTELEMETRY_PREFIX}"
      -DWITH_EXAMPLES=OFF)

  set(OPENTELEMETRY_PREFIX_PATH_LIST)
  # Don't specify the DEPENDS unless we actually have dependencies, else
  # Ninja/other build systems may consider this target to always be dirty
  set(_OPENTELEMETRY_DEPENDENCIES)
  add_custom_target(opentelemetry_dependencies)

  set(_OPENTELEMETRY_DEPENDENCIES "opentelemetry_dependencies")
  list(APPEND ARROW_BUNDLED_STATIC_LIBS ${OPENTELEMETRY_LIBRARIES})
  list(APPEND OPENTELEMETRY_PREFIX_PATH_LIST ${NLOHMANN_JSON_PREFIX})

  get_target_property(OPENTELEMETRY_PROTOBUF_INCLUDE_DIR ${ARROW_PROTOBUF_LIBPROTOBUF}
                      INTERFACE_INCLUDE_DIRECTORIES)
  get_target_property(OPENTELEMETRY_PROTOBUF_LIBRARY ${ARROW_PROTOBUF_LIBPROTOBUF}
                      IMPORTED_LOCATION)
  get_target_property(OPENTELEMETRY_PROTOC_EXECUTABLE ${ARROW_PROTOBUF_PROTOC}
                      IMPORTED_LOCATION)
  list(APPEND
       OPENTELEMETRY_CMAKE_ARGS
       -DWITH_OTLP_HTTP=ON
       -DWITH_OTLP_GRPC=OFF
       # Disabled because it seemed to cause linking errors. May be worth a closer look.
       -DWITH_FUNC_TESTS=OFF
       # These options are slated for removal in v1.14 and their features are deemed stable
       # as of v1.13. However, setting their corresponding ENABLE_* macros in headers seems
       # finicky - resulting in build failures or ABI-related runtime errors during HTTP
       # client initialization. There may still be a solution, but we disable them for now.
       -DWITH_OTLP_HTTP_SSL_PREVIEW=OFF
       -DWITH_OTLP_HTTP_SSL_TLS_PREVIEW=OFF
       "-DProtobuf_INCLUDE_DIR=${OPENTELEMETRY_PROTOBUF_INCLUDE_DIR}"
       "-DProtobuf_LIBRARY=${OPENTELEMETRY_PROTOBUF_INCLUDE_DIR}"
       "-DProtobuf_PROTOC_EXECUTABLE=${OPENTELEMETRY_PROTOC_EXECUTABLE}")

  # OpenTelemetry with OTLP enabled requires Protobuf definitions from a
  # submodule. This submodule path is hardcoded into their CMake definitions,
  # and submodules are not included in their releases. Add a custom build step
  # to download and extract the Protobufs.

  # Adding such a step is rather complicated, so instead: create a separate
  # ExternalProject that just fetches the Protobufs, then add a custom step
  # to the main build to copy the Protobufs.
  externalproject_add(opentelemetry_proto_ep
                      ${EP_COMMON_OPTIONS}
                      URL_HASH "SHA256=${ARROW_OPENTELEMETRY_PROTO_BUILD_SHA256_CHECKSUM}"
                      URL ${OPENTELEMETRY_PROTO_SOURCE_URL}
                      BUILD_COMMAND ""
                      CONFIGURE_COMMAND ""
                      INSTALL_COMMAND ""
                      EXCLUDE_FROM_ALL OFF)

  add_dependencies(opentelemetry_dependencies nlohmann_json::nlohmann_json
                   opentelemetry_proto_ep ${ARROW_PROTOBUF_LIBPROTOBUF})

  string(JOIN "${EP_LIST_SEPARATOR}" OPENTELEMETRY_PREFIX_PATH
         ${OPENTELEMETRY_PREFIX_PATH_LIST})
  list(APPEND OPENTELEMETRY_CMAKE_ARGS "-DCMAKE_PREFIX_PATH=${OPENTELEMETRY_PREFIX_PATH}")

  if(CMAKE_SYSTEM_PROCESSOR STREQUAL "s390x")
    # OpenTelemetry tries to determine the processor arch for vcpkg, which fails
    # on s390x, even though it doesn't use vcpkg there. Tell it ARCH manually
    externalproject_add(opentelemetry_ep
                        ${EP_COMMON_OPTIONS}
                        URL_HASH "SHA256=${ARROW_OPENTELEMETRY_BUILD_SHA256_CHECKSUM}"
                        CONFIGURE_COMMAND ${CMAKE_COMMAND} -E env ARCH=s390x
                                          ${CMAKE_COMMAND} -G ${CMAKE_GENERATOR}
                                          "<SOURCE_DIR><SOURCE_SUBDIR>"
                                          ${OPENTELEMETRY_CMAKE_ARGS}
                        BUILD_COMMAND ${CMAKE_COMMAND} --build "<BINARY_DIR>" --target all
                        INSTALL_COMMAND ${CMAKE_COMMAND} --build "<BINARY_DIR>" --target
                                        install
                        URL ${OPENTELEMETRY_SOURCE_URL}
                        BUILD_BYPRODUCTS ${OPENTELEMETRY_BUILD_BYPRODUCTS}
                        EXCLUDE_FROM_ALL NOT
                        ${ARROW_WITH_OPENTELEMETRY}
                        DEPENDS ${_OPENTELEMETRY_DEPENDENCIES})
  else()
    externalproject_add(opentelemetry_ep
                        ${EP_COMMON_OPTIONS}
                        URL_HASH "SHA256=${ARROW_OPENTELEMETRY_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${OPENTELEMETRY_CMAKE_ARGS}
                        URL ${OPENTELEMETRY_SOURCE_URL}
                        BUILD_BYPRODUCTS ${OPENTELEMETRY_BUILD_BYPRODUCTS}
                        EXCLUDE_FROM_ALL NOT
                        ${ARROW_WITH_OPENTELEMETRY}
                        DEPENDS ${_OPENTELEMETRY_DEPENDENCIES})
  endif()

  externalproject_add_step(opentelemetry_ep download_proto
                           COMMAND ${CMAKE_COMMAND} -E copy_directory
                                   $<TARGET_PROPERTY:opentelemetry_proto_ep,_EP_SOURCE_DIR>/opentelemetry
                                   $<TARGET_PROPERTY:opentelemetry_ep,_EP_SOURCE_DIR>/third_party/opentelemetry-proto/opentelemetry
                           DEPENDEES download
                           DEPENDERS configure)

  set(OPENTELEMETRY_VENDORED 1)

  target_link_libraries(opentelemetry-cpp::common
                        INTERFACE opentelemetry-cpp::api opentelemetry-cpp::sdk
                                  Threads::Threads)
  target_link_libraries(opentelemetry-cpp::resources INTERFACE opentelemetry-cpp::common)
  target_link_libraries(opentelemetry-cpp::trace INTERFACE opentelemetry-cpp::common
                                                           opentelemetry-cpp::resources)
  target_link_libraries(opentelemetry-cpp::logs INTERFACE opentelemetry-cpp::common
                                                          opentelemetry-cpp::resources)
  target_link_libraries(opentelemetry-cpp::http_client_curl
                        INTERFACE opentelemetry-cpp::common opentelemetry-cpp::ext
                                  CURL::libcurl)
  target_link_libraries(opentelemetry-cpp::proto INTERFACE ${ARROW_PROTOBUF_LIBPROTOBUF})
  target_link_libraries(opentelemetry-cpp::otlp_recordable
                        INTERFACE opentelemetry-cpp::logs opentelemetry-cpp::trace
                                  opentelemetry-cpp::resources opentelemetry-cpp::proto)
  target_link_libraries(opentelemetry-cpp::otlp_http_client
                        INTERFACE opentelemetry-cpp::common opentelemetry-cpp::proto
                                  opentelemetry-cpp::http_client_curl
                                  nlohmann_json::nlohmann_json)
  target_link_libraries(opentelemetry-cpp::otlp_http_exporter
                        INTERFACE opentelemetry-cpp::otlp_recordable
                                  opentelemetry-cpp::otlp_http_client)
  target_link_libraries(opentelemetry-cpp::otlp_http_log_record_exporter
                        INTERFACE opentelemetry-cpp::otlp_recordable
                                  opentelemetry-cpp::otlp_http_client)

  foreach(_OPENTELEMETRY_LIB ${_OPENTELEMETRY_LIBS})
    add_dependencies(opentelemetry-cpp::${_OPENTELEMETRY_LIB} opentelemetry_ep)
    list(APPEND ARROW_BUNDLED_STATIC_LIBS opentelemetry-cpp::${_OPENTELEMETRY_LIB})
  endforeach()

  # Work around https://gitlab.kitware.com/cmake/cmake/issues/15052
  file(MAKE_DIRECTORY ${OPENTELEMETRY_INCLUDE_DIR})
endmacro()

if(ARROW_WITH_OPENTELEMETRY)
  if(NOT ARROW_ENABLE_THREADING)
    message(FATAL_ERROR "Can't use OpenTelemetry with ARROW_ENABLE_THREADING=OFF")
  endif()

  # cURL is required whether we build from source or use an existing installation
  # (OTel's cmake files do not call find_curl for you)
  find_curl()
  set(opentelemetry-cpp_SOURCE "AUTO")
  resolve_dependency(opentelemetry-cpp)
  set(ARROW_OPENTELEMETRY_LIBS
      opentelemetry-cpp::trace
      opentelemetry-cpp::logs
      opentelemetry-cpp::otlp_http_log_record_exporter
      opentelemetry-cpp::ostream_log_record_exporter
      opentelemetry-cpp::ostream_span_exporter
      opentelemetry-cpp::otlp_http_exporter)
  get_target_property(OPENTELEMETRY_INCLUDE_DIR opentelemetry-cpp::api
                      INTERFACE_INCLUDE_DIRECTORIES)
  message(STATUS "Found OpenTelemetry headers: ${OPENTELEMETRY_INCLUDE_DIR}")
endif()

# ----------------------------------------------------------------------
# AWS SDK for C++

include(AWSSDKVariables)

macro(build_awssdk)
  message(STATUS "Building AWS C++ SDK from source")
  set(AWSSDK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/awssdk_ep-install")
  set(AWSSDK_INCLUDE_DIR "${AWSSDK_PREFIX}/include")

  set(AWSSDK_COMMON_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      -DCPP_STANDARD=${CMAKE_CXX_STANDARD}
      -DCMAKE_INSTALL_PREFIX=${AWSSDK_PREFIX}
      -DCMAKE_PREFIX_PATH=${AWSSDK_PREFIX}
      -DENABLE_TESTING=OFF
      -DENABLE_UNITY_BUILD=ON
      -DOPENSSL_CRYPTO_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}
      -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
      -DOPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY}
      -Dcrypto_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
      -Dcrypto_LIBRARY=${OPENSSL_CRYPTO_LIBRARY})
  if(ARROW_OPENSSL_USE_SHARED)
    list(APPEND AWSSDK_COMMON_CMAKE_ARGS
         -Dcrypto_SHARED_LIBRARY=${OPENSSL_CRYPTO_LIBRARY})
  else()
    list(APPEND AWSSDK_COMMON_CMAKE_ARGS
         -Dcrypto_STATIC_LIBRARY=${OPENSSL_CRYPTO_LIBRARY})
  endif()
  set(AWSSDK_CMAKE_ARGS
      ${AWSSDK_COMMON_CMAKE_ARGS}
      -DBUILD_DEPS=OFF
      -DBUILD_ONLY=config\\$<SEMICOLON>s3\\$<SEMICOLON>transfer\\$<SEMICOLON>identity-management\\$<SEMICOLON>sts
      -DMINIMIZE_SIZE=ON)
  # Remove unused directories to save build directory storage.
  # 807MB -> 31MB
  set(AWSSDK_PATCH_COMMAND ${CMAKE_COMMAND} -E)
  if(CMAKE_VERSION VERSION_LESS 3.17)
    list(APPEND AWSSDK_PATCH_COMMAND remove_directory)
  else()
    list(APPEND AWSSDK_PATCH_COMMAND rm -rf)
  endif()
  list(APPEND AWSSDK_PATCH_COMMAND ${AWSSDK_UNUSED_DIRECTORIES})

  if(UNIX)
    # on Linux and macOS curl seems to be required
    find_curl()
    get_filename_component(CURL_ROOT_HINT "${CURL_INCLUDE_DIRS}" DIRECTORY)
    get_filename_component(ZLIB_ROOT_HINT "${ZLIB_INCLUDE_DIRS}" DIRECTORY)

    # provide hint for AWS SDK to link with the already located libcurl and zlib
    list(APPEND
         AWSSDK_CMAKE_ARGS
         -DCURL_INCLUDE_DIR=${CURL_ROOT_HINT}/include
         -DCURL_LIBRARY=${CURL_ROOT_HINT}/lib
         -DZLIB_INCLUDE_DIR=${ZLIB_ROOT_HINT}/include
         -DZLIB_LIBRARY=${ZLIB_ROOT_HINT}/lib)
  endif()

  file(MAKE_DIRECTORY ${AWSSDK_INCLUDE_DIR})

  # AWS C++ SDK related libraries to link statically
  set(_AWSSDK_LIBS
      aws-cpp-sdk-identity-management
      aws-cpp-sdk-sts
      aws-cpp-sdk-cognito-identity
      aws-cpp-sdk-s3
      aws-cpp-sdk-core
      aws-crt-cpp
      aws-c-s3
      aws-c-auth
      aws-c-mqtt
      aws-c-http
      aws-c-compression
      aws-c-sdkutils
      aws-c-event-stream
      aws-c-io
      aws-c-cal
      aws-checksums
      aws-c-common)

  # aws-lc needs to be installed on a separate folder to hide from unintended use
  set(AWS_LC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/aws_lc_ep-install")
  set(AWS_LC_INCLUDE_DIR "${AWS_LC_PREFIX}/include")

  if(UNIX AND NOT APPLE) # aws-lc and s2n-tls only needed on linux
    file(MAKE_DIRECTORY ${AWS_LC_INCLUDE_DIR})
    list(APPEND _AWSSDK_LIBS s2n-tls aws-lc)
  endif()

  set(AWSSDK_LIBRARIES)
  foreach(_AWSSDK_LIB ${_AWSSDK_LIBS})
    # aws-c-common -> AWS-C-COMMON
    string(TOUPPER ${_AWSSDK_LIB} _AWSSDK_LIB_UPPER)
    # AWS-C-COMMON -> AWS_C_COMMON
    string(REPLACE "-" "_" _AWSSDK_LIB_NAME_PREFIX ${_AWSSDK_LIB_UPPER})
    set(_AWSSDK_STATIC_LIBRARY
        "${AWSSDK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${_AWSSDK_LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    if(${_AWSSDK_LIB} STREQUAL "s2n-tls") # Build output of s2n-tls is libs2n.a
      set(_AWSSDK_STATIC_LIBRARY
          "${AWSSDK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}s2n${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    elseif(${_AWSSDK_LIB} STREQUAL "aws-lc") # We only need libcrypto from aws-lc
      set(_AWSSDK_STATIC_LIBRARY
          "${AWS_LC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}crypto${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    endif()
    if(${_AWSSDK_LIB} MATCHES "^aws-cpp-sdk-")
      set(_AWSSDK_TARGET_NAME ${_AWSSDK_LIB})
    elseif(${_AWSSDK_LIB} STREQUAL "aws-lc")
      set(_AWSSDK_TARGET_NAME AWS::crypto)
    else()
      set(_AWSSDK_TARGET_NAME AWS::${_AWSSDK_LIB})
    endif()
    add_library(${_AWSSDK_TARGET_NAME} STATIC IMPORTED)
    set_target_properties(${_AWSSDK_TARGET_NAME} PROPERTIES IMPORTED_LOCATION
                                                            ${_AWSSDK_STATIC_LIBRARY})
    target_include_directories(${_AWSSDK_TARGET_NAME} BEFORE
                               INTERFACE "${AWSSDK_INCLUDE_DIR}")
    if(${_AWSSDK_LIB} STREQUAL "aws-lc")
      set_target_properties(${_AWSSDK_TARGET_NAME} PROPERTIES IMPORTED_LOCATION
                                                              ${_AWSSDK_STATIC_LIBRARY})
      target_include_directories(${_AWSSDK_TARGET_NAME} BEFORE
                                 INTERFACE "${AWS_LC_INCLUDE_DIR}")
    endif()
    set("${_AWSSDK_LIB_NAME_PREFIX}_STATIC_LIBRARY" ${_AWSSDK_STATIC_LIBRARY})

    if(NOT ${_AWSSDK_LIB} STREQUAL "aws-lc")
      # aws-lc only linked against s2n but not arrow
      list(APPEND AWSSDK_LIBRARIES ${_AWSSDK_TARGET_NAME})
    endif()
  endforeach()

  externalproject_add(aws_c_common_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_COMMON_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_COMMON_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_COMMON_STATIC_LIBRARY})
  add_dependencies(AWS::aws-c-common aws_c_common_ep)

  set(AWS_CHECKSUMS_CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS})
  if(NOT WIN32)
    # On non-Windows, always build in release mode.
    # Especially with gcc, debug builds can fail with "asm constraint" errors:
    # https://github.com/TileDB-Inc/TileDB/issues/1351
    list(APPEND AWS_CHECKSUMS_CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release)
  endif()
  externalproject_add(aws_checksums_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_CHECKSUMS_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_CHECKSUMS_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWS_CHECKSUMS_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_CHECKSUMS_STATIC_LIBRARY}
                      DEPENDS aws_c_common_ep)
  add_dependencies(AWS::aws-checksums aws_checksums_ep)

  if("s2n-tls" IN_LIST _AWSSDK_LIBS)
    # Remove unused directories to save build directory storage.
    # 169MB -> 105MB
    set(AWS_LC_PATCH_COMMAND ${CMAKE_COMMAND} -E)
    if(CMAKE_VERSION VERSION_LESS 3.17)
      list(APPEND AWS_LC_PATCH_COMMAND remove_directory)
    else()
      list(APPEND AWS_LC_PATCH_COMMAND rm -rf)
    endif()
    list(APPEND AWS_LC_PATCH_COMMAND fuzz)

    set(AWS_LC_C_FLAGS ${EP_C_FLAGS})
    string(APPEND AWS_LC_C_FLAGS " -Wno-error=overlength-strings -Wno-error=pedantic")
    # Link time optimization is causing trouble like #34349
    string(REPLACE "-flto=auto" "" AWS_LC_C_FLAGS "${AWS_LC_C_FLAGS}")
    string(REPLACE "-ffat-lto-objects" "" AWS_LC_C_FLAGS "${AWS_LC_C_FLAGS}")

    set(AWS_LC_CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS})
    list(APPEND AWS_LC_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${AWS_LC_PREFIX}
         -DCMAKE_C_FLAGS=${AWS_LC_C_FLAGS})

    externalproject_add(aws_lc_ep
                        ${EP_COMMON_OPTIONS}
                        URL ${AWS_LC_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_AWS_LC_BUILD_SHA256_CHECKSUM}"
                        PATCH_COMMAND ${AWS_LC_PATCH_COMMAND}
                        CMAKE_ARGS ${AWS_LC_CMAKE_ARGS}
                        BUILD_BYPRODUCTS ${AWS_LC_STATIC_LIBRARY})
    add_dependencies(AWS::crypto aws_lc_ep)

    set(S2N_TLS_C_FLAGS ${EP_C_FLAGS})
    # Link time optimization is causing trouble like #34349
    string(REPLACE "-flto=auto" "" S2N_TLS_C_FLAGS "${S2N_TLS_C_FLAGS}")
    string(REPLACE "-ffat-lto-objects" "" S2N_TLS_C_FLAGS "${S2N_TLS_C_FLAGS}")

    set(S2N_TLS_CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS})
    list(APPEND
         S2N_TLS_CMAKE_ARGS
         # internalize libcrypto to avoid name conflict with OpenSSL
         -DS2N_INTERN_LIBCRYPTO=ON
         # path to find crypto provided by aws-lc
         -DCMAKE_PREFIX_PATH=${AWS_LC_PREFIX}
         -DCMAKE_C_FLAGS=${S2N_TLS_C_FLAGS}
         # paths to find crypto provided by aws-lc
         -Dcrypto_INCLUDE_DIR=${AWS_LC_PREFIX}/include
         -Dcrypto_LIBRARY=${AWS_LC_STATIC_LIBRARY}
         -Dcrypto_STATIC_LIBRARY=${AWS_LC_STATIC_LIBRARY})

    externalproject_add(s2n_tls_ep
                        ${EP_COMMON_OPTIONS}
                        URL ${S2N_TLS_SOURCE_URL}
                        URL_HASH "SHA256=${ARROW_S2N_TLS_BUILD_SHA256_CHECKSUM}"
                        CMAKE_ARGS ${S2N_TLS_CMAKE_ARGS}
                        BUILD_BYPRODUCTS ${S2N_TLS_STATIC_LIBRARY}
                        DEPENDS aws_lc_ep)
    add_dependencies(AWS::s2n-tls s2n_tls_ep)
  endif()

  externalproject_add(aws_c_cal_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_CAL_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_CAL_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_CAL_STATIC_LIBRARY}
                      DEPENDS aws_c_common_ep)
  add_dependencies(AWS::aws-c-cal aws_c_cal_ep)

  set(AWS_C_IO_DEPENDS aws_c_common_ep aws_c_cal_ep)
  if(TARGET s2n_tls_ep)
    list(APPEND AWS_C_IO_DEPENDS s2n_tls_ep)
  endif()
  externalproject_add(aws_c_io_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_IO_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_IO_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_IO_STATIC_LIBRARY}
                      DEPENDS ${AWS_C_IO_DEPENDS})
  add_dependencies(AWS::aws-c-io aws_c_io_ep)

  externalproject_add(aws_c_event_stream_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_EVENT_STREAM_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_EVENT_STREAM_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_EVENT_STREAM_STATIC_LIBRARY}
                      DEPENDS aws_checksums_ep aws_c_io_ep)
  add_dependencies(AWS::aws-c-event-stream aws_c_event_stream_ep)

  externalproject_add(aws_c_sdkutils_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_SDKUTILS_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_SDKUTILS_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_SDKUTILS_STATIC_LIBRARY}
                      DEPENDS aws_c_common_ep)
  add_dependencies(AWS::aws-c-sdkutils aws_c_sdkutils_ep)

  externalproject_add(aws_c_compression_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_COMPRESSION_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_COMPRESSION_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_COMPRESSION_STATIC_LIBRARY}
                      DEPENDS aws_c_common_ep)
  add_dependencies(AWS::aws-c-compression aws_c_compression_ep)

  externalproject_add(aws_c_http_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_HTTP_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_HTTP_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_HTTP_STATIC_LIBRARY}
                      DEPENDS aws_c_io_ep aws_c_compression_ep)
  add_dependencies(AWS::aws-c-http aws_c_http_ep)

  externalproject_add(aws_c_mqtt_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_MQTT_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_MQTT_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_MQTT_STATIC_LIBRARY}
                      DEPENDS aws_c_http_ep)
  add_dependencies(AWS::aws-c-mqtt aws_c_mqtt_ep)

  externalproject_add(aws_c_auth_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_AUTH_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_AUTH_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_AUTH_STATIC_LIBRARY}
                      DEPENDS aws_c_sdkutils_ep aws_c_cal_ep aws_c_http_ep)
  add_dependencies(AWS::aws-c-auth aws_c_auth_ep)

  externalproject_add(aws_c_s3_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_C_S3_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_C_S3_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_COMMON_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_C_S3_STATIC_LIBRARY}
                      DEPENDS aws_checksums_ep aws_c_auth_ep)
  add_dependencies(AWS::aws-c-s3 aws_c_s3_ep)

  externalproject_add(aws_crt_cpp_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWS_CRT_CPP_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWS_CRT_CPP_BUILD_SHA256_CHECKSUM}"
                      CMAKE_ARGS ${AWSSDK_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_CRT_CPP_STATIC_LIBRARY}
                      DEPENDS aws_c_auth_ep
                              aws_c_cal_ep
                              aws_c_common_ep
                              aws_c_event_stream_ep
                              aws_c_http_ep
                              aws_c_io_ep
                              aws_c_mqtt_ep
                              aws_c_s3_ep
                              aws_checksums_ep)
  add_dependencies(AWS::aws-crt-cpp aws_crt_cpp_ep)

  externalproject_add(awssdk_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${AWSSDK_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_AWSSDK_BUILD_SHA256_CHECKSUM}"
                      PATCH_COMMAND ${AWSSDK_PATCH_COMMAND}
                      CMAKE_ARGS ${AWSSDK_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${AWS_CPP_SDK_COGNITO_IDENTITY_STATIC_LIBRARY}
                                       ${AWS_CPP_SDK_CORE_STATIC_LIBRARY}
                                       ${AWS_CPP_SDK_IDENTITY_MANAGEMENT_STATIC_LIBRARY}
                                       ${AWS_CPP_SDK_S3_STATIC_LIBRARY}
                                       ${AWS_CPP_SDK_STS_STATIC_LIBRARY}
                      DEPENDS aws_crt_cpp_ep)
  foreach(_AWSSDK_LIB ${_AWSSDK_LIBS})
    if(${_AWSSDK_LIB} MATCHES "^aws-cpp-sdk-")
      add_dependencies(${_AWSSDK_LIB} awssdk_ep)
    endif()
  endforeach()

  set(AWSSDK_VENDORED TRUE)
  list(APPEND ARROW_BUNDLED_STATIC_LIBS ${AWSSDK_LIBRARIES})
  set(AWSSDK_LINK_LIBRARIES ${AWSSDK_LIBRARIES})
  if(UNIX)
    # on Linux and macOS curl seems to be required
    set_property(TARGET aws-cpp-sdk-core
                 APPEND
                 PROPERTY INTERFACE_LINK_LIBRARIES CURL::libcurl)
    set_property(TARGET AWS::aws-c-cal
                 APPEND
                 PROPERTY INTERFACE_LINK_LIBRARIES OpenSSL::Crypto OpenSSL::SSL)
    if(APPLE)
      set_property(TARGET AWS::aws-c-cal
                   APPEND
                   PROPERTY INTERFACE_LINK_LIBRARIES "-framework Security")
    endif()
    if(ZLIB_VENDORED)
      set_property(TARGET aws-cpp-sdk-core
                   APPEND
                   PROPERTY INTERFACE_LINK_LIBRARIES ZLIB::ZLIB)
      add_dependencies(awssdk_ep zlib_ep)
    endif()
    set_property(TARGET AWS::aws-c-io
                 APPEND
                 PROPERTY INTERFACE_LINK_LIBRARIES ${CMAKE_DL_LIBS})
  elseif(WIN32)
    set_property(TARGET aws-cpp-sdk-core
                 APPEND
                 PROPERTY INTERFACE_LINK_LIBRARIES
                          "winhttp.lib"
                          "bcrypt.lib"
                          "wininet.lib"
                          "userenv.lib"
                          "version.lib")
    set_property(TARGET AWS::aws-c-cal
                 APPEND
                 PROPERTY INTERFACE_LINK_LIBRARIES
                          "bcrypt.lib"
                          "ncrypt.lib"
                          "Secur32.lib"
                          "Shlwapi.lib")
    set_property(TARGET AWS::aws-c-io
                 APPEND
                 PROPERTY INTERFACE_LINK_LIBRARIES "crypt32.lib")
  endif()

  # AWSSDK is static-only build
endmacro()

if(ARROW_S3)
  resolve_dependency(AWSSDK HAVE_ALT TRUE)

  message(STATUS "Found AWS SDK headers: ${AWSSDK_INCLUDE_DIR}")
  message(STATUS "Found AWS SDK libraries: ${AWSSDK_LINK_LIBRARIES}")

  if(ARROW_BUILD_STATIC)
    if(${AWSSDK_SOURCE} STREQUAL "SYSTEM")
      foreach(AWSSDK_LINK_LIBRARY ${AWSSDK_LINK_LIBRARIES})
        string(APPEND ARROW_PC_LIBS_PRIVATE " $<TARGET_FILE:${AWSSDK_LINK_LIBRARY}>")
      endforeach()
    else()
      if(UNIX)
        string(APPEND ARROW_PC_REQUIRES_PRIVATE " libcurl")
      endif()
      string(APPEND ARROW_PC_REQUIRES_PRIVATE " openssl")
      if(APPLE)
        string(APPEND ARROW_PC_LIBS_PRIVATE " -framework Security")
      endif()
    endif()
  endif()

  if(APPLE)
    # CoreFoundation's path is hardcoded in the CMake files provided by
    # aws-sdk-cpp to use the macOS SDK provided by XCode which makes
    # XCode a hard dependency. Command Line Tools is often used instead
    # of the full XCode suite, so let the linker to find it.
    set_target_properties(AWS::aws-c-common
                          PROPERTIES INTERFACE_LINK_LIBRARIES
                                     "-pthread;pthread;-framework CoreFoundation")
  endif()
endif()

# ----------------------------------------------------------------------
# Azure SDK for C++

function(build_azure_sdk)
  if(CMAKE_VERSION VERSION_LESS 3.22)
    # We can't disable installing Azure SDK for C++ by
    # "set_property(DIRECTORY ${azure_sdk_SOURCE_DIR} PROPERTY
    # EXCLUDE_FROM_ALL TRUE)" with CMake 3.16.
    #
    # At least CMake 3.22 on Ubuntu 22.04 works. So we use 3.22
    # here. We may be able to use more earlier version here.
    message(FATAL_ERROR "Building Azure SDK for C++ requires at least CMake 3.22. "
                        "(At least we can't use CMake 3.16)")
  endif()
  message(STATUS "Building Azure SDK for C++ from source")
  fetchcontent_declare(azure_sdk
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${ARROW_AZURE_SDK_URL}
                       URL_HASH "SHA256=${ARROW_AZURE_SDK_BUILD_SHA256_CHECKSUM}")
  prepare_fetchcontent()
  set(BUILD_PERFORMANCE_TESTS FALSE)
  set(BUILD_SAMPLES FALSE)
  set(BUILD_TESTING FALSE)
  set(BUILD_WINDOWS_UWP TRUE)
  set(CMAKE_UNITY_BUILD FALSE)
  set(DISABLE_AZURE_CORE_OPENTELEMETRY TRUE)
  set(ENV{AZURE_SDK_DISABLE_AUTO_VCPKG} TRUE)
  set(WARNINGS_AS_ERRORS FALSE)
  fetchcontent_makeavailable(azure_sdk)
  if(CMAKE_VERSION VERSION_LESS 3.28)
    set_property(DIRECTORY ${azure_sdk_SOURCE_DIR} PROPERTY EXCLUDE_FROM_ALL TRUE)
  endif()
  set(AZURE_SDK_VENDORED
      TRUE
      PARENT_SCOPE)
  list(APPEND
       ARROW_BUNDLED_STATIC_LIBS
       Azure::azure-core
       Azure::azure-identity
       Azure::azure-storage-blobs
       Azure::azure-storage-common
       Azure::azure-storage-files-datalake)
  set(ARROW_BUNDLED_STATIC_LIBS
      ${ARROW_BUNDLED_STATIC_LIBS}
      PARENT_SCOPE)
endfunction()

if(ARROW_WITH_AZURE_SDK)
  resolve_dependency(Azure REQUIRED_VERSION 1.10.2)
  set(AZURE_SDK_LINK_LIBRARIES Azure::azure-storage-files-datalake
                               Azure::azure-storage-blobs Azure::azure-identity)
endif()
# ----------------------------------------------------------------------
# ucx - communication framework for modern, high-bandwidth and low-latency networks

macro(build_ucx)
  message(STATUS "Building UCX from source")

  set(UCX_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/ucx_ep-install")

  # link with static ucx libraries leads to test failures, use shared libs instead
  set(UCX_SHARED_LIB_UCP "${UCX_PREFIX}/lib/libucp${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(UCX_SHARED_LIB_UCT "${UCX_PREFIX}/lib/libuct${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(UCX_SHARED_LIB_UCS "${UCX_PREFIX}/lib/libucs${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(UCX_SHARED_LIB_UCM "${UCX_PREFIX}/lib/libucm${CMAKE_SHARED_LIBRARY_SUFFIX}")

  set(UCX_CONFIGURE_COMMAND ./autogen.sh COMMAND ./configure)
  list(APPEND
       UCX_CONFIGURE_COMMAND
       "CC=${CMAKE_C_COMPILER}"
       "CXX=${CMAKE_CXX_COMPILER}"
       "CFLAGS=${EP_C_FLAGS}"
       "CXXFLAGS=${EP_CXX_FLAGS}"
       "--prefix=${UCX_PREFIX}"
       "--enable-mt"
       "--enable-shared")
  if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    list(APPEND
         UCX_CONFIGURE_COMMAND
         "--enable-profiling"
         "--enable-frame-pointer"
         "--enable-stats"
         "--enable-fault-injection"
         "--enable-debug-data")
  else()
    list(APPEND
         UCX_CONFIGURE_COMMAND
         "--disable-logging"
         "--disable-debug"
         "--disable-assertions"
         "--disable-params-check")
  endif()
  set(UCX_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
  externalproject_add(ucx_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${ARROW_UCX_SOURCE_URL}
                      URL_HASH "SHA256=${ARROW_UCX_BUILD_SHA256_CHECKSUM}"
                      CONFIGURE_COMMAND ${UCX_CONFIGURE_COMMAND}
                      BUILD_IN_SOURCE 1
                      BUILD_COMMAND ${UCX_BUILD_COMMAND}
                      BUILD_BYPRODUCTS "${UCX_SHARED_LIB_UCP}" "${UCX_SHARED_LIB_UCT}"
                                       "${UCX_SHARED_LIB_UCS}" "${UCX_SHARED_LIB_UCM}"
                      INSTALL_COMMAND ${MAKE} install)

  # ucx cmake module sets UCX_INCLUDE_DIRS
  set(UCX_INCLUDE_DIRS "${UCX_PREFIX}/include")
  file(MAKE_DIRECTORY "${UCX_INCLUDE_DIRS}")

  add_library(ucx::ucp SHARED IMPORTED)
  set_target_properties(ucx::ucp PROPERTIES IMPORTED_LOCATION "${UCX_SHARED_LIB_UCP}")
  add_library(ucx::uct SHARED IMPORTED)
  set_target_properties(ucx::uct PROPERTIES IMPORTED_LOCATION "${UCX_SHARED_LIB_UCT}")
  add_library(ucx::ucs SHARED IMPORTED)
  set_target_properties(ucx::ucs PROPERTIES IMPORTED_LOCATION "${UCX_SHARED_LIB_UCS}")

  add_dependencies(ucx::ucp ucx_ep)
  add_dependencies(ucx::uct ucx_ep)
  add_dependencies(ucx::ucs ucx_ep)
endmacro()

if(ARROW_WITH_UCX)
  resolve_dependency(ucx
                     ARROW_CMAKE_PACKAGE_NAME
                     ArrowFlight
                     ARROW_PC_PACKAGE_NAME
                     arrow-flight
                     PC_PACKAGE_NAMES
                     ucx)
  add_library(ucx::ucx INTERFACE IMPORTED)
  target_include_directories(ucx::ucx INTERFACE "${UCX_INCLUDE_DIRS}")
  target_link_libraries(ucx::ucx INTERFACE ucx::ucp ucx::uct ucx::ucs)
endif()

message(STATUS "All bundled static libraries: ${ARROW_BUNDLED_STATIC_LIBS}")
