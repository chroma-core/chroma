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

# Check if the target architecture and compiler supports some special
# instruction sets that would boost performance.
include(CheckCXXCompilerFlag)
include(CheckCXXSourceCompiles)
# Get cpu architecture

message(STATUS "System processor: ${CMAKE_SYSTEM_PROCESSOR}")

if(NOT DEFINED ARROW_CPU_FLAG)
  if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    set(ARROW_CPU_FLAG "emscripten")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "AMD64|amd64|X86|x86|i[3456]86|x64")
    set(ARROW_CPU_FLAG "x86")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|ARM64|arm64")
    set(ARROW_CPU_FLAG "aarch64")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm$|armv[4-7]")
    set(ARROW_CPU_FLAG "aarch32")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "powerpc|ppc")
    set(ARROW_CPU_FLAG "ppc")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "s390x")
    set(ARROW_CPU_FLAG "s390x")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "riscv64")
    set(ARROW_CPU_FLAG "riscv64")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "loongarch64")
    set(ARROW_CPU_FLAG "loongarch64")
  else()
    message(FATAL_ERROR "Unknown system processor")
  endif()
endif()

# Check architecture specific compiler flags
if(ARROW_CPU_FLAG STREQUAL "x86")
  # x86/amd64 compiler flags, msvc/gcc/clang
  if(MSVC)
    set(ARROW_SSE4_2_FLAG "")
    set(ARROW_AVX2_FLAG "/arch:AVX2")
    # MSVC has no specific flag for BMI2, it seems to be enabled with AVX2
    set(ARROW_BMI2_FLAG "/arch:AVX2")
    set(ARROW_AVX512_FLAG "/arch:AVX512")
    set(CXX_SUPPORTS_SSE4_2 TRUE)
  else()
    set(ARROW_SSE4_2_FLAG "-msse4.2")
    set(ARROW_AVX2_FLAG "-march=haswell")
    set(ARROW_BMI2_FLAG "-mbmi2")
    # skylake-avx512 consists of AVX512F,AVX512BW,AVX512VL,AVX512CD,AVX512DQ
    set(ARROW_AVX512_FLAG "-march=skylake-avx512")
    # Append the avx2/avx512 subset option also, fix issue ARROW-9877 for homebrew-cpp
    set(ARROW_AVX2_FLAG "${ARROW_AVX2_FLAG} -mavx2")
    set(ARROW_AVX512_FLAG
        "${ARROW_AVX512_FLAG} -mavx512f -mavx512cd -mavx512vl -mavx512dq -mavx512bw")
    check_cxx_compiler_flag(${ARROW_SSE4_2_FLAG} CXX_SUPPORTS_SSE4_2)
  endif()
  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    # Check for AVX extensions on 64-bit systems only, as 32-bit support seems iffy
    check_cxx_compiler_flag(${ARROW_AVX2_FLAG} CXX_SUPPORTS_AVX2)
    if(MINGW)
      # https://gcc.gnu.org/bugzilla/show_bug.cgi?id=65782
      message(STATUS "Disable AVX512 support on MINGW for now")
    else()
      # Check for AVX512 support in the compiler.
      set(OLD_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
      set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${ARROW_AVX512_FLAG}")
      check_cxx_source_compiles("
        #ifdef _MSC_VER
        #include <intrin.h>
        #else
        #include <immintrin.h>
        #endif

        int main() {
          __m512i mask = _mm512_set1_epi32(0x1);
          char out[32];
          _mm512_storeu_si512(out, mask);
          return 0;
        }"
                                CXX_SUPPORTS_AVX512)
      set(CMAKE_REQUIRED_FLAGS ${OLD_CMAKE_REQUIRED_FLAGS})
    endif()
  endif()
  # Runtime SIMD level it can get from compiler and ARROW_RUNTIME_SIMD_LEVEL
  if(CXX_SUPPORTS_SSE4_2 AND ARROW_RUNTIME_SIMD_LEVEL MATCHES
                             "^(SSE4_2|AVX2|AVX512|MAX)$")
    set(ARROW_HAVE_RUNTIME_SSE4_2 ON)
    add_definitions(-DARROW_HAVE_RUNTIME_SSE4_2)
  endif()
  # Note: for now we assume that AVX2 support should also enable BMI2 support,
  # at least at compile-time (more care may be required for runtime dispatch).
  if(CXX_SUPPORTS_AVX2 AND ARROW_RUNTIME_SIMD_LEVEL MATCHES "^(AVX2|AVX512|MAX)$")
    set(ARROW_HAVE_RUNTIME_AVX2 ON)
    set(ARROW_HAVE_RUNTIME_BMI2 ON)
    add_definitions(-DARROW_HAVE_RUNTIME_AVX2 -DARROW_HAVE_RUNTIME_BMI2)
  endif()
  if(CXX_SUPPORTS_AVX512 AND ARROW_RUNTIME_SIMD_LEVEL MATCHES "^(AVX512|MAX)$")
    set(ARROW_HAVE_RUNTIME_AVX512 ON)
    add_definitions(-DARROW_HAVE_RUNTIME_AVX512)
  endif()
  if(ARROW_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ARROW_SIMD_LEVEL "SSE4_2")
  endif()
elseif(ARROW_CPU_FLAG STREQUAL "ppc")
  # power compiler flags, gcc/clang only
  set(ARROW_ALTIVEC_FLAG "-maltivec")
  check_cxx_compiler_flag(${ARROW_ALTIVEC_FLAG} CXX_SUPPORTS_ALTIVEC)
  if(ARROW_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ARROW_SIMD_LEVEL "NONE")
  endif()
elseif(ARROW_CPU_FLAG STREQUAL "aarch64")
  # Arm64 compiler flags, gcc/clang only
  set(ARROW_ARMV8_MARCH "armv8-a")
  check_cxx_compiler_flag("-march=${ARROW_ARMV8_MARCH}+sve" CXX_SUPPORTS_SVE)
  if(ARROW_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ARROW_SIMD_LEVEL "NEON")
  endif()
endif()

# Support C11
if(NOT DEFINED CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 11)
endif()

# This ensures that things like c++17 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
elseif(${CMAKE_CXX_STANDARD} VERSION_LESS 17)
  message(FATAL_ERROR "Cannot set a CMAKE_CXX_STANDARD smaller than 17")
endif()

# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ARROW-6848: Do not use GNU (or other CXX) extensions
set(CMAKE_CXX_EXTENSIONS OFF)

# Build with -fPIC so that can static link our libraries into other people's
# shared libraries
set(CMAKE_POSITION_INDEPENDENT_CODE ${ARROW_POSITION_INDEPENDENT_CODE})

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

set(UNKNOWN_COMPILER_MESSAGE
    "Unknown compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

# compiler flags that are common across debug/release builds
if(WIN32)
  # TODO(wesm): Change usages of C runtime functions that MSVC says are
  # insecure, like std::getenv
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)

  # Disable static assertion in Microsoft C++ standard library.
  #
  # """[...]\include\type_traits(1271): error C2338:
  # You've instantiated std::aligned_storage<Len, Align> with an extended
  # alignment (in other words, Align > alignof(max_align_t)).
  # Before VS 2017 15.8, the member type would non-conformingly have an
  # alignment of only alignof(max_align_t). VS 2017 15.8 was fixed to handle
  # this correctly, but the fix inherently changes layout and breaks binary
  # compatibility (*only* for uses of aligned_storage with extended alignments).
  # Please define either (1) _ENABLE_EXTENDED_ALIGNED_STORAGE to acknowledge
  # that you understand this message and that you actually want a type with
  # an extended alignment, or (2) _DISABLE_EXTENDED_ALIGNED_STORAGE to silence
  # this message and get the old non-conformant behavior."""
  add_definitions(-D_ENABLE_EXTENDED_ALIGNED_STORAGE)

  if(MSVC)
    # ARROW-1931 See https://github.com/google/googletest/issues/1318
    #
    # This is added to CMAKE_CXX_FLAGS instead of CXX_COMMON_FLAGS since only the
    # former is passed into the external projects
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")

    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      # clang-cl
      set(CXX_COMMON_FLAGS "-EHsc")
    else()
      # Fix annoying D9025 warning
      string(REPLACE "/W3" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

      # Set desired warning level (e.g. set /W4 for more warnings)
      #
      # ARROW-2986: Without /EHsc we get C4530 warning
      set(CXX_COMMON_FLAGS "/W3 /EHsc")
    endif()

    # Disable C5105 (macro expansion producing 'defined' has undefined
    # behavior) warning because there are codes that produce this
    # warning in Windows Kits. e.g.:
    #
    #   #define _CRT_INTERNAL_NONSTDC_NAMES                                            \
    #        (                                                                          \
    #            ( defined _CRT_DECLARE_NONSTDC_NAMES && _CRT_DECLARE_NONSTDC_NAMES) || \
    #            (!defined _CRT_DECLARE_NONSTDC_NAMES && !__STDC__                 )    \
    #        )
    #
    # See also:
    # * C5105: https://docs.microsoft.com/en-US/cpp/error-messages/compiler-warnings/c5105
    # * Related reports:
    #   * https://developercommunity.visualstudio.com/content/problem/387684/c5105-with-stdioh-and-experimentalpreprocessor.html
    #   * https://developercommunity.visualstudio.com/content/problem/1249671/stdc17-generates-warning-compiling-windowsh.html
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /wd5105")

    if(ARROW_USE_CCACHE)
      foreach(c_flag
              CMAKE_CXX_FLAGS
              CMAKE_CXX_FLAGS_RELEASE
              CMAKE_CXX_FLAGS_DEBUG
              CMAKE_CXX_FLAGS_MINSIZEREL
              CMAKE_CXX_FLAGS_RELWITHDEBINFO
              CMAKE_C_FLAGS
              CMAKE_C_FLAGS_RELEASE
              CMAKE_C_FLAGS_DEBUG
              CMAKE_C_FLAGS_MINSIZEREL
              CMAKE_C_FLAGS_RELWITHDEBINFO)
        # ccache doesn't work with /Zi.
        # See also: https://github.com/ccache/ccache/issues/1040
        string(REPLACE "/Zi" "/Z7" ${c_flag} "${${c_flag}}")
      endforeach()
    endif()

    if(ARROW_USE_STATIC_CRT)
      foreach(c_flag
              CMAKE_CXX_FLAGS
              CMAKE_CXX_FLAGS_RELEASE
              CMAKE_CXX_FLAGS_DEBUG
              CMAKE_CXX_FLAGS_MINSIZEREL
              CMAKE_CXX_FLAGS_RELWITHDEBINFO
              CMAKE_C_FLAGS
              CMAKE_C_FLAGS_RELEASE
              CMAKE_C_FLAGS_DEBUG
              CMAKE_C_FLAGS_MINSIZEREL
              CMAKE_C_FLAGS_RELWITHDEBINFO)
        string(REPLACE "/MD" "/MT" ${c_flag} "${${c_flag}}")
      endforeach()
    endif()

    # Support large object code
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /bigobj")

    # We may use UTF-8 in source code such as
    # cpp/src/arrow/compute/kernels/scalar_string_test.cc
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /utf-8")
  else()
    # MinGW
    check_cxx_compiler_flag(-Wa,-mbig-obj CXX_SUPPORTS_BIG_OBJ)
    if(CXX_SUPPORTS_BIG_OBJ)
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wa,-mbig-obj")
    endif()
  endif(MSVC)
else()
  # Common flags set below with warning level
  set(CXX_COMMON_FLAGS "")
endif()

# BUILD_WARNING_LEVEL add warning/error compiler flags. The possible values are
# - PRODUCTION: Build with `-Wall` but do not add `-Werror`, so warnings do not
#   halt the build.
# - CHECKIN: Build with `-Wall` and `-Wextra`.  Also, add `-Werror` in debug mode
#   so that any important warnings fail the build.
# - EVERYTHING: Like `CHECKIN`, but possible extra flags depending on the
#               compiler, including `-Wextra`, `-Weverything`, `-pedantic`.
#               This is the most aggressive warning level.

# Defaults BUILD_WARNING_LEVEL to `CHECKIN`, unless CMAKE_BUILD_TYPE is
# `RELEASE`, then it will default to `PRODUCTION`. The goal of defaulting to
# `CHECKIN` is to avoid friction with long response time from CI.
if(NOT BUILD_WARNING_LEVEL)
  if("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(BUILD_WARNING_LEVEL PRODUCTION)
  else()
    set(BUILD_WARNING_LEVEL CHECKIN)
  endif()
endif(NOT BUILD_WARNING_LEVEL)
string(TOUPPER ${BUILD_WARNING_LEVEL} BUILD_WARNING_LEVEL)

message(STATUS "Arrow build warning level: ${BUILD_WARNING_LEVEL}")

macro(arrow_add_werror_if_debug)
  # Treat all compiler warnings as errors
  if(MSVC)
    string(APPEND CMAKE_C_FLAGS_DEBUG " /WX")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " /WX")
  else()
    string(APPEND CMAKE_C_FLAGS_DEBUG " -Werror")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " -Werror")
  endif()
endmacro()

if("${BUILD_WARNING_LEVEL}" STREQUAL "CHECKIN")
  # Pre-checkin builds
  if(MSVC)
    # https://docs.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warnings-by-compiler-version
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /W3")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /wd4365")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /wd4267")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /wd4838")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                        "Clang")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wextra")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wdocumentation")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DARROW_WARN_DOCUMENTATION")
    if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
      # size_t is 32 bit in Emscripten wasm32 - ignore conversion errors
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-shorten-64-to-32")
    else()
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wshorten-64-to-32")
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-missing-braces")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-unused-parameter")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-constant-logical-operand")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-return-stack-address")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wdate-time")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-conversion")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-sign-conversion")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wdate-time")
    string(APPEND CXX_ONLY_FLAGS " -Wredundant-move")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wunused-result")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Intel" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                   "IntelLLVM")
    if(WIN32)
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /Wall")
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /Wno-deprecated")
    else()
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated")
    endif()
  else()
    message(FATAL_ERROR "${UNKNOWN_COMPILER_MESSAGE}")
  endif()
  arrow_add_werror_if_debug()

elseif("${BUILD_WARNING_LEVEL}" STREQUAL "EVERYTHING")
  # Pedantic builds for fixing warnings
  if(MSVC)
    string(REPLACE "/W3" "" CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS}")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /Wall")
    # https://docs.microsoft.com/en-us/cpp/build/reference/compiler-option-warning-level
    # /wdnnnn disables a warning where "nnnn" is a warning number
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                        "Clang")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Weverything")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-c++98-compat")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-c++98-compat-pedantic")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wpedantic")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wextra")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-unused-parameter")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wunused-result")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Intel" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                   "IntelLLVM")
    if(WIN32)
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /Wall")
    else()
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
    endif()
  else()
    message(FATAL_ERROR "${UNKNOWN_COMPILER_MESSAGE}")
  endif()
  arrow_add_werror_if_debug()

else()
  # Production builds (warning are not treated as errors)
  if(MSVC)
    # https://docs.microsoft.com/en-us/cpp/build/reference/compiler-option-warning-level
    # TODO: Enable /Wall and disable individual warnings until build compiles without errors
    # /wdnnnn disables a warning where "nnnn" is a warning number
    string(REPLACE "/W3" "" CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS}")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /W3")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"
         OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
         OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Intel" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                   "IntelLLVM")
    if(WIN32)
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /Wall")
    else()
      set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
    endif()
  else()
    message(FATAL_ERROR "${UNKNOWN_COMPILER_MESSAGE}")
  endif()

endif()

if(MSVC)
  # Disable annoying "performance warning" about int-to-bool conversion
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /wd4800")

  # Disable unchecked iterator warnings, equivalent to /D_SCL_SECURE_NO_WARNINGS
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /wd4996")

  # Disable "switch statement contains 'default' but no 'case' labels" warning
  # (required for protobuf, see https://github.com/protocolbuffers/protobuf/issues/6885)
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} /wd4065")

elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "7.0" OR CMAKE_CXX_COMPILER_VERSION
                                                       VERSION_GREATER "7.0")
    # Without this, gcc >= 7 warns related to changes in C++17
    set(CXX_ONLY_FLAGS "${CXX_ONLY_FLAGS} -Wno-noexcept-type")
  endif()

  if(CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "13.0" OR CMAKE_CXX_COMPILER_VERSION
                                                        VERSION_GREATER "13.0")
    # -Wself-move added in GCC 13 warns when a value is moved to itself
    # See https://gcc.gnu.org/gcc-13/changes.html
    set(CXX_ONLY_FLAGS "${CXX_ONLY_FLAGS} -Wno-self-move")
  endif()

  # Disabling semantic interposition allows faster calling conventions
  # when calling global functions internally, and can also help inlining.
  # See https://stackoverflow.com/questions/35745543/new-option-in-gcc-5-3-fno-semantic-interposition
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-semantic-interposition")

  # Add colors when paired with ninja
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")

  if(CMAKE_UNITY_BUILD)
    # Work around issue similar to https://bugs.webkit.org/show_bug.cgi?id=176869
    set(CXX_ONLY_FLAGS "${CXX_ONLY_FLAGS} -Wno-subobject-linkage")
  endif()

elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                      "Clang")
  # Clang options for all builds

  # Using Clang with ccache causes a bunch of spurious warnings that are
  # purportedly fixed in the next version of ccache. See the following for details:
  #
  #   http://petereisentraut.blogspot.com/2011/05/ccache-and-clang.html
  #   http://petereisentraut.blogspot.com/2011/09/ccache-and-clang-part-2.html
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Qunused-arguments")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")

  # Avoid error when an unknown warning flag is passed
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-unknown-warning-option")
  # Add colors when paired with ninja
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")

  # Don't complain about optimization passes that were not possible
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-pass-failed")

  if(APPLE)
    # Avoid clang / libc++ error about C++17 aligned allocation on macOS.
    # See https://chromium.googlesource.com/chromium/src/+/eee44569858fc650b635779c4e34be5cb0c73186%5E%21/#F0
    # for details.
    string(APPEND CXX_ONLY_FLAGS " -fno-aligned-new")

    if(CMAKE_HOST_SYSTEM_VERSION VERSION_LESS 20)
      # Avoid C++17 std::get 'not available' issue on macOS 10.13
      # This will be required until at least R 4.4 is released and
      # CRAN (hopefully) stops checking on 10.13
      string(APPEND CXX_ONLY_FLAGS " -D_LIBCPP_DISABLE_AVAILABILITY")
    endif()
  endif()
endif()

# if build warning flags is set, add to CXX_COMMON_FLAGS
if(BUILD_WARNING_FLAGS)
  # Use BUILD_WARNING_FLAGS with BUILD_WARNING_LEVEL=everything to disable
  # warnings (use with Clang's -Weverything flag to find potential errors)
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${BUILD_WARNING_FLAGS}")
endif(BUILD_WARNING_FLAGS)

# Only enable additional instruction sets if they are supported
if(ARROW_CPU_FLAG STREQUAL "x86")
  if(MINGW)
    # Enable _xgetbv() intrinsic to query OS support for ZMM register saves
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -mxsave")
  endif()
  if(ARROW_SIMD_LEVEL STREQUAL "AVX512")
    if(NOT CXX_SUPPORTS_AVX512)
      message(FATAL_ERROR "AVX512 required but compiler doesn't support it.")
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${ARROW_AVX512_FLAG}")
    add_definitions(-DARROW_HAVE_AVX512 -DARROW_HAVE_AVX2 -DARROW_HAVE_BMI2
                    -DARROW_HAVE_SSE4_2)
  elseif(ARROW_SIMD_LEVEL STREQUAL "AVX2")
    if(NOT CXX_SUPPORTS_AVX2)
      message(FATAL_ERROR "AVX2 required but compiler doesn't support it.")
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${ARROW_AVX2_FLAG}")
    add_definitions(-DARROW_HAVE_AVX2 -DARROW_HAVE_BMI2 -DARROW_HAVE_SSE4_2)
  elseif(ARROW_SIMD_LEVEL STREQUAL "SSE4_2")
    if(NOT CXX_SUPPORTS_SSE4_2)
      message(FATAL_ERROR "SSE4.2 required but compiler doesn't support it.")
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${ARROW_SSE4_2_FLAG}")
    add_definitions(-DARROW_HAVE_SSE4_2)
  elseif(NOT ARROW_SIMD_LEVEL STREQUAL "NONE")
    message(WARNING "ARROW_SIMD_LEVEL=${ARROW_SIMD_LEVEL} not supported by x86.")
  endif()
endif()

if(ARROW_CPU_FLAG STREQUAL "ppc")
  if(CXX_SUPPORTS_ALTIVEC AND ARROW_ALTIVEC)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${ARROW_ALTIVEC_FLAG}")
  endif()
endif()

if(ARROW_CPU_FLAG STREQUAL "aarch64")
  if(ARROW_SIMD_LEVEL MATCHES "NEON|SVE[0-9]*")
    set(ARROW_HAVE_NEON ON)
    add_definitions(-DARROW_HAVE_NEON)
    if(ARROW_SIMD_LEVEL MATCHES "SVE[0-9]*")
      if(NOT CXX_SUPPORTS_SVE)
        message(FATAL_ERROR "SVE required but compiler doesn't support it.")
      endif()
      # -march=armv8-a+sve
      set(ARROW_ARMV8_MARCH "${ARROW_ARMV8_MARCH}+sve")
      string(REGEX MATCH "[0-9]+" SVE_VECTOR_BITS ${ARROW_SIMD_LEVEL})
      if(SVE_VECTOR_BITS)
        set(ARROW_HAVE_SVE${SVE_VECTOR_BITS} ON)
        add_definitions(-DARROW_HAVE_SVE${SVE_VECTOR_BITS})
        # -msve-vector-bits=256
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -msve-vector-bits=${SVE_VECTOR_BITS}")
      else()
        set(ARROW_HAVE_SVE_SIZELESS ON)
        add_definitions(-DARROW_HAVE_SVE_SIZELESS)
      endif()
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -march=${ARROW_ARMV8_MARCH}")
  elseif(NOT ARROW_SIMD_LEVEL STREQUAL "NONE")
    message(WARNING "ARROW_SIMD_LEVEL=${ARROW_SIMD_LEVEL} not supported by Arm.")
  endif()
endif()

# ----------------------------------------------------------------------
# Setup Gold linker, if available. Code originally from Apache Kudu

# Interrogates the linker version via the C++ compiler to determine whether
# we're using the gold linker, and if so, extracts its version.
#
# If the gold linker is being used, sets GOLD_VERSION in the parent scope with
# the extracted version.
#
# Any additional arguments are passed verbatim into the C++ compiler invocation.
function(GET_GOLD_VERSION)
  # The gold linker is only for ELF binaries, which macOS doesn't use.
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} "-Wl,--version" ${ARGN}
                  ERROR_QUIET
                  OUTPUT_VARIABLE LINKER_OUTPUT)
  # We're expecting LINKER_OUTPUT to look like one of these:
  #   GNU gold (version 2.24) 1.11
  #   GNU gold (GNU Binutils for Ubuntu 2.30) 1.15
  if(LINKER_OUTPUT MATCHES "GNU gold")
    string(REGEX MATCH "GNU gold \\([^\\)]*\\) (([0-9]+\\.?)+)" _ "${LINKER_OUTPUT}")
    if(NOT CMAKE_MATCH_1)
      message(SEND_ERROR "Could not extract GNU gold version. "
                         "Linker version output: ${LINKER_OUTPUT}")
    endif()
    set(GOLD_VERSION
        "${CMAKE_MATCH_1}"
        PARENT_SCOPE)
  endif()
endfunction()

# Is the compiler hard-wired to use the gold linker?
if(NOT WIN32 AND NOT APPLE)
  get_gold_version()
  if(GOLD_VERSION)
    set(MUST_USE_GOLD 1)
  elseif(ARROW_USE_LD_GOLD)
    # Can the compiler optionally enable the gold linker?
    get_gold_version("-fuse-ld=gold")

    # We can't use the gold linker if it's inside devtoolset because the compiler
    # won't find it when invoked directly from make/ninja (which is typically
    # done outside devtoolset).
    execute_process(COMMAND which ld.gold
                    OUTPUT_VARIABLE GOLD_LOCATION
                    OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
    if("${GOLD_LOCATION}" MATCHES "^/opt/rh/devtoolset")
      message(STATUS "Skipping optional gold linker (version ${GOLD_VERSION}) because "
                     "it's in devtoolset")
      set(GOLD_VERSION)
    endif()
  endif()

  if(GOLD_VERSION)
    # Older versions of the gold linker are vulnerable to a bug [1] which
    # prevents weak symbols from being overridden properly. This leads to
    # omitting of dependencies like tcmalloc (used in Kudu, where this
    # workaround was written originally)
    #
    # How we handle this situation depends on other factors:
    # - If gold is optional, we won't use it.
    # - If gold is required, we'll either:
    #   - Raise an error in RELEASE builds (we shouldn't release such a product), or
    #   - Drop tcmalloc in all other builds.
    #
    # 1. https://sourceware.org/bugzilla/show_bug.cgi?id=16979.
    if("${GOLD_VERSION}" VERSION_LESS "1.12")
      set(ARROW_BUGGY_GOLD 1)
    endif()
    if(MUST_USE_GOLD)
      message(STATUS "Using hard-wired gold linker (version ${GOLD_VERSION})")
      if(ARROW_BUGGY_GOLD)
        if("${ARROW_LINK}" STREQUAL "d" AND "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
          message(SEND_ERROR "Configured to use buggy gold with dynamic linking "
                             "in a RELEASE build")
        endif()
      endif()
    elseif(NOT ARROW_BUGGY_GOLD)
      # The Gold linker must be manually enabled.
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fuse-ld=gold")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fuse-ld=gold")
      message(STATUS "Using optional gold linker (version ${GOLD_VERSION})")
    else()
      message(STATUS "Optional gold linker is buggy, using ld linker instead")
    endif()
  else()
    message(STATUS "Using ld linker")
  endif()
endif()

if(NOT WIN32 AND NOT APPLE)
  if(ARROW_USE_MOLD)
    find_program(LD_MOLD ld.mold)
    if(LD_MOLD)
      unset(MOLD_LINKER_FLAGS)
      if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12.1.0")
          set(MOLD_LINKER_FLAGS "-fuse-ld=mold")
        else()
          message(STATUS "Need GCC 12.1.0 or later to use mold linker: ${CMAKE_CXX_COMPILER_VERSION}"
          )
        endif()
      elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12.0.0")
          set(MOLD_LINKER_FLAGS "--ld-path=${LD_MOLD}")
        else()
          message(STATUS "Need clang 12.0.0 or later to use mold linker: ${CMAKE_CXX_COMPILER_VERSION}"
          )
        endif()
      else()
        message(STATUS "Using the default linker because compiler doesn't support mold: ${CMAKE_CXX_COMPILER_ID}"
        )
      endif()
      if(MOLD_LINKER_FLAGS)
        message(STATUS "Using optional mold linker")
        string(APPEND CMAKE_EXE_LINKER_FLAGS " ${MOLD_LINKER_FLAGS}")
        string(APPEND CMAKE_MODULE_LINKER_FLAGS " ${MOLD_LINKER_FLAGS}")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS " ${MOLD_LINKER_FLAGS}")
      endif()
    else()
      message(STATUS "Using the default linker because mold isn't found")
    endif()
  endif()
endif()

if(ARROW_USE_LLD)
  find_program(LD_LLD ld.lld)
  if(LD_LLD)
    unset(LLD_LINKER_FLAGS)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "9.1.0")
        set(LLD_LINKER_FLAGS "-fuse-ld=lld")
      else()
        message(STATUS "Need GCC 9.1.0 or later to use LLD linker: ${CMAKE_CXX_COMPILER_VERSION}"
        )
      endif()
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12.0.0")
        set(LLD_LINKER_FLAGS "--ld-path=${LD_LLD}")
      else()
        message(STATUS "Need clang 12.0.0 or later to use LLD linker: ${CMAKE_CXX_COMPILER_VERSION}"
        )
      endif()
    else()
      message(STATUS "Using the default linker because compiler doesn't support LLD: ${CMAKE_CXX_COMPILER_ID}"
      )
    endif()
    if(LLD_LINKER_FLAGS)
      message(STATUS "Using optional LLVM LLD linker")
      string(APPEND CMAKE_EXE_LINKER_FLAGS " ${LLD_LINKER_FLAGS}")
      string(APPEND CMAKE_MODULE_LINKER_FLAGS " ${LLD_LINKER_FLAGS}")
      string(APPEND CMAKE_SHARED_LINKER_FLAGS " ${LLD_LINKER_FLAGS}")
    else()
      message(STATUS "Using the default linker because the LLD isn't supported")
    endif()
  endif()
endif()

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=Release
#   -O2 (not -O3): Enable compiler optimizations
#   Debug symbols are stripped for reduced binary size.
# For CMAKE_BUILD_TYPE=RelWithDebInfo
#   Same as Release, except with debug symbols enabled.

if(NOT MSVC)
  set(C_RELEASE_FLAGS "")
  if(CMAKE_C_FLAGS_RELEASE MATCHES "-O3")
    string(APPEND C_RELEASE_FLAGS " -O2")
  endif()
  set(CXX_RELEASE_FLAGS "")
  if(CMAKE_CXX_FLAGS_RELEASE MATCHES "-O3")
    string(APPEND CXX_RELEASE_FLAGS " -O2")
  endif()
  set(C_RELWITHDEBINFO_FLAGS "")
  if(CMAKE_C_FLAGS_RELWITHDEBINFO MATCHES "-O3")
    string(APPEND C_RELWITHDEBINFO_FLAGS " -O2")
  endif()
  set(CXX_RELWITHDEBINFO_FLAGS "")
  if(CMAKE_CXX_FLAGS_RELWITHDEBINFO MATCHES "-O3")
    string(APPEND CXX_RELWITHDEBINFO_FLAGS " -O2")
  endif()
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    string(APPEND C_RELEASE_FLAGS " -ftree-vectorize")
    string(APPEND CXX_RELEASE_FLAGS " -ftree-vectorize")
    string(APPEND C_RELWITHDEBINFO_FLAGS " -ftree-vectorize")
    string(APPEND CXX_RELWITHDEBINFO_FLAGS " -ftree-vectorize")
  endif()
  set(C_DEBUG_FLAGS "")
  set(CXX_DEBUG_FLAGS "")
  if(NOT MSVC)
    if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
      # with -g it uses DWARF debug info, which is really slow to build
      # on emscripten (and uses tons of memory)
      string(REPLACE "-g" " " CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
      string(REPLACE "-g" " " CMAKE_C_FLAGS_DEBUG ${CMAKE_C_FLAGS_DEBUG})
      string(APPEND C_DEBUG_FLAGS " -g2")
      string(APPEND CXX_DEBUG_FLAGS " -g2")
      string(APPEND C_RELWITHDEBINFO_FLAGS " -g2")
      string(APPEND CXX_RELWITHDEBINFO_FLAGS " -g2")
      # without -O1, emscripten executables are *MASSIVE*. Don't use -O0
      if(NOT CMAKE_C_FLAGS_DEBUG MATCHES "-O")
        string(APPEND C_DEBUG_FLAGS " -O1")
      endif()
      if(NOT CMAKE_CXX_FLAGS_DEBUG MATCHES "-O")
        string(APPEND CXX_DEBUG_FLAGS " -O1")
      endif()
    else()
      if(NOT CMAKE_C_FLAGS_DEBUG MATCHES "-O")
        string(APPEND C_DEBUG_FLAGS " -O0")
      endif()
      if(NOT CMAKE_CXX_FLAGS_DEBUG MATCHES "-O")
        string(APPEND CXX_DEBUG_FLAGS " -O0")
      endif()

      if(ARROW_GGDB_DEBUG)
        string(APPEND C_DEBUG_FLAGS " -ggdb")
        string(APPEND CXX_DEBUG_FLAGS " -ggdb")
        string(APPEND C_RELWITHDEBINFO_FLAGS " -ggdb")
        string(APPEND CXX_RELWITHDEBINFO_FLAGS " -ggdb")
      endif()
    endif()
  endif()

  string(APPEND CMAKE_C_FLAGS_RELEASE "${C_RELEASE_FLAGS} ${ARROW_C_FLAGS_RELEASE}")
  string(APPEND CMAKE_CXX_FLAGS_RELEASE "${CXX_RELEASE_FLAGS} ${ARROW_CXX_FLAGS_RELEASE}")
  string(APPEND CMAKE_C_FLAGS_DEBUG "${C_DEBUG_FLAGS} ${ARROW_C_FLAGS_DEBUG}")
  string(APPEND CMAKE_CXX_FLAGS_DEBUG "${CXX_DEBUG_FLAGS} ${ARROW_CXX_FLAGS_DEBUG}")
  string(APPEND CMAKE_C_FLAGS_RELWITHDEBINFO
         "${C_RELWITHDEBINFO_FLAGS} ${ARROW_C_FLAGS_RELWITHDEBINFO}")
  string(APPEND CMAKE_CXX_FLAGS_RELWITHDEBINFO
         "${CXX_RELWITHDEBINFO_FLAGS} ${ARROW_CXX_FLAGS_RELWITHDEBINFO}")
endif()

message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")

# ----------------------------------------------------------------------
# MSVC-specific linker options

if(MSVC)
  set(MSVC_LINKER_FLAGS)
  if(MSVC_LINK_VERBOSE)
    set(MSVC_LINKER_FLAGS "${MSVC_LINKER_FLAGS} /VERBOSE:LIB")
  endif()
  if(NOT ARROW_USE_STATIC_CRT)
    set(MSVC_LINKER_FLAGS "${MSVC_LINKER_FLAGS} /NODEFAULTLIB:LIBCMT")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${MSVC_LINKER_FLAGS}")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${MSVC_LINKER_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${MSVC_LINKER_FLAGS}")
  endif()
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
  # flags are:
  # 1) We force *everything* to build as position independent
  # 2) And with support for C++ exceptions
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -fexceptions")
  # deprecated-literal-operator error is thrown in datetime (vendored lib in arrow)
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -fPIC -fexceptions -Wno-error=deprecated-literal-operator")

  # flags for creating shared libraries (only used in pyarrow, because
  # Emscripten builds libarrow as static)
  # flags are:
  # 1) Tell it to use JavaScript / WebAssembly 64 bit number support.
  # 2) Tell it to build with support for C++ exceptions
  # 3) Skip linker flags error which happens with -soname parameter
  set(ARROW_EMSCRIPTEN_LINKER_FLAGS "-sWASM_BIGINT=1 -fexceptions -Wno-error=linkflags")
  set(CMAKE_SHARED_LIBRARY_CREATE_C_FLAGS
      "-sSIDE_MODULE=1 ${ARROW_EMSCRIPTEN_LINKER_FLAGS}")
  set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS
      "-sSIDE_MODULE=1 ${ARROW_EMSCRIPTEN_LINKER_FLAGS}")
  set(CMAKE_SHARED_LINKER_FLAGS "-sSIDE_MODULE=1 ${ARROW_EMSCRIPTEN_LINKER_FLAGS}")
  if(ARROW_TESTING)
    # flags for building test executables for use in node
    if("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
      set(CMAKE_EXE_LINKER_FLAGS
          "${ARROW_EMSCRIPTEN_LINKER_FLAGS} -sALLOW_MEMORY_GROWTH -lnodefs.js -lnoderawfs.js --pre-js ${BUILD_SUPPORT_DIR}/emscripten-test-init.js"
      )
    else()
      set(CMAKE_EXE_LINKER_FLAGS
          "${ARROW_EMSCRIPTEN_LINKER_FLAGS} -sERROR_ON_WASM_CHANGES_AFTER_LINK=1 -sALLOW_MEMORY_GROWTH -lnodefs.js -lnoderawfs.js --pre-js ${BUILD_SUPPORT_DIR}/emscripten-test-init.js"
      )
    endif()
  else()
    set(CMAKE_EXE_LINKER_FLAGS "${ARROW_EMSCRIPTEN_LINKER_FLAGS} -sALLOW_MEMORY_GROWTH")
  endif()
endif()
