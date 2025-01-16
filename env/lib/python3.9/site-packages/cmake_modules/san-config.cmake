#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

# Clang does not support using ASAN and TSAN simultaneously.
if("${ARROW_USE_ASAN}" AND "${ARROW_USE_TSAN}")
  message(SEND_ERROR "Can only enable one of ASAN or TSAN at a time")
endif()

# Flag to enable clang address sanitizer
# This will only build if clang or a recent enough gcc is the chosen compiler
if(${ARROW_USE_ASAN})
  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"
     OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
     OR (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                                  VERSION_GREATER "4.8"))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -DADDRESS_SANITIZER")
  else()
    message(SEND_ERROR "Cannot use ASAN without clang or gcc >= 4.8")
  endif()
endif()

# Flag to enable clang undefined behavior sanitizer
# We explicitly don't enable all of the sanitizer flags:
# - disable 'vptr' because of RTTI issues across shared libraries (?)
# - disable 'alignment' because unaligned access is really OK on Nehalem and we do it
#   all over the place.
# - disable 'function' because it appears to give a false positive
#   (https://github.com/google/sanitizers/issues/911)
# - disable 'float-divide-by-zero' on clang, which considers it UB
#   (https://bugs.llvm.org/show_bug.cgi?id=17000#c1)
#   Note: GCC does not support the 'function' flag.
if(${ARROW_USE_UBSAN})
  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                    "Clang")
    set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr,function,float-divide-by-zero -fno-sanitize-recover=all"
    )
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                                  VERSION_GREATER_EQUAL "5.1")
    set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr -fno-sanitize-recover=all"
    )
  else()
    message(SEND_ERROR "Cannot use UBSAN without clang or gcc >= 5.1")
  endif()
endif()

# Flag to enable thread sanitizer (clang or gcc 4.8)
if(${ARROW_USE_TSAN})
  if(NOT
     (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"
      OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
      OR (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                                   VERSION_GREATER "4.8")))
    message(SEND_ERROR "Cannot use TSAN without clang or gcc >= 4.8")
  endif()

  add_definitions("-fsanitize=thread")

  # Enables dynamic_annotations.h to actually generate code
  add_definitions("-DDYNAMIC_ANNOTATIONS_ENABLED")

  # changes atomicops to use the tsan implementations
  add_definitions("-DTHREAD_SANITIZER")

  # Disables using the precompiled template specializations for std::string, shared_ptr, etc
  # so that the annotations in the header actually take effect.
  add_definitions("-D_GLIBCXX_EXTERN_TEMPLATE=0")

  # Some of the above also need to be passed to the linker.
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pie -fsanitize=thread")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -pie -fsanitize=thread")

  # Strictly speaking, TSAN doesn't require dynamic linking. But it does
  # require all code to be position independent, and the easiest way to
  # guarantee that is via dynamic linking (not all 3rd party archives are
  # compiled with -fPIC e.g. boost).
  if("${ARROW_LINK}" STREQUAL "a")
    message(STATUS "Using dynamic linking for TSAN")
    set(ARROW_LINK "d")
  elseif("${ARROW_LINK}" STREQUAL "s")
    message(SEND_ERROR "Cannot use TSAN with static linking")
  endif()
endif()

if(${ARROW_USE_COVERAGE})
  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                    "Clang")
    add_definitions("-fsanitize-coverage=pc-table,inline-8bit-counters,edge,no-prune,trace-cmp,trace-div,trace-gep"
    )

    set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} -fsanitize-coverage=pc-table,inline-8bit-counters,edge,no-prune,trace-cmp,trace-div,trace-gep"
    )
  else()
    message(SEND_ERROR "You can only enable coverage with clang")
  endif()
endif()

if("${ARROW_USE_UBSAN}"
   OR "${ARROW_USE_ASAN}"
   OR "${ARROW_USE_TSAN}")
  # GCC 4.8 and 4.9 (latest as of this writing) don't allow you to specify
  # disallowed entries for the sanitizer.
  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL
                                                    "Clang")
    set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} -fsanitize-blacklist=${BUILD_SUPPORT_DIR}/sanitizer-disallowed-entries.txt"
    )
  else()
    message(WARNING "GCC does not support specifying a sanitizer disallowed entries list. Known sanitizer check failures will not be suppressed."
    )
  endif()
endif()
