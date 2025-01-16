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
#
# Tries to find the clang-tidy and clang-format modules
#
# Usage of this module as follows:
#
#  find_package(ClangTools)
#
# Variables used by this module which can change the default behaviour and need
# to be set before calling find_package:
#
#  CLANG_FORMAT_VERSION -
#   The version of clang-format to find. If this is not specified, clang-format
#   will not be searched for.
#
#  ClangTools_PATH -
#   When set, this path is inspected in addition to standard library binary locations
#   to find clang-tidy and clang-format
#
# This module defines
#  CLANG_TIDY_BIN, The  path to the clang tidy binary
#  CLANG_TIDY_FOUND, Whether clang tidy was found
#  CLANG_FORMAT_BIN, The path to the clang format binary
#  CLANG_FORMAT_FOUND, Whether clang format was found

set(CLANG_TOOLS_SEARCH_PATHS
    ${ClangTools_PATH}
    $ENV{CLANG_TOOLS_PATH}
    /usr/local/bin
    /usr/bin
    "C:/Program Files/LLVM/bin" # Windows, non-conda
    "$ENV{CONDA_PREFIX}/Library/bin" # Windows, conda
    "$ENV{CONDA_PREFIX}/bin") # Unix, conda
if(APPLE)
  find_program(BREW brew)
  if(BREW)
    execute_process(COMMAND ${BREW} --prefix "llvm@${ARROW_CLANG_TOOLS_VERSION_MAJOR}"
                    OUTPUT_VARIABLE CLANG_TOOLS_BREW_PREFIX
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT CLANG_TOOLS_BREW_PREFIX)
      execute_process(COMMAND ${BREW} --prefix llvm
                      OUTPUT_VARIABLE CLANG_TOOLS_BREW_PREFIX
                      OUTPUT_STRIP_TRAILING_WHITESPACE)
    endif()
    if(CLANG_TOOLS_BREW_PREFIX)
      list(APPEND CLANG_TOOLS_SEARCH_PATHS "${CLANG_TOOLS_BREW_PREFIX}/bin")
    endif()
  endif()
endif()

function(FIND_CLANG_TOOL NAME OUTPUT VERSION_CHECK_PATTERN)
  unset(CLANG_TOOL_BIN CACHE)
  find_program(CLANG_TOOL_BIN
               NAMES ${NAME}-${ARROW_CLANG_TOOLS_VERSION}
                     ${NAME}-${ARROW_CLANG_TOOLS_VERSION_MAJOR}
               PATHS ${CLANG_TOOLS_SEARCH_PATHS}
               NO_DEFAULT_PATH)
  if(NOT CLANG_TOOL_BIN)
    # try searching for non-versioned tool and check the version
    find_program(CLANG_TOOL_BIN
                 NAMES ${NAME}
                 PATHS ${CLANG_TOOLS_SEARCH_PATHS}
                 NO_DEFAULT_PATH)
    if(CLANG_TOOL_BIN)
      unset(CLANG_TOOL_VERSION_MESSAGE)
      execute_process(COMMAND ${CLANG_TOOL_BIN} "-version"
                      OUTPUT_VARIABLE CLANG_TOOL_VERSION_MESSAGE
                      OUTPUT_STRIP_TRAILING_WHITESPACE)
      if(NOT (${CLANG_TOOL_VERSION_MESSAGE} MATCHES ${VERSION_CHECK_PATTERN}))
        message(STATUS "${NAME} found, but version did not match \"${VERSION_CHECK_PATTERN}\""
        )
        set(CLANG_TOOL_BIN "CLANG_TOOL_BIN-NOTFOUND")
      endif()
    endif()
  endif()
  if(CLANG_TOOL_BIN)
    set(${OUTPUT}
        ${CLANG_TOOL_BIN}
        PARENT_SCOPE)
  else()
    set(${OUTPUT}
        "${OUTPUT}-NOTFOUND"
        PARENT_SCOPE)
  endif()
endfunction()

string(REGEX REPLACE "\\." "\\\\." ARROW_CLANG_TOOLS_VERSION_ESCAPED
                     "${ARROW_CLANG_TOOLS_VERSION}")

find_clang_tool(clang-tidy CLANG_TIDY_BIN
                "LLVM version ${ARROW_CLANG_TOOLS_VERSION_ESCAPED}")
if(CLANG_TIDY_BIN)
  set(CLANG_TIDY_FOUND 1)
  message(STATUS "clang-tidy found at ${CLANG_TIDY_BIN}")
else()
  set(CLANG_TIDY_FOUND 0)
  message(STATUS "clang-tidy ${ARROW_CLANG_TOOLS_VERSION} not found")
endif()

find_clang_tool(clang-format CLANG_FORMAT_BIN
                "clang-format version ${ARROW_CLANG_TOOLS_VERSION_ESCAPED}")
if(CLANG_FORMAT_BIN)
  set(CLANG_FORMAT_FOUND 1)
  message(STATUS "clang-format found at ${CLANG_FORMAT_BIN}")
else()
  set(CLANG_FORMAT_FOUND 0)
  message(STATUS "clang-format ${ARROW_CLANG_TOOLS_VERSION} not found")
endif()

find_package_handle_standard_args(ClangTools REQUIRED_VARS CLANG_FORMAT_BIN
                                                           CLANG_TIDY_BIN)
