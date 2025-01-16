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
#
# Usage of this module as follows:
#
#  find_package(LLVMAlt)

if(LLVMAlt_FOUND)
  return()
endif()

if(DEFINED LLVM_ROOT)
  # if llvm source is set to conda then prefer conda llvm over system llvm even
  # if the system one is newer
  foreach(ARROW_LLVM_VERSION ${ARROW_LLVM_VERSIONS})
    find_package(LLVM
                 ${ARROW_LLVM_VERSION}
                 CONFIG
                 NO_DEFAULT_PATH
                 HINTS
                 ${LLVM_ROOT})
    if(LLVM_FOUND)
      break()
    endif()
  endforeach()
endif()

if(NOT LLVM_FOUND)
  foreach(ARROW_LLVM_VERSION ${ARROW_LLVM_VERSIONS})
    set(LLVM_HINTS ${LLVM_ROOT} ${LLVM_DIR} /usr/lib /usr/share)

    if(APPLE)
      find_program(BREW brew)
      if(BREW)
        string(REGEX REPLACE "^([0-9]+)(\\..+)?" "\\1" ARROW_LLVM_VERSION_MAJOR
                             "${ARROW_LLVM_VERSION}")
        execute_process(COMMAND ${BREW} --prefix "llvm@${ARROW_LLVM_VERSION_MAJOR}"
                        OUTPUT_VARIABLE LLVM_BREW_PREFIX
                        OUTPUT_STRIP_TRAILING_WHITESPACE)
        list(APPEND LLVM_HINTS ${LLVM_BREW_PREFIX})
      endif()
    endif()

    find_package(LLVM
                 ${ARROW_LLVM_VERSION}
                 CONFIG
                 HINTS
                 ${LLVM_HINTS})

    if(LLVM_FOUND)
      break()
    endif()
  endforeach()
endif()

if(LLVM_FOUND)
  find_program(LLVM_LINK_EXECUTABLE llvm-link HINTS ${LLVM_TOOLS_BINARY_DIR})

  find_program(CLANG_EXECUTABLE
               NAMES clang-${LLVM_PACKAGE_VERSION}
                     clang-${LLVM_VERSION_MAJOR}.${LLVM_VERSION_MINOR}
                     clang-${LLVM_VERSION_MAJOR} clang
               HINTS ${LLVM_TOOLS_BINARY_DIR})

  add_library(LLVM::LLVM_HEADERS INTERFACE IMPORTED)
  set_target_properties(LLVM::LLVM_HEADERS
                        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${LLVM_INCLUDE_DIRS}"
                                   INTERFACE_COMPILE_FLAGS "${LLVM_DEFINITIONS}")

  add_library(LLVM::LLVM_LIBS INTERFACE IMPORTED)
  if(ARROW_LLVM_USE_SHARED)
    target_link_libraries(LLVM::LLVM_LIBS INTERFACE LLVM)
  else()
    # Find the libraries that correspond to the LLVM components
    set(LLVM_TARGET_COMPONENTS
        analysis
        bitreader
        core
        debuginfodwarf
        ipo
        linker
        native
        orcjit
        target)
    if(LLVM_VERSION_MAJOR GREATER_EQUAL 14)
      list(APPEND LLVM_TARGET_COMPONENTS passes)
    endif()
    llvm_map_components_to_libnames(LLVM_LIBS ${LLVM_TARGET_COMPONENTS})
    target_link_libraries(LLVM::LLVM_LIBS INTERFACE ${LLVM_LIBS})

    if(TARGET LLVMSupport AND NOT ARROW_ZSTD_USE_SHARED)
      get_target_property(LLVM_SUPPORT_INTERFACE_LINK_LIBRARIES LLVMSupport
                          INTERFACE_LINK_LIBRARIES)
      list(FIND LLVM_SUPPORT_INTERFACE_LINK_LIBRARIES zstd::libzstd_shared
           LLVM_SUPPORT_LIBZSTD_INDEX)
      if(NOT LLVM_SUPPORT_LIBZSTD_INDEX EQUAL -1)
        list(REMOVE_AT LLVM_SUPPORT_INTERFACE_LINK_LIBRARIES
             ${LLVM_SUPPORT_LIBZSTD_INDEX})
        list(INSERT LLVM_SUPPORT_INTERFACE_LINK_LIBRARIES ${LLVM_SUPPORT_LIBZSTD_INDEX}
             zstd::libzstd_static)
      endif()
      set_target_properties(LLVMSupport
                            PROPERTIES INTERFACE_LINK_LIBRARIES
                                       "${LLVM_SUPPORT_INTERFACE_LINK_LIBRARIES}")
    endif()
  endif()
endif()

mark_as_advanced(CLANG_EXECUTABLE LLVM_LINK_EXECUTABLE)

find_package_handle_standard_args(
  LLVMAlt
  REQUIRED_VARS # The first variable is used for display.
                LLVM_PACKAGE_VERSION CLANG_EXECUTABLE LLVM_FOUND LLVM_LINK_EXECUTABLE)
if(LLVMAlt_FOUND)
  message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
  message(STATUS "Found llvm-link ${LLVM_LINK_EXECUTABLE}")
  message(STATUS "Found clang ${CLANG_EXECUTABLE}")
endif()
