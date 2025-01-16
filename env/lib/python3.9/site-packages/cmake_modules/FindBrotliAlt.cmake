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
# Tries to find Brotli headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(BrotliAlt)

if(BrotliAlt_FOUND)
  return()
endif()

if(ARROW_PACKAGE_KIND STREQUAL "vcpkg" OR ARROW_PACKAGE_KIND STREQUAL "conan")
  set(find_package_args "")
  if(BrotliAlt_FIND_VERSION)
    list(APPEND find_package_args ${BrotliAlt_FIND_VERSION})
  endif()
  if(BrotliAlt_FIND_QUIETLY)
    list(APPEND find_package_args QUIET)
  endif()
  if(BrotliAlt_FIND_REQUIRED)
    list(APPEND find_package_args REQUIRED)
  endif()
  if(ARROW_PACKAGE_KIND STREQUAL "vcpkg")
    find_package(BrotliAlt NAMES unofficial-brotli ${find_package_args})
  else()
    find_package(BrotliAlt NAMES brotli ${find_package_args})
  endif()
  set(Brotli_FOUND ${BrotliAlt_FOUND})
  if(BrotliAlt_FOUND)
    if(ARROW_PACKAGE_KIND STREQUAL "vcpkg")
      add_library(Brotli::brotlicommon ALIAS unofficial::brotli::brotlicommon)
      add_library(Brotli::brotlienc ALIAS unofficial::brotli::brotlienc)
      add_library(Brotli::brotlidec ALIAS unofficial::brotli::brotlidec)
    else()
      add_library(Brotli::brotlicommon ALIAS brotli::brotlicommon)
      add_library(Brotli::brotlienc ALIAS brotli::brotlienc)
      add_library(Brotli::brotlidec ALIAS brotli::brotlidec)
    endif()
    return()
  endif()
endif()

if(ARROW_BROTLI_USE_SHARED)
  set(BROTLI_COMMON_LIB_NAMES
      brotlicommon
      ${CMAKE_SHARED_LIBRARY_PREFIX}brotlicommon${CMAKE_SHARED_LIBRARY_SUFFIX})

  set(BROTLI_ENC_LIB_NAMES
      brotlienc ${CMAKE_SHARED_LIBRARY_PREFIX}brotlienc${CMAKE_SHARED_LIBRARY_SUFFIX})

  set(BROTLI_DEC_LIB_NAMES
      brotlidec ${CMAKE_SHARED_LIBRARY_PREFIX}brotlidec${CMAKE_SHARED_LIBRARY_SUFFIX})
else()
  set(BROTLI_COMMON_LIB_NAMES
      brotlicommon-static
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon-static${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon_static${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon${CMAKE_STATIC_LIBRARY_SUFFIX})

  set(BROTLI_ENC_LIB_NAMES
      brotlienc-static
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc-static${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc_static${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc${CMAKE_STATIC_LIBRARY_SUFFIX})

  set(BROTLI_DEC_LIB_NAMES
      brotlidec-static
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec-static${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec_static${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

if(BROTLI_ROOT)
  find_library(BROTLI_COMMON_LIBRARY
               NAMES ${BROTLI_COMMON_LIB_NAMES}
               PATHS ${BROTLI_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_library(BROTLI_ENC_LIBRARY
               NAMES ${BROTLI_ENC_LIB_NAMES}
               PATHS ${BROTLI_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_library(BROTLI_DEC_LIBRARY
               NAMES ${BROTLI_DEC_LIB_NAMES}
               PATHS ${BROTLI_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(BROTLI_INCLUDE_DIR
            NAMES brotli/decode.h
            PATHS ${BROTLI_ROOT}
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES}
            NO_DEFAULT_PATH)
else()
  find_package(PkgConfig QUIET)
  pkg_check_modules(BROTLI_PC libbrotlicommon libbrotlienc libbrotlidec)
  if(BROTLI_PC_FOUND)
    set(BROTLI_INCLUDE_DIR "${BROTLI_PC_libbrotlicommon_INCLUDEDIR}")

    # Some systems (e.g. Fedora) don't fill Brotli_LIBRARY_DIRS, so add the other dirs here.
    list(APPEND BROTLI_PC_LIBRARY_DIRS "${BROTLI_PC_libbrotlicommon_LIBDIR}")
    list(APPEND BROTLI_PC_LIBRARY_DIRS "${BROTLI_PC_libbrotlienc_LIBDIR}")
    list(APPEND BROTLI_PC_LIBRARY_DIRS "${BROTLI_PC_libbrotlidec_LIBDIR}")

    find_library(BROTLI_COMMON_LIBRARY
                 NAMES ${BROTLI_COMMON_LIB_NAMES}
                 PATHS ${BROTLI_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(BROTLI_ENC_LIBRARY
                 NAMES ${BROTLI_ENC_LIB_NAMES}
                 PATHS ${BROTLI_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(BROTLI_DEC_LIBRARY
                 NAMES ${BROTLI_DEC_LIB_NAMES}
                 PATHS ${BROTLI_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
  else()
    find_library(BROTLI_COMMON_LIBRARY
                 NAMES ${BROTLI_COMMON_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_library(BROTLI_ENC_LIBRARY
                 NAMES ${BROTLI_ENC_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_library(BROTLI_DEC_LIBRARY
                 NAMES ${BROTLI_DEC_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_path(BROTLI_INCLUDE_DIR
              NAMES brotli/decode.h
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
endif()

find_package_handle_standard_args(
  BrotliAlt REQUIRED_VARS BROTLI_COMMON_LIBRARY BROTLI_ENC_LIBRARY BROTLI_DEC_LIBRARY
                          BROTLI_INCLUDE_DIR)
set(Brotli_FOUND ${BrotliAlt_FOUND})
if(BrotliAlt_FOUND)
  add_library(Brotli::brotlicommon UNKNOWN IMPORTED)
  set_target_properties(Brotli::brotlicommon
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_COMMON_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
  add_library(Brotli::brotlienc UNKNOWN IMPORTED)
  set_target_properties(Brotli::brotlienc
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_ENC_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
  add_library(Brotli::brotlidec UNKNOWN IMPORTED)
  set_target_properties(Brotli::brotlidec
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_DEC_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
endif()
