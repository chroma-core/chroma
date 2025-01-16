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

if(GTestAlt_FOUND)
  return()
endif()

set(find_package_args)
if(GTestAlt_FIND_VERSION)
  list(APPEND find_package_args ${GTestAlt_FIND_VERSION})
endif()
if(GTestAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
if(CMAKE_VERSION VERSION_LESS 3.23)
  list(APPEND find_package_args CONFIG)
endif()
# We can't find shred library version of GoogleTest on Windows with
# Conda's gtest package because it doesn't provide GTestConfig.cmake
# provided by GoogleTest and CMake's built-in FindGTtest.cmake
# doesn't support gtest_dll.dll.
find_package(GTest ${find_package_args})

set(GTestAlt_FOUND ${GTest_FOUND})
if(GTestAlt_FOUND AND GTestAlt_NEED_CXX_STANDARD_CHECK)
  set(KEEP_CMAKE_TRY_COMPILE_TARGET_TYPE ${CMAKE_TRY_COMPILE_TARGET_TYPE})
  set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
  set(GTestAlt_CXX_STANDARD_TEST_SOURCE
      "${CMAKE_CURRENT_BINARY_DIR}/gtest_cxx_standard_test.cc")
  file(WRITE ${GTestAlt_CXX_STANDARD_TEST_SOURCE}
       "
#include <string_view>
#include <gtest/gtest.h>

TEST(CXX_STANDARD, MatcherStringView) {
  testing::Matcher matcher(std::string_view(\"hello\"));
}
       ")
  try_compile(GTestAlt_CXX_STANDARD_AVAILABLE ${CMAKE_CURRENT_BINARY_DIR}
              SOURCES ${GTestAlt_CXX_STANDARD_TEST_SOURCE}
              CMAKE_FLAGS "-DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}"
              LINK_LIBRARIES GTest::gtest_main
              OUTPUT_VARIABLE GTestAlt_CXX_STANDARD_OUTPUT)
  set(CMAKE_TRY_COMPILE_TARGET_TYPE ${KEEP_CMAKE_TRY_COMPILE_TARGET_TYPE})
  if(NOT GTestAlt_CXX_STANDARD_AVAILABLE)
    message(STATUS "GTest can't be used with C++${CMAKE_CXX_STANDARD}.")
    message(STATUS "Use -DGTest_SOURCE=BUNDLED.")
    message(STATUS "Output:\n${GTestAlt_CXX_STANDARD_OUTPUT}")
    find_package_handle_standard_args(GTestAlt
                                      REQUIRED_VARS GTestAlt_CXX_STANDARD_AVAILABLE)
  endif()

  target_link_libraries(GTest::gmock INTERFACE GTest::gtest)
  target_link_libraries(GTest::gtest_main INTERFACE GTest::gtest)
endif()
