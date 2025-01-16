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

# Common path suffixes to be searched by find_library or find_path.
# Windows artifacts may be found under "<root>/Library", so
# search there as well.
set(ARROW_LIBRARY_PATH_SUFFIXES
    "${CMAKE_LIBRARY_ARCHITECTURE}"
    "lib/${CMAKE_LIBRARY_ARCHITECTURE}"
    "lib64"
    "lib32"
    "lib"
    "bin"
    "Library"
    "Library/lib"
    "Library/bin")
set(ARROW_INCLUDE_PATH_SUFFIXES "include" "Library" "Library/include")

function(add_thirdparty_lib LIB_NAME LIB_TYPE LIB)
  set(options)
  set(one_value_args)
  set(multi_value_args DEPS INCLUDE_DIRECTORIES)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  add_library(${LIB_NAME} ${LIB_TYPE} IMPORTED)
  if(${LIB_TYPE} STREQUAL "STATIC")
    set_target_properties(${LIB_NAME} PROPERTIES IMPORTED_LOCATION "${LIB}")
    message(STATUS "Added static library dependency ${LIB_NAME}: ${LIB}")
  else()
    if(WIN32)
      # Mark the ".lib" location as part of a Windows DLL
      set_target_properties(${LIB_NAME} PROPERTIES IMPORTED_IMPLIB "${LIB}")
    else()
      set_target_properties(${LIB_NAME} PROPERTIES IMPORTED_LOCATION "${LIB}")
    endif()
    message(STATUS "Added shared library dependency ${LIB_NAME}: ${LIB}")
  endif()
  if(ARG_DEPS)
    set_target_properties(${LIB_NAME} PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
  endif()
  if(ARG_INCLUDE_DIRECTORIES)
    set_target_properties(${LIB_NAME} PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                 "${ARG_INCLUDE_DIRECTORIES}")
  endif()
endfunction()

function(REUSE_PRECOMPILED_HEADER_LIB TARGET_NAME LIB_NAME)
  if(ARROW_USE_PRECOMPILED_HEADERS)
    target_precompile_headers(${TARGET_NAME} REUSE_FROM ${LIB_NAME})
  endif()
endfunction()

# Based on MIT-licensed
# https://gist.github.com/cristianadam/ef920342939a89fae3e8a85ca9459b49
function(arrow_create_merged_static_lib output_target)
  set(options)
  set(one_value_args NAME ROOT)
  set(multi_value_args TO_MERGE)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  file(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
  set(output_lib_path
      ${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_STATIC_LIBRARY_PREFIX}${ARG_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}
  )

  set(all_library_paths $<TARGET_FILE:${ARG_ROOT}>)
  foreach(lib ${ARG_TO_MERGE})
    list(APPEND all_library_paths $<TARGET_FILE:${lib}>)
  endforeach()

  if(APPLE)
    # The apple-distributed libtool is what we want for bundling, but there is
    # a GNU libtool that has a namecollision (and happens to be bundled with R, too).
    # We are not compatible with GNU libtool, so we need to avoid it.

    # check in the obvious places first to find Apple's libtool
    # HINTS is used before system paths and before PATHS, so we use that
    # even though hard coded paths should go in PATHS
    # TODO: use a VALIDATOR when we require cmake >= 3.25
    find_program(LIBTOOL_MACOS libtool HINTS /usr/bin
                                             /Library/Developer/CommandLineTools/usr/bin)

    # confirm that the libtool we found is not GNU libtool
    execute_process(COMMAND ${LIBTOOL_MACOS} -V
                    OUTPUT_VARIABLE LIBTOOL_V_OUTPUT
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT "${LIBTOOL_V_OUTPUT}" MATCHES ".*cctools-([0-9.]+).*")
      message(FATAL_ERROR "libtool found appears to be the incompatible GNU libtool: ${LIBTOOL_MACOS}"
      )
    endif()

    set(BUNDLE_COMMAND ${LIBTOOL_MACOS} "-no_warning_for_no_symbols" "-static" "-o"
                       ${output_lib_path} ${all_library_paths})
  elseif(CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|GNU|Intel|IntelLLVM)$")
    set(ar_script_path ${CMAKE_BINARY_DIR}/${ARG_NAME}.ar)

    file(WRITE ${ar_script_path}.in "CREATE ${output_lib_path}\n")
    file(APPEND ${ar_script_path}.in "ADDLIB $<TARGET_FILE:${ARG_ROOT}>\n")

    foreach(lib ${ARG_TO_MERGE})
      file(APPEND ${ar_script_path}.in "ADDLIB $<TARGET_FILE:${lib}>\n")
    endforeach()

    file(APPEND ${ar_script_path}.in "SAVE\nEND\n")
    file(GENERATE
         OUTPUT ${ar_script_path}
         INPUT ${ar_script_path}.in)
    set(ar_tool ${CMAKE_AR})

    if(CMAKE_INTERPROCEDURAL_OPTIMIZATION)
      set(ar_tool ${CMAKE_CXX_COMPILER_AR})
    endif()

    set(BUNDLE_COMMAND ${ar_tool} -M < ${ar_script_path})

  elseif(MSVC)
    if(CMAKE_LIBTOOL)
      set(BUNDLE_TOOL ${CMAKE_LIBTOOL})
    else()
      find_program(BUNDLE_TOOL lib HINTS "${CMAKE_CXX_COMPILER}/..")
      if(NOT BUNDLE_TOOL)
        message(FATAL_ERROR "Cannot locate lib.exe to bundle libraries")
      endif()
    endif()
    set(BUNDLE_COMMAND ${BUNDLE_TOOL} /NOLOGO /OUT:${output_lib_path}
                       ${all_library_paths})
  else()
    message(FATAL_ERROR "Unknown bundle scenario!")
  endif()

  add_custom_target(${output_target}_merge ALL
                    ${BUNDLE_COMMAND}
                    DEPENDS ${ARG_ROOT} ${ARG_TO_MERGE}
                    BYPRODUCTS ${output_lib_path}
                    COMMENT "Bundling ${output_lib_path}"
                    VERBATIM)

  message(STATUS "Creating bundled static library target ${output_target} at ${output_lib_path}"
  )

  add_library(${output_target} STATIC IMPORTED)
  set_target_properties(${output_target} PROPERTIES IMPORTED_LOCATION ${output_lib_path})
  add_dependencies(${output_target} ${output_target}_merge)
endfunction()

function(arrow_install_cmake_package PACKAGE_NAME EXPORT_NAME)
  set(CONFIG_CMAKE "${PACKAGE_NAME}Config.cmake")
  set(BUILT_CONFIG_CMAKE "${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_CMAKE}")
  configure_package_config_file("${CONFIG_CMAKE}.in" "${BUILT_CONFIG_CMAKE}"
                                INSTALL_DESTINATION "${ARROW_CMAKE_DIR}/${PACKAGE_NAME}")
  set(CONFIG_VERSION_CMAKE "${PACKAGE_NAME}ConfigVersion.cmake")
  set(BUILT_CONFIG_VERSION_CMAKE "${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_VERSION_CMAKE}")
  write_basic_package_version_file("${BUILT_CONFIG_VERSION_CMAKE}"
                                   COMPATIBILITY SameMajorVersion)
  install(FILES "${BUILT_CONFIG_CMAKE}" "${BUILT_CONFIG_VERSION_CMAKE}"
          DESTINATION "${ARROW_CMAKE_DIR}/${PACKAGE_NAME}")
  set(TARGETS_CMAKE "${PACKAGE_NAME}Targets.cmake")
  install(EXPORT ${EXPORT_NAME}
          DESTINATION "${ARROW_CMAKE_DIR}/${PACKAGE_NAME}"
          NAMESPACE "${PACKAGE_NAME}::"
          FILE "${TARGETS_CMAKE}")
endfunction()

# \arg OUTPUTS list to append built targets to
function(ADD_ARROW_LIB LIB_NAME)
  set(options)
  set(one_value_args
      BUILD_SHARED
      BUILD_STATIC
      CMAKE_PACKAGE_NAME
      INSTALL_ARCHIVE_DIR
      INSTALL_LIBRARY_DIR
      INSTALL_RUNTIME_DIR
      PKG_CONFIG_NAME
      PRECOMPILED_HEADER_LIB
      SHARED_LINK_FLAGS)
  set(multi_value_args
      SOURCES
      PRECOMPILED_HEADERS
      OUTPUTS
      STATIC_LINK_LIBS
      SHARED_LINK_LIBS
      SHARED_PRIVATE_LINK_LIBS
      EXTRA_INCLUDES
      PRIVATE_INCLUDES
      DEPENDENCIES
      DEFINITIONS
      SHARED_INSTALL_INTERFACE_LIBS
      STATIC_INSTALL_INTERFACE_LIBS
      OUTPUT_PATH)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS})
  endif()

  # Allow overriding ARROW_BUILD_SHARED and ARROW_BUILD_STATIC
  if(DEFINED ARG_BUILD_SHARED)
    set(BUILD_SHARED ${ARG_BUILD_SHARED})
  else()
    set(BUILD_SHARED ${ARROW_BUILD_SHARED})
  endif()
  if(DEFINED ARG_BUILD_STATIC)
    set(BUILD_STATIC ${ARG_BUILD_STATIC})
  else()
    set(BUILD_STATIC ${ARROW_BUILD_STATIC})
  endif()
  if(ARG_OUTPUT_PATH)
    set(OUTPUT_PATH ${ARG_OUTPUT_PATH})
  else()
    set(OUTPUT_PATH ${BUILD_OUTPUT_ROOT_DIRECTORY})
  endif()

  if(WIN32
     OR CMAKE_GENERATOR STREQUAL Xcode
     OR NOT ARROW_POSITION_INDEPENDENT_CODE)
    # We need to compile C++ separately for each library kind (shared and static)
    # because of dllexport declarations on Windows.
    # The Xcode generator doesn't reliably work with Xcode as target names are not
    # guessed correctly.
    set(USE_OBJLIB OFF)
  else()
    set(USE_OBJLIB ON)
  endif()

  if(USE_OBJLIB)
    # Generate a single "objlib" from all C++ modules and link
    # that "objlib" into each library kind, to avoid compiling twice
    add_library(${LIB_NAME}_objlib OBJECT ${ARG_SOURCES})
    # Necessary to make static linking into other shared libraries work properly
    set_property(TARGET ${LIB_NAME}_objlib PROPERTY POSITION_INDEPENDENT_CODE ON)
    if(ARG_DEPENDENCIES)
      add_dependencies(${LIB_NAME}_objlib ${ARG_DEPENDENCIES})
    endif()
    if(ARG_DEFINITIONS)
      target_compile_definitions(${LIB_NAME}_objlib PRIVATE ${ARG_DEFINITIONS})
    endif()
    if(ARG_PRECOMPILED_HEADER_LIB)
      reuse_precompiled_header_lib(${LIB_NAME}_objlib ${ARG_PRECOMPILED_HEADER_LIB})
    endif()
    if(ARG_PRECOMPILED_HEADERS AND ARROW_USE_PRECOMPILED_HEADERS)
      target_precompile_headers(${LIB_NAME}_objlib PRIVATE ${ARG_PRECOMPILED_HEADERS})
    endif()
    set(LIB_DEPS $<TARGET_OBJECTS:${LIB_NAME}_objlib>)
    set(EXTRA_DEPS)

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_objlib)
    endif()

    if(ARG_EXTRA_INCLUDES)
      target_include_directories(${LIB_NAME}_objlib SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()
    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_objlib PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()
    if(BUILD_SHARED)
      if(ARG_SHARED_LINK_LIBS)
        target_link_libraries(${LIB_NAME}_objlib PRIVATE ${ARG_SHARED_LINK_LIBS})
      endif()
      if(ARG_SHARED_PRIVATE_LINK_LIBS)
        target_link_libraries(${LIB_NAME}_objlib PRIVATE ${ARG_SHARED_PRIVATE_LINK_LIBS})
      endif()
    endif()
    if(BUILD_STATIC AND ARG_STATIC_LINK_LIBS)
      target_link_libraries(${LIB_NAME}_objlib PRIVATE ${ARG_STATIC_LINK_LIBS})
    endif()
  else()
    # Prepare arguments for separate compilation of static and shared libs below
    # TODO: add PCH directives
    set(LIB_DEPS ${ARG_SOURCES})
    set(EXTRA_DEPS ${ARG_DEPENDENCIES})
  endif()

  if(ARG_EXTRA_INCLUDES)
    set(LIB_INCLUDES ${ARG_EXTRA_INCLUDES})
  else()
    set(LIB_INCLUDES "")
  endif()

  if(ARG_INSTALL_ARCHIVE_DIR)
    set(INSTALL_ARCHIVE_DIR ${ARG_INSTALL_ARCHIVE_DIR})
  else()
    set(INSTALL_ARCHIVE_DIR ${CMAKE_INSTALL_LIBDIR})
  endif()
  if(ARG_INSTALL_LIBRARY_DIR)
    set(INSTALL_LIBRARY_DIR ${ARG_INSTALL_LIBRARY_DIR})
  else()
    set(INSTALL_LIBRARY_DIR ${CMAKE_INSTALL_LIBDIR})
  endif()
  if(ARG_INSTALL_RUNTIME_DIR)
    set(INSTALL_RUNTIME_DIR ${ARG_INSTALL_RUNTIME_DIR})
  else()
    set(INSTALL_RUNTIME_DIR bin)
  endif()

  if(BUILD_SHARED)
    add_library(${LIB_NAME}_shared SHARED ${LIB_DEPS})
    if(EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_shared ${EXTRA_DEPS})
    endif()

    if(ARG_DEFINITIONS)
      target_compile_definitions(${LIB_NAME}_shared PRIVATE ${ARG_DEFINITIONS})
    endif()

    if(ARG_PRECOMPILED_HEADER_LIB)
      reuse_precompiled_header_lib(${LIB_NAME}_shared ${ARG_PRECOMPILED_HEADER_LIB})
    endif()

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_shared)
    endif()

    if(LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_shared SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()

    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_shared PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()

    # On iOS, specifying -undefined conflicts with enabling bitcode
    if(APPLE
       AND NOT IOS
       AND NOT DEFINED ENV{EMSCRIPTEN})
      # On OS X, you can avoid linking at library load time and instead
      # expecting that the symbols have been loaded separately. This happens
      # with libpython* where there can be conflicts between system Python and
      # the Python from a thirdparty distribution
      #
      # When running with the Emscripten Compiler, we need not worry about
      # python, and the Emscripten Compiler does not support this option.
      set(ARG_SHARED_LINK_FLAGS "-undefined dynamic_lookup ${ARG_SHARED_LINK_FLAGS}")
    endif()

    set_target_properties(${LIB_NAME}_shared
                          PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${OUTPUT_PATH}"
                                     RUNTIME_OUTPUT_DIRECTORY "${OUTPUT_PATH}"
                                     PDB_OUTPUT_DIRECTORY "${OUTPUT_PATH}"
                                     LINK_FLAGS "${ARG_SHARED_LINK_FLAGS}"
                                     OUTPUT_NAME ${LIB_NAME}
                                     VERSION "${ARROW_FULL_SO_VERSION}"
                                     SOVERSION "${ARROW_SO_VERSION}")

    target_link_libraries(${LIB_NAME}_shared
                          PUBLIC "$<BUILD_INTERFACE:${ARG_SHARED_LINK_LIBS}>"
                                 "$<INSTALL_INTERFACE:${ARG_SHARED_INSTALL_INTERFACE_LIBS}>"
                          PRIVATE ${ARG_SHARED_PRIVATE_LINK_LIBS})

    if(USE_OBJLIB)
      # Ensure that dependencies are built before compilation of objects in
      # object library, rather than only before the final link step
      foreach(SHARED_LINK_LIB ${ARG_SHARED_LINK_LIBS})
        if(TARGET ${SHARED_LINK_LIB})
          add_dependencies(${LIB_NAME}_objlib ${SHARED_LINK_LIB})
        endif()
      endforeach()
    endif()

    if(ARROW_RPATH_ORIGIN)
      if(APPLE)
        set(_lib_install_rpath "@loader_path")
      else()
        set(_lib_install_rpath "\$ORIGIN")
      endif()
      set_target_properties(${LIB_NAME}_shared PROPERTIES INSTALL_RPATH
                                                          ${_lib_install_rpath})
    endif()

    if(APPLE)
      if(ARROW_INSTALL_NAME_RPATH)
        set(_lib_install_name "@rpath")
      else()
        set(_lib_install_name "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
      endif()
      set_target_properties(${LIB_NAME}_shared
                            PROPERTIES BUILD_WITH_INSTALL_RPATH ON INSTALL_NAME_DIR
                                                                   "${_lib_install_name}")
    endif()

    install(TARGETS ${LIB_NAME}_shared ${INSTALL_IS_OPTIONAL}
            EXPORT ${LIB_NAME}_targets
            ARCHIVE DESTINATION ${INSTALL_ARCHIVE_DIR}
            LIBRARY DESTINATION ${INSTALL_LIBRARY_DIR}
            RUNTIME DESTINATION ${INSTALL_RUNTIME_DIR}
            INCLUDES
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  endif()

  if(BUILD_STATIC)
    add_library(${LIB_NAME}_static STATIC ${LIB_DEPS})
    if(EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_static ${EXTRA_DEPS})
    endif()

    if(ARG_DEFINITIONS)
      target_compile_definitions(${LIB_NAME}_static PRIVATE ${ARG_DEFINITIONS})
    endif()

    if(ARG_PRECOMPILED_HEADER_LIB)
      reuse_precompiled_header_lib(${LIB_NAME}_static ${ARG_PRECOMPILED_HEADER_LIB})
    endif()

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_static)
    endif()

    if(LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_static SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()

    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_static PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()

    if(MSVC_TOOLCHAIN)
      set(LIB_NAME_STATIC ${LIB_NAME}_static)
    else()
      set(LIB_NAME_STATIC ${LIB_NAME})
    endif()

    if(WIN32)
      target_compile_definitions(${LIB_NAME}_static PUBLIC ARROW_STATIC)
      target_compile_definitions(${LIB_NAME}_static PUBLIC ARROW_FLIGHT_STATIC)
      target_compile_definitions(${LIB_NAME}_static PUBLIC ARROW_FLIGHT_SQL_STATIC)
    endif()

    set_target_properties(${LIB_NAME}_static
                          PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${OUTPUT_PATH}"
                                     OUTPUT_NAME ${LIB_NAME_STATIC})

    if(ARG_STATIC_INSTALL_INTERFACE_LIBS)
      target_link_libraries(${LIB_NAME}_static
                            INTERFACE "$<INSTALL_INTERFACE:${ARG_STATIC_INSTALL_INTERFACE_LIBS}>"
      )
    endif()

    if(ARG_STATIC_LINK_LIBS)
      target_link_libraries(${LIB_NAME}_static
                            PUBLIC "$<BUILD_INTERFACE:${ARG_STATIC_LINK_LIBS}>")
      if(USE_OBJLIB)
        # Ensure that dependencies are built before compilation of objects in
        # object library, rather than only before the final link step
        foreach(STATIC_LINK_LIB ${ARG_STATIC_LINK_LIBS})
          if(TARGET ${STATIC_LINK_LIB})
            add_dependencies(${LIB_NAME}_objlib ${STATIC_LINK_LIB})
          endif()
        endforeach()
      endif()
    endif()

    install(TARGETS ${LIB_NAME}_static ${INSTALL_IS_OPTIONAL}
            EXPORT ${LIB_NAME}_targets
            ARCHIVE DESTINATION ${INSTALL_ARCHIVE_DIR}
            LIBRARY DESTINATION ${INSTALL_LIBRARY_DIR}
            RUNTIME DESTINATION ${INSTALL_RUNTIME_DIR}
            INCLUDES
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  endif()

  if(ARG_CMAKE_PACKAGE_NAME)
    arrow_install_cmake_package(${ARG_CMAKE_PACKAGE_NAME} ${LIB_NAME}_targets)
  endif()

  if(ARG_PKG_CONFIG_NAME)
    arrow_add_pkg_config("${ARG_PKG_CONFIG_NAME}")
  endif()

  # Modify variable in calling scope
  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS}
        ${${ARG_OUTPUTS}}
        PARENT_SCOPE)
  endif()
endfunction()

#
# Benchmarking
#
# Add a new micro benchmark, with or without an executable that should be built.
# If benchmarks are enabled then they will be run along side unit tests with ctest.
# 'make benchmark' and 'make unittest' to build/run only benchmark or unittests,
# respectively.
#
# REL_BENCHMARK_NAME is the name of the benchmark app. It may be a single component
# (e.g. monotime-benchmark) or contain additional components (e.g.
# net/net_util-benchmark). Either way, the last component must be a globally
# unique name.

# The benchmark will registered as unit test with ctest with a label
# of 'benchmark'.
#
# Arguments after the test name will be passed to set_tests_properties().
#
# \arg PREFIX a string to append to the name of the benchmark executable. For
# example, if you have src/arrow/foo/bar-benchmark.cc, then PREFIX "foo" will
# create test executable foo-bar-benchmark
# \arg LABELS the benchmark label or labels to assign the unit tests to. By
# default, benchmarks will go in the "benchmark" group. Custom targets for the
# group names must exist
function(ADD_BENCHMARK REL_BENCHMARK_NAME)
  set(options)
  set(one_value_args)
  set(multi_value_args
      EXTRA_LINK_LIBS
      STATIC_LINK_LIBS
      DEPENDENCIES
      SOURCES
      PREFIX
      LABELS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(NO_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCHMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  if(ARG_PREFIX)
    set(BENCHMARK_NAME "${ARG_PREFIX}-${BENCHMARK_NAME}")
  endif()

  if(ARG_SOURCES)
    set(SOURCES ${ARG_SOURCES})
  else()
    set(SOURCES "${REL_BENCHMARK_NAME}.cc")
  endif()

  # Make sure the executable name contains only hyphens, not underscores
  string(REPLACE "_" "-" BENCHMARK_NAME ${BENCHMARK_NAME})

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME}.cc)
    # This benchmark has a corresponding .cc file, set it up as an executable.
    set(BENCHMARK_PATH "${EXECUTABLE_OUTPUT_PATH}/${BENCHMARK_NAME}")
    add_executable(${BENCHMARK_NAME} ${SOURCES})

    if(ARG_STATIC_LINK_LIBS)
      # Customize link libraries
      target_link_libraries(${BENCHMARK_NAME} PRIVATE ${ARG_STATIC_LINK_LIBS})
    else()
      target_link_libraries(${BENCHMARK_NAME} PRIVATE ${ARROW_BENCHMARK_LINK_LIBS})
    endif()
    add_dependencies(benchmark ${BENCHMARK_NAME})

    if(ARG_EXTRA_LINK_LIBS)
      target_link_libraries(${BENCHMARK_NAME} PRIVATE ${ARG_EXTRA_LINK_LIBS})
    endif()
  else()
    # No executable, just invoke the benchmark (probably a script) directly.
    set(BENCHMARK_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME})
  endif()

  # With OSX and conda, we need to set the correct RPATH so that dependencies
  # are found. The installed libraries with conda have an RPATH that matches
  # for executables and libraries lying in $ENV{CONDA_PREFIX}/bin or
  # $ENV{CONDA_PREFIX}/lib but our test libraries and executables are not
  # installed there.
  if(NOT "$ENV{CONDA_PREFIX}" STREQUAL "" AND APPLE)
    set_target_properties(${BENCHMARK_NAME}
                          PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE
                                     INSTALL_RPATH_USE_LINK_PATH TRUE
                                     INSTALL_RPATH
                                     "$ENV{CONDA_PREFIX}/lib;${EXECUTABLE_OUTPUT_PATH}")
  endif()

  # Add test as dependency of relevant label targets
  add_dependencies(all-benchmarks ${BENCHMARK_NAME})
  foreach(TARGET ${ARG_LABELS})
    add_dependencies(${TARGET} ${BENCHMARK_NAME})
  endforeach()

  if(ARG_DEPENDENCIES)
    add_dependencies(${BENCHMARK_NAME} ${ARG_DEPENDENCIES})
  endif()

  if(ARG_LABELS)
    set(ARG_LABELS "benchmark;${ARG_LABELS}")
  else()
    set(ARG_LABELS benchmark)
  endif()

  if(ARROW_BUILD_DETAILED_BENCHMARKS)
    target_compile_definitions(${BENCHMARK_NAME} PRIVATE ARROW_BUILD_DETAILED_BENCHMARKS)
  endif()

  add_test(${BENCHMARK_NAME}
           ${BUILD_SUPPORT_DIR}/run-test.sh
           ${CMAKE_BINARY_DIR}
           benchmark
           ${BENCHMARK_PATH})

  set_property(TEST ${BENCHMARK_NAME}
               APPEND
               PROPERTY LABELS ${ARG_LABELS})
endfunction()

#
# Testing
#
# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# If given, SOURCES is the list of C++ source files to compile into the test
# executable.  Otherwise, "REL_TEST_NAME.cc" is used.
#
# The unit test is added with a label of "unittest" to support filtering with
# ctest.
#
# Arguments after the test name will be passed to set_tests_properties().
#
# \arg ENABLED if passed, add this unit test even if ARROW_BUILD_TESTS is off
# \arg PREFIX a string to append to the name of the test executable. For
# example, if you have src/arrow/foo/bar-test.cc, then PREFIX "foo" will create
# test executable foo-bar-test
# \arg LABELS the unit test label or labels to assign the unit tests
# to. By default, unit tests will go in the "unittest" group, but if we have
# multiple unit tests in some subgroup, you can assign a test to multiple
# groups use the syntax unittest;GROUP2;GROUP3. Custom targets for the group
# names must exist
function(ADD_TEST_CASE REL_TEST_NAME)
  set(options NO_VALGRIND ENABLED)
  set(one_value_args PRECOMPILED_HEADER_LIB)
  set(multi_value_args
      SOURCES
      PRECOMPILED_HEADERS
      STATIC_LINK_LIBS
      EXTRA_LINK_LIBS
      EXTRA_INCLUDES
      EXTRA_DEPENDENCIES
      LABELS
      EXTRA_LABELS
      TEST_ARGUMENTS
      PREFIX
      DEFINITIONS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(NO_TESTS AND NOT ARG_ENABLED)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(ARG_PREFIX)
    set(TEST_NAME "${ARG_PREFIX}-${TEST_NAME}")
  endif()

  if(ARG_SOURCES)
    set(SOURCES ${ARG_SOURCES})
  else()
    set(SOURCES "${REL_TEST_NAME}.cc")
  endif()

  # Make sure the executable name contains only hyphens, not underscores
  string(REPLACE "_" "-" TEST_NAME ${TEST_NAME})

  set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
  add_executable(${TEST_NAME} ${SOURCES})

  # With OSX and conda, we need to set the correct RPATH so that dependencies
  # are found. The installed libraries with conda have an RPATH that matches
  # for executables and libraries lying in $ENV{CONDA_PREFIX}/bin or
  # $ENV{CONDA_PREFIX}/lib but our test libraries and executables are not
  # installed there.
  if(NOT "$ENV{CONDA_PREFIX}" STREQUAL "" AND APPLE)
    set_target_properties(${TEST_NAME}
                          PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE
                                     INSTALL_RPATH_USE_LINK_PATH TRUE
                                     INSTALL_RPATH
                                     "${EXECUTABLE_OUTPUT_PATH};$ENV{CONDA_PREFIX}/lib")
  endif()

  if(ARG_STATIC_LINK_LIBS)
    # Customize link libraries
    target_link_libraries(${TEST_NAME} PRIVATE ${ARG_STATIC_LINK_LIBS})
  else()
    target_link_libraries(${TEST_NAME} PRIVATE ${ARROW_TEST_LINK_LIBS})
  endif()

  if(ARG_PRECOMPILED_HEADER_LIB)
    reuse_precompiled_header_lib(${TEST_NAME} ${ARG_PRECOMPILED_HEADER_LIB})
  endif()

  if(ARG_PRECOMPILED_HEADERS AND ARROW_USE_PRECOMPILED_HEADERS)
    target_precompile_headers(${TEST_NAME} PRIVATE ${ARG_PRECOMPILED_HEADERS})
  endif()

  if(ARG_EXTRA_LINK_LIBS)
    target_link_libraries(${TEST_NAME} PRIVATE ${ARG_EXTRA_LINK_LIBS})
  endif()

  if(ARG_EXTRA_INCLUDES)
    target_include_directories(${TEST_NAME} SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
  endif()

  if(ARG_EXTRA_DEPENDENCIES)
    add_dependencies(${TEST_NAME} ${ARG_EXTRA_DEPENDENCIES})
  endif()

  if(ARG_DEFINITIONS)
    target_compile_definitions(${TEST_NAME} PRIVATE ${ARG_DEFINITIONS})
  endif()

  if(ARROW_TEST_MEMCHECK AND NOT ARG_NO_VALGRIND)
    add_test(${TEST_NAME}
             bash
             -c
             "cd '${CMAKE_SOURCE_DIR}'; \
               valgrind --suppressions=valgrind.supp --tool=memcheck --gen-suppressions=all \
                 --num-callers=500 --leak-check=full --leak-check-heuristics=stdstring \
                 --error-exitcode=1 ${TEST_PATH} ${ARG_TEST_ARGUMENTS}")
  elseif(WIN32 OR CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME} ${ARG_TEST_ARGUMENTS})
  else()
    add_test(${TEST_NAME}
             ${BUILD_SUPPORT_DIR}/run-test.sh
             ${CMAKE_BINARY_DIR}
             test
             ${TEST_PATH}
             ${ARG_TEST_ARGUMENTS})
  endif()

  # Add test as dependency of relevant targets
  add_dependencies(all-tests ${TEST_NAME})
  foreach(TARGET ${ARG_LABELS})
    add_dependencies(${TARGET} ${TEST_NAME})
  endforeach()

  set(LABELS)
  list(APPEND LABELS "unittest")
  if(ARG_LABELS)
    list(APPEND LABELS ${ARG_LABELS})
  endif()
  # EXTRA_LABELS don't create their own dependencies, they are only used
  # to ease running certain test categories.
  if(ARG_EXTRA_LABELS)
    list(APPEND LABELS ${ARG_EXTRA_LABELS})
  endif()

  foreach(LABEL ${ARG_LABELS})
    # ensure there is a cmake target which exercises tests with this LABEL
    set(LABEL_TEST_NAME "test-${LABEL}")
    if(NOT TARGET ${LABEL_TEST_NAME})
      add_custom_target(${LABEL_TEST_NAME}
                        ctest -L "${LABEL}" --output-on-failure
                        USES_TERMINAL)
    endif()
    # ensure the test is (re)built before the LABEL test runs
    add_dependencies(${LABEL_TEST_NAME} ${TEST_NAME})
  endforeach()

  set_property(TEST ${TEST_NAME}
               APPEND
               PROPERTY LABELS ${LABELS})
endfunction()

#
# Examples
#
# Add a new example, with or without an executable that should be built.
# If examples are enabled then they will be run along side unit tests with ctest.
# 'make runexample' to build/run only examples.
#
# REL_EXAMPLE_NAME is the name of the example app. It may be a single component
# (e.g. monotime-example) or contain additional components (e.g.
# net/net_util-example). Either way, the last component must be a globally
# unique name.

# The example will registered as unit test with ctest with a label
# of 'example'.
#
# Arguments after the test name will be passed to set_tests_properties().
#
# \arg PREFIX a string to append to the name of the example executable. For
# example, if you have src/arrow/foo/bar-example.cc, then PREFIX "foo" will
# create test executable foo-bar-example
function(ADD_ARROW_EXAMPLE REL_EXAMPLE_NAME)
  set(options)
  set(one_value_args)
  set(multi_value_args
      EXTRA_INCLUDES
      EXTRA_LINK_LIBS
      EXTRA_SOURCES
      DEPENDENCIES
      PREFIX)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(NO_EXAMPLES)
    return()
  endif()
  get_filename_component(EXAMPLE_NAME ${REL_EXAMPLE_NAME} NAME_WE)

  if(ARG_PREFIX)
    set(EXAMPLE_NAME "${ARG_PREFIX}-${EXAMPLE_NAME}")
  endif()

  # Make sure the executable name contains only hyphens, not underscores
  string(REPLACE "_" "-" EXAMPLE_NAME ${EXAMPLE_NAME})

  if(EXISTS ${CMAKE_SOURCE_DIR}/examples/arrow/${REL_EXAMPLE_NAME}.cc)
    # This example has a corresponding .cc file, set it up as an executable.
    set(EXAMPLE_PATH "${EXECUTABLE_OUTPUT_PATH}/${EXAMPLE_NAME}")
    add_executable(${EXAMPLE_NAME} "${REL_EXAMPLE_NAME}.cc" ${ARG_EXTRA_SOURCES})
    target_link_libraries(${EXAMPLE_NAME} ${ARROW_EXAMPLE_LINK_LIBS})
    add_dependencies(runexample ${EXAMPLE_NAME})

    if(ARG_EXTRA_LINK_LIBS)
      target_link_libraries(${EXAMPLE_NAME} ${ARG_EXTRA_LINK_LIBS})
    endif()
  endif()

  if(ARG_DEPENDENCIES)
    add_dependencies(${EXAMPLE_NAME} ${ARG_DEPENDENCIES})
  endif()

  if(ARG_EXTRA_INCLUDES)
    target_include_directories(${EXAMPLE_NAME} SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
  endif()

  add_test(${EXAMPLE_NAME} ${EXAMPLE_PATH})
  set_tests_properties(${EXAMPLE_NAME} PROPERTIES LABELS "example")
endfunction()

#
# Fuzzing
#
# Add new fuzz target executable.
#
# The single source file must define a function:
#   extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size)
#
# No main function must be present within the source file!
#
function(ADD_FUZZ_TARGET REL_FUZZING_NAME)
  set(options)
  set(one_value_args PREFIX)
  set(multi_value_args LINK_LIBS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(NOT ARROW_FUZZING)
    return()
  endif()

  get_filename_component(FUZZING_NAME ${REL_FUZZING_NAME} NAME_WE)

  # Make sure the executable name contains only hyphens, not underscores
  string(REPLACE "_" "-" FUZZING_NAME ${FUZZING_NAME})

  if(ARG_PREFIX)
    set(FUZZING_NAME "${ARG_PREFIX}-${FUZZING_NAME}")
  endif()

  # For OSS-Fuzz
  # (https://google.github.io/oss-fuzz/advanced-topics/ideal-integration/)
  if(DEFINED ENV{LIB_FUZZING_ENGINE})
    set(FUZZ_LDFLAGS $ENV{LIB_FUZZING_ENGINE})
  else()
    set(FUZZ_LDFLAGS "-fsanitize=fuzzer")
  endif()

  add_executable(${FUZZING_NAME} "${REL_FUZZING_NAME}.cc")
  target_link_libraries(${FUZZING_NAME} ${LINK_LIBS})
  target_compile_options(${FUZZING_NAME} PRIVATE ${FUZZ_LDFLAGS})
  set_target_properties(${FUZZING_NAME} PROPERTIES LINK_FLAGS ${FUZZ_LDFLAGS} LABELS
                                                                              "fuzzing")
endfunction()

function(ARROW_INSTALL_ALL_HEADERS PATH)
  set(options)
  set(one_value_args)
  set(multi_value_args PATTERN)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(NOT ARG_PATTERN)
    # The .hpp extension is used by some vendored libraries
    set(ARG_PATTERN "*.h" "*.hpp")
  endif()
  file(GLOB CURRENT_DIRECTORY_HEADERS ${ARG_PATTERN})

  set(PUBLIC_HEADERS)
  foreach(HEADER ${CURRENT_DIRECTORY_HEADERS})
    get_filename_component(HEADER_BASENAME ${HEADER} NAME)
    if(HEADER_BASENAME MATCHES "internal")
      continue()
    endif()
    list(APPEND PUBLIC_HEADERS ${HEADER})
  endforeach()
  install(FILES ${PUBLIC_HEADERS} DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${PATH}")
endfunction()

function(ARROW_ADD_PKG_CONFIG MODULE)
  configure_file(${MODULE}.pc.in "${CMAKE_CURRENT_BINARY_DIR}/${MODULE}.pc.generate.in"
                 @ONLY)
  file(GENERATE
       OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/${MODULE}.pc"
       INPUT "${CMAKE_CURRENT_BINARY_DIR}/${MODULE}.pc.generate.in")
  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/${MODULE}.pc"
          DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")
endfunction()

# Implementations of lisp "car" and "cdr" functions
macro(ARROW_CAR var)
  set(${var} ${ARGV1})
endmacro()

macro(ARROW_CDR var rest)
  set(${var} ${ARGN})
endmacro()
