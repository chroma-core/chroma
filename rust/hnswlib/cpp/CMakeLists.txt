cmake_minimum_required (VERSION 2.6)
project(hnsw_lib
    LANGUAGES CXX)

add_library(hnswlib INTERFACE)
target_include_directories(hnswlib INTERFACE .)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(CMAKE_CXX_STANDARD 11)

    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      SET( CMAKE_CXX_FLAGS "-Ofast -DNDEBUG -std=c++11 -DHAVE_CXX0X -openmp -march=native -fpic -ftree-vectorize")
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      SET( CMAKE_CXX_FLAGS  "-Ofast -lrt -DNDEBUG -std=c++11 -DHAVE_CXX0X -march=native -fpic -w -fopenmp -ftree-vectorize -ftree-vectorizer-verbose=0" )
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
      SET( CMAKE_CXX_FLAGS  "-Ofast -lrt -DNDEBUG -std=c++11 -DHAVE_CXX0X -openmp -march=native -fpic -w -fopenmp -ftree-vectorize" )
    endif()

    # examples
    add_executable(example_search examples/cpp/example_search.cpp)
    target_link_libraries(example_search hnswlib)

    add_executable(example_filter examples/cpp/example_filter.cpp)
    target_link_libraries(example_filter hnswlib)

    add_executable(example_replace_deleted examples/cpp/example_replace_deleted.cpp)
    target_link_libraries(example_replace_deleted hnswlib)

    add_executable(example_mt_search examples/cpp/example_mt_search.cpp)
    target_link_libraries(example_mt_search hnswlib)

    add_executable(example_mt_filter examples/cpp/example_mt_filter.cpp)
    target_link_libraries(example_mt_filter hnswlib)

    add_executable(example_mt_replace_deleted examples/cpp/example_mt_replace_deleted.cpp)
    target_link_libraries(example_mt_replace_deleted hnswlib)

    # tests
    add_executable(test_updates tests/cpp/updates_test.cpp)
    target_link_libraries(test_updates hnswlib)

    add_executable(searchKnnCloserFirst_test tests/cpp/searchKnnCloserFirst_test.cpp)
    target_link_libraries(searchKnnCloserFirst_test hnswlib)

    add_executable(searchKnnWithFilter_test tests/cpp/searchKnnWithFilter_test.cpp)
    target_link_libraries(searchKnnWithFilter_test hnswlib)

    add_executable(multiThreadLoad_test tests/cpp/multiThreadLoad_test.cpp)
    target_link_libraries(multiThreadLoad_test hnswlib)

    add_executable(multiThread_replace_test tests/cpp/multiThread_replace_test.cpp)
    target_link_libraries(multiThread_replace_test hnswlib)

    add_executable(main tests/cpp/main.cpp tests/cpp/sift_1b.cpp)
    target_link_libraries(main hnswlib)

    add_executable(getUnormalized_test tests/cpp/getUnormalized_test.cpp)
    target_link_libraries(getUnormalized_test hnswlib)

    add_executable(persistent_test tests/cpp/persistent_test.cpp)
    target_link_libraries(persistent_test hnswlib)

    add_executable(api_tests tests/cpp/api_test.cpp)
    target_link_libraries(api_tests hnswlib)
endif()
