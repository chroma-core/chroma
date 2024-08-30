cmake_minimum_required(VERSION 3.14)
project(embeddings)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set options for llama.cpp
set(LLAMA_BUILD_EXAMPLES OFF CACHE BOOL "llama: build examples" FORCE)
set(LLAMA_BUILD_TESTS OFF CACHE BOOL "llama: build tests" FORCE)
set(LLAMA_BUILD_SERVER OFF CACHE BOOL "llama: build server" FORCE)

# Add llama.cpp as a subdirectory
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../vendor/llama.cpp ${CMAKE_CURRENT_BINARY_DIR}/llama.cpp)

# Create our shared library
add_library(embeddings SHARED embeddings.cpp)

# Link against llama and other necessary libraries
target_link_libraries(embeddings PRIVATE llama common)

# Include directories
target_include_directories(embeddings
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../vendor/llama.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../vendor/llama.cpp/ggml/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../vendor/llama.cpp/common
)

# Set custom output directories
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

# Set output name prefix and suffix
set_target_properties(embeddings PROPERTIES
    PREFIX "lib"
)
