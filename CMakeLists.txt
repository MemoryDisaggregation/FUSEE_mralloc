cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

project(DDCKV LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)

find_package(Boost REQUIRED)

include_directories(src)
set(CMAKE_BUILD_TYPE debug)

add_subdirectory(src)

add_subdirectory(ycsb-test)
add_subdirectory(crash-recover-test)
add_subdirectory(micro-test)
link_directories(lib)
add_library(mralloc STATIC IMPORTED)
set_target_properties(mralloc PROPERTIES IMPORTED_LOCATION ${PROJECT_SOURCE_DIR}/lib/libmralloc.a)

enable_testing()
add_subdirectory(tests)