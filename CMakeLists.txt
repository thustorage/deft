cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(Deft)

# disable boost warning
add_definitions(-DBOOST_COROUTINES_NO_DEPRECATION_WARNING)

#Compiler options
set(CMAKE_CXX_STANDARD 20)
add_compile_options(-Wall -Wno-deprecated-declarations -Wsign-compare)

# Link Options
set(LINKS_FLAGS "-lcityhash -lboost_system -lboost_coroutine -lpthread -libverbs -lmemcached -lgflags")

set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${INCLUDE_BASE})

#Source file define
set(COMMON_SRC ${PROJECT_SOURCE_DIR}/src)

#Used by both server and clients
file(GLOB_RECURSE COMMON_FILE ${COMMON_SRC}/*.cpp)
add_library(deft STATIC ${COMMON_FILE})
link_libraries(deft)

file(GLOB TEST_SRC ${PROJECT_SOURCE_DIR}/test/*.cpp)
foreach (TEST ${TEST_SRC})
    get_filename_component(TEST_NAME ${TEST} NAME_WE)
    add_executable(${TEST_NAME} ${TEST})
    target_link_libraries(${TEST_NAME} ${LINKS_FLAGS})
endforeach()

