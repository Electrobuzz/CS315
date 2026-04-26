cmake_minimum_required(VERSION 3.12)
project(minidb VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)
include_directories(src)

set(SOURCES
    src/common/types.cpp
    src/storage/page.cpp
    src/storage/buffer_pool_manager.cpp
    src/storage/disk_manager.cpp
    src/catalog/schema.cpp
    src/execution/executor.cpp
    src/db/database.cpp
)

set(HEADERS
    src/common/types.h
    src/storage/page.h
    src/storage/buffer_pool_manager.h
    src/storage/disk_manager.h
    src/catalog/schema.h
    src/execution/executor.h
    src/db/database.h
)

add_library(minidb_lib ${SOURCES} ${HEADERS})

target_include_directories(minidb_lib PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_executable(minidb_demo examples/demo.cpp)
target_link_libraries(minidb_demo minidb_lib)

# enable_testing()
# add_subdirectory(tests)
