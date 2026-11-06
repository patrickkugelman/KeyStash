cmake_minimum_required(VERSION 3.16)
project(kvserver LANGUAGES CXX)

# The server is built on epoll.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "kvserver requires Linux (epoll); on Windows build inside WSL")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(KV_ASAN "Enable AddressSanitizer" OFF)
option(KV_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(KV_TSAN "Enable ThreadSanitizer" OFF)
option(KV_WERROR "Treat warnings as errors" OFF)

if(KV_TSAN AND KV_ASAN)
  message(FATAL_ERROR "KV_TSAN cannot be combined with KV_ASAN")
endif()

add_compile_options(-Wall -Wextra -Wpedantic -Wshadow -Wconversion)
if(KV_WERROR)
  add_compile_options(-Werror)
endif()

set(KV_SAN_FLAGS "")
if(KV_ASAN)
  list(APPEND KV_SAN_FLAGS -fsanitize=address)
endif()
if(KV_UBSAN)
  # Abort on the first report so a run cannot pass with UB in its log.
  list(APPEND KV_SAN_FLAGS -fsanitize=undefined -fno-sanitize-recover=all)
endif()
if(KV_TSAN)
  list(APPEND KV_SAN_FLAGS -fsanitize=thread)
endif()
if(KV_SAN_FLAGS)
  add_compile_options(${KV_SAN_FLAGS} -fno-omit-frame-pointer -g)
  add_link_options(${KV_SAN_FLAGS})
endif()

find_package(Threads REQUIRED)

add_executable(kvserver
  src/main.cpp
  src/net/event_loop.cpp
  src/net/connection.cpp
  src/net/server.cpp
  src/proto/resp.cpp
  src/engine/std_table.cpp
  src/engine/hash_table.cpp
  src/engine/store.cpp
  src/engine/dispatcher.cpp
)
target_include_directories(kvserver PRIVATE src)
target_link_libraries(kvserver PRIVATE Threads::Threads)
