cmake_minimum_required(VERSION 3.25)

project(taskflow VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TASKFLOW_BUILD_TESTS "Build the TaskFlow test suites" ON)
option(TASKFLOW_WARNINGS_AS_ERRORS "Treat warnings in project code as errors" ON)
set(TASKFLOW_SANITIZERS "" CACHE STRING
    "Semicolon-separated sanitizers to enable (address;undefined or thread)")

find_package(Drogon CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(hiredis CONFIG REQUIRED)

configure_file(cmake/version.hpp.in ${CMAKE_BINARY_DIR}/generated/taskflow/version.hpp @ONLY)

# Warnings and sanitizers apply to project code only, never to dependencies.
add_library(taskflow_options INTERFACE)
target_compile_options(taskflow_options INTERFACE -Wall -Wextra -Wpedantic -Wshadow)
if(TASKFLOW_WARNINGS_AS_ERRORS)
  target_compile_options(taskflow_options INTERFACE -Werror)
endif()
if(TASKFLOW_SANITIZERS)
  string(REPLACE ";" "," _san "${TASKFLOW_SANITIZERS}")
  target_compile_options(taskflow_options INTERFACE -fsanitize=${_san} -fno-omit-frame-pointer)
  target_link_options(taskflow_options INTERFACE -fsanitize=${_san})
endif()

add_library(taskflow_core STATIC
  src/common/errors.cpp
  src/config/config.cpp
  src/logging/logging.cpp
  src/logging/json_formatter.cpp
  src/db/postgres.cpp
  src/db/migration_runner.cpp
  src/db/job_repository.cpp
  src/jobs/job.cpp
  src/jobs/job_state.cpp
  src/scheduler/priority_queue.cpp
  src/scheduler/schedule_queue.cpp
  src/scheduler/scheduler.cpp
  src/redis/redis_client.cpp
  src/api/health_service.cpp
  src/api/http_server.cpp
  src/app/application.cpp
  src/app/signal_watcher.cpp
)
target_include_directories(taskflow_core PUBLIC
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_BINARY_DIR}/generated)
target_link_libraries(taskflow_core
  PUBLIC  spdlog::spdlog Drogon::Drogon PostgreSQL::PostgreSQL hiredis::hiredis
  PRIVATE taskflow_options)

add_executable(taskflow src/main.cpp)
target_link_libraries(taskflow PRIVATE taskflow_core taskflow_options)

option(TASKFLOW_BUILD_BENCHMARKS "Build the benchmark executables" ON)
if(TASKFLOW_BUILD_BENCHMARKS)
  add_executable(taskflow_scheduler_bench bench/scheduler_bench.cpp)
  target_compile_definitions(taskflow_scheduler_bench PRIVATE
    TASKFLOW_SOURCE_MIGRATIONS_DIR="${CMAKE_SOURCE_DIR}/migrations")
  target_link_libraries(taskflow_scheduler_bench PRIVATE taskflow_core taskflow_options)
endif()

if(TASKFLOW_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
