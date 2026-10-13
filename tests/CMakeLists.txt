find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

# Three executables, because they have different requirements:
#   unit         - no external services
#   integration  - needs PostgreSQL and Redis (TASKFLOW_* environment, see .env.example)
#   http         - starts the real server; Drogon is a per-process singleton, so it gets its own process
function(taskflow_add_tests target label)
  add_executable(${target} ${ARGN})
  target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/support)
  target_compile_definitions(${target} PRIVATE
    TASKFLOW_SOURCE_MIGRATIONS_DIR="${CMAKE_SOURCE_DIR}/migrations")
  target_link_libraries(${target} PRIVATE taskflow_core taskflow_options GTest::gtest GTest::gtest_main)
  gtest_discover_tests(${target}
    DISCOVERY_MODE PRE_TEST
    PROPERTIES LABELS ${label} TIMEOUT 120)
endfunction()

taskflow_add_tests(taskflow_unit_tests unit
  unit/config_test.cpp
  unit/errors_test.cpp
  unit/resource_pool_test.cpp
  unit/logging_test.cpp
  unit/health_service_test.cpp
  unit/migration_loader_test.cpp
  unit/job_state_test.cpp
  unit/job_model_test.cpp
  unit/priority_queue_test.cpp)

taskflow_add_tests(taskflow_integration_tests integration
  integration/postgres_test.cpp
  integration/migration_runner_test.cpp
  integration/redis_test.cpp
  integration/schema_test.cpp
  integration/scheduler_repository_test.cpp
  integration/schedule_queue_test.cpp
  integration/scheduler_test.cpp
  integration/job_repository_test.cpp
  integration/application_test.cpp)

taskflow_add_tests(taskflow_http_tests http
  http/health_endpoint_test.cpp)
