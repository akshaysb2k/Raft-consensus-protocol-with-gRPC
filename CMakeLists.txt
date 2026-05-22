cmake_minimum_required(VERSION 3.10)
project(Rafty)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TRACING "Enable OTEL tracing." OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Debug' as none was specified.")
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS 
               "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -Wall -Wextra")
if (TRACING)
    message(STATUS "Building with OTEL tracing enabled.")
    add_definitions(-DTRACING)
else()
    message(STATUS "Building without OTEL tracing.")
endif()
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

# This depends on how the setup script install the grpc
set(grpc_prefix "$ENV{HOME}/.local")
set(CMAKE_PREFIX_PATH ${grpc_prefix} CACHE PATH "libs prefix path")

add_subdirectory(libs/spdlog)
add_subdirectory(libs/googletest)

# includes grpc
include(cmake/common.cmake)

if (TRACING)
  # Find OpenTelemetry packages for tracing
  find_package(opentelemetry-cpp CONFIG REQUIRED)
  message(STATUS "Using opentelemetry-cpp ${opentelemetry-cpp_VERSION}")
  
  add_subdirectory(cmake/wrappers/otel_spdlog)
endif()

# Add the include directories to the project
include_directories(${CMAKE_SOURCE_DIR}/inc)
include_directories(${CMAKE_SOURCE_DIR}/generated)

# Add subdirectories
add_subdirectory(proto)
add_subdirectory(src)
add_subdirectory(app)
add_subdirectory(unittests)
add_subdirectory(integration_tests)