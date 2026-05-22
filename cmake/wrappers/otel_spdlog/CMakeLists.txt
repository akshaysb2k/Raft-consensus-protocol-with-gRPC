# ==============================================================================
# OpenTelemetry spdlog Instrumentation Wrapper
# ==============================================================================
#
# This wrapper enables the OpenTelemetry spdlog instrumentation to work with
# spdlog built via add_subdirectory() rather than requiring find_package().
#
# Background:
# - The otel instrumentation CMakeLists.txt calls find_package(spdlog REQUIRED)
# - But we build spdlog via add_subdirectory(), which creates the spdlog::spdlog
#   target but doesn't generate package config files in a usable way
# - This wrapper provides a shim spdlogConfig.cmake that satisfies find_package()
#   while using the existing spdlog::spdlog target
#
# How it works:
# 1. Point spdlog_DIR to our shim directory containing spdlogConfig.cmake
# 2. Add the real otel instrumentation subdirectory
# 3. When it calls find_package(spdlog), CMake finds our shim config
# 4. The shim verifies spdlog::spdlog exists and marks the package as found
# 5. Build proceeds successfully
# ==============================================================================

# Point find_package(spdlog) to our shim package config
set(spdlog_DIR "${CMAKE_CURRENT_SOURCE_DIR}/spdlog_shim" CACHE PATH "Shim for spdlog package" FORCE)

# Now add the real OpenTelemetry spdlog instrumentation
# It will call find_package(spdlog REQUIRED) which will use our shim
add_subdirectory(
  ${CMAKE_SOURCE_DIR}/libs/opentelemetry-cpp-contrib/instrumentation/spdlog
  ${CMAKE_BINARY_DIR}/otel_spdlog_instrumentation
)

# Override the overly aggressive PUBLIC compile options from otel spdlog instrumentation
# Keep the flags for the library itself but don't propagate -Werror to consumers
get_target_property(otel_compile_options opentelemetry_spdlog_sink COMPILE_OPTIONS)
set_target_properties(opentelemetry_spdlog_sink PROPERTIES 
  COMPILE_OPTIONS ""
  INTERFACE_COMPILE_OPTIONS ""
)
target_compile_options(opentelemetry_spdlog_sink PRIVATE ${otel_compile_options})

# Create the namespaced alias that matches the installed export name
# The otel instrumentation creates target "opentelemetry_spdlog_sink" but only
# exports it with namespace "opentelemetry_contrib::" during install.
# Since we use add_subdirectory(), we need to create the alias manually.
add_library(opentelemetry_contrib::spdlog_sink ALIAS opentelemetry_spdlog_sink)
