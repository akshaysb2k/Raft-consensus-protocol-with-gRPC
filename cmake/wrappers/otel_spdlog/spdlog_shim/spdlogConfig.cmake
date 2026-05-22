# ==============================================================================
# spdlog Package Config Shim
# ==============================================================================
#
# This is a minimal "shim" package config file for spdlog.
#
# Purpose:
# - Satisfies find_package(spdlog REQUIRED) calls
# - Uses the spdlog::spdlog target that already exists from add_subdirectory()
# - Avoids the need to install spdlog or generate real package configs
#
# This file does NOT create the spdlog targets - they must already exist
# from add_subdirectory(libs/spdlog) called earlier in the build.
# ==============================================================================

# Extract spdlog version dynamically from version.h
file(READ "${CMAKE_SOURCE_DIR}/libs/spdlog/include/spdlog/version.h" _spdlog_version_header)
string(REGEX MATCH "SPDLOG_VER_MAJOR ([0-9]+)" _ "${_spdlog_version_header}")
set(_spdlog_ver_major ${CMAKE_MATCH_1})
string(REGEX MATCH "SPDLOG_VER_MINOR ([0-9]+)" _ "${_spdlog_version_header}")
set(_spdlog_ver_minor ${CMAKE_MATCH_1})
string(REGEX MATCH "SPDLOG_VER_PATCH ([0-9]+)" _ "${_spdlog_version_header}")
set(_spdlog_ver_patch ${CMAKE_MATCH_1})
set(spdlog_VERSION "${_spdlog_ver_major}.${_spdlog_ver_minor}.${_spdlog_ver_patch}")

# Mark the package as found
set(spdlog_FOUND TRUE)

# Verify that the spdlog::spdlog target exists
# (It should, from add_subdirectory(libs/spdlog) in the main CMakeLists.txt)
if(NOT TARGET spdlog::spdlog)
    message(FATAL_ERROR 
        "spdlog::spdlog target not found in shim config. "
        "Ensure add_subdirectory(libs/spdlog) is called before using this wrapper.")
endif()

# spdlog requires Threads, so find it (required by package contract)
find_package(Threads REQUIRED)

# Clean up internal variables
unset(_spdlog_version_header)
unset(_spdlog_ver_major)
unset(_spdlog_ver_minor)
unset(_spdlog_ver_patch)
