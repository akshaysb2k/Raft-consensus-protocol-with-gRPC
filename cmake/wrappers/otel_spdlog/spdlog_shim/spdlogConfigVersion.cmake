# ==============================================================================
# spdlog Package Version Config Shim
# ==============================================================================
#
# This file provides version compatibility checking for find_package(spdlog).
# The version is extracted dynamically from spdlog's version.h header.
# ==============================================================================

# Extract spdlog version dynamically from version.h
file(READ "${CMAKE_SOURCE_DIR}/libs/spdlog/include/spdlog/version.h" _spdlog_version_header)
string(REGEX MATCH "SPDLOG_VER_MAJOR ([0-9]+)" _ "${_spdlog_version_header}")
set(_spdlog_ver_major ${CMAKE_MATCH_1})
string(REGEX MATCH "SPDLOG_VER_MINOR ([0-9]+)" _ "${_spdlog_version_header}")
set(_spdlog_ver_minor ${CMAKE_MATCH_1})
string(REGEX MATCH "SPDLOG_VER_PATCH ([0-9]+)" _ "${_spdlog_version_header}")
set(_spdlog_ver_patch ${CMAKE_MATCH_1})

set(PACKAGE_VERSION "${_spdlog_ver_major}.${_spdlog_ver_minor}.${_spdlog_ver_patch}")

# Check whether the requested PACKAGE_FIND_VERSION is compatible
if(PACKAGE_FIND_VERSION)
    if("${PACKAGE_VERSION}" VERSION_LESS "${PACKAGE_FIND_VERSION}")
        set(PACKAGE_VERSION_COMPATIBLE FALSE)
    else()
        set(PACKAGE_VERSION_COMPATIBLE TRUE)
        if("${PACKAGE_VERSION}" VERSION_EQUAL "${PACKAGE_FIND_VERSION}")
            set(PACKAGE_VERSION_EXACT TRUE)
        endif()
    endif()
else()
    # No version requested, so we're compatible
    set(PACKAGE_VERSION_COMPATIBLE TRUE)
endif()

# Clean up internal variables
unset(_spdlog_version_header)
unset(_spdlog_ver_major)
unset(_spdlog_ver_minor)
unset(_spdlog_ver_patch)
