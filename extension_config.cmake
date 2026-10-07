# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ducklake
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
duckdb_extension_statically_link(ducklake)

if(NOT DEFINED ENV{DISABLE_EXTENSIONS_FOR_TEST})
    duckdb_extension_load(icu)
    duckdb_extension_load(json)
    duckdb_extension_load(tpch)
    duckdb_extension_statically_link(icu json tpch)
endif()

# Linked only when built via CORE_EXTENSIONS
duckdb_extension_statically_link(httpfs aws)

set(DUCKLAKE_EXTENSION_CONFIG_DIR "${CMAKE_CURRENT_LIST_DIR}/.github/config/extensions/")
if($ENV{ENABLE_SQLITE_SCANNER})
    include("${DUCKLAKE_EXTENSION_CONFIG_DIR}/sqlite_scanner.cmake")
endif()

if($ENV{ENABLE_POSTGRES_SCANNER})
    include("${DUCKLAKE_EXTENSION_CONFIG_DIR}/postgres_scanner.cmake")
endif()

if($ENV{ENABLE_QUACK})
    include_directories(
            ${CMAKE_CURRENT_LIST_DIR}/duckdb/third_party/httplib
            ${CMAKE_CURRENT_LIST_DIR}/duckdb/extension/autocomplete/include
    )
    duckdb_extension_load(quack
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-quack.git
            GIT_TAG 974927a394b188755284682b73398ed50e86316c
            SUBMODULES "extension-ci-tools"
            APPLY_PATCHES
    )
    duckdb_extension_statically_link(quack)
endif()