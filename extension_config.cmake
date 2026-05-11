# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ducklake
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

if($ENV{ENABLE_QUACK})
    include_directories(
            ${CMAKE_CURRENT_LIST_DIR}/duckdb/third_party/httplib
            ${CMAKE_CURRENT_LIST_DIR}/duckdb/extension/autocomplete/include
    )

    duckdb_extension_load(quack
            LOAD_TESTS
            GIT_URL git@github.com:pdet/duckdb-quack.git
            GIT_TAG d1870ae6b95c1a903c0d97548d637096a4bf789a
    )
endif()

if(NOT DEFINED ENV{DISABLE_EXTENSIONS_FOR_TEST})
    duckdb_extension_load(icu)
    duckdb_extension_load(json)
    duckdb_extension_load(tpch)
endif()

set(EXTENSION_CONFIG_BASE_DIR "${CMAKE_CURRENT_LIST_DIR}/.github/config/extensions/")
if($ENV{ENABLE_SQLITE_SCANNER})
    include("${EXTENSION_CONFIG_BASE_DIR}/sqlite_scanner.cmake")
endif()

if($ENV{ENABLE_POSTGRES_SCANNER})
    include("${EXTENSION_CONFIG_BASE_DIR}/postgres_scanner.cmake")
endif()
