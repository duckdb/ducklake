# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ducklake
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

if(NOT DEFINED ENV{DISABLE_EXTENSIONS_FOR_TEST})
    duckdb_extension_load(icu)
    duckdb_extension_load(json)
    duckdb_extension_load(tpch)
endif()

if($ENV{ENABLE_SQLITE_SCANNER})
    duckdb_extension_load(sqlite_scanner
        DONT_LINK
        APPLY_PATCHES
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 833e105cbcaa0f6e8d34d334f3b920ce86f6fdf9
    )
endif()

if($ENV{ENABLE_POSTGRES_SCANNER})
    duckdb_extension_load(postgres_scanner
        DONT_LINK
        APPLY_PATCHES
        GIT_URL https://github.com/duckdb/duckdb-postgres
        GIT_TAG f012a4f99cea1d276d1787d0dc84b1f1a0e0f0b2
    )
endif()