# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ducklake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

duckdb_extension_load(icu)
duckdb_extension_load(json)
duckdb_extension_load(tpch)

duckdb_extension_load(sqlite_scanner
        DONT_LINK
        APPLY_PATCHES
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 801922e968dd9b72b055ed0f9857cd7421200e6f
)

duckdb_extension_load(postgres_scanner
        DONT_LINK
        APPLY_PATCHES
        GIT_URL https://github.com/duckdb/duckdb-postgres
        GIT_TAG e58cd1dfed98a04ce3e928d5b941e5e2c533ba12
)
