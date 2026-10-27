duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 60e4170f3e744164df08a0c6b8c23236d8dbcd6d
        SUBMODULES database-connector
        )
# Static linking on windows does not properly work due to symbol collision
if (NOT WIN32)
    duckdb_extension_statically_link(sqlite_scanner)
endif()
