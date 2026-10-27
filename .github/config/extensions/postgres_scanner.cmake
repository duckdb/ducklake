# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG 719691f4df92b79755730fbf4f2b8730007a853c
            SUBMODULES database-connector
            )
 endif()
