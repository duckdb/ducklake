if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG f77b0cb511748fd70fb8a4eb265e2990599d286c
            SUBMODULES database-connector
            )
endif()
