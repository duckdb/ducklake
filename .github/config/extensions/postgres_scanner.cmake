if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG a4e03aad76a002e913e676cce1fc2600f64a614f
            SUBMODULES database-connector
            )
endif()
