if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG a42c490df0019406658073c003b7d89dd4338466
            APPLY_PATCHES
            )
endif()
