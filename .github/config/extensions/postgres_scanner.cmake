# postgres_scanner needs DONT_LINK because it depends on libpq/OpenSSL
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK APPLY_PATCHES
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG c0e9256a60371a062e8d6cfc4045c71a317b5ee8
            )
endif()
