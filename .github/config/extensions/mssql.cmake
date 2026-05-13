if (NOT ${WASM_ENABLED})
    set(MSSQL_PATCH_DIR "${CMAKE_SOURCE_DIR}/.github/patches/extensions/mssql")
    file(MAKE_DIRECTORY "${MSSQL_PATCH_DIR}")
    file(GLOB MSSQL_PATCHES "${CMAKE_CURRENT_LIST_DIR}/../../patches/extensions/mssql/*.patch")
    file(COPY ${MSSQL_PATCHES} DESTINATION "${MSSQL_PATCH_DIR}")

    duckdb_extension_load(mssql
            DONT_LINK APPLY_PATCHES
            GIT_URL https://github.com/hugr-lab/mssql-extension
            GIT_TAG v0.1.18
            )
endif()
