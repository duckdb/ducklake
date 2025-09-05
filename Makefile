PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducklake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# We have to overwrite the test_release_internal from extension-ci-tools/makefiles/duckdb_extension.Makefile to run all
# Of our catalog shebang
test_release_internal:
	./build/release/$(TEST_PATH) "test/*" --test-config test/configs/postgres.json
	./build/release/$(TEST_PATH) "test/*" --test-config test/configs/sqlite.json
	./build/release/$(TEST_PATH) "test/*"
