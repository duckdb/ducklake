PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducklake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions that we need for crucial testing
DEFAULT_TEST_EXTENSION_DEPS=
# For cloud testing we also need these extensions
FULL_TEST_EXTENSION_DEPS=httpfs

PATCH_DIR := $(PROJ_DIR).github/patches/extensions/ducklake
PATCH_STAMP := build/extension_configuration/duckdb_patches_applied

EXTENSION_CONFIG_STEP := $(PATCH_STAMP)

$(PATCH_STAMP):
	mkdir -p $(dir $(PATCH_STAMP))
	cd $(PROJ_DIR)duckdb && python3 scripts/apply_extension_patches.py $(PATCH_DIR)/
	touch $(PATCH_STAMP)

build/extension_configuration/vcpkg.json: $(PATCH_STAMP)

# Aws and Azure have vcpkg dependencies and therefore need vcpkg merging
ifeq (${BUILD_EXTENSION_TEST_DEPS}, full)
USE_MERGED_VCPKG_MANIFEST:=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

wasm_mvp: $(PATCH_STAMP)
wasm_eh: $(PATCH_STAMP)
wasm_threads: $(PATCH_STAMP)
