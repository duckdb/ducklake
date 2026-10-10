//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_initializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_catalog.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
class DuckLakeTransaction;

class DuckLakeInitializer {
public:
	DuckLakeInitializer(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeOptions &options);

public:
	void Initialize();

private:
	void InitializeNewDuckLake(DuckLakeTransaction &transaction, bool has_explicit_schema);
	void LoadExistingDuckLake(DuckLakeTransaction &transaction, bool skip_dev_migration = false);
	//! Loads an existing DuckLake, retrying when another attach migrates it at the same time
	void LoadExistingDuckLakeWithRetries(DuckLakeTransaction &transaction);
	void AttachMetadata(DuckLakeTransaction &transaction);
	void RestartMetadataTransaction(DuckLakeTransaction &transaction);
	//! True for a DuckDB metadata file whose storage version the user did not set
	bool ChoosesMetadataStorageVersion();
	//! Attaches a writable DuckDB metadata file again when its storage version is too old for DuckLake
	void RaiseMetadataStorageVersion(DuckLakeTransaction &transaction);
	bool DuckLakeIsInitialized(DuckLakeTransaction &transaction);
	void InitializeDataPath();
	string GetAttachOptions();
	void SetVersionedMetadataManager(DuckLakeTransaction &transaction, DuckLakeVersion version);
	DuckLakeVersion ResolveTargetVersion(DuckLakeVersion catalog_version, const string &catalog_version_str);

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeOptions &options;
};

} // namespace duckdb
