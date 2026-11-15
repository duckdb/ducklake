//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/sqlite_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

//! The manager of a SQLite catalog, whose behaviour the profile of the sqlite extension describes
class SQLiteMetadataManager : public DuckLakeMetadataManager {
public:
	explicit SQLiteMetadataManager(DuckLakeTransaction &transaction);

	//! Whether the metadata type names a catalog of the sqlite extension
	static bool Manages(const string &metadata_type);

	//! SQLite runs the statements of the load, the list aggregates travel as json text
	DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot) override;

private:
	//! Whether the loaded sqlite extension has a sqlite_query with typed results, looked up once
	bool NativeLoadAvailable();

	//! The outcome of the lookup, unset until the first native load
	optional_idx native_load_available;
};

} // namespace duckdb
