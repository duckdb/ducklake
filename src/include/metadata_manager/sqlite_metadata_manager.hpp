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

	//! A failing native statement reports the error of the read with the statement SQLite ran
	using DuckLakeMetadataManager::Query;
	unique_ptr<QueryResult> Query(string &query) override;
	//! The error of a failing native statement without the statement that Query appends
	string CommitErrorWithoutQuery(const string &message) const override;

protected:
	//! SQLite runs the head snapshot read and the table stats read as one statement each
	string GetLatestSnapshotQuery() const override;
	string GetGlobalTableStatsQuery(const vector<TableIndex> &table_ids) const override;

private:
	//! Whether the loaded sqlite extension has a sqlite_query with typed results, looked up once
	bool NativeLoadAvailable() const;
	//! The statement SQLite runs through sqlite_query, which returns the columns with the given types
	string NativeStatement(const string &statement, const vector<CatalogLoadColumn> &columns) const;

	//! The outcome of the lookup, unset until the first native read
	mutable optional_idx native_load_available;
};

} // namespace duckdb
