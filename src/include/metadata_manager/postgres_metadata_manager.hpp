//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class PostgresMetadataManager : public DuckLakeMetadataManager {
public:
	explicit PostgresMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<PostgresMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	string CastColumnToTarget(const string &stats, const LogicalType &type) override;

	string GetColumnTypeInternal(const LogicalType &type) override;

	unique_ptr<QueryResult> Execute(string query) override;
	unique_ptr<QueryResult> Query(string query) override;

protected:
	string GetLatestSnapshotQuery() const override;

	//! Wrap field selections with list aggregation using Postgres jsonb syntax
	string WrapWithListAggregation(const vector<pair<string, string>> &fields) const override;

	//! Cast stats columns to target type using Postgres syntax (no TRY_CAST)
	string CastStatsToTarget(const string &stats, const LogicalType &type) override;

	//! Load tags from JSON result
	vector<DuckLakeTag> LoadTags(const Value &tag_map) const override;

	//! Load inlined data tables from JSON result
	vector<DuckLakeInlinedTableInfo> LoadInlinedDataTables(const Value &list) const override;

	//! Load macro implementations from JSON result
	vector<DuckLakeMacroImplementation> LoadMacroImplementations(const Value &list) const override;

private:
	unique_ptr<QueryResult> ExecuteQuery(string &query, string command);
};

} // namespace duckdb
