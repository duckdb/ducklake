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

	string GetColumnTypeInternal(const LogicalType &type) override;

	unique_ptr<QueryResult> Execute(string query) override;
	unique_ptr<QueryResult> Query(string query) override;

protected:
	string GetLatestSnapshotQuery() const override;

	//! Wrap field selections with list aggregation using Postgres jsonb syntax
	string WrapWithListAggregation(const unordered_map<string, string> &fields) const override;

	//! Parse tag list from JSON query result (override for Postgres JSONB handling)
	vector<DuckLakeTag> LoadTags(const Value &tag_map) const override;

	//! Parse inlined data tables list from JSON query result (override for Postgres JSONB handling)
	vector<DuckLakeInlinedTableInfo> LoadInlinedDataTables(const Value &list) const override;

	//! Parse macro implementations list from JSON query result (override for Postgres JSONB handling)
	vector<DuckLakeMacroImplementation> LoadMacroImplementations(const Value &list) const override;

private:
	unique_ptr<QueryResult> ExecuteQuery(string &query, string command);
};

} // namespace duckdb
