//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/sqlserver_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class SQLServerMetadataManager : public DuckLakeMetadataManager {
public:
	explicit SQLServerMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<SQLServerMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool SupportsInlining(const LogicalType &type) override {
		return false;
	}
	bool SupportsAppender() const override {
		return false;
	}
	idx_t MaxIdentifierLength() const override {
		return 128;
	}

	string GetColumnTypeInternal(const LogicalType &type) override;
	string CastColumnToTarget(const string &column, const LogicalType &type) override;

	void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) override;
	string GetCreateTableStatements() override;
	string InsertSnapshot() override;

	unique_ptr<DuckLakeSnapshot> GetSnapshot() override;
	unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound) override;
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;

protected:
	string GetLatestSnapshotQuery() const override;

private:
	void RefreshMetadataCache();
	void ExpandPlaceholders(DuckLakeSnapshot snapshot, string &query);
	unique_ptr<QueryResult> ExecuteQuery(DuckLakeSnapshot snapshot, string &query, bool returns_rows);
};

} // namespace duckdb
