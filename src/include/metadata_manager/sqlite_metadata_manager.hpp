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

class SQLiteMetadataManager : public DuckLakeMetadataManager {
public:
	explicit SQLiteMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<SQLiteMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool DroppedTablesStayReadable() const override {
		return false;
	}
	idx_t MaxColumnCount() const override {
		// the default SQLITE_MAX_COLUMN
		return 2000;
	}
	vector<string> SystemColumnNames() const override {
		// the names of the row id, which the sqlite extension deletes and updates rows by
		return {"rowid", "oid", "_rowid_"};
	}
	bool IsRetryableCommitError(const string &message) const override;
	string CommitErrorHint(const string &message) const override;
	void RenameInlinedColumns(const map<string, vector<pair<string, string>>> &renames) override;

	string GetColumnTypeInternal(const LogicalType &type) override;
	string GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id,
	                                      const string &metadata_table_prefix) override;
	string GetRewriteSnapshotsQuery(idx_t first_snapshot, idx_t last_snapshot) const override;
};

} // namespace duckdb
