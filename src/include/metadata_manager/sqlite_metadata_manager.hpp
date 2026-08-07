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
	bool SupportsInlining(const LogicalType &type) override;
	bool SupportsAppender() const override {
		return false;
	}
	//! sqlite_scanner rejects ON CONFLICT outright, so this backend keeps the pre-upsert path
	//! and its concurrent-first-insert lost update. Lift once sqlite_scanner supports upserts.
	bool SupportsUpsert() const override {
		return false;
	}
	// No MIN/MAX override: sqlite_scanner means DuckDB parses this SQL, and DuckDB knows MIN/MAX
	// only as aggregates. Inherited LEAST/GREATEST are correct here.
	string GetColumnTypeInternal(const LogicalType &type) override;
};

} // namespace duckdb
