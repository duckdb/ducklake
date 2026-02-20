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

	string GetColumnTypeInternal(const LogicalType &type) override;
};

} // namespace duckdb
