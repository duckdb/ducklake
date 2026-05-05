//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/vgi_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class VgiMetadataManager : public DuckLakeMetadataManager {
public:
	explicit VgiMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<VgiMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool SupportsInlining(const LogicalType &type) override;
	bool SupportsAppender() const override {
		return false;
	}

	string GetColumnTypeInternal(const LogicalType &type) override;

	void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) override;
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
};

} // namespace duckdb
