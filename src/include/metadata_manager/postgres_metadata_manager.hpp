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

	bool TypeIsNativelySupported(const LogicalType &type) override;

protected:
	string GetLatestSnapshotQuery() const override;

private:
	string GenerateCTESectionFromRequirements(const unordered_map<idx_t, CTERequirement> &requirements,
	                                          TableIndex table_id) override;
};

} // namespace duckdb
