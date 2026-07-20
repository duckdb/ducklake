//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_fast_metadata_manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "metadata_manager/postgres_metadata_manager.hpp"

namespace duckdb {

class PostgresFastMetadataManager : public PostgresMetadataManager {
public:
	explicit PostgresFastMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<PostgresFastMetadataManager>(transaction);
	}

	void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) override;
	vector<DuckLakeSnapshotInfo> GetAllSnapshots(const string &filter = string()) override;
	vector<DuckLakeTableSizeInfo> GetTableSizes(DuckLakeSnapshot snapshot) override;
};

} // namespace duckdb
