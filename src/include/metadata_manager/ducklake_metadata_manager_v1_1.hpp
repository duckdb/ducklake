//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/ducklake_metadata_manager_v1_1.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class DuckLakeMetadataManagerV1_1 : public DuckLakeMetadataManager {
public:
	explicit DuckLakeMetadataManagerV1_1(DuckLakeTransaction &transaction);

	string GetCreateTableStatements() override;
	string GetVersionString() override;
};

} // namespace duckdb
