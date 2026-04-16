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

template <typename Base>
class DuckLakeMetadataManagerV1_1 : public Base {
public:
	explicit DuckLakeMetadataManagerV1_1(DuckLakeTransaction &transaction) : Base(transaction) {
	}

	string GetCreateTableStatements() override;
	string GetVersionString() override;
};

} // namespace duckdb
