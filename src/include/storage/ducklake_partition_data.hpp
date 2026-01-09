//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_partition_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "common/index.hpp"

namespace duckdb {
class BaseStatistics;

enum class DuckLakeTransformType { IDENTITY, BUCKET, YEAR, MONTH, DAY, HOUR };

struct DuckLakeTransform {
	DuckLakeTransformType type;
	idx_t bucket_count = 0; // only for BUCKET
};

struct DuckLakePartitionField {
	idx_t partition_key_index = 0;
	FieldIndex field_id;
	DuckLakeTransform transform;
};

struct DuckLakePartition {
	idx_t partition_id = 0;
	vector<DuckLakePartitionField> fields;
};

} // namespace duckdb
