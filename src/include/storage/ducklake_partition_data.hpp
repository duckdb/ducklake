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

enum class DuckLakeTransformType { IDENTITY, YEAR, MONTH, DAY, HOUR };

struct DuckLakeTransform {
	DuckLakeTransformType type;
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

struct DuckLakePartitionUtils {
	//! Get the hive partition key name for a partition field, while also resolving name collisions e.g., year_dt

	static string GetPartitionKeyName(DuckLakeTransformType transform_type, const string &field_name,
	                                  case_insensitive_set_t &used_names) {
		string prefix;
		switch (transform_type) {
		case DuckLakeTransformType::IDENTITY:
			return field_name;
		case DuckLakeTransformType::YEAR:
			prefix = "year";
			break;
		case DuckLakeTransformType::MONTH:
			prefix = "month";
			break;
		case DuckLakeTransformType::DAY:
			prefix = "day";
			break;
		case DuckLakeTransformType::HOUR:
			prefix = "hour";
			break;
		default:
			throw NotImplementedException("Unsupported partition transform type");
		}
		if (used_names.find(prefix) == used_names.end()) {
			return prefix;
		}
		return prefix + "_" + field_name;
	}
};

} // namespace duckdb
