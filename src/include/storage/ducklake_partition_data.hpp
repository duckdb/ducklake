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
	                                  case_insensitive_set_t &used_names);

	//! Get a SQL expression string for a partition field (e.g., "col" for identity, "year(col)" for year transform)
	static string GetPartitionSQLExpression(DuckLakeTransformType transform_type, const string &col_name);

	//! Returns Logical Type for a given partition key
	static LogicalType GetPartitionKeyType(DuckLakeTransformType transform_type, const LogicalType &source_type);

	//! Build a SQL WHERE filter matching the given partition values (e.g., "region = 'east' AND year(ts) = 2020")
	static string BuildPartitionFilter(const vector<string> &partition_sql_exprs,
	                                   const vector<Value> &partition_values);
};

} // namespace duckdb
