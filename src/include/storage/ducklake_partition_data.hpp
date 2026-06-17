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

	bool operator==(const DuckLakeTransform &other) const {
		return type == other.type && bucket_count == other.bucket_count;
	}
};

struct DuckLakePartitionField {
	idx_t partition_key_index = 0;
	FieldIndex field_id;
	DuckLakeTransform transform;

	bool operator==(const DuckLakePartitionField &other) const {
		return partition_key_index == other.partition_key_index && field_id == other.field_id &&
		       transform == other.transform;
	}
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
	static string GetPartitionSQLExpression(const DuckLakeTransform &transform, const string &col_name);

	//! Returns Logical Type for a given partition key
	static LogicalType GetPartitionKeyType(DuckLakeTransformType transform_type, const LogicalType &source_type);

	//! Build a SQL WHERE filter matching the given partition values (e.g., "region = 'east' AND year(ts) = 2020")
	static string BuildPartitionFilter(const vector<string> &partition_sql_exprs,
	                                   const vector<Value> &partition_values);

	//! Wrap a column expression in a named scalar function (e.g. "year", "hash")
	static unique_ptr<Expression> ApplyScalarFunction(ClientContext &context, const string &function_name,
	                                                  unique_ptr<Expression> column_expr);

	//! Compute murmur3_32(column_expr) % bucket_count (Iceberg-compatible bucket transform)
	static unique_ptr<Expression> ApplyBucketTransform(ClientContext &context, unique_ptr<Expression> column_expr,
	                                                   idx_t bucket_count);

	//! Apply the appropriate partition transform to a column expression based on the field's transform type
	static unique_ptr<Expression> ApplyPartitionTransform(ClientContext &context, unique_ptr<Expression> column_expr,
	                                                      const DuckLakePartitionField &field);
};

} // namespace duckdb
