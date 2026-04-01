#include "storage/ducklake_partition_data.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

string DuckLakePartitionUtils::GetPartitionKeyName(DuckLakeTransformType transform_type, const string &field_name,
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

string DuckLakePartitionUtils::GetPartitionSQLExpression(DuckLakeTransformType transform_type, const string &col_name) {
	if (transform_type == DuckLakeTransformType::IDENTITY) {
		return col_name;
	}
	case_insensitive_set_t used_names;
	string func_name = GetPartitionKeyName(transform_type, col_name, used_names);
	return func_name + "(" + col_name + ")";
}

LogicalType DuckLakePartitionUtils::GetPartitionKeyType(DuckLakeTransformType transform_type,
                                                        const LogicalType &source_type) {
	switch (transform_type) {
	case DuckLakeTransformType::IDENTITY:
		return source_type;
	case DuckLakeTransformType::YEAR:
	case DuckLakeTransformType::MONTH:
	case DuckLakeTransformType::DAY:
	case DuckLakeTransformType::HOUR:
		return LogicalType::BIGINT;
	default:
		throw NotImplementedException("Unsupported partition transform type");
	}
}

string DuckLakePartitionUtils::BuildPartitionFilter(const vector<string> &partition_sql_exprs,
                                                    const vector<Value> &partition_values) {
	string filter;
	for (idx_t p = 0; p < partition_sql_exprs.size(); p++) {
		if (p > 0) {
			filter += " AND ";
		}
		auto &val = partition_values[p];
		if (val.IsNull()) {
			filter += partition_sql_exprs[p] + " IS NULL";
		} else {
			filter += partition_sql_exprs[p] + " = " + val.ToSQLString();
		}
	}
	return filter;
}

} // namespace duckdb
