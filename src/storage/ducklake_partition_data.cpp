#include "storage/ducklake_partition_data.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

string DuckLakePartitionUtils::GetPartitionKeyName(DuckLakeTransformType transform_type, const string &field_name,
                                                   case_insensitive_set_t &used_names) {
	string prefix;
	switch (transform_type) {
	case DuckLakeTransformType::IDENTITY:
		prefix = field_name;
		break;
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
	case DuckLakeTransformType::BUCKET:
		prefix = "bucket";
		break;
	default:
		throw NotImplementedException("Unsupported partition transform type");
	}
	if (used_names.find(prefix) == used_names.end()) {
		return prefix;
	}
	return prefix + "_" + field_name;
}

string DuckLakePartitionUtils::GetPartitionSQLExpression(const DuckLakeTransform &transform, const string &col_name) {
	if (transform.type == DuckLakeTransformType::IDENTITY) {
		return col_name;
	}
	if (transform.type == DuckLakeTransformType::BUCKET) {
		// Return the actual SQL expression that computes the bucket assignment
		return "(murmur3_32(" + col_name + ") & 2147483647) % " + to_string(transform.bucket_count);
	}
	case_insensitive_set_t used_names;
	string func_name = GetPartitionKeyName(transform.type, col_name, used_names);
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
	case DuckLakeTransformType::BUCKET:
		return LogicalType::INTEGER;
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

unique_ptr<Expression> DuckLakePartitionUtils::ApplyScalarFunction(ClientContext &context, const string &function_name,
                                                                   unique_ptr<Expression> column_expr) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(column_expr));
	ErrorData error;
	FunctionBinder binder(context);
	auto function = binder.BindScalarFunction(DEFAULT_SCHEMA, function_name, std::move(children), error, false);
	if (!function) {
		error.Throw();
	}
	return function;
}

unique_ptr<Expression> DuckLakePartitionUtils::ApplyBucketTransform(ClientContext &context,
                                                                    unique_ptr<Expression> column_expr,
                                                                    idx_t bucket_count) {
	D_ASSERT(bucket_count > 0);

	// Iceberg-compatible: murmur3_x86_32 with seed 0
	auto hash_expr = ApplyScalarFunction(context, "murmur3_32", std::move(column_expr));

	// Iceberg bucket: (hash & Integer.MAX_VALUE) % N
	// Mask off sign bit to ensure non-negative result
	vector<unique_ptr<Expression>> and_children;
	and_children.push_back(std::move(hash_expr));
	and_children.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(NumericLimits<int32_t>::Maximum())));

	ErrorData error;
	FunctionBinder binder(context);
	auto and_expr = binder.BindScalarFunction(DEFAULT_SCHEMA, "&", std::move(and_children), error, false);
	if (!and_expr) {
		error.Throw();
	}

	vector<unique_ptr<Expression>> mod_children;
	mod_children.push_back(std::move(and_expr));
	mod_children.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(NumericCast<int32_t>(bucket_count))));

	auto mod_expr = binder.BindScalarFunction(DEFAULT_SCHEMA, "%", std::move(mod_children), error, false);
	if (!mod_expr) {
		error.Throw();
	}
	return mod_expr;
}

unique_ptr<Expression> DuckLakePartitionUtils::ApplyPartitionTransform(ClientContext &context,
                                                                       unique_ptr<Expression> column_expr,
                                                                       const DuckLakePartitionField &field) {
	switch (field.transform.type) {
	case DuckLakeTransformType::IDENTITY:
		return column_expr;
	case DuckLakeTransformType::YEAR:
		return ApplyScalarFunction(context, "year", std::move(column_expr));
	case DuckLakeTransformType::MONTH:
		return ApplyScalarFunction(context, "month", std::move(column_expr));
	case DuckLakeTransformType::DAY:
		return ApplyScalarFunction(context, "day", std::move(column_expr));
	case DuckLakeTransformType::HOUR:
		return ApplyScalarFunction(context, "hour", std::move(column_expr));
	case DuckLakeTransformType::BUCKET:
		return ApplyBucketTransform(context, std::move(column_expr), field.transform.bucket_count);
	default:
		throw NotImplementedException("Unsupported partition transform type");
	}
}

} // namespace duckdb
