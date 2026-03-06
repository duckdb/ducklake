#include "storage/ducklake_partition_data.hpp"

#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

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

	// Not compatible with Iceberg since we're not using murmur3 hash, but instead DuckDB's own hash.
	auto hash_expr = ApplyScalarFunction(context, "hash", std::move(column_expr));

	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(hash_expr));
	// UBIGINT to match hash() return type - ensures modulo result is always in [0, N)
	children.push_back(make_uniq<BoundConstantExpression>(Value::UBIGINT(bucket_count)));

	ErrorData error;
	FunctionBinder binder(context);
	auto mod_expr = binder.BindScalarFunction(DEFAULT_SCHEMA, "%", std::move(children), error, false);
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
