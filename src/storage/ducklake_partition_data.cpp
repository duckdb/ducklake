#include "storage/ducklake_partition_data.hpp"
#include "common/ducklake_murmur3.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/value.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include <cstring>

namespace duckdb {

static constexpr StringUtil::EnumStringLiteral TRANSFORM_NAMES[] = {
    {static_cast<uint32_t>(DuckLakeTransformType::IDENTITY), "identity"},
    {static_cast<uint32_t>(DuckLakeTransformType::BUCKET), "bucket"},
    {static_cast<uint32_t>(DuckLakeTransformType::YEAR), "year"},
    {static_cast<uint32_t>(DuckLakeTransformType::MONTH), "month"},
    {static_cast<uint32_t>(DuckLakeTransformType::DAY), "day"},
    {static_cast<uint32_t>(DuckLakeTransformType::HOUR), "hour"},
    {static_cast<uint32_t>(DuckLakeTransformType::EPOCH_YEAR), "epoch_year"},
    {static_cast<uint32_t>(DuckLakeTransformType::EPOCH_MONTH), "epoch_month"},
    {static_cast<uint32_t>(DuckLakeTransformType::EPOCH_DAY), "epoch_day"},
    {static_cast<uint32_t>(DuckLakeTransformType::EPOCH_HOUR), "epoch_hour"}};

string DuckLakePartitionUtils::GetTransformName(DuckLakeTransformType transform_type) {
	return StringUtil::EnumToString(TRANSFORM_NAMES, sizeof(TRANSFORM_NAMES) / sizeof(TRANSFORM_NAMES[0]),
	                                "DuckLakeTransformType", static_cast<uint32_t>(transform_type));
}

bool DuckLakePartitionUtils::TryGetTransformType(const string &name, DuckLakeTransformType &result) {
	for (auto &entry : TRANSFORM_NAMES) {
		if (name == entry.string) {
			result = static_cast<DuckLakeTransformType>(entry.number);
			return true;
		}
	}
	return false;
}

string DuckLakePartitionUtils::GetPartitionKeyName(DuckLakeTransformType transform_type, const string &field_name,
                                                   case_insensitive_set_t &used_names) {
	auto prefix = transform_type == DuckLakeTransformType::IDENTITY ? field_name : GetTransformName(transform_type);
	if (used_names.find(prefix) == used_names.end()) {
		return prefix;
	}
	string base_name = prefix + "_" + field_name;
	if (used_names.find(base_name) == used_names.end()) {
		return base_name;
	}
	string candidate = base_name;
	int counter = 2;
	while (used_names.find(candidate) != used_names.end()) {
		candidate = base_name + "_" + to_string(counter++);
	}
	return candidate;
}

vector<string> DuckLakePartitionUtils::GetPartitionKeyNames(const DuckLakePartition &partition,
                                                            const DuckLakeFieldData &field_data) {
	vector<string> result;
	case_insensitive_set_t used_names;
	for (auto &field : partition.fields) {
		auto field_id = field_data.GetByFieldIndex(field.field_id);
		if (!field_id) {
			throw InternalException("DuckLake partition field id not found");
		}
		result.push_back(GetPartitionKeyName(field.transform.type, field_id->Name(), used_names));
		used_names.insert(result.back());
	}
	return result;
}

bool DuckLakePartitionUtils::IsEpochTransform(DuckLakeTransformType transform_type) {
	switch (transform_type) {
	case DuckLakeTransformType::EPOCH_YEAR:
	case DuckLakeTransformType::EPOCH_MONTH:
	case DuckLakeTransformType::EPOCH_DAY:
	case DuckLakeTransformType::EPOCH_HOUR:
		return true;
	default:
		return false;
	}
}

static string GetEpochTransformPart(DuckLakeTransformType transform_type) {
	switch (transform_type) {
	case DuckLakeTransformType::EPOCH_YEAR:
		return "year";
	case DuckLakeTransformType::EPOCH_MONTH:
		return "month";
	case DuckLakeTransformType::EPOCH_DAY:
		return "day";
	case DuckLakeTransformType::EPOCH_HOUR:
		return "hour";
	default:
		throw InternalException("Transform is not an epoch transform");
	}
}

LogicalType DuckLakePartitionUtils::GetPartitionKeyType(DuckLakeTransformType transform_type,
                                                        const LogicalType &source_type) {
	switch (transform_type) {
	// LCOV_EXCL_START
	case DuckLakeTransformType::IDENTITY:
		return source_type;
	// LCOV_EXCL_STOP
	case DuckLakeTransformType::YEAR:
	case DuckLakeTransformType::MONTH:
	case DuckLakeTransformType::DAY:
	case DuckLakeTransformType::HOUR:
	case DuckLakeTransformType::EPOCH_YEAR:
	case DuckLakeTransformType::EPOCH_MONTH:
	case DuckLakeTransformType::EPOCH_DAY:
	case DuckLakeTransformType::EPOCH_HOUR:
		return LogicalType::BIGINT;
	case DuckLakeTransformType::BUCKET:
		return LogicalType::INTEGER;
	default:
		throw NotImplementedException("Unsupported partition transform type");
	}
}

string DuckLakePartitionUtils::BuildHivePartitionPath(DuckLakeTableEntry &table, const vector<Value> &partition_values,
                                                      const string &separator) {
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		// LCOV_EXCL_START
		return string();
		// LCOV_EXCL_STOP
	}
	if (partition_data->fields.size() != partition_values.size()) {
		throw InternalException("DuckLake partition value count does not match partition spec");
	}
	string result;
	auto key_names = GetPartitionKeyNames(*partition_data, table.GetFieldData());
	for (idx_t field_idx = 0; field_idx < partition_data->fields.size(); field_idx++) {
		auto &field = partition_data->fields[field_idx];
		if (field.partition_key_index >= partition_values.size()) {
			throw InternalException("DuckLake partition key index is out of range");
		}
		auto &partition_value = partition_values[field.partition_key_index];
		if (!result.empty()) {
			result += separator;
		}
		auto component = HivePartitioning::Escape(key_names[field_idx]) + "=";
		component += partition_value.IsNull() ? HivePartitioning::DEFAULT_PARTITION_NAME
		                                      : HivePartitioning::EscapeValue(partition_value.ToString());
		result += GetPartitionDirectoryName(component);
	}
	if (!result.empty()) {
		result += separator;
	}
	return result;
}

//! Whether distinct values of the type always have distinct hive directory names
static bool HasDistinctHiveNames(const LogicalType &type) {
	if (type.IsNumeric() || type.IsTemporal()) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UUID:
	case LogicalTypeId::ENUM:
		return true;
	default:
		return false;
	}
}

string DuckLakePartitionUtils::GetPartitionDirectoryName(const string &hive_component) {
	if (hive_component.size() <= DuckLakeUtil::MAX_FILE_NAME_LENGTH) {
		return hive_component;
	}
	MD5Context md5;
	md5.Add(hive_component);
	return md5.FinishHex();
}

unique_ptr<Expression> DuckLakePartitionUtils::CreatePartitionPathExpression(ClientContext &context,
                                                                             const LogicalCopyToFile &copy) {
	for (auto col_idx : copy.partition_columns) {
		if (!HasDistinctHiveNames(copy.expected_types[col_idx])) {
			return nullptr;
		}
	}
	FunctionBinder binder(context);
	vector<unique_ptr<Expression>> components;
	for (idx_t i = 0; i < copy.partition_columns.size(); i++) {
		auto col_idx = copy.partition_columns[i];
		vector<unique_ptr<Expression>> children;
		children.push_back(make_uniq<BoundConstantExpression>(Value(copy.names[col_idx].GetIdentifierName())));
		children.push_back(make_uniq<BoundReferenceExpression>(copy.expected_types[col_idx], i));
		auto component = binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier("hive_partition_component"),
		                                           std::move(children));
		auto fits = BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_LESSTHANOREQUALTO, ApplyScalarFunction(context, "strlen", component->Copy()),
		    make_uniq<BoundConstantExpression>(Value::BIGINT(DuckLakeUtil::MAX_FILE_NAME_LENGTH)));
		auto hashed = ApplyScalarFunction(context, "md5", component->Copy());
		components.push_back(make_uniq<BoundCaseExpression>(std::move(fits), std::move(component), std::move(hashed)));
	}
	return binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier("path_join"), std::move(components));
}

unique_ptr<Expression> DuckLakePartitionUtils::ApplyScalarFunction(ClientContext &context, const string &function_name,
                                                                   unique_ptr<Expression> column_expr) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(column_expr));
	FunctionBinder binder(context);
	return binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(function_name), std::move(children));
}

static unique_ptr<Expression> BindBinaryOp(ClientContext &context, const string &op, unique_ptr<Expression> left,
                                           unique_ptr<Expression> right) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));
	FunctionBinder binder(context);
	return binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(op), std::move(children));
}

unique_ptr<Expression> DuckLakePartitionUtils::ApplyBucketTransform(ClientContext &context,
                                                                    unique_ptr<Expression> column_expr,
                                                                    idx_t bucket_count) {
	D_ASSERT(bucket_count > 0);

	// Iceberg-compatible: murmur3_x86_32 with seed 0
	auto hash_expr = ApplyScalarFunction(context, "murmur3_32", std::move(column_expr));

	// Iceberg bucket: (hash & Integer.MAX_VALUE) % N
	// Mask off sign bit to ensure non-negative result
	auto and_expr = BindBinaryOp(context, "&", std::move(hash_expr),
	                             make_uniq<BoundConstantExpression>(Value::INTEGER(NumericLimits<int32_t>::Maximum())));
	return BindBinaryOp(context, "%", std::move(and_expr),
	                    make_uniq<BoundConstantExpression>(Value::INTEGER(NumericCast<int32_t>(bucket_count))));
}

//! Types with the same bucket hash type give every value the same murmur3_32 hash
static LogicalType GetBucketHashType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		// hashed as a BIGINT like the unscaled value of a DECIMAL up to 18 digits
		return LogicalType::DECIMAL(Decimal::MAX_WIDTH_INT64, 0);
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		// hashed as their text like a wider DECIMAL
		return LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, 0);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type) <= Decimal::MAX_WIDTH_INT64 ? Decimal::MAX_WIDTH_INT64
		                                                                     : Decimal::MAX_WIDTH_DECIMAL;
		return LogicalType::DECIMAL(width, DecimalType::GetScale(type));
	}
	case LogicalTypeId::FLOAT:
		return LogicalType::DOUBLE;
	default:
		return type;
	}
}

bool DuckLakePartitionUtils::BucketChangesWithType(const LogicalType &source_type, const LogicalType &target_type) {
	return GetBucketHashType(source_type) != GetBucketHashType(target_type);
}

static unique_ptr<Expression> FloorNanosToMicros(ClientContext &context, unique_ptr<Expression> column_expr) {
	auto bigint_const = [](int64_t val) {
		return make_uniq<BoundConstantExpression>(Value::BIGINT(val));
	};
	auto nanos = DuckLakePartitionUtils::ApplyScalarFunction(context, "epoch_ns", std::move(column_expr));
	auto nanos_copy = nanos->Copy();
	auto remainder = BindBinaryOp(context, "%", std::move(nanos_copy), bigint_const(1000));
	auto shifted = BindBinaryOp(context, "+", std::move(remainder), bigint_const(1000));
	auto nonneg_remainder = BindBinaryOp(context, "%", std::move(shifted), bigint_const(1000));
	auto floored = BindBinaryOp(context, "-", std::move(nanos), std::move(nonneg_remainder));
	auto micros = BindBinaryOp(context, "//", std::move(floored), bigint_const(1000));
	return DuckLakePartitionUtils::ApplyScalarFunction(context, "make_timestamp", std::move(micros));
}

//! A TIMESTAMPTZ as the timestamp of its UTC instant, so its transforms do not depend on the session TimeZone
static unique_ptr<Expression> TimestampTZToUTC(ClientContext &context, unique_ptr<Expression> column_expr) {
	if (column_expr->GetReturnType().id() != LogicalTypeId::TIMESTAMP_TZ) {
		return column_expr;
	}
	column_expr = DuckLakePartitionUtils::ApplyScalarFunction(context, "epoch_us", std::move(column_expr));
	return DuckLakePartitionUtils::ApplyScalarFunction(context, "make_timestamp", std::move(column_expr));
}

static unique_ptr<Expression> ApplyEpochTransform(ClientContext &context, unique_ptr<Expression> column_expr,
                                                  DuckLakeTransformType transform_type) {
	auto source_id = column_expr->GetReturnType().id();
	if (source_id == LogicalTypeId::TIMESTAMP_NS || source_id == LogicalTypeId::TIMESTAMP_TZ_NS) {
		// the implicit cast to microseconds rounds, Iceberg requires flooring
		column_expr = FloorNanosToMicros(context, std::move(column_expr));
	} else {
		// Iceberg computes epoch transforms on the UTC instant
		column_expr = TimestampTZToUTC(context, std::move(column_expr));
	}
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value(GetEpochTransformPart(transform_type))));
	children.push_back(make_uniq<BoundConstantExpression>(Value::DATE(Date::FromDate(1970, 1, 1))));
	children.push_back(std::move(column_expr));
	FunctionBinder binder(context);
	return binder.BindScalarFunction(Identifier::DefaultSchema(), "date_diff", std::move(children));
}

static bool EpochTransformSupportsType(const LogicalType &type) {
	// date_diff also binds through an implicit cast, as from VARIANT, which then fails on values that are not dates
	return type.id() == LogicalTypeId::DATE || type.id() == LogicalTypeId::TIMESTAMP_TZ_NS ||
	       LogicalType::TypeIsTimestamp(type);
}

void DuckLakePartitionUtils::VerifyTransformSupportsType(ClientContext &context, const DuckLakePartitionField &field,
                                                         const string &column_name, const LogicalType &type) {
	bool supported = !IsEpochTransform(field.transform.type) || EpochTransformSupportsType(type);
	if (supported) {
		try {
			ApplyPartitionTransform(context, make_uniq<BoundReferenceExpression>(type, 0ULL), field);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() != ExceptionType::BINDER) {
				throw;
			}
			supported = false;
		}
	}
	if (!supported) {
		throw InvalidInputException("Cannot partition by %s(%s) - the transform does not support columns of type %s",
		                            GetTransformName(field.transform.type), column_name, type);
	}
}

unique_ptr<Expression> DuckLakePartitionUtils::ApplyPartitionTransform(ClientContext &context,
                                                                       unique_ptr<Expression> column_expr,
                                                                       const DuckLakePartitionField &field) {
	switch (field.transform.type) {
	case DuckLakeTransformType::IDENTITY:
		return column_expr;
	case DuckLakeTransformType::YEAR:
	case DuckLakeTransformType::MONTH:
	case DuckLakeTransformType::DAY:
	case DuckLakeTransformType::HOUR:
		return ApplyScalarFunction(context, GetTransformName(field.transform.type),
		                           TimestampTZToUTC(context, std::move(column_expr)));
	case DuckLakeTransformType::EPOCH_YEAR:
	case DuckLakeTransformType::EPOCH_MONTH:
	case DuckLakeTransformType::EPOCH_DAY:
	case DuckLakeTransformType::EPOCH_HOUR:
		return ApplyEpochTransform(context, std::move(column_expr), field.transform.type);
	case DuckLakeTransformType::BUCKET:
		return ApplyBucketTransform(context, std::move(column_expr), field.transform.bucket_count);
	default:
		throw NotImplementedException("Unsupported partition transform type");
	}
}

} // namespace duckdb
