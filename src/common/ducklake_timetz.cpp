#include "common/ducklake_timetz.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "storage/ducklake_stats.hpp"

namespace duckdb {

static constexpr int64_t DAY = Interval::MICROS_PER_DAY;
static constexpr int64_t SECOND = Interval::MICROS_PER_SEC;

static unique_ptr<Expression> BindFunction(ClientContext &context, const string &name,
                                           vector<unique_ptr<Expression>> children) {
	FunctionBinder binder(context);
	return binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(name), std::move(children));
}

//! Wraps a TIMETZ in the collation that DuckDB compares it with
static unique_ptr<Expression> CollateTimeTZ(ClientContext &context, unique_ptr<Expression> expr) {
	ExpressionBinder::PushCollation(context, expr, LogicalType::TIME_TZ);
	return expr;
}

//! The TIMETZ with the given offset at the end of the UTC day
static Value AtEndOfDay(int32_t offset) {
	return Value::TIMETZ(dtime_tz_t(dtime_t(DAY + offset * SECOND), offset));
}

LogicalType DuckLakeTimeTZ::StorageType() {
	child_list_t<LogicalType> fields;
	fields.emplace_back(TIME_FIELD, LogicalType::TIME);
	fields.emplace_back(OFFSET_FIELD, LogicalType::INTEGER);
	return LogicalType::STRUCT(std::move(fields));
}

unique_ptr<Expression> DuckLakeTimeTZ::ToStorage(ClientContext &context, unique_ptr<Expression> expr) {
	// the UTC time is the time at offset +00
	vector<unique_ptr<Expression>> utc_children;
	utc_children.push_back(make_uniq<BoundConstantExpression>(Value::INTERVAL(interval_t())));
	utc_children.push_back(expr->Copy());
	auto utc_time = BoundCastExpression::AddCastToType(
	    context, BindFunction(context, "timezone", std::move(utc_children)), LogicalType::TIME);
	// timezone wraps the end of the day to its start, so the values at that instant store the end of the day
	auto at_end = BoundBetweenExpression::Create(
	    CollateTimeTZ(context, expr->Copy()), CollateTimeTZ(context, make_uniq<BoundConstantExpression>(AtEndOfDay(0))),
	    CollateTimeTZ(context, make_uniq<BoundConstantExpression>(AtEndOfDay(dtime_tz_t::MIN_OFFSET))), true, true);
	utc_time = make_uniq<BoundCaseExpression>(
	    std::move(at_end), make_uniq<BoundConstantExpression>(Value::TIME(dtime_t(DAY))), std::move(utc_time));
	vector<unique_ptr<Expression>> offset_children;
	offset_children.push_back(expr->Copy());
	auto offset = BoundCastExpression::AddCastToType(
	    context, BindFunction(context, "timezone", std::move(offset_children)), LogicalType::INTEGER);
	utc_time->SetAlias(Identifier(TIME_FIELD));
	offset->SetAlias(Identifier(OFFSET_FIELD));
	vector<unique_ptr<Expression>> fields;
	fields.push_back(std::move(utc_time));
	fields.push_back(std::move(offset));
	// a NULL TIMETZ is a NULL struct, so that NULL filters on the file see it
	auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
	is_null->GetChildrenMutable().push_back(std::move(expr));
	return make_uniq<BoundCaseExpression>(std::move(is_null), make_uniq<BoundConstantExpression>(Value(StorageType())),
	                                      BindFunction(context, "struct_pack", std::move(fields)));
}

static unique_ptr<Expression> ExtractField(ClientContext &context, unique_ptr<Expression> expr, const char *field) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(expr));
	children.push_back(make_uniq<BoundConstantExpression>(Value(field)));
	return BindFunction(context, "struct_extract", std::move(children));
}

unique_ptr<Expression> DuckLakeTimeTZ::FromStorage(ClientContext &context, unique_ptr<Expression> expr) {
	auto utc_time = ExtractField(context, expr->Copy(), TIME_FIELD);
	auto offset = ExtractField(context, std::move(expr), OFFSET_FIELD);
	vector<unique_ptr<Expression>> interval_children;
	interval_children.push_back(BoundCastExpression::AddCastToType(context, offset->Copy(), LogicalType::DOUBLE));
	vector<unique_ptr<Expression>> children;
	children.push_back(BindFunction(context, "to_seconds", std::move(interval_children)));
	// the default cast keeps offset +00, where the cast of ICU takes the session time zone
	children.push_back(BoundCastExpression::AddDefaultCastToType(utc_time->Copy(), LogicalType::TIME_TZ));
	auto local_time = BindFunction(context, "timezone", std::move(children));
	// timezone wraps the end of the day to its start, so a value at offset +00 keeps its UTC time
	auto at_end = make_uniq<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_AND,
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(utc_time),
	                                      make_uniq<BoundConstantExpression>(Value::TIME(dtime_t(DAY)))),
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(offset),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(0))));
	return make_uniq<BoundCaseExpression>(std::move(at_end), make_uniq<BoundConstantExpression>(AtEndOfDay(0)),
	                                      std::move(local_time));
}

void DuckLakeTimeTZ::ReplaceStorageCasts(ClientContext &context, unique_ptr<Expression> &expr) {
	if (BoundCastExpression::IsCast(*expr)) {
		auto &cast = expr->Cast<BoundFunctionExpression>();
		if (BoundCastExpression::TargetType(cast).id() == LogicalTypeId::TIME_TZ &&
		    BoundCastExpression::SourceType(cast) == StorageType()) {
			expr = FromStorage(context, std::move(BoundCastExpression::ChildMutable(cast)));
			return;
		}
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { ReplaceStorageCasts(context, child); });
}

void DuckLakeTimeTZ::SetBounds(DuckLakeColumnStats &time_stats, const DuckLakeColumnStats &offset_stats) {
	if (!time_stats.has_min || !time_stats.has_max || !offset_stats.has_min || !offset_stats.has_max) {
		time_stats.has_min = false;
		time_stats.has_max = false;
		return;
	}
	// a TIMETZ orders by its local time minus its offset, which is its UTC time a day earlier or later when the local
	// time wraps around midnight
	auto utc_min = Value(time_stats.min).DefaultCastAs(LogicalType::TIME).GetValue<dtime_t>().value;
	auto utc_max = Value(time_stats.max).DefaultCastAs(LogicalType::TIME).GetValue<dtime_t>().value;
	auto offset_min = Value(offset_stats.min).DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	auto offset_max = Value(offset_stats.max).DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	int64_t lowest = utc_min;
	// only a positive offset moves a local time past the end of the day
	if (offset_max > 0 && utc_max + offset_max * SECOND >= DAY) {
		lowest = MaxValue<int64_t>(utc_min, DAY - offset_max * SECOND) - DAY;
	}
	int64_t highest = utc_max;
	if (utc_min + offset_min * SECOND < 0) {
		highest = MinValue<int64_t>(utc_max, -offset_min * SECOND - 1) + DAY;
	}
	// among equal instants a higher offset orders first, so the bounds take the most extreme offset they can hold
	auto low_offset = MinValue<int64_t>(dtime_tz_t::MAX_OFFSET, (DAY - 1 - lowest) / SECOND);
	auto high_offset = MaxValue<int64_t>(dtime_tz_t::MIN_OFFSET, -(highest / SECOND));
	auto low = dtime_tz_t(dtime_t(lowest + low_offset * SECOND), static_cast<int32_t>(low_offset));
	auto high = dtime_tz_t(dtime_t(highest + high_offset * SECOND), static_cast<int32_t>(high_offset));
	time_stats.min = Value::TIMETZ(low).ToString();
	time_stats.max = Value::TIMETZ(high).ToString();
	time_stats.min_is_exact = false;
	time_stats.max_is_exact = false;
}

} // namespace duckdb
