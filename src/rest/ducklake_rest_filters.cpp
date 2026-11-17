#include "rest/ducklake_rest_filters.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Filters to SQL
//===--------------------------------------------------------------------===//
static bool FindFieldPath(const DuckLakeFieldId &field, FieldIndex field_index, vector<string> &path) {
	path.push_back(field.Name().GetIdentifierName());
	if (field.GetFieldIndex() == field_index) {
		return true;
	}
	// only fields of structs can be referred to by name
	if (field.Type().id() == LogicalTypeId::STRUCT) {
		for (auto &child : field.Children()) {
			if (FindFieldPath(*child, field_index, path)) {
				return true;
			}
		}
	}
	path.pop_back();
	return false;
}

//! The SQL that refers to a (nested) field of the table, empty if it cannot be referred to
static string ColumnSQL(const DuckLakeTableEntry &table, idx_t field_index) {
	for (auto &field : table.GetFieldData().GetFieldIds()) {
		vector<string> path;
		if (FindFieldPath(*field, FieldIndex(field_index), path)) {
			vector<string> quoted;
			for (auto &name : path) {
				quoted.push_back(SQLIdentifier::ToString(name));
			}
			return StringUtil::Join(quoted, ".");
		}
	}
	return string(); // LCOV_EXCL_LINE: pushed down filters are on fields of the table
}

static string ComparisonOperator(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		return string();
	}
}

static string ConstantSQL(const Value &value) {
	return ConstantExpression::FromValue(value)->ToString();
}

//! The SQL of a single-column filter expression, empty if it cannot be expressed
static string FilterExpressionSQL(const Expression &expr, const string &column) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		ExpressionType comparison_type;
		auto constant =
		    ExpressionFilter::TryGetColumnConstantComparison(expr.Cast<BoundFunctionExpression>(), comparison_type);
		if (!constant) {
			return string();
		}
		auto op = ComparisonOperator(comparison_type);
		auto value = ConstantSQL(constant->GetValue());
		if (op.empty() || value.empty()) {
			return string();
		}
		return column + " " + op + " " + value;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_OPERATOR: {
		auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		if (children.empty() || !ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
			return string();
		}
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			return children.size() == 1 ? column + " IS NULL" : string();
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			return children.size() == 1 ? column + " IS NOT NULL" : string();
		case ExpressionType::COMPARE_IN: {
			vector<string> values;
			for (idx_t i = 1; i < children.size(); i++) {
				if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return string();
				}
				auto &constant = children[i]->Cast<BoundConstantExpression>().GetValue();
				// a NULL in an IN list never matches, but it stays part of the list as a plain NULL
				values.push_back(constant.IsNull() ? string("NULL") : ConstantSQL(constant));
			}
			return values.empty() ? string() : column + " IN (" + StringUtil::Join(values, ", ") + ")";
		}
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto is_or = expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
		vector<string> children;
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			auto child_sql = FilterExpressionSQL(*child, column);
			if (child_sql.empty()) {
				if (is_or) {
					// leaving out a part of an OR would prune files that match it
					return string();
				}
				continue;
			}
			children.push_back("(" + child_sql + ")");
		}
		return StringUtil::Join(children, is_or ? " OR " : " AND ");
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto optional_child = ExpressionFilter::GetOptionalFilterChild(expr);
		return optional_child ? FilterExpressionSQL(*optional_child, column) : string();
	}
	default:
		return string();
	}
}

static string ColumnFilterSQL(const ColumnFilterInfo &filter, const DuckLakeTableEntry &table) {
	if (!filter.table_filter || ExpressionFilter::GetOptionalDynamicFilterData(*filter.table_filter) ||
	    !filter.table_filter->expr) {
		return string();
	}
	auto column = ColumnSQL(table, filter.column_field_index);
	if (column.empty()) {
		return string(); // LCOV_EXCL_LINE
	}
	try {
		return FilterExpressionSQL(*filter.table_filter->expr, column);
	} catch (ConversionException &) { // LCOV_EXCL_START
		// a constant that cannot be printed, e.g. a timestamp outside the printable range
		return string();
	} // LCOV_EXCL_STOP
}

static string FilterNodeSQL(const DuckLakeFilterNode &node, const DuckLakeTableEntry &table) {
	switch (node.type) {
	case DuckLakeFilterNodeType::COLUMN_FILTER:
		return ColumnFilterSQL(*node.column_filter, table);
	case DuckLakeFilterNodeType::MATCH_NONE:
		return "false";
	case DuckLakeFilterNodeType::CONJUNCTION_AND:
	case DuckLakeFilterNodeType::CONJUNCTION_OR: {
		auto is_or = node.type == DuckLakeFilterNodeType::CONJUNCTION_OR;
		vector<string> children;
		for (auto &child : node.children) {
			auto child_sql = FilterNodeSQL(*child, table);
			if (child_sql.empty()) {
				if (is_or) {
					return string();
				}
				continue;
			}
			children.push_back("(" + child_sql + ")");
		}
		return StringUtil::Join(children, is_or ? " OR " : " AND ");
	}
	default:
		return string(); // LCOV_EXCL_LINE
	}
}

RESTScanFilters DuckLakeRESTFilters::Write(const FilterPushdownInfo &filters, const DuckLakeTableEntry &table) {
	RESTScanFilters result;
	for (auto &entry : filters.column_filters) {
		auto sql = ColumnFilterSQL(entry.second, table);
		if (!sql.empty()) {
			result.expressions.push_back(std::move(sql));
		}
	}
	for (auto &tree : filters.filter_trees) {
		auto sql = FilterNodeSQL(*tree.root, table);
		if (!sql.empty()) {
			result.expressions.push_back(std::move(sql));
		}
	}
	result.runtime_filter_columns = filters.runtime_filter_columns;
	for (auto &entry : filters.column_filters) {
		auto &filter = entry.second;
		if (!filter.table_filter) {
			continue;
		}
		if (filter.table_filter->expr &&
		    ExpressionFilter::ContainsInternalFunction(*filter.table_filter->expr, PrefixRangeScalarFun::NAME)) {
			// prefix ranges are evaluated against the min/max of the files at runtime
			result.runtime_filter_columns.push_back(filter.column_field_index);
		}
		auto dynamic_filter_data = ExpressionFilter::GetOptionalDynamicFilterData(*filter.table_filter);
		if (!dynamic_filter_data) {
			continue;
		}
		ExpressionType comparison_type;
		{
			annotated_shared_lock<annotated_shared_mutex> guard(dynamic_filter_data->lock);
			comparison_type = dynamic_filter_data->comparison_type;
		}
		// a top-N reads the files with the best values first
		bool seeking_high_values = comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
		                           comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		bool seeking_low_values = comparison_type == ExpressionType::COMPARE_LESSTHAN ||
		                          comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
		if (result.order_by.empty() && (seeking_high_values || seeking_low_values)) {
			RESTFileOrder order;
			order.function = seeking_high_values ? "max" : "min";
			order.column_id = filter.column_field_index;
			order.sort_order = seeking_high_values ? "DESC" : "ASC";
			result.order_by.push_back(std::move(order));
		}
		result.runtime_filter_columns.push_back(filter.column_field_index);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// SQL to Filters
//===--------------------------------------------------------------------===//
//! The field of the table that a column reference refers to
static optional_ptr<const DuckLakeFieldId> ResolveColumn(const DuckLakeTableEntry &table,
                                                         const ColumnRefExpression &column_ref) {
	auto &names = column_ref.ColumnNames();
	optional_ptr<const DuckLakeFieldId> field;
	for (auto &root : table.GetFieldData().GetFieldIds()) {
		if (root->Name() == names[0]) {
			field = root.get();
			break;
		}
	}
	for (idx_t i = 1; field && i < names.size(); i++) {
		if (field->Type().id() != LogicalTypeId::STRUCT) {
			return nullptr;
		}
		field = field->GetChildByName(names[i]);
	}
	return field;
}

//! Collects the fields an expression refers to, returns false if it refers to something that is not a field
static bool CollectFields(const DuckLakeTableEntry &table, const ParsedExpression &expr, set<idx_t> &field_indexes) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto field = ResolveColumn(table, expr.Cast<ColumnRefExpression>());
		if (!field) {
			return false;
		}
		field_indexes.insert(field->GetFieldIndex().index);
		return true;
	}
	bool valid = true;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { valid = valid && CollectFields(table, child, field_indexes); });
	return valid;
}

//! The value of a constant expression, NULL if it is not a constant
static Value FoldConstant(ClientContext &context, const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
		return expr.Cast<ConstantExpression>().GetLiteral().ToValue();
	case ExpressionClass::CAST: {
		auto &cast = expr.Cast<CastExpression>();
		auto child = FoldConstant(context, cast.Child());
		if (child.IsNull()) {
			return child;
		}
		auto target = TransformStringToLogicalType(cast.TargetType().ToString(), context);
		if (!cast.IsTryCast()) {
			return child.CastAs(context, target);
		}
		auto result = child.TryCastAs(context, target);
		return result ? std::move(*result) : Value(target);
	}
	default:
		return Value();
	}
}

//! Whether a cast to the type depends on the time zone of the session
static bool DependsOnTimeZone(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIME_TZ:
		return true;
	default:
		return false;
	}
}

//! Whether a comparison that casts the field to the common type compares as the values of the field do
static bool FieldKeepsOrder(const LogicalType &type, const LogicalType &common_type) {
	if (common_type == type) {
		return true;
	}
	// the cast keeps the order if it is invertible, but a cast to a type with a time zone is not one-to-one in every
	// zone, e.g. a skipped day
	return BoundCastExpression::CastIsInvertible(type, common_type) && !DependsOnTimeZone(common_type);
}

//! The constant as a value of the field, if it is one
static bool CastConstantToField(ClientContext &context, const Value &value, const LogicalType &type, Value &result) {
	if (value.type() == type) {
		result = value;
		return true;
	}
	auto casted = value.TryCastAs(context, type, nullptr, true);
	if (!casted) {
		return false;
	}
	// the constant must be a value of the field, e.g. not 1.25 for a DECIMAL(3,1)
	auto back = casted->TryCastAs(context, value.type());
	if (!back || !Value::NotDistinctFrom(*back, value)) {
		return false;
	}
	result = std::move(*casted);
	return true;
}

//! The constant as a value of the field, if DuckDB compares the two in the type of the field. The binder resolves
//! types of equal priority by the order of the operands, the field is on the left unless field_on_right is set
static bool TryCastToField(ClientContext &context, const Value &value, const LogicalType &type,
                           ExpressionType comparison_type, bool field_on_right, Value &result) {
	if (value.type() == type) {
		result = value;
		return true;
	}
	LogicalType common_type;
	auto &left_type = field_on_right ? value.type() : type;
	auto &right_type = field_on_right ? type : value.type();
	if (!BoundComparisonExpression::TryBindComparison(context, left_type, right_type, common_type, comparison_type) ||
	    !FieldKeepsOrder(type, common_type)) {
		return false;
	}
	return CastConstantToField(context, value, type, result);
}

class DuckLakeRESTFilterBinder {
public:
	DuckLakeRESTFilterBinder(ClientContext &context, const DuckLakeTableEntry &table) : context(context), table(table) {
	}

	//! A filter expression on a single field, with the field as the first input
	unique_ptr<Expression> BindColumnFilter(const ParsedExpression &expr, const DuckLakeFieldId &field) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::COMPARISON: {
			auto &comparison = expr.Cast<ComparisonExpression>();
			auto comparison_type = expr.GetExpressionType();
			if (ComparisonOperator(comparison_type).empty()) {
				return nullptr;
			}
			Value value;
			bool field_on_right = false;
			if (comparison.Left().GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				value = FoldConstant(context, comparison.Right());
			} else if (comparison.Right().GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				value = FoldConstant(context, comparison.Left());
				comparison_type = FlipComparisonExpression(comparison_type);
				field_on_right = true;
			}
			Value field_value;
			if (value.IsNull() ||
			    !TryCastToField(context, value, field.Type(), comparison_type, field_on_right, field_value)) {
				return nullptr;
			}
			auto column = make_uniq<BoundReferenceExpression>(field.Type(), 0ULL);
			return BoundComparisonExpression::Create(comparison_type, std::move(column),
			                                         make_uniq<BoundConstantExpression>(std::move(field_value)));
		}
		case ExpressionClass::OPERATOR: {
			auto &children = expr.Cast<OperatorExpression>().GetChildren();
			if (children.empty() || children[0]->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				return nullptr;
			}
			switch (expr.GetExpressionType()) {
			case ExpressionType::OPERATOR_IS_NULL:
			case ExpressionType::OPERATOR_IS_NOT_NULL:
				return ExpressionFilter::CreateNullCheckExpression(
				    make_uniq<BoundReferenceExpression>(field.Type(), 0ULL), expr.GetExpressionType());
			case ExpressionType::COMPARE_IN: {
				// DuckDB compares the field and the elements in the common type of the whole list
				vector<Value> constants;
				auto common_type = field.Type();
				for (idx_t i = 1; i < children.size(); i++) {
					// a NULL never matches, but an element that is not a constant can match any row
					if (children[i]->GetExpressionClass() == ExpressionClass::CONSTANT &&
					    children[i]->Cast<ConstantExpression>().GetLiteral().ToValue().IsNull()) {
						constants.emplace_back();
						continue;
					}
					auto value = FoldConstant(context, *children[i]);
					if (value.IsNull() ||
					    !BoundComparisonExpression::TryBindComparison(context, common_type, value.type(), common_type,
					                                                  ExpressionType::COMPARE_IN)) {
						return nullptr;
					}
					constants.push_back(std::move(value));
				}
				if (!FieldKeepsOrder(field.Type(), common_type)) {
					return nullptr;
				}
				vector<Value> values;
				for (auto &constant : constants) {
					Value field_value(field.Type());
					if (!constant.IsNull() && !CastConstantToField(context, constant, field.Type(), field_value)) {
						return nullptr;
					}
					values.push_back(std::move(field_value));
				}
				auto column = make_uniq<BoundReferenceExpression>(field.Type(), 0ULL);
				return ExpressionFilter::CreateInExpression(std::move(column), std::move(values));
			}
			default:
				return nullptr;
			}
		}
		case ExpressionClass::CONJUNCTION: {
			auto is_or = expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
			auto result = make_uniq<BoundConjunctionExpression>(expr.GetExpressionType());
			for (auto &child : expr.Cast<ConjunctionExpression>().GetChildren()) {
				auto bound_child = BindColumnFilter(*child, field);
				if (!bound_child) {
					if (is_or) {
						return nullptr;
					}
					continue;
				}
				result->GetChildrenMutable().push_back(std::move(bound_child));
			}
			if (result->GetChildren().empty()) {
				return nullptr;
			}
			if (result->GetChildren().size() == 1) {
				return std::move(result->GetChildrenMutable()[0]);
			}
			return std::move(result);
		}
		default:
			return nullptr;
		}
	}

	//! A filter on the fields of an expression, if it refers to a single field
	unique_ptr<ColumnFilterInfo> BindColumnFilter(const ParsedExpression &expr) {
		set<idx_t> field_indexes;
		if (!CollectFields(table, expr, field_indexes) || field_indexes.size() != 1) {
			return nullptr;
		}
		auto field = table.GetFieldData().GetByFieldIndex(FieldIndex(*field_indexes.begin()));
		if (!field) {
			return nullptr; // LCOV_EXCL_LINE: CollectFields found the field
		}
		auto bound = BindColumnFilter(expr, *field);
		if (!bound) {
			return nullptr;
		}
		return make_uniq<ColumnFilterInfo>(field->GetFieldIndex().index, field->Type(),
		                                   make_uniq<ExpressionFilter>(std::move(bound)));
	}

	//! A filter tree for an expression on several fields
	unique_ptr<DuckLakeFilterNode> BindFilterNode(const ParsedExpression &expr) {
		if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto value = FoldConstant(context, expr);
			if (value.type().id() == LogicalTypeId::BOOLEAN && !value.IsNull() && !BooleanValue::Get(value)) {
				return make_uniq<DuckLakeFilterNode>(DuckLakeFilterNodeType::MATCH_NONE);
			}
			return nullptr;
		}
		if (expr.GetExpressionClass() == ExpressionClass::CONJUNCTION) {
			auto is_or = expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
			auto result = make_uniq<DuckLakeFilterNode>(is_or ? DuckLakeFilterNodeType::CONJUNCTION_OR
			                                                  : DuckLakeFilterNodeType::CONJUNCTION_AND);
			for (auto &child : expr.Cast<ConjunctionExpression>().GetChildren()) {
				auto bound_child = BindFilterNode(*child);
				if (!bound_child) {
					if (is_or) {
						return nullptr;
					}
					continue;
				}
				result->children.push_back(std::move(bound_child));
			}
			if (result->children.empty()) {
				return nullptr;
			}
			return result;
		}
		auto column_filter = BindColumnFilter(expr);
		if (!column_filter) {
			return nullptr;
		}
		return make_uniq<DuckLakeFilterNode>(std::move(*column_filter));
	}

private:
	ClientContext &context;
	const DuckLakeTableEntry &table;
};

static void SplitConjuncts(unique_ptr<ParsedExpression> expr, vector<unique_ptr<ParsedExpression>> &result) {
	if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr->Cast<ConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			SplitConjuncts(child->Copy(), result);
		}
		return;
	}
	result.push_back(std::move(expr));
}

static void AddColumnFilter(FilterPushdownInfo &filters, ColumnFilterInfo filter) {
	auto entry = filters.column_filters.find(filter.column_field_index);
	if (entry == filters.column_filters.end()) {
		auto column_field_index = filter.column_field_index;
		filters.column_filters.emplace(column_field_index, std::move(filter));
		return;
	}
	// filters on the same field have to hold together
	auto &existing = entry->second;
	auto combined = make_uniq<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_AND, std::move(existing.table_filter->expr), std::move(filter.table_filter->expr));
	existing.table_filter = make_uniq<ExpressionFilter>(std::move(combined));
}

//! An order of the files: the minimum or the maximum of a column in the file, with a direction
static void ReadFileOrder(const DuckLakeTableEntry &table, const RESTFileOrder &order, FilterPushdownInfo &filters) {
	if (order.function != "min" && order.function != "max") {
		return;
	}
	auto field = table.GetFieldData().GetByFieldIndex(FieldIndex(order.column_id));
	if (!field) {
		return;
	}
	filters.file_order.push_back({order.column_id, order.function == "max", order.sort_order == "DESC", field->Type()});
}

unique_ptr<FilterPushdownInfo> DuckLakeRESTFilters::Read(ClientContext &context, const RESTScanFilters &filters,
                                                         const DuckLakeTableEntry &table) {
	auto result = make_uniq<FilterPushdownInfo>();
	DuckLakeRESTFilterBinder binder(context, table);
	for (auto &expression : filters.expressions) {
		vector<unique_ptr<ParsedExpression>> conjuncts;
		try {
			auto parsed = Parser::GetBuiltinParser().ParseExpressionList(expression);
			for (auto &expr : parsed) {
				SplitConjuncts(std::move(expr), conjuncts);
			}
		} catch (std::exception &) {
			continue;
		}
		for (auto &conjunct : conjuncts) {
			auto column_filter = binder.BindColumnFilter(*conjunct);
			if (column_filter) {
				AddColumnFilter(*result, std::move(*column_filter));
				continue;
			}
			auto root = binder.BindFilterNode(*conjunct);
			if (!root) {
				continue;
			}
			DuckLakeFilterTree tree;
			tree.root = std::move(root);
			tree.source = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
			result->filter_trees.push_back(std::move(tree));
		}
	}
	result->runtime_filter_columns = filters.runtime_filter_columns;
	for (auto &order : filters.order_by) {
		ReadFileOrder(table, order, *result);
	}
	return result;
}

} // namespace duckdb
