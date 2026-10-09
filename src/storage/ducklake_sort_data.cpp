#include "storage/ducklake_sort_data.hpp"

#include "storage/ducklake_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"

namespace duckdb {

//! Replace the column references of a sort expression with the inlined column of the same field
static void MapToInlinedColumns(unique_ptr<ParsedExpression> &expr, const DuckLakeTableEntry &current_table,
                                const DuckLakeTableEntry &inlined_table) {
	auto expression_class = expr->GetExpressionClass();
	if (expression_class != ExpressionClass::POSITIONAL_REFERENCE && expression_class != ExpressionClass::COLUMN_REF) {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
			MapToInlinedColumns(child, current_table, inlined_table);
		});
		return;
	}
	auto &columns = current_table.GetColumns();
	auto &column = expression_class == ExpressionClass::POSITIONAL_REFERENCE
	                   ? columns.GetColumn(LogicalIndex(expr->Cast<PositionalReferenceExpression>().Index() - 1))
	                   : columns.GetColumn(expr->Cast<ColumnRefExpression>().GetColumnName());
	auto &field_id = current_table.GetFieldId(column.Physical());
	auto inlined_field_id = inlined_table.GetFieldId(field_id.GetFieldIndex());
	if (inlined_field_id) {
		expr = make_uniq<ColumnRefExpression>(Identifier(inlined_field_id->Name()));
	} else {
		// the inlined rows were written before this column existed
		expr = field_id.GetInitialDefault();
	}
}

// FIXME: macros and other user catalog references fail to bind on the metadata connection
string DuckLakeSort::BuildSortOrderSQL(const vector<OrderByNode> &orders, const DuckLakeTableEntry &current_table,
                                       const DuckLakeTableEntry &inlined_table) {
	string result;
	for (auto &order : orders) {
		if (!result.empty()) {
			result += ", ";
		}
		auto expression = order.expression->Copy();
		MapToInlinedColumns(expression, current_table, inlined_table);
		result += expression->ToString();
		result += (order.type == OrderType::ASCENDING) ? " ASC" : " DESC";
		result += (order.null_order == OrderByNullType::NULLS_FIRST) ? " NULLS FIRST" : " NULLS LAST";
	}
	return result;
}

} // namespace duckdb
