#include "storage/ducklake_sort_data.hpp"

#include "storage/ducklake_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"

namespace duckdb {

static void MapToSchemaVersionRecursive(unique_ptr<ParsedExpression> &expr, const DuckLakeTableEntry &current_table,
                                        const DuckLakeTableEntry &table, const SortColumnMapper &map_column,
                                        vector<identifier_set_t> &lambda_params) {
	auto expression_class = expr->GetExpressionClass();
	if (expression_class == ExpressionClass::LAMBDA) {
		auto &lambda = expr->Cast<LambdaExpression>();
		if (lambda.CopiedExprMutable()) {
			MapToSchemaVersionRecursive(lambda.CopiedExprMutable(), current_table, table, map_column, lambda_params);
		}
		string error_message;
		identifier_set_t parameters;
		for (auto &parameter : lambda.ExtractColumnRefExpressions(error_message)) {
			parameters.insert(parameter.get().Cast<ColumnRefExpression>().GetColumnName());
		}
		lambda_params.push_back(std::move(parameters));
		MapToSchemaVersionRecursive(lambda.RightMutable(), current_table, table, map_column, lambda_params);
		lambda_params.pop_back();
		return;
	}
	if (expression_class != ExpressionClass::POSITIONAL_REFERENCE && expression_class != ExpressionClass::COLUMN_REF) {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
			MapToSchemaVersionRecursive(child, current_table, table, map_column, lambda_params);
		});
		return;
	}
	if (expression_class == ExpressionClass::COLUMN_REF &&
	    LambdaExpression::IsLambdaParameter(lambda_params, expr->Cast<ColumnRefExpression>().GetColumnName())) {
		return;
	}
	auto &columns = current_table.GetColumns();
	auto &column = expression_class == ExpressionClass::POSITIONAL_REFERENCE
	                   ? columns.GetColumn(LogicalIndex(expr->Cast<PositionalReferenceExpression>().Index() - 1))
	                   : columns.GetColumn(expr->Cast<ColumnRefExpression>().GetColumnName());
	auto &field_id = current_table.GetFieldId(column.Physical());
	auto version_field_id = table.GetFieldId(field_id.GetFieldIndex());
	if (!version_field_id) {
		// the rows were written before this column existed
		expr = field_id.GetInitialDefault();
		expr->SetAlias(column.Name());
		return;
	}
	map_column(expr, column, *version_field_id);
}

void DuckLakeSort::MapToSchemaVersion(unique_ptr<ParsedExpression> &expr, const DuckLakeTableEntry &current_table,
                                      const DuckLakeTableEntry &table, const SortColumnMapper &map_column) {
	vector<identifier_set_t> lambda_params;
	MapToSchemaVersionRecursive(expr, current_table, table, map_column, lambda_params);
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
		MapToSchemaVersion(expression, current_table, inlined_table,
		                   [](unique_ptr<ParsedExpression> &column_ref, const ColumnDefinition &,
		                      const DuckLakeFieldId &inlined_field_id) {
			                   column_ref = make_uniq<ColumnRefExpression>(Identifier(inlined_field_id.Name()));
		                   });
		result += expression->ToString();
		result += (order.type == OrderType::ASCENDING) ? " ASC" : " DESC";
		result += (order.null_order == OrderByNullType::NULLS_FIRST) ? " NULLS FIRST" : " NULLS LAST";
	}
	return result;
}

} // namespace duckdb
