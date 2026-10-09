#include "storage/ducklake_sort_data.hpp"

#include "storage/ducklake_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

//! Replace the column references of a sort expression with the inlined column of the same field
static void MapToInlinedColumns(unique_ptr<ParsedExpression> &expr, const DuckLakeTableEntry &current_table,
                                const DuckLakeTableEntry &inlined_table) {
	if (expr->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
			MapToInlinedColumns(child, current_table, inlined_table);
		});
		return;
	}
	const auto &column_ref = expr->Cast<ColumnRefExpression>();
	if (!column_ref.IsQualified()) {
		// Lambda parameters are not table columns.
		return;
	}
	const auto &column = current_table.GetColumns().GetColumn(column_ref.GetColumnName());
	auto &field_id = current_table.GetFieldId(column.Physical());
	auto inlined_field_id = inlined_table.GetFieldId(field_id.GetFieldIndex());
	if (inlined_field_id) {
		expr = make_uniq<ColumnRefExpression>(Identifier(inlined_field_id->Name()), Identifier("inlined_data"));
	} else {
		// the inlined rows were written before this column existed
		expr = field_id.GetInitialDefault();
	}
}

string DuckLakeSort::BuildSortOrderSQL(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                       const DuckLakeTableEntry &current_table,
                                       const DuckLakeTableEntry &inlined_table) {
	BoundExpressionSQLExportContext export_context;
	export_context.client_context = &context;
	export_context.resolve_binding = [&](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		const auto &column = current_table.GetColumns().GetColumn(LogicalIndex(binding.column_index.GetIndex()));
		return ResolvedSQLColumnReference {{Identifier("inlined_data"), column.Name()}, column.Type(), {}};
	};
	string result;
	for (const auto &order : orders) {
		if (!result.empty()) {
			result += ", ";
		}
		// Export the bound expression so macros are expanded in the user's transaction.
		auto exported = BoundExpressionSQLExporter::Export(*order.expression, export_context);
		if (exported.HasError()) {
			throw NotImplementedException("Cannot export DuckLake sort expression for inline flush: %s",
			                              exported.GetIssues()[0].message);
		}
		auto expression = std::move(exported.GetValue());
		MapToInlinedColumns(expression, current_table, inlined_table);
		result += expression->ToString();
		result += (order.type == OrderType::ASCENDING) ? " ASC" : " DESC";
		result += (order.null_order == OrderByNullType::NULLS_FIRST) ? " NULLS FIRST" : " NULLS LAST";
	}
	return result;
}

} // namespace duckdb
