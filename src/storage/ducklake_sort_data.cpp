#include "storage/ducklake_sort_data.hpp"

#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

//! Replace the column references of an exported sort expression with the inlined column of the same field
static void MapToInlinedColumns(unique_ptr<ParsedExpression> &expr, const DuckLakeTableEntry &current_table,
                                const DuckLakeTableEntry &inlined_table) {
	if (expr->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
			MapToInlinedColumns(child, current_table, inlined_table);
		});
		return;
	}
	auto &column_ref = expr->Cast<ColumnRefExpression>();
	if (!column_ref.IsQualified()) {
		// the exported columns are qualified, so this is a lambda parameter
		return;
	}
	auto &column = current_table.GetColumns().GetColumn(column_ref.GetColumnName());
	auto &field_id = current_table.GetFieldId(column.Physical());
	auto inlined_field_id = inlined_table.GetFieldId(field_id.GetFieldIndex());
	if (inlined_field_id) {
		expr = make_uniq<ColumnRefExpression>(Identifier(inlined_field_id->Name()),
		                                      Identifier(DuckLakeMetadataManager::INLINED_FLUSH_ALIAS));
	} else {
		// the inlined rows were written before this column existed
		expr = field_id.GetInitialDefault();
	}
}

//! The bound sort expression as SQL with its macros expanded, or nullptr when it has no SQL form
static unique_ptr<ParsedExpression> ExportSortExpression(ClientContext &context, const Expression &expression,
                                                         const DuckLakeTableEntry &current_table) {
	BoundExpressionSQLExportContext export_context;
	export_context.client_context = &context;
	export_context.resolve_binding = [&](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		auto &column = current_table.GetColumns().GetColumn(LogicalIndex(binding.column_index.GetIndex()));
		vector<Identifier> names {Identifier(DuckLakeMetadataManager::INLINED_FLUSH_ALIAS), column.Name()};
		return ResolvedSQLColumnReference {std::move(names), column.Type(), {}};
	};
	auto exported = BoundExpressionSQLExporter::Export(expression, export_context);
	if (exported.HasError()) {
		return nullptr;
	}
	return std::move(exported.GetValue());
}

string DuckLakeSort::BuildSortOrderSQL(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                       const DuckLakeTableEntry &current_table,
                                       const DuckLakeTableEntry &inlined_table) {
	vector<string> result;
	for (auto &order : orders) {
		// the metadata connection cannot see the macros of the table, so they are expanded here
		auto expression = ExportSortExpression(context, *order.expression, current_table);
		if (!expression) {
			return string();
		}
		MapToInlinedColumns(expression, current_table, inlined_table);
		auto null_order = order.null_order == OrderByNullType::NULLS_FIRST ? OrderByNullType::NULLS_FIRST
		                                                                   : OrderByNullType::NULLS_LAST;
		result.push_back(OrderByNode(order.type, null_order, std::move(expression)).ToString());
	}
	return StringUtil::Join(result, ", ");
}

} // namespace duckdb
