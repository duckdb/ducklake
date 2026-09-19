#include "storage/ducklake_sort_data.hpp"

#include "duckdb/parser/column_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

// Round-trip user sort expressions through the parser so non-bare-column expressions (e.g.
// `(id + 0)`) survive into the deletes-position query.

namespace {

// VisitExpressionMutable only exposes a reference, so it cannot swap a column ref for a cast
void RewriteColumnRefs(unique_ptr<ParsedExpression> &expr,
                       const std::function<void(unique_ptr<ParsedExpression> &)> &rewrite) {
	if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		rewrite(expr);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { RewriteColumnRefs(child, rewrite); });
}

} // namespace

// FIXME: TODO: Macros and other user-catalog references will fail at bind time on the metadata connection
string DuckLakeSort::BuildSortOrderSQL(const DuckLakeSort &sort_data, const ColumnList &current_columns,
                                       const ColumnList &inlined_columns, DuckLakeMetadataManager &metadata_manager) {
	// Build rename map: current physical name -> inlined physical name (only entries that differ).
	case_insensitive_map_t<string> rename_map;
	case_insensitive_map_t<LogicalType> type_map;
	bool any_cast = false;
	auto column_count = MinValue(current_columns.PhysicalColumnCount(), inlined_columns.PhysicalColumnCount());
	for (idx_t i = 0; i < column_count; i++) {
		auto &current_column = current_columns.GetColumn(PhysicalIndex(i));
		auto &current_name = current_column.Name();
		auto &inlined_name = inlined_columns.GetColumn(PhysicalIndex(i)).Name();
		if (current_name.GetIdentifierName() != inlined_name.GetIdentifierName()) {
			rename_map[current_name.GetIdentifierName()] = inlined_name.GetIdentifierName();
		}
		auto &type = current_column.Type();
		type_map[current_name.GetIdentifierName()] = type;
		if (!any_cast) {
			auto probe = SQLIdentifier::ToString(current_name.GetIdentifierName());
			any_cast = metadata_manager.CastInlinedColumnToTarget(probe, type) != probe;
		}
	}

	string result;
	for (auto &field : sort_data.fields) {
		if (field.dialect != "duckdb") {
			continue;
		}
		if (!result.empty()) {
			result += ", ";
		}
		if (rename_map.empty() && !any_cast) {
			result += field.expression;
		} else {
			auto parsed = Parser::ParseExpressionList(field.expression);
			D_ASSERT(parsed.size() == 1);
			RewriteColumnRefs(parsed[0], [&](unique_ptr<ParsedExpression> &child) {
				auto &colref = child->Cast<ColumnRefExpression>();
				auto name = colref.GetColumnName().GetIdentifierName();
				auto rename_entry = rename_map.find(name);
				if (rename_entry != rename_map.end()) {
					colref.ColumnNamesMutable().back() = Identifier(rename_entry->second);
				}
				auto type_entry = type_map.find(name);
				if (type_entry == type_map.end()) {
					return;
				}
				auto rendered = colref.ToString();
				auto cast = metadata_manager.CastInlinedColumnToTarget(rendered, type_entry->second);
				if (cast == rendered) {
					return;
				}
				auto cast_expr = Parser::ParseExpressionList(cast);
				D_ASSERT(cast_expr.size() == 1);
				child = std::move(cast_expr[0]);
			});
			result += parsed[0]->ToString();
		}
		result += (field.sort_direction == OrderType::ASCENDING) ? " ASC" : " DESC";
		result += (field.null_order == OrderByNullType::NULLS_FIRST) ? " NULLS FIRST" : " NULLS LAST";
	}
	return result;
}

} // namespace duckdb
