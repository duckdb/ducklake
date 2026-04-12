#include "storage/ducklake_sort_data.hpp"

#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

namespace {
// Recursively replace column references in an expression with the renamed column names.
void ReplaceColumnRefs(ParsedExpression &expr, const case_insensitive_map_t<string> &rename_map) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto it = rename_map.find(colref.GetColumnName());
		if (it != rename_map.end()) {
			colref.column_names.back() = it->second;
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr,
	                                            [&](ParsedExpression &child) { ReplaceColumnRefs(child, rename_map); });
}
} // namespace

string DuckLakeSort::BuildSortOrderSQL(const DuckLakeSort &sort_data, const ColumnList &current_columns,
                                       const ColumnList &inlined_columns) {
	string result;
	for (auto &field : sort_data.fields) {
		if (field.dialect != "duckdb") {
			continue;
		}
		// Check if expression matches a column name in the current table, then map to inlined table's name by index
		string mapped_col;
		for (idx_t i = 0; i < current_columns.PhysicalColumnCount(); i++) {
			if (current_columns.GetColumn(PhysicalIndex(i)).Name() == field.expression) {
				if (i < inlined_columns.PhysicalColumnCount()) {
					mapped_col = inlined_columns.GetColumn(PhysicalIndex(i)).Name();
				}
				break;
			}
		}
		if (!result.empty()) {
			result += ", ";
		}
		if (!mapped_col.empty()) {
			result += StringUtil::Format("%s", SQLIdentifier(mapped_col));
		} else {
			// Expression sort key: parse it and remap any renamed column references so the ORDER BY runs correctly
			// against the inlined metadata table's original column names.
			auto parsed = Parser::ParseExpressionList(field.expression);
			D_ASSERT(parsed.size() == 1);
			idx_t mapped_count = MinValue(current_columns.PhysicalColumnCount(), inlined_columns.PhysicalColumnCount());
			// Map current column names to inlined column names, unqualified.
			case_insensitive_map_t<string> rename_map;
			for (idx_t i = 0; i < mapped_count; i++) {
				const auto &cur = current_columns.GetColumn(PhysicalIndex(i)).Name();
				const auto &inl = inlined_columns.GetColumn(PhysicalIndex(i)).Name();
				if (!StringUtil::CIEquals(cur, inl)) {
					rename_map[cur] = inl;
				}
			}
			if (!rename_map.empty()) {
				ReplaceColumnRefs(*parsed[0], rename_map);
			}
			result += parsed[0]->ToString();
		}
		result += (field.sort_direction == OrderType::ASCENDING) ? " ASC" : " DESC";
		result += (field.null_order == OrderByNullType::NULLS_FIRST) ? " NULLS FIRST" : " NULLS LAST";
	}
	return result;
}

} // namespace duckdb
