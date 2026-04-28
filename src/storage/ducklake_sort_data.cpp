#include "storage/ducklake_sort_data.hpp"

#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception_format_value.hpp"

namespace duckdb {

string DuckLakeSort::BuildSortOrderSQL(const DuckLakeSort &sort_data, const ColumnList &current_columns,
                                       const ColumnList &inlined_columns) {
	string result;
	for (auto &field : sort_data.fields) {
		if (field.dialect != "duckdb") {
			continue;
		}
		// Parse the expression to extract the unquoted column name
		auto parsed = Parser::ParseExpressionList(field.expression);
		if (parsed.empty() || parsed[0]->type != ExpressionType::COLUMN_REF) {
			return string();
		}
		auto &colref = parsed[0]->Cast<ColumnRefExpression>();
		auto &col_name = colref.GetColumnName();

		// Check if column name matches a column in the current table, then map to inlined table's name by index
		string mapped_col;
		for (idx_t i = 0; i < current_columns.PhysicalColumnCount(); i++) {
			if (StringUtil::CIEquals(current_columns.GetColumn(PhysicalIndex(i)).Name(), col_name)) {
				if (i < inlined_columns.PhysicalColumnCount()) {
					mapped_col = inlined_columns.GetColumn(PhysicalIndex(i)).Name();
				}
				break;
			}
		}
		if (mapped_col.empty()) {
			// Not a simple column reference or column not found
			return string();
		}
		if (!result.empty()) {
			result += ", ";
		}
		result += StringUtil::Format("%s", SQLIdentifier(mapped_col));
		result += (field.sort_direction == OrderType::ASCENDING) ? " ASC" : " DESC";
		result += (field.null_order == OrderByNullType::NULLS_FIRST) ? " NULLS FIRST" : " NULLS LAST";
	}
	return result;
}

} // namespace duckdb
