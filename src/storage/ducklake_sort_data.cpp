#include "storage/ducklake_sort_data.hpp"

#include "duckdb/parser/column_list.hpp"
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
