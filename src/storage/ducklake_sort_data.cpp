#include "storage/ducklake_sort_data.hpp"

#include "duckdb/parser/column_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception_format_value.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

// Round-trip user sort expressions through the parser so non-bare-column expressions (e.g.
// `(id + 0)`) survive into the deletes-position query.

// FIXME: TODO: Macros and other user-catalog references will fail at bind time on the metadata connection
string DuckLakeSort::BuildSortOrderSQL(const DuckLakeSort &sort_data, const ColumnList &current_columns,
                                       const ColumnList &inlined_columns) {
	// Build rename map: current physical name -> inlined physical name (only entries that differ).
	case_insensitive_map_t<string> rename_map;
	auto column_count = MinValue(current_columns.PhysicalColumnCount(), inlined_columns.PhysicalColumnCount());
	for (idx_t i = 0; i < column_count; i++) {
		auto &current_name = current_columns.GetColumn(PhysicalIndex(i)).Name();
		auto &inlined_name = inlined_columns.GetColumn(PhysicalIndex(i)).Name();
		if (current_name.GetIdentifierName() != inlined_name.GetIdentifierName()) {
			rename_map[current_name.GetIdentifierName()] = inlined_name.GetIdentifierName();
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
		// Only re-parse + rewrite when columns were renamed between the inlined-table
		// write and the flush.
		if (rename_map.empty()) {
			result += field.expression;
		} else {
			auto parsed = Parser::ParseExpressionList(field.expression);
			D_ASSERT(parsed.size() == 1);
			ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(
			    *parsed[0], [&](ColumnRefExpression &colref) {
				    auto entry = rename_map.find(colref.GetColumnName().GetIdentifierName());
				    if (entry != rename_map.end()) {
					    colref.ColumnNamesMutable().back() = Identifier(entry->second);
				    }
			    });
			result += parsed[0]->ToString();
		}
		result += (field.sort_direction == OrderType::ASCENDING) ? " ASC" : " DESC";
		result += (field.null_order == OrderByNullType::NULLS_FIRST) ? " NULLS FIRST" : " NULLS LAST";
	}
	return result;
}

} // namespace duckdb
