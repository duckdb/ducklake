//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_sort_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "common/index.hpp"
#include "duckdb/common/enums/order_type.hpp"

#include <functional>

namespace duckdb {

class ColumnDefinition;
class DuckLakeFieldId;
class DuckLakeTableEntry;
class ParsedExpression;
struct OrderByNode;

using SortColumnMapper = std::function<void(unique_ptr<ParsedExpression> &expr, const ColumnDefinition &column,
                                            const DuckLakeFieldId &field)>;

struct DuckLakeSortField {
	idx_t sort_key_index = 0;
	string expression;
	string dialect;
	OrderType sort_direction;
	OrderByNullType null_order;
};

struct DuckLakeSort {
	idx_t sort_id = 0;
	vector<DuckLakeSortField> fields;

	//! Map the columns of a sort expression onto another schema version, columns it lacks become their default
	static void MapToSchemaVersion(unique_ptr<ParsedExpression> &expr, const DuckLakeTableEntry &current_table,
	                               const DuckLakeTableEntry &table, const SortColumnMapper &map_column);
	//! Build a SQL ORDER BY clause from the parsed sort orders, mapping inlined columns
	static string BuildSortOrderSQL(const vector<OrderByNode> &orders, const DuckLakeTableEntry &current_table,
	                                const DuckLakeTableEntry &inlined_table);
};

} // namespace duckdb
