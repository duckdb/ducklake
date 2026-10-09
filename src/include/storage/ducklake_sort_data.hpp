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

namespace duckdb {

class DuckLakeTableEntry;
class ParsedExpression;
struct OrderByNode;

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

	//! Replace the columns of a sort expression with the columns of the same fields in another schema version
	static void MapToSchemaVersion(unique_ptr<ParsedExpression> &expr, const DuckLakeTableEntry &current_table,
	                               const DuckLakeTableEntry &table);
	//! Build a SQL ORDER BY clause from the parsed sort orders, mapping inlined columns
	static string BuildSortOrderSQL(const vector<OrderByNode> &orders, const DuckLakeTableEntry &current_table,
	                                const DuckLakeTableEntry &inlined_table);
};

} // namespace duckdb
