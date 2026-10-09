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

class ClientContext;
class DuckLakeTableEntry;
struct BoundOrderByNode;

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

	//! Build a SQL ORDER BY clause over the inlined flush source, or nothing when a sort key has no SQL form
	static string BuildSortOrderSQL(ClientContext &context, const vector<BoundOrderByNode> &orders,
	                                const DuckLakeTableEntry &current_table, const DuckLakeTableEntry &inlined_table);
};

} // namespace duckdb
