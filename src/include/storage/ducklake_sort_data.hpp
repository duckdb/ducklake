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

class ColumnList;

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

	//! Build a SQL ORDER BY clause from the sort fields, mapping inlined columns
	static string BuildSortOrderSQL(const DuckLakeSort &sort_data, const ColumnList &current_columns,
	                                const ColumnList &inlined_columns);
};

} // namespace duckdb
