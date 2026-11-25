//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_sort_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

class DuckLakeTableEntry;
class ClientContext;
struct BoundOrderByNode;

struct DuckLakeSort {
	idx_t sort_id = 0;
	vector<DuckLakeSortFieldInfo> fields;

	//! Build a SQL ORDER BY clause from the bound sort orders, mapping inlined columns
	static string BuildSortOrderSQL(ClientContext &context, const vector<BoundOrderByNode> &orders,
	                                const DuckLakeTableEntry &current_table, const DuckLakeTableEntry &inlined_table);
};

} // namespace duckdb
