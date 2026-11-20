//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
class ClientContext;

//! The tables that the REST API sends as Arrow IPC streams: the rows of the columns of a table
struct RESTArrow {
	//! The Arrow IPC stream of rows. Types that Arrow does not have (UUID, HUGEINT, GEOMETRY, ...) are extension types
	static string Write(ClientContext &context, const vector<string> &names, const ColumnDataCollection &rows);
	//! Reads the rows of an Arrow IPC stream from a region of the data. The columns are cast to the types, if there
	//! are any
	static unique_ptr<ColumnDataCollection> Read(ClientContext &context, shared_ptr<const string> data, idx_t offset,
	                                             idx_t size, const vector<LogicalType> &types, vector<string> &names);
};

} // namespace duckdb
