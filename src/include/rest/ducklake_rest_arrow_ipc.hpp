//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_arrow_ipc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! An Arrow IPC stream that was read: the schema of a table and its record batches
struct RESTArrowStream {
	RESTArrowStream();
	~RESTArrowStream();
	RESTArrowStream(const RESTArrowStream &) = delete;
	RESTArrowStream &operator=(const RESTArrowStream &) = delete;

	//! A struct schema with a child per column
	ArrowSchema schema;
	//! The batches, as struct arrays with a child per column
	vector<ArrowArray> batches;
};

//! A writer and a reader of Arrow IPC streams. They handle the types that DuckDB exports to Arrow without extension
//! types, dictionaries and views.
struct RESTArrowIPC {
	//! The Arrow IPC stream of a table: the schema is a struct with a child per column, the batches are struct arrays
	static string Write(const ArrowSchema &schema, const vector<ArrowArray> &batches);
	//! Reads a stream from a region of the data. The arrays of the stream refer to the data
	static unique_ptr<RESTArrowStream> Read(shared_ptr<const string> data, idx_t offset, idx_t size);
};

} // namespace duckdb
