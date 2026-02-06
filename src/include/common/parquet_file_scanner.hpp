//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/parquet_file_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

//! Utility class for scanning parquet files directly
class ParquetFileScanner {
public:
	ParquetFileScanner(ClientContext &context, const DuckLakeFileData &file);
	ParquetFileScanner(ClientContext &context, const DuckLakeFileData &file,
	                   table_function_get_multi_file_reader_t multi_file_reader_creator,
	                   shared_ptr<TableFunctionInfo> function_info = nullptr);

	const vector<LogicalType> &GetTypes() const;
	const vector<string> &GetNames() const;

	//! Find a column by name, returns invalid index if not found
	optional_idx FindColumn(const string &name) const;

	//! Set filters to push down to the scan
	void SetFilters(unique_ptr<TableFilterSet> filters);

	//! Set which columns to read (by index). If not called, reads all columns.
	void SetColumnIds(vector<column_t> column_ids);

	//! Initialize the scan
	void InitializeScan();

	//! Scan the next chunk. Returns false when done.
	bool Scan(DataChunk &chunk);

private:
	ClientContext &context;
	TableFunction parquet_scan;
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> return_types;
	vector<string> return_names;

	unique_ptr<TableFilterSet> filters;
	vector<column_t> column_ids;

	unique_ptr<ThreadContext> thread_context;
	unique_ptr<ExecutionContext> execution_context;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;

	bool initialized = false;
};

} // namespace duckdb
