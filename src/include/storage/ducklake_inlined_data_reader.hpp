//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_inlined_data_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {
class DuckLakeFieldData;
struct DuckLakeFunctionInfo;

enum class InlinedVirtualColumn { NONE, COLUMN_ROW_ID, COLUMN_FILE_ROW_NUMBER, COLUMN_EMPTY };

class DuckLakeInlinedDataReader : public BaseFileReader {
public:
	//! Initialize an inlined data reader over a set of data stored within a table in the metadata catalog
	DuckLakeInlinedDataReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info, string table_name,
	                          vector<MultiFileColumnDefinition> columns);
	//! Initialize an inlined data reader over a set of data
	DuckLakeInlinedDataReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info,
	                          shared_ptr<DuckLakeInlinedData> data, vector<MultiFileColumnDefinition> columns);

public:
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;

	string GetReaderType() const override;

	void AddVirtualColumn(column_t virtual_column_id) override;

private:
	bool TryEvaluateExpression(ClientContext &context, idx_t virtual_col_idx, Vector &input_vector,
	                           const LogicalType &input_type, Vector &output_vector);
	//! Reads the next page of rows from the metadata catalog, false when every row was read
	bool LoadNextPage(ClientContext &context);
	//! Maps the deleted row ids of the rows of the page to their ordinals
	void MapDeletedRows();
	unique_ptr<QueryResult> ReadPage(const DuckLakeInlinedDataPage &query_page);
	//! Reads the keys of the next rows, false when there are none
	bool ReadNextKeys();
	//! Orders the rows of a page of listed rows like their keys
	void OrderListedRows(ClientContext &context, const vector<pair<idx_t, idx_t>> &page_keys);

	mutex lock;
	DuckLakeFunctionInfo &read_info;
	string table_name;
	shared_ptr<DuckLakeInlinedData> data;
	bool initialized_scan = false;
	vector<InlinedVirtualColumn> virtual_columns;
	int64_t file_row_number = 0;
	vector<column_t> scan_column_ids;
	ColumnDataScanState state;
	DataChunk scan_chunk;
	//! Expression executors for expression_map entries, keyed by column_t (from column_ids)
	unordered_map<column_t, unique_ptr<ExpressionExecutor>> expression_executors;

	//! The rows of a table in the metadata catalog are read one page at a time, so memory holds one page
	vector<string> columns_to_read;
	vector<LogicalType> expected_types;
	DuckLakeInlinedDataPage page;
	bool read_all_pages = false;
	//! The size of the largest row read so far
	idx_t max_row_bytes = 0;
	//! Whether the rows are read by their keys, read ahead of their values, since a row can be large
	bool read_by_keys = false;
	bool page_type_set = false;
	DuckLakeInlinedDataPage key_page;
	vector<pair<idx_t, idx_t>> keys;
	idx_t key_position = 0;
	bool read_all_keys = false;
	//! The deleted row ids, ascending, and the first one not yet found in a page
	vector<idx_t> deleted_row_ids;
	idx_t deleted_row_idx = 0;
};

} // namespace duckdb
