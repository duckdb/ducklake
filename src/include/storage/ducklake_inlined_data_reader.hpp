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

namespace duckdb {
class DuckLakeFieldData;
class DuckLakeMetadataManager;
struct DuckLakeFunctionInfo;
struct DuckLakeInlinedColNames;

enum class InlinedVirtualColumn { NONE, COLUMN_ROW_ID, COLUMN_FILE_ROW_NUMBER, COLUMN_EMPTY };

class DuckLakeInlinedDataReader : public BaseFileReader {
public:
	//! Initialize an inlined data reader over a set of data stored within a table in the metadata catalog
	DuckLakeInlinedDataReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info, Identifier table_name,
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

	//! Scan rows read before instead of querying the inlined table
	void SetCommittedRows(shared_ptr<DuckLakeCommittedInlinedRows> rows);

	//! Read the columns of a committed inlined table, and the next file id of the catalog when it has rows
	static shared_ptr<DuckLakeCommittedInlinedRows> ReadCommittedRows(DuckLakeFunctionInfo &read_info,
	                                                                  const Identifier &table_name,
	                                                                  const vector<MultiFileColumnDefinition> &columns,
	                                                                  optional_idx &next_file_id);

private:
	static void GetColumnToRead(const DuckLakeFunctionInfo &read_info, DuckLakeMetadataManager &metadata_manager,
	                            const DuckLakeInlinedColNames &col_names, const MultiFileColumnDefinition &col,
	                            string &column, LogicalType &type);
	bool HasPushdownExtract() const;
	//! Whether the field extracted into this column is computed after filtering, only for the rows that pass
	bool IsDeferredExtract(idx_t column_idx) const;
	//! The scanned column of a deferred extract that has not been computed yet, removing it from the list
	optional_idx TakeDeferredExtract(idx_t column_idx);
	//! Keep the selected rows of the output, and of the scanned rows the deferred extracts are computed from
	void SliceRows(DataChunk &chunk, const SelectionVector &sel, idx_t count);
	void ComputeDeferredExtract(ClientContext &context, idx_t column_idx, idx_t scan_column, DataChunk &chunk);
	bool TryEvaluateExpression(ClientContext &context, idx_t virtual_col_idx, Vector &input_vector,
	                           const LogicalType &input_type, Vector &output_vector);

	mutex lock;
	DuckLakeFunctionInfo &read_info;
	Identifier table_name;
	shared_ptr<DuckLakeInlinedData> data;
	shared_ptr<DuckLakeCommittedInlinedRows> committed_rows;
	bool initialized_scan = false;
	vector<InlinedVirtualColumn> virtual_columns;
	int64_t file_row_number = 0;
	vector<column_t> scan_column_ids;
	ColumnDataScanState state;
	DataChunk scan_chunk;
	//! Expression executors for expression_map entries, keyed by the position of the column in the scan
	unordered_map<idx_t, unique_ptr<ExpressionExecutor>> expression_executors;
	//! (output column, scanned column) of the extracts that wait for the filters of the current chunk
	vector<pair<idx_t, idx_t>> deferred_extracts;
};

} // namespace duckdb
