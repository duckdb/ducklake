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
struct DuckLakeFunctionInfo;

enum class InlinedVirtualColumn { NONE, COLUMN_ROW_ID, COLUMN_EMPTY };

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
};

} // namespace duckdb
