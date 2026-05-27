//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_vortex_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/vortex_file_scanner.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

enum class VortexReaderColumn { NONE, FILE_ROW_NUMBER, EMPTY };

class DuckLakeVortexFileReader : public BaseFileReader {
public:
	DuckLakeVortexFileReader(ClientContext &context, const OpenFileInfo &info, DuckLakeFileData file_data);

	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;

	string GetReaderType() const override;

	void AddVirtualColumn(column_t virtual_column_id) override;

private:
	bool TryEvaluateExpression(ClientContext &context, idx_t virtual_col_idx, Vector &input_vector,
	                           const LogicalType &input_type, Vector &output_vector);

private:
	mutex lock;
	DuckLakeFileData file_data;
	unique_ptr<VortexFileScanner> scanner;
	bool initialized_scan = false;
	vector<VortexReaderColumn> projected_columns;
	vector<column_t> scan_column_ids;
	DataChunk scan_chunk;
	int64_t file_row_number = 0;
	unordered_map<column_t, unique_ptr<ExpressionExecutor>> expression_executors;
};

} // namespace duckdb
