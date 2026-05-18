#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct DuckLakeCommitBindData : public TableFunctionData {
	string commit_uuid;
	bool emitted = false;
};

static unique_ptr<FunctionData> DuckLakeCommitBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("ducklake_commit arguments cannot be NULL");
	}
	auto result = make_uniq<DuckLakeCommitBindData>();
	result->commit_uuid = input.inputs[1].GetValue<string>();

	names.emplace_back("committed_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeCommitInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlobalTableFunctionState>();
}

static void DuckLakeCommitExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DuckLakeCommitBindData>();
	if (data.emitted) {
		output.SetCardinality(0);
		return;
	}
	data.emitted = true;
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(0));
}

DuckLakeCommitFunction::DuckLakeCommitFunction()
    : TableFunction("ducklake_commit", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckLakeCommitExecute,
                    DuckLakeCommitBind, DuckLakeCommitInit) {
}

} // namespace duckdb
