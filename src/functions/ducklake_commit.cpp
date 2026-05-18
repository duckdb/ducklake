#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct DuckLakeCommitBindData : public TableFunctionData {
	string commit_uuid;
	int64_t snapshot_id = 0;
	int64_t schema_version = 0;
	bool emitted = false;
};

static unique_ptr<FunctionData> DuckLakeCommitBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	for (idx_t i = 0; i < 4; i++) {
		if (input.inputs[i].IsNull()) {
			throw BinderException("ducklake_commit arguments cannot be NULL");
		}
	}
	auto result = make_uniq<DuckLakeCommitBindData>();
	result->commit_uuid = input.inputs[1].GetValue<string>();
	result->snapshot_id = input.inputs[2].GetValue<int64_t>();
	result->schema_version = input.inputs[3].GetValue<int64_t>();

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
    : TableFunction("ducklake_commit",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
                    DuckLakeCommitExecute, DuckLakeCommitBind, DuckLakeCommitInit) {
}

} // namespace duckdb
