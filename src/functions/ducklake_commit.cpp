#include "functions/ducklake_table_functions.hpp"

#include "storage/ducklake_server_side_commit.hpp"

namespace duckdb {

struct DuckLakeCommitBindData : public TableFunctionData {
	string metadata_schema_name;
	int64_t schema_version = 0;
	DuckLakeRetryConfig retry_config;
	bool emitted = false;
};

static unique_ptr<FunctionData> DuckLakeCommitBind(ClientContext &, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	for (idx_t i = 0; i < 2; i++) {
		if (input.inputs[i].IsNull()) {
			throw BinderException("ducklake_commit arguments cannot be NULL");
		}
	}
	auto result = make_uniq<DuckLakeCommitBindData>();
	result->metadata_schema_name = StringValue::Get(input.inputs[0]);
	result->schema_version = input.inputs[1].GetValue<int64_t>();
	for (auto &entry : input.named_parameters) {
		if (entry.second.IsNull()) {
			continue;
		}
		if (entry.first == "max_retry_count") {
			result->retry_config.max_retry_count = static_cast<idx_t>(entry.second.GetValue<int64_t>());
		} else if (entry.first == "retry_wait_ms") {
			result->retry_config.retry_wait_ms = static_cast<idx_t>(entry.second.GetValue<int64_t>());
		} else if (entry.first == "retry_backoff") {
			result->retry_config.retry_backoff = entry.second.GetValue<double>();
		} else {
			throw BinderException("Unknown named parameter \"%s\" for ducklake_commit", entry.first);
		}
	}
	names.emplace_back("committed_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("committed_schema_version");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("had_flushes");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeCommitInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GlobalTableFunctionState>();
}

static void DuckLakeCommitExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DuckLakeCommitBindData>();
	if (data.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	data.emitted = true;

	DuckLakeServerSideCommit commit(context, data.metadata_schema_name, data.schema_version);
	commit.SetRetryConfigOverride(data.retry_config);
	auto result = commit.Run();

	output.data[0].Append(Value::BIGINT(result.committed_snapshot_id));
	output.data[1].Append(Value::BIGINT(result.committed_schema_version));
	output.data[2].Append(Value::BOOLEAN(result.had_flushes));
	output.SetChildCardinality(1);
}

DuckLakeCommitFunction::DuckLakeCommitFunction()
    : TableFunction("ducklake_commit", {LogicalType::VARCHAR, LogicalType::BIGINT}, DuckLakeCommitExecute,
                    DuckLakeCommitBind, DuckLakeCommitInit) {
	named_parameters["max_retry_count"] = LogicalType::BIGINT;
	named_parameters["retry_wait_ms"] = LogicalType::BIGINT;
	named_parameters["retry_backoff"] = LogicalType::DOUBLE;
}

} // namespace duckdb
