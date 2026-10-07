#include "functions/ducklake_table_functions.hpp"

#include "storage/ducklake_server_side_commit.hpp"

namespace duckdb {

struct DuckLakeCommitBindData : public TableFunctionData {
	string metadata_schema_name;
	int64_t schema_version = 0;
	DuckLakeRetryConfig retry_config;
};

static unique_ptr<FunctionData> DuckLakeCommitBind(ClientContext &, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
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

static void DuckLakeCommitExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	auto &data = data_p.bind_data->Cast<DuckLakeCommitBindData>();
	if (state.executed) {
		output.SetChildCardinality(0);
		return;
	}
	state.executed = true;

	DuckLakeServerSideCommit commit(context, data.metadata_schema_name, data.schema_version);
	commit.SetRetryConfigOverride(data.retry_config);
	auto result = commit.Run();

	output.data[0].Append(Value::BIGINT(result.committed_snapshot_id));
	output.data[1].Append(Value::BIGINT(result.committed_schema_version));
	output.data[2].Append(Value::BOOLEAN(result.had_flushes));
	output.SetChildCardinality(1);
}

DuckLakeCommitFunction::DuckLakeCommitFunction()
    : TableFunction("ducklake_commit",
                    FunctionSignature()
                        .AddPositionalOnly("metadata_schema", LogicalType::VARCHAR)
                        .AddPositionalOnly("schema_version", LogicalType::BIGINT),
                    DuckLakeCommitExecute, DuckLakeCommitBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("max_retry_count", LogicalType::BIGINT)
		    .Add("retry_wait_ms", LogicalType::BIGINT)
		    .Add("retry_backoff", LogicalType::DOUBLE);
	});
}

} // namespace duckdb
