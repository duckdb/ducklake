#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {
struct DuckLakeSetCommitMessageData final : public TableFunctionData {
	DuckLakeSetCommitMessageData(Catalog &catalog, const Value &author, const Value &commit_message,
	                             const Value &commit_extra_info)
	    : catalog(catalog) {
		snapshot_commit_info.author = author;
		snapshot_commit_info.commit_message = commit_message;
		snapshot_commit_info.commit_extra_info = commit_extra_info;
		snapshot_commit_info.is_commit_info_set = true;
	}
	Catalog &catalog;
	DuckLakeSnapshotCommit snapshot_commit_info;
};

struct DuckLakeSetCommitMessageState final : public GlobalTableFunctionState {
	DuckLakeSetCommitMessageState() {
	}

	bool finished = false;
};

unique_ptr<GlobalTableFunctionState> DuckLakeSetCommitMessageInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<DuckLakeSetCommitMessageState>();
}

static unique_ptr<FunctionData> DuckLakeSetCommitMessageBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	auto extra_info_entry = input.named_parameters.find("extra_info");
	Value extra_info;
	if (extra_info_entry != input.named_parameters.end()) {
		extra_info = extra_info_entry->second;
	}
	return make_uniq<DuckLakeSetCommitMessageData>(catalog, input.inputs[1], input.inputs[2], extra_info);
}

void DuckLakeSetCommitMessageExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeSetCommitMessageState>();
	auto &bind_data = data_p.bind_data->Cast<DuckLakeSetCommitMessageData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
	transaction.SetCommitMessage(bind_data.snapshot_commit_info);
	state.finished = true;
}

DuckLakeSetCommitMessage::DuckLakeSetCommitMessage()
    : TableFunction("ducklake_set_commit_message", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    DuckLakeSetCommitMessageExecute, DuckLakeSetCommitMessageBind, DuckLakeSetCommitMessageInit) {
	named_parameters["extra_info"] = LogicalType::VARCHAR;
}
} // namespace duckdb
