#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
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

static unique_ptr<FunctionData> DuckLakeSetCommitMessageBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types,
                                                             vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
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
	auto &bind_data = data_p.bind_data->Cast<DuckLakeSetCommitMessageData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
	transaction.SetCommitMessage(bind_data.snapshot_commit_info);
}

DuckLakeSetCommitMessage::DuckLakeSetCommitMessage()
    : TableFunction("ducklake_set_commit_message",
                    FunctionSignature()
                        .AddPositionalOnly("catalog", LogicalType::VARCHAR)
                        .AddPositionalOnly("author", LogicalType::VARCHAR)
                        .AddPositionalOnly("commit_message", LogicalType::VARCHAR),
                    DuckLakeSetCommitMessageExecute, DuckLakeSetCommitMessageBind) {
	GetSignature().WithTypedKwargs("options",
	                               [&](TypedKwargs &options) { options.Add("extra_info", LogicalType::VARCHAR); });
}
} // namespace duckdb
