#include "common/ducklake_util.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

struct DuckLakeSnapshotTagData : public TableFunctionData {
	DuckLakeSnapshotTagData(DuckLakeCatalog &catalog, string name) : catalog(catalog), name(std::move(name)) {
	}

	DuckLakeCatalog &catalog;
	string name;
	//! The tagged snapshot of ducklake_create_tag
	idx_t snapshot_id = DConstants::INVALID_INDEX;
};

static DuckLakeCatalog &GetTagCatalog(ClientContext &context, TableFunctionBindInput &input,
                                      DatabaseModificationType modification) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	if (!catalog.SupportsSnapshotTags()) {
		ThrowUnsupportedByVersion(catalog.GetDuckLakeVersion(), "snapshot tags");
	}
	catalog.VerifyWritable();
	if (input.binder) {
		input.binder->GetStatementProperties().RegisterDBModify(catalog, context, modification);
	}
	return catalog;
}

static string TagNameFromInput(const Value &input) {
	if (input.IsNull() || StringValue::Get(input).empty()) {
		throw BinderException("Tag name cannot be NULL or empty");
	}
	return StringValue::Get(input);
}

static void TagReturnTypes(vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
}

static unique_ptr<FunctionData> CreateTagBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = GetTagCatalog(context, input, DatabaseModificationType::CREATE_CATALOG_ENTRY);
	auto result = make_uniq<DuckLakeSnapshotTagData>(catalog, TagNameFromInput(input.inputs[1]));
	// a string that reads as a snapshot id keeps naming that snapshot
	if (DuckLakeUtil::TryParseSnapshotId(input.inputs[1])) {
		throw BinderException("A snapshot tag cannot be named like a snapshot id, got \"%s\"", result->name);
	}
	TagReturnTypes(return_types, names);
	Value version;
	Value timestamp;
	bool has_version = DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "version", "integer", version);
	bool has_timestamp = DuckLakeTableFunctionUtil::TryGetTimestampOption(input, "timestamp", timestamp);
	if (has_version && has_timestamp) {
		throw BinderException("ducklake_create_tag: specify either version or timestamp, not both");
	}
	unique_ptr<BoundAtClause> at_clause;
	if (has_version) {
		at_clause = make_uniq<BoundAtClause>(DuckLakeTableFunctionUtil::AtClauseFromValue(version));
	} else if (has_timestamp) {
		at_clause = make_uniq<BoundAtClause>(DuckLakeTableFunctionUtil::AtClauseFromValue(timestamp));
	}
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	result->snapshot_id = transaction.GetSnapshot(at_clause.get()).snapshot_id;
	return std::move(result);
}

static void CreateTagExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<DuckLakeSnapshotTagData>();
	auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
	transaction.CreateSnapshotTag(data.name, data.snapshot_id);
}

static unique_ptr<FunctionData> DropTagBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = GetTagCatalog(context, input, DatabaseModificationType::DROP_CATALOG_ENTRY);
	auto result = make_uniq<DuckLakeSnapshotTagData>(catalog, TagNameFromInput(input.inputs[1]));
	TagReturnTypes(return_types, names);
	// the commit conflicts with tag changes after the snapshot the drop was validated against
	DuckLakeTransaction::Get(context, catalog).GetSnapshot();
	return std::move(result);
}

static void DropTagExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<DuckLakeSnapshotTagData>();
	auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
	transaction.DropSnapshotTag(data.name);
}

static unique_ptr<FunctionData> TagsBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto result = make_uniq<MetadataBindData>();
	names.emplace_back("tag_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("snapshot_time");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);
	names.emplace_back("created_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	if (!catalog.SupportsSnapshotTags()) {
		return std::move(result);
	}
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	for (auto &tag : transaction.GetSnapshotTags()) {
		vector<Value> row;
		row.emplace_back(tag.name);
		row.push_back(Value::BIGINT(NumericCast<int64_t>(tag.snapshot_id)));
		row.push_back(tag.snapshot_time);
		if (tag.created_snapshot_id.IsValid()) {
			row.push_back(Value::BIGINT(NumericCast<int64_t>(tag.created_snapshot_id.GetIndex())));
		} else {
			row.emplace_back(LogicalType::BIGINT);
		}
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

DuckLakeCreateTagFunction::DuckLakeCreateTagFunction()
    : TableFunction("ducklake_create_tag",
                    FunctionSignature()
                        .AddPositionalOnly("catalog", LogicalType::VARCHAR)
                        .AddPositionalOnly("tag_name", LogicalType::VARCHAR),
                    CreateTagExecute, CreateTagBind) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("version", LogicalType::BIGINT).Add("timestamp", LogicalType::TIMESTAMP_TZ);
	});
}

DuckLakeDropTagFunction::DuckLakeDropTagFunction()
    : TableFunction("ducklake_drop_tag",
                    FunctionSignature()
                        .AddPositionalOnly("catalog", LogicalType::VARCHAR)
                        .AddPositionalOnly("tag_name", LogicalType::VARCHAR),
                    DropTagExecute, DropTagBind) {
}

DuckLakeTagsFunction::DuckLakeTagsFunction() : DuckLakeBaseMetadataFunction("ducklake_tags", TagsBind) {
}

} // namespace duckdb
