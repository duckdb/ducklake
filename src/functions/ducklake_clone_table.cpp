#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

struct DuckLakeCloneTableData : public TableFunctionData {
	explicit DuckLakeCloneTableData(DuckLakeCatalog &catalog) : catalog(catalog) {
	}

	DuckLakeCatalog &catalog;
	QualifiedName source;
	//! The schema path and the name of the copy
	vector<Identifier> target_schema;
	Identifier target_name;
	unique_ptr<BoundAtClause> at_clause;
};

//! The components of a table name with its optional schema path, like main.orders
static vector<Identifier> ParseTableName(const Value &input, const char *argument) {
	if (input.IsNull() || StringValue::Get(input).empty()) {
		throw BinderException("ducklake_clone_table: the %s table cannot be NULL or empty", argument);
	}
	return QualifiedName::ParseComponents(StringValue::Get(input));
}

static unique_ptr<FunctionData> CloneTableBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	if (!catalog.SupportsFileIdentity()) {
		ThrowUnsupportedByVersion(catalog.GetDuckLakeVersion(), "table clones");
	}
	catalog.VerifyWritable();
	if (input.binder) {
		input.binder->GetStatementProperties().RegisterDBModify(catalog, context,
		                                                        DatabaseModificationType::CREATE_CATALOG_ENTRY);
	}
	auto result = make_uniq<DuckLakeCloneTableData>(catalog);
	auto source = ParseTableName(input.inputs[1], "source");
	auto source_name = std::move(source.back());
	source.pop_back();
	result->source =
	    QualifiedName::FromCatalogSchema(Identifier(catalog.GetName()), std::move(source), std::move(source_name));
	result->target_schema = ParseTableName(input.inputs[2], "target");
	result->target_name = std::move(result->target_schema.back());
	result->target_schema.pop_back();
	if (result->target_schema.empty()) {
		result->target_schema.emplace_back(DEFAULT_SCHEMA);
	}
	Value version;
	Value timestamp;
	bool has_version = DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "version", "integer", version);
	bool has_timestamp = DuckLakeTableFunctionUtil::TryGetTimestampOption(input, "timestamp", timestamp);
	if (has_version && has_timestamp) {
		throw BinderException("ducklake_clone_table: specify either version or timestamp, not both");
	}
	if (has_version) {
		result->at_clause = make_uniq<BoundAtClause>(DuckLakeTableFunctionUtil::AtClauseFromValue(version));
	} else if (has_timestamp) {
		result->at_clause = make_uniq<BoundAtClause>(DuckLakeTableFunctionUtil::AtClauseFromValue(timestamp));
	}
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	return std::move(result);
}

static void CloneTableExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<DuckLakeCloneTableData>();
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	if (state.executed) {
		return;
	}
	state.executed = true;
	auto &catalog = data.catalog;
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	auto at_clause = data.at_clause.get();
	auto snapshot = transaction.GetSnapshot(at_clause);

	CatalogEntryRetriever retriever(context);
	EntryLookupInfo source_lookup(CatalogType::TABLE_ENTRY, data.source, at_clause, QueryErrorContext());
	auto entry = catalog.LookupEntry(retriever, source_lookup, OnEntryNotFound::THROW_EXCEPTION).entry;
	if (entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException("\"%s\" is a %s, not a table", data.source.ToString(), CatalogTypeToString(entry->type));
	}
	auto &source = entry->Cast<DuckLakeTableEntry>();
	// the copy holds the committed state of the source at the snapshot
	if (source.IsTransactionLocal() || transaction.HasAnyLocalChanges(source.GetTableId())) {
		throw NotImplementedException(
		    "Cannot clone table \"%s\", it has changes in this transaction, commit them first",
		    source.name.GetIdentifierName());
	}
	transaction.GetMetadataManager().CheckTableFork(source.GetTableId(), snapshot.snapshot_id);

	CatalogTransaction catalog_transaction(catalog, context);
	auto schema = catalog.GetSchema(catalog_transaction, data.target_schema, OnEntryNotFound::THROW_EXCEPTION);
	schema->Cast<DuckLakeSchemaEntry>().CloneTable(catalog_transaction, source, snapshot.snapshot_id,
	                                               data.target_name.GetIdentifierName());
}

DuckLakeCloneTableFunction::DuckLakeCloneTableFunction()
    : TableFunction("ducklake_clone_table",
                    FunctionSignature()
                        .AddPositionalOnly("catalog", LogicalType::VARCHAR)
                        .AddPositionalOnly("source", LogicalType::VARCHAR)
                        .AddPositionalOnly("target", LogicalType::VARCHAR),
                    CloneTableExecute, CloneTableBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("version", LogicalType::BIGINT).Add("timestamp", LogicalType::TIMESTAMP_TZ);
	});
}

} // namespace duckdb
