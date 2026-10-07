#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

string DuckLakeTableFunctionUtil::FormatTimestampOlderThan(const string &interval_str) {
	interval_t interval;
	if (!Interval::FromString(interval_str, interval)) {
		throw InvalidInputException("Failed to parse interval: '%s'", interval_str);
	}
	auto current_time = Timestamp::GetCurrentTimestamp();
	auto target_timestamp = SubtractOperator::Operation<timestamp_t, interval_t, timestamp_t>(current_time, interval);
	return FormatTimestampISO8601(target_timestamp);
}

bool DuckLakeTableFunctionUtil::TryGetNonNullOption(const TableFunctionBindInput &input, const string &name,
                                                    const string &type_name, Value &result) {
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end()) {
		return false;
	}
	if (entry->second.IsNull()) {
		throw BinderException("The %s option must be a non-null %s.", name, type_name);
	}
	result = entry->second;
	return true;
}

string DuckLakeTableFunctionUtil::GetStringOption(const TableFunctionBindInput &input, const string &name) {
	Value result;
	if (!TryGetNonNullOption(input, name, "string", result)) {
		return string();
	}
	return StringValue::Get(result);
}

string DuckLakeTableFunctionUtil::GetTableName(const Value &input) {
	if (input.IsNull()) {
		throw BinderException("Table cannot be NULL");
	}
	return input.GetValue<string>();
}

BoundAtClause DuckLakeTableFunctionUtil::AtClauseFromValue(const Value &input) {
	if (input.IsNull()) {
		throw BinderException("Snapshot identifier cannot be NULL");
	}
	switch (input.type().id()) {
	case LogicalTypeId::BIGINT:
		return BoundAtClause("version", input);
	case LogicalTypeId::TIMESTAMP_TZ:
		return BoundAtClause("timestamp", input);
	default:
		throw InternalException("Unsupported type for At Clause");
	}
}

DuckLakeCatalog &DuckLakeBaseMetadataFunction::GetCatalog(ClientContext &context, TableFunctionBindInput &input) {
	if (input.binder) {
		// the bind result depends on the transaction so prepared statements are rebound on every execution
		input.binder->SetAlwaysRequireRebind();
	}
	auto &catalog_name = input.inputs[0];
	if (catalog_name.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = catalog_name.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "ducklake") {
		throw BinderException("Attached database \"%s\" does not refer to a DuckLake database", db_name);
	}
	return catalog.Cast<DuckLakeCatalog>();
}

DuckLakeTableEntry &DuckLakeBaseMetadataFunction::GetTableEntry(ClientContext &context, DuckLakeCatalog &catalog,
                                                                const string &schema, const EntryLookupInfo &lookup,
                                                                const string &not_a_table_hint) {
	EntryLookupInfo qualified_lookup(lookup, catalog.ResolveEntryName(context, schema, lookup.GetEntryName(),
	                                                                  lookup.GetCatalogType(), lookup.GetAtClause()));
	CatalogEntryRetriever retriever(context);
	auto entry = catalog.LookupEntry(retriever, qualified_lookup, OnEntryNotFound::THROW_EXCEPTION).entry;
	if (entry->type != CatalogType::TABLE_ENTRY) {
		auto error =
		    StringUtil::Format("\"%s\" is a %s, not a table.", lookup.GetEntryName(), CatalogTypeToString(entry->type));
		if (!not_a_table_hint.empty()) {
			error += " " + not_a_table_hint;
		}
		throw CatalogException(error);
	}
	return entry->Cast<DuckLakeTableEntry>();
}

DuckLakeTableEntry &DuckLakeBaseMetadataFunction::GetTableEntry(ClientContext &context, DuckLakeCatalog &catalog,
                                                                const string &schema, const string &table) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table)));
	return GetTableEntry(context, catalog, schema, lookup);
}

vector<reference<DuckLakeTableEntry>> DuckLakeBaseMetadataFunction::GetTablesInSchema(ClientContext &context,
                                                                                      SchemaCatalogEntry &schema) {
	vector<reference<DuckLakeTableEntry>> result;
	schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (entry.type == CatalogType::TABLE_ENTRY) {
			result.push_back(entry.Cast<DuckLakeTableEntry>());
		}
	});
	return result;
}

vector<reference<DuckLakeTableEntry>> DuckLakeBaseMetadataFunction::GetTablesInScope(ClientContext &context,
                                                                                     DuckLakeCatalog &catalog,
                                                                                     const string &schema,
                                                                                     const string &table) {
	if (!table.empty()) {
		return {GetTableEntry(context, catalog, schema, table)};
	}
	if (!schema.empty()) {
		return GetTablesInSchema(context, catalog.ResolveSchema(context, schema));
	}
	vector<reference<DuckLakeTableEntry>> result;
	for (auto &schema_entry : catalog.GetSchemas(context)) {
		auto tables = GetTablesInSchema(context, schema_entry.get());
		result.insert(result.end(), tables.begin(), tables.end());
	}
	return result;
}

void DuckLakeBaseMetadataFunction::ScanRows(const vector<vector<Value>> &rows, idx_t &offset, DataChunk &output) {
	idx_t count = 0;
	while (offset < rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = rows[offset++];
		if (entry.size() != output.ColumnCount()) {
			throw InternalException("Unaligned metadata row in result");
		}
		for (idx_t c = 0; c < entry.size(); c++) {
			output.data[c].Append(entry[c]);
		}
		count++;
	}
	output.SetChildCardinality(count);
}

static void MetadataFunctionExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<MetadataBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	DuckLakeBaseMetadataFunction::ScanRows(data.rows, state.offset, output);
}

unique_ptr<GlobalTableFunctionState> DuckLakeRunOnceState::Init(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DuckLakeRunOnceState>();
}

DuckLakeBaseMetadataFunction::DuckLakeBaseMetadataFunction(Identifier name_p, table_function_bind_t bind)
    : TableFunction(std::move(name_p), FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR),
                    MetadataFunctionExecute, bind, DuckLakeRunOnceState::Init) {
}

} // namespace duckdb
