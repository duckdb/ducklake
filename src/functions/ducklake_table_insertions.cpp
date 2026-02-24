#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "storage/ducklake_scan.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

string GetTableName(const Value &input) {
	if (input.IsNull()) {
		throw BinderException("Table cannot be NULL");
	}
	return input.GetValue<string>();
}

TableCatalogEntry &GetTableEntry(ClientContext &context, Catalog &catalog, const EntryLookupInfo &lookup,
                                 const Value &schema) {
	if (schema.IsNull()) {
		throw BinderException("Schema cannot be NULL");
	}
	auto schema_name = schema.GetValue<string>();
	auto entry = catalog.GetEntry(context, schema_name, lookup, OnEntryNotFound::THROW_EXCEPTION);
	return entry->Cast<TableCatalogEntry>();
}

BoundAtClause AtClauseFromValue(const Value &input) {
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

static unique_ptr<FunctionData> DuckLakeTableChangesBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names,
                                                         DuckLakeScanType scan_type) {
	auto start_at_clause = AtClauseFromValue(input.inputs[3]);
	auto end_at_clause = AtClauseFromValue(input.inputs[4]);

	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto table_name = GetTableName(input.inputs[2]);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table_name, end_at_clause, QueryErrorContext());
	auto &table = GetTableEntry(context, catalog, lookup, input.inputs[1]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	unique_ptr<FunctionData> bind_data;
	input.table_function = table.GetScanFunction(context, bind_data, lookup);

	auto &function_info = input.table_function.function_info->Cast<DuckLakeFunctionInfo>();
	names = function_info.column_names;
	return_types = function_info.column_types;
	function_info.start_snapshot =
	    make_uniq<DuckLakeSnapshot>(transaction.GetSnapshot(start_at_clause, SnapshotBound::LOWER_BOUND));
	function_info.scan_type = scan_type;
	return bind_data;
}

static unique_ptr<FunctionData> DuckLakeTableInsertionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	return DuckLakeTableChangesBind(context, input, return_types, names, DuckLakeScanType::SCAN_INSERTIONS);
}

static unique_ptr<FunctionData> DuckLakeTableDeletionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return DuckLakeTableChangesBind(context, input, return_types, names, DuckLakeScanType::SCAN_DELETIONS);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeChangesInit(ClientContext &context, TableFunctionInitInput &input) {
	throw InternalException("DuckLakeChangesInit should never be called");
}

static void DuckLakeChangesExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	throw InternalException("DuckLakeChangesExecute should never be called");
}

TableFunctionSet DuckLakeTableInsertionsFunction::GetFunctions() {
	TableFunctionSet set("ducklake_table_insertions");
	vector<LogicalType> at_types {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ};
	for (auto &type : at_types) {
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, type, type},
		                              DuckLakeChangesExecute, DuckLakeTableInsertionsBind, DuckLakeChangesInit));
	}
	return set;
}

TableFunctionSet DuckLakeTableDeletionsFunction::GetFunctions() {
	TableFunctionSet set("ducklake_table_deletions");
	vector<LogicalType> at_types {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ};
	for (auto &type : at_types) {
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, type, type},
		                              DuckLakeChangesExecute, DuckLakeTableDeletionsBind, DuckLakeChangesInit));
	}
	return set;
}
} // namespace duckdb
