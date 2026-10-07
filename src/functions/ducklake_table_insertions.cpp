#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_scan.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeTableChangesBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names,
                                                         DuckLakeScanType scan_type) {
	auto start_at_clause = DuckLakeTableFunctionUtil::AtClauseFromValue(input.inputs[3]);
	auto end_at_clause = DuckLakeTableFunctionUtil::AtClauseFromValue(input.inputs[4]);

	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto table_name = DuckLakeTableFunctionUtil::GetTableName(input.inputs[2]);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)), end_at_clause,
	                       QueryErrorContext());
	if (input.inputs[1].IsNull()) {
		throw BinderException("Schema cannot be NULL");
	}
	auto &table =
	    DuckLakeBaseMetadataFunction::GetTableEntry(context, catalog, input.inputs[1].GetValue<string>(), lookup,
	                                                "Data change feed functions only support tables.");
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	unique_ptr<FunctionData> bind_data;
	input.table_function = BoundTableFunction(table.GetScanFunction(context, bind_data, lookup));

	auto &function_info = input.table_function.function_info->Cast<DuckLakeFunctionInfo>();
	names = StringsToIdentifiers(function_info.column_names);
	return_types = function_info.column_types;
	function_info.start_snapshot =
	    make_uniq<DuckLakeSnapshot>(transaction.GetSnapshot(start_at_clause, SnapshotBound::LOWER_BOUND));
	function_info.scan_type = scan_type;
	return bind_data;
}

static unique_ptr<FunctionData> DuckLakeTableInsertionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	return DuckLakeTableChangesBind(context, input, return_types, names, DuckLakeScanType::SCAN_INSERTIONS);
}

static unique_ptr<FunctionData> DuckLakeTableDeletionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<Identifier> &names) {
	return DuckLakeTableChangesBind(context, input, return_types, names, DuckLakeScanType::SCAN_DELETIONS);
}

static void DuckLakeChangesExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	throw InternalException("DuckLakeChangesExecute should never be called");
}

static TableFunctionSet GetChangesFunctions(const char *name, table_function_bind_t bind) {
	TableFunctionSet set(name);
	vector<LogicalType> at_types {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ};
	for (auto &type : at_types) {
		set.AddFunction(TableFunction(FunctionSignature()
		                                  .AddPositionalOnly("catalog", LogicalType::VARCHAR)
		                                  .AddPositionalOnly("schema_name", LogicalType::VARCHAR)
		                                  .AddPositionalOnly("table_name", LogicalType::VARCHAR)
		                                  .AddPositionalOnly("start_snapshot", type)
		                                  .AddPositionalOnly("end_snapshot", type),
		                              DuckLakeChangesExecute, bind));
	}
	return set;
}

TableFunctionSet DuckLakeTableInsertionsFunction::GetFunctions() {
	return GetChangesFunctions("ducklake_table_insertions", DuckLakeTableInsertionsBind);
}

TableFunctionSet DuckLakeTableDeletionsFunction::GetFunctions() {
	return GetChangesFunctions("ducklake_table_deletions", DuckLakeTableDeletionsBind);
}
} // namespace duckdb
