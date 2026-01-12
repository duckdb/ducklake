#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_stats.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

static InsertionOrderPreservingMap<string> DuckLakeFunctionToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;

	if (input.table_function.function_info) {
		auto &table_info = input.table_function.function_info->Cast<DuckLakeFunctionInfo>();
		result["Table"] = table_info.table_name;
	}

	return result;
}

unique_ptr<BaseStatistics> DuckLakeStatistics(ClientContext &context, const FunctionData *bind_data,
                                              column_t column_index) {
	if (IsVirtualColumn(column_index)) {
		return nullptr;
	}
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<DuckLakeMultiFileList>();
	if (file_list.HasTransactionLocalData()) {
		// don't read stats if we have transaction-local inserts
		// FIXME: we could unify the stats with the global stats
		return nullptr;
	}
	auto &table = file_list.GetTable();
	return table.GetStatistics(context, column_index);
}

BindInfo DuckLakeBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<DuckLakeMultiFileList>();
	return BindInfo(file_list.GetTable());
}

void DuckLakeScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                           const TableFunction &function) {
	throw NotImplementedException("DuckLakeScan not implemented");
}

unique_ptr<FunctionData> DuckLakeScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("DuckLakeScan not implemented");
}

virtual_column_map_t DuckLakeVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<DuckLakeMultiFileList>();
	auto result = file_list.GetTable().GetVirtualColumns();
	bind_data.virtual_columns = result;
	return result;
}

vector<column_t> DuckLakeGetRowIdColumn(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	vector<column_t> result;
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

TableFunction DuckLakeFunctions::GetDuckLakeScanFunction(DatabaseInstance &instance) {
	// Parquet extension needs to be loaded for this to make sense
	ExtensionHelper::AutoLoadExtension(instance, "parquet");

	// The ducklake_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// DuckLakeMultiFileReader into it to create a DuckLake-based multi file read
	ExtensionLoader loader(instance, "ducklake");
	auto &parquet_scan = loader.GetTableFunction("parquet_scan");
	auto function = parquet_scan.functions.GetFunctionByOffset(0);

	// Register the MultiFileReader as the driver for reads
	function.get_multi_file_reader = DuckLakeMultiFileReader::CreateInstance;

	function.statistics = DuckLakeStatistics;
	function.get_bind_info = DuckLakeBindInfo;
	function.get_virtual_columns = DuckLakeVirtualColumns;
	function.get_row_id_columns = DuckLakeGetRowIdColumn;

	// Unset all of these: they are either broken, very inefficient.
	// TODO: implement/fix these
	function.serialize = DuckLakeScanSerialize;
	function.deserialize = DuckLakeScanDeserialize;

	function.to_string = DuckLakeFunctionToString;

	function.name = "ducklake_scan";
	return function;
}

DuckLakeFunctionInfo::DuckLakeFunctionInfo(DuckLakeTableEntry &table, DuckLakeTransaction &transaction_p,
                                           DuckLakeSnapshot snapshot)
    : table(table), transaction(transaction_p.shared_from_this()), snapshot(snapshot) {
}

shared_ptr<DuckLakeTransaction> DuckLakeFunctionInfo::GetTransaction() {
	auto result = transaction.lock();
	if (!result) {
		throw NotImplementedException(
		    "Scanning a DuckLake table after the transaction has ended - this use case is not yet supported");
	}
	return result;
}

} // namespace duckdb
