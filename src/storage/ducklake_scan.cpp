#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/partition_stats.hpp"
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

static InsertionOrderPreservingMap<string> DuckLakeDynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.global_state) {
		return result;
	}
	auto &gstate = input.global_state->Cast<MultiFileGlobalState>();
	auto &file_list = gstate.file_list.Cast<DuckLakeMultiFileList>();

	// Count different types of files
	auto files_loaded = gstate.file_index.load();
	auto &files = file_list.GetFiles();
	idx_t data_files_read = 0;
	idx_t data_files_skipped = 0;
	idx_t inlined_tables_read = 0;
	for (idx_t i = 0; i < files_loaded && i < files.size() && i < gstate.readers.size(); i++) {
		bool is_skipped = gstate.readers[i]->file_state == MultiFileFileState::SKIPPED;
		switch (files[i].data_type) {
		case DuckLakeDataType::DATA_FILE:
			if (is_skipped) {
				data_files_skipped++;
			} else {
				data_files_read++;
			}
			break;
		case DuckLakeDataType::INLINED_DATA:
		case DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA:
			if (!is_skipped) {
				inlined_tables_read++;
			}
			break;
		}
	}

	result.insert(make_pair("Total Files Read", std::to_string(data_files_read)));
	if (data_files_skipped > 0) {
		result.insert(make_pair("Total Files Skipped", std::to_string(data_files_skipped)));
	}
	if (inlined_tables_read > 0) {
		result.insert(make_pair("Inlined Tables Read", std::to_string(inlined_tables_read)));
	}

	// Build filename list showing only actual data files (not inlined data tables)
	constexpr size_t FILE_NAME_LIST_LIMIT = 5;
	vector<string> file_path_names;
	for (idx_t i = 0; i < files.size() && file_path_names.size() <= FILE_NAME_LIST_LIMIT; i++) {
		if (files[i].data_type == DuckLakeDataType::DATA_FILE) {
			file_path_names.push_back(files[i].file.path);
		}
	}
	if (!file_path_names.empty()) {
		if (file_path_names.size() > FILE_NAME_LIST_LIMIT) {
			file_path_names.resize(FILE_NAME_LIST_LIMIT);
			file_path_names.push_back("...");
		}
		auto list_of_files = StringUtil::Join(file_path_names, ", ");
		result.insert(make_pair("Filename(s)", list_of_files));
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

vector<PartitionStatistics> DuckLakeGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input) {
	vector<PartitionStatistics> result;

	if (!input.table_function.function_info) {
		return result;
	}
	auto &func_info = input.table_function.function_info->Cast<DuckLakeFunctionInfo>();

	// Only use partition stats for regular table scans
	if (func_info.scan_type != DuckLakeScanType::SCAN_TABLE) {
		return result;
	}

	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<DuckLakeMultiFileList>();
	auto &table = file_list.GetTable();
	auto transaction = func_info.GetTransaction();

	auto table_id = table.GetTableId();

	// Check if this is a time travel query - if so, fall back to scanning
	// After merge_adjacent_files, multiple files are merged into one with a combined record_count.
	// The merged file contains an embedded snapshot_id column for time travel filtering,
	// but the metadata record_count represents ALL rows, not per-snapshot counts.
	// Only a full scan can filter by snapshot_id to get the correct historical count.
	// Time travel can occur via: (1) per-query AT clause, or (2) catalog attached at historical snapshot
	auto current_snapshot = transaction->GetSnapshot();
	if (func_info.snapshot.snapshot_id != current_snapshot.snapshot_id || transaction->GetCatalog().CatalogSnapshot()) {
		return result;
	}

	// Check if this is a transaction-local table (no committed stats)
	if (table.IsTransactionLocal()) {
		return result;
	}

	// If there are any transaction-local changes fall back to scanning
	// Accounting for transaction local changes gets difficult, especially when entire
	// files are dropped.
	if (transaction->HasAnyLocalChanges(table_id)) {
		return result;
	}

	idx_t count = table.GetNetDataFileRowCount(*transaction) + table.GetNetInlinedRowCount(*transaction);

	// Return single partition with total count
	PartitionStatistics stats;
	stats.count = count;
	stats.count_type = CountType::COUNT_EXACT;
	result.push_back(std::move(stats));
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
	function.get_partition_stats = DuckLakeGetPartitionStats;

	// Unset all of these: they are either broken, very inefficient.
	// TODO: implement/fix these
	function.serialize = DuckLakeScanSerialize;
	function.deserialize = DuckLakeScanDeserialize;

	function.to_string = DuckLakeFunctionToString;
	function.dynamic_to_string = DuckLakeDynamicToString;

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
