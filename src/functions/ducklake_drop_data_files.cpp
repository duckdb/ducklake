#include "functions/ducklake_table_functions.hpp"
#include "common/ducklake_data_file.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_partition_filter.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct DropDataFile {
	DataFileIndex file_id;
	string path;
	idx_t row_count = 0;
	optional_idx partition_id;
	vector<DuckLakeFilePartition> partition_values;
};

struct DuckLakeDropDataFilesData : public TableFunctionData {
	DuckLakeDropDataFilesData(Catalog &catalog, DuckLakeTableEntry &table) : catalog(catalog), table(table) {
	}

	Catalog &catalog;
	DuckLakeTableEntry &table;
	DuckLakePartitionFilter partition_filter;
	bool dry_run = false;
};

struct DuckLakeDropDataFilesState : public GlobalTableFunctionState {
	idx_t offset = 0;
	bool finished = false;
	vector<vector<Value>> rows;
};

static unique_ptr<FunctionData> DuckLakeDropDataFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	string schema_name;
	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	if (input.named_parameters.find("schema") != input.named_parameters.end()) {
		schema_name = StringValue::Get(input.named_parameters["schema"]);
	}
	auto table_name = StringValue::Get(input.inputs[1]);
	auto entry =
	    catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name, OnEntryNotFound::THROW_EXCEPTION);
	auto &table = entry->Cast<DuckLakeTableEntry>();

	auto result = make_uniq<DuckLakeDropDataFilesData>(catalog, table);
	auto partition_values_entry = input.named_parameters.find("partition_values");
	if (partition_values_entry == input.named_parameters.end()) {
		throw InvalidInputException("partition_values is required");
	}
	result->partition_filter = DuckLakePartitionFilter::Parse(table, partition_values_entry->second);
	for (auto &entry : input.named_parameters) {
		auto lower = StringUtil::Lower(entry.first);
		if (lower == "dry_run") {
			result->dry_run = BooleanValue::Get(entry.second);
		} else if (lower != "schema" && lower != "partition_values") {
			throw InvalidInputException("Unknown named parameter %s for drop_data_files", entry.first);
		}
	}

	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("row_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeDropDataFilesInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<DuckLakeDropDataFilesState>();
}

static vector<DuckLakeFilePartition> ConvertPartitionValues(const vector<DuckLakeFilePartitionInfo> &partition_values) {
	vector<DuckLakeFilePartition> result;
	for (auto &partition : partition_values) {
		DuckLakeFilePartition converted;
		converted.partition_column_idx = partition.partition_column_idx;
		converted.partition_value = partition.partition_value;
		result.push_back(std::move(converted));
	}
	return result;
}

static vector<DuckLakeFileListExtendedEntry> GetDropDataFiles(DuckLakeTransaction &transaction,
                                                              DuckLakeTableEntry &table) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto active_files = metadata_manager.GetExtendedFilesForTable(table, transaction.GetSnapshot(), nullptr);
	vector<DuckLakeFileListExtendedEntry> result;
	for (auto &file : active_files) {
		if (file.data_type != DuckLakeDataType::DATA_FILE || !file.file_id.IsValid()) {
			continue;
		}
		if (transaction.FileIsDropped(file.file.path)) {
			continue;
		}
		result.push_back(std::move(file));
	}
	return result;
}

static vector<DropDataFile> GetTransactionLocalDropDataFiles(DuckLakeTransaction &transaction, TableIndex table_id) {
	vector<DropDataFile> result;
	auto transaction_local_files = transaction.GetTransactionLocalFiles(table_id);
	for (auto &file : transaction_local_files) {
		DropDataFile drop_file;
		drop_file.path = file.file_name;
		drop_file.row_count = file.row_count;
		for (auto &partition : file.partition_values) {
			drop_file.partition_values.push_back(partition);
		}
		result.push_back(std::move(drop_file));
	}
	return result;
}

static void DuckLakeDropDataFilesExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeDropDataFilesState>();
	if (!state.finished) {
		auto &bind_data = data_p.bind_data->Cast<DuckLakeDropDataFilesData>();
		auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
		auto files = GetDropDataFiles(transaction, bind_data.table);
		for (auto &file : files) {
			auto partition_values = ConvertPartitionValues(file.partition_values);
			if (!bind_data.partition_filter.Matches(file.partition_id, partition_values)) {
				continue;
			}
			if (!bind_data.dry_run) {
				transaction.DropFile(bind_data.table.GetTableId(), file.file_id, file.file.path);
			}
			state.rows.push_back({Value(file.file.path), Value::UBIGINT(file.row_count)});
		}
		auto transaction_local_files = GetTransactionLocalDropDataFiles(transaction, bind_data.table.GetTableId());
		for (auto &file : transaction_local_files) {
			if (!bind_data.partition_filter.Matches(file.partition_id, file.partition_values)) {
				continue;
			}
			if (!bind_data.dry_run) {
				transaction.DropTransactionLocalFile(bind_data.table.GetTableId(), file.path);
			}
			state.rows.push_back({Value(file.path), Value::UBIGINT(file.row_count)});
		}
		state.finished = true;
	}

	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.rows[state.offset++];
		output.data[0].Append(entry[0]);
		output.data[1].Append(entry[1]);
		count++;
	}
	output.SetChildCardinality(count);
}

TableFunctionSet DuckLakeDropDataFilesFunction::GetFunctions() {
	TableFunctionSet set("ducklake_drop_data_files");
	TableFunction function("ducklake_drop_data_files", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       DuckLakeDropDataFilesExecute, DuckLakeDropDataFilesBind, DuckLakeDropDataFilesInit);
	function.named_parameters["schema"] = LogicalType::VARCHAR;
	function.named_parameters["partition_values"] = LogicalType::ANY;
	function.named_parameters["dry_run"] = LogicalType::BOOLEAN;
	set.AddFunction(function);
	return set;
}

} // namespace duckdb
