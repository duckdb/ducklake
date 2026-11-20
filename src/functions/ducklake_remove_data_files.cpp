#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "common/ducklake_data_file.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct DuckLakeRemoveDataFilesData : public TableFunctionData {
	DuckLakeRemoveDataFilesData(DuckLakeCatalog &catalog, DuckLakeTableEntry &table) : catalog(catalog), table(table) {
	}

	DuckLakeCatalog &catalog;
	DuckLakeTableEntry &table;
	vector<string> paths;
};

static unique_ptr<FunctionData> DuckLakeRemoveDataFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	catalog.VerifyWritable();
	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	auto schema_name = DuckLakeTableFunctionUtil::GetStringOption(input, "schema");
	const auto table_name = StringValue::Get(input.inputs[1]);
	auto &table = DuckLakeBaseMetadataFunction::GetTableEntry(context, catalog, schema_name, table_name);

	auto result = make_uniq<DuckLakeRemoveDataFilesData>(catalog, table);
	auto &file_list = input.inputs[2];
	if (file_list.IsNull()) {
		throw InvalidInputException("File list cannot be NULL");
	}
	if (file_list.type() == LogicalType::VARCHAR) {
		result->paths.push_back(StringValue::Get(file_list));
	} else if (file_list.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
		for (auto &path : ListValue::GetChildren(file_list)) {
			if (path.IsNull()) {
				throw InvalidInputException("File list cannot contain NULL");
			}
			result->paths.push_back(StringValue::Get(path));
		}
	} else {
		throw InvalidInputException("File list must be a string or a list of strings");
	}
	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

static void DuckLakeRemoveDataFilesExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<DuckLakeRemoveDataFilesData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
	auto &table = bind_data.table;
	auto table_id = table.GetTableId();

	// the committed files are listed as ducklake_list_files does, the transaction-local files are kept separately
	vector<DuckLakeFileListExtendedEntry> committed_files;
	if (!table.IsTransactionLocal()) {
		auto &metadata_manager = transaction.GetMetadataManager();
		committed_files = metadata_manager.GetExtendedFilesForTable(table, transaction.GetSnapshot());
	}
	auto local_files = transaction.GetTransactionLocalFiles(table_id);
	// paths are matched with the separators of the platform, a registered path keeps the separators it was given
	auto &fs = FileSystem::GetFileSystem(context);
	auto normalize = [&](const string &p) {
		return fs.ConvertSeparators(p);
	};
	// the local files are listed once, so a path removed by this call is not a data file of the table anymore
	set<string> removed_paths;
	for (auto &input_path : bind_data.paths) {
		auto path = normalize(input_path);
		bool found = false;
		bool removed = removed_paths.find(path) != removed_paths.end();
		if (!removed) {
			for (auto &file : committed_files) {
				// a committed file dropped earlier in the transaction can be registered again as a local file
				if (normalize(file.file.path) != path || transaction.FileIsDropped(file.file.path)) {
					continue;
				}
				// the file keeps serving older snapshots, it is only ended at the commit snapshot
				transaction.DropFile(table_id, file.file_id, file.file.path, file.row_count, file.file.file_size_bytes);
				found = true;
				break;
			}
		}
		if (found) {
			continue;
		}
		for (auto &file : local_files) {
			if (removed || normalize(file.file_name) != path) {
				continue;
			}
			removed_paths.insert(path);
			if (!transaction.TryDropTransactionLocalFile(table_id, file.file_name)) {
				throw InvalidInputException("Cannot remove file \"%s\" - it holds rows flushed from earlier snapshots",
				                            input_path);
			}
			found = true;
			break;
		}
		if (!found) {
			throw InvalidInputException("File \"%s\" is not a data file of table \"%s\" - ducklake_list_files lists "
			                            "the data files of a table",
			                            input_path, table.name);
		}
	}
}

TableFunctionSet DuckLakeRemoveDataFilesFunction::GetFunctions() {
	TableFunctionSet set("ducklake_remove_data_files");
	vector<LogicalType> path_types {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)};
	for (auto &type : path_types) {
		TableFunction function("ducklake_remove_data_files",
		                       FunctionSignature()
		                           .AddPositionalOnly("catalog", LogicalType::VARCHAR)
		                           .AddPositionalOnly("table_name", LogicalType::VARCHAR)
		                           .AddPositionalOnly(type.id() == LogicalTypeId::LIST ? "paths" : "path", type),
		                       DuckLakeRemoveDataFilesExecute, DuckLakeRemoveDataFilesBind);
		function.GetSignature().WithTypedKwargs(
		    "options", [&](TypedKwargs &options) { options.Add("schema", LogicalType::VARCHAR); });
		set.AddFunction(function);
	}
	return set;
}

} // namespace duckdb
