#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

static void AddFileInfo(DuckLakeFileData &file_info, vector<Value> &row_values) {
	// add the file info - if we have a file
	if (file_info.path.empty()) {
		// no file - push NULL values
		// this can happen for delete files
		row_values.emplace_back();
		row_values.emplace_back();
		row_values.emplace_back();
		row_values.emplace_back();
		return;
	}
	row_values.emplace_back(file_info.path);
	row_values.push_back(Value::UBIGINT(file_info.file_size_bytes));
	if (file_info.footer_size.IsValid()) {
		row_values.push_back(Value::UBIGINT(file_info.footer_size.GetIndex()));
	} else {
		row_values.emplace_back();
	}
	if (file_info.encryption_key.empty()) {
		row_values.emplace_back();
	} else {
		row_values.emplace_back(Value::BLOB_RAW(file_info.encryption_key));
	}
}

static unique_ptr<FunctionData> DuckLakeListFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	names.emplace_back("data_file");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("data_file_size_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("data_file_footer_size");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("data_file_encryption_key");
	return_types.emplace_back(LogicalType::BLOB);

	names.emplace_back("delete_file");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("delete_file_size_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("delete_file_footer_size");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("delete_file_encryption_key");
	return_types.emplace_back(LogicalType::BLOB);

	string schema;
	auto schema_entry = input.named_parameters.find("schema");
	if (schema_entry != input.named_parameters.end()) {
		schema = StringValue::Get(schema_entry->second);
	}
	// generate the AT clause for the given list of parameters
	unique_ptr<BoundAtClause> at_clause;
	auto version_entry = input.named_parameters.find("snapshot_version");
	if (version_entry != input.named_parameters.end()) {
		at_clause = make_uniq<BoundAtClause>("version", version_entry->second);
	}
	auto time_entry = input.named_parameters.find("snapshot_time");
	if (time_entry != input.named_parameters.end()) {
		if (at_clause) {
			throw InvalidInputException("Either snapshot_version OR snapshot_time must be specified - not both");
		}
		at_clause = make_uniq<BoundAtClause>("timestamp", time_entry->second);
	}
	auto table_name = StringValue::Get(input.inputs[1]);
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, table_name, at_clause.get(), QueryErrorContext());
	auto table_entry = catalog.GetEntry(context, schema, table_lookup, OnEntryNotFound::THROW_EXCEPTION);
	auto &ducklake_table = table_entry->Cast<DuckLakeTableEntry>();
	auto snapshot = transaction.GetSnapshot(at_clause.get());

	// fetch the file list
	auto &metadata_manager = transaction.GetMetadataManager();
	// FIXME: support predicate pushdown
	auto files = metadata_manager.GetFilesForTable(ducklake_table, snapshot, nullptr);
	// generate the result
	auto result = make_uniq<MetadataBindData>();
	for (auto &file : files) {
		vector<Value> row_values;
		// data file
		auto &data_file = file.file;
		AddFileInfo(data_file, row_values);

		auto &delete_file = file.delete_file;
		AddFileInfo(delete_file, row_values);
		result->rows.push_back(std::move(row_values));
	}
	return std::move(result);
}

DuckLakeListFilesFunction::DuckLakeListFilesFunction()
    : DuckLakeBaseMetadataFunction("ducklake_list_files", DuckLakeListFilesBind) {
	arguments.push_back(LogicalType::VARCHAR);
	named_parameters["schema"] = LogicalType::VARCHAR;
	named_parameters["snapshot_version"] = LogicalType::BIGINT;
	named_parameters["snapshot_time"] = LogicalType::TIMESTAMP_TZ;
}

} // namespace duckdb
