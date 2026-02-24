#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

struct DuckLakeOptionMetadata {
	const char *name;
	const char *description;
};

using ducklake_option_array = std::array<DuckLakeOptionMetadata, 19>;

static constexpr const ducklake_option_array DUCKLAKE_OPTIONS = {
    {{"data_inlining_row_limit", "Maximum amount of rows to inline in a single insert"},
     {"parquet_compression",
      "Compression algorithm for Parquet files (uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw)"},
     {"parquet_version", "Parquet format version (1 or 2)"},
     {"parquet_compression_level", "Compression level for Parquet files"},
     {"parquet_row_group_size", "Number of rows per row group in Parquet files"},
     {"parquet_row_group_size_bytes", "Number of bytes per row group in Parquet files"},
     {"hive_file_pattern", "If partitioned data should be written in a hive-like folder structure"},
     {"target_file_size", "The target data file size for insertion and compaction operations"},
     {"version", "DuckLake format version"},
     {"created_by", "Tool used to write the DuckLake"},
     {"data_path", "Path to data files"},
     {"require_commit_message", "If an explicit commit message is required for a snapshot commit."},
     {"rewrite_delete_threshold", "A threshold that determines the minimum amount of data that must be "
                                  "removed from a file before a rewrite is warranted. From 0 - 1."},
     {"delete_older_than", "How old unused files must be to be removed by the 'ducklake_delete_orphaned_files' and "
                           "'ducklake_cleanup_old_files' cleanup functions."},
     {"expire_older_than", "How old snapshots must be, by default, to be expired by: 'ducklake_expire_snapshots'"},
     {"auto_compact", "Pre-defined schema used as a default value for the following compaction functions "
                      "'ducklake_flush_inlined_data','ducklake_merge_adjacent_files', "
                      "'ducklake_rewrite_data_files', 'ducklake_delete_orphaned_files'"},
     {"encrypted", "Whether or not to encrypt Parquet files written to the data path"},
     {"per_thread_output", "Whether to create separate output files per thread during parallel insertion"}}};

struct DuckLakeOptionsData : public TableFunctionData {
	explicit DuckLakeOptionsData(Catalog &catalog) : catalog(catalog) {
	}

	Catalog &catalog;
};

struct DuckLakeOptionInfo {
	string option_name;
	Value description;
	string value;
	string scope;
	string scope_entry;
};

struct DuckLakeOptionsState : public GlobalTableFunctionState {
	DuckLakeOptionsState() : offset(0) {
	}

	vector<DuckLakeOptionInfo> options;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckLakeOptionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);

	names.emplace_back("option_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope_entry");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<DuckLakeOptionsData>(catalog);
}

static Value GetOptionDescription(const string &option_name) {
	for (auto &opt : DUCKLAKE_OPTIONS) {
		if (StringUtil::CIEquals(opt.name, option_name)) {
			return opt.description;
		}
	}
	return Value();
}

unique_ptr<GlobalTableFunctionState> DuckLakeOptionsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DuckLakeOptionsData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
	auto &ducklake_catalog = bind_data.catalog.Cast<DuckLakeCatalog>();
	auto &metadata_manager = transaction.GetMetadataManager();

	auto result = make_uniq<DuckLakeOptionsState>();
	auto metadata = metadata_manager.LoadDuckLake();

	// Global options
	for (auto &tag : metadata.tags) {
		DuckLakeOptionInfo option_info;
		option_info.option_name = tag.key;
		option_info.value = tag.value;
		option_info.description = GetOptionDescription(tag.key);
		option_info.scope = "GLOBAL";
		result->options.push_back(std::move(option_info));
	}

	auto snapshot = transaction.GetSnapshot();

	// Schema options
	for (auto &schema_setting : metadata.schema_settings) {
		DuckLakeOptionInfo option_info;
		option_info.option_name = schema_setting.tag.key;
		option_info.value = schema_setting.tag.value;
		option_info.description = GetOptionDescription(schema_setting.tag.key);
		option_info.scope = "SCHEMA";
		auto schema_entry = ducklake_catalog.GetEntryById(transaction, snapshot, schema_setting.schema_id);
		if (schema_entry) {
			option_info.scope_entry = schema_entry->name;
		}
		result->options.push_back(std::move(option_info));
	}

	// Table options
	for (auto &table_setting : metadata.table_settings) {
		DuckLakeOptionInfo option_info;
		option_info.option_name = table_setting.tag.key;
		option_info.value = table_setting.tag.value;
		option_info.description = GetOptionDescription(table_setting.tag.key);
		option_info.scope = "TABLE";
		auto table_entry = ducklake_catalog.GetEntryById(transaction, snapshot, table_setting.table_id);
		if (table_entry) {
			auto &table_catalog_entry = table_entry->Cast<TableCatalogEntry>();
			option_info.scope_entry = table_catalog_entry.ParentSchema().name + "." + table_entry->name;
		}
		result->options.push_back(std::move(option_info));
	}

	std::sort(result->options.begin(), result->options.end(),
	          [](const DuckLakeOptionInfo &a, const DuckLakeOptionInfo &b) { return a.option_name < b.option_name; });
	return std::move(result);
}

void DuckLakeOptionsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeOptionsState>();

	if (state.offset >= state.options.size()) {
		return;
	}

	idx_t count = 0;
	while (state.offset < state.options.size() && count < STANDARD_VECTOR_SIZE) {
		auto &option = state.options[state.offset++];
		output.SetValue(0, count, Value(option.option_name));
		output.SetValue(1, count, option.description);
		output.SetValue(2, count, Value(option.value));
		output.SetValue(3, count, Value(option.scope));
		output.SetValue(4, count, option.scope_entry.empty() ? Value() : Value(option.scope_entry));
		count++;
	}
	output.SetCardinality(count);
}

DuckLakeOptionsFunction::DuckLakeOptionsFunction()
    : DuckLakeBaseMetadataFunction("ducklake_options", DuckLakeOptionsBind) {
	init_global = DuckLakeOptionsInit;
	function = DuckLakeOptionsExecute;
}

} // namespace duckdb
