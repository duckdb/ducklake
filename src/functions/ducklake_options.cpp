#include "duckdb/parser/qualified_name.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

struct DuckLakeOptionMetadata {
	const char *name;
	const char *description;
};

static constexpr DuckLakeOptionMetadata DUCKLAKE_OPTIONS[] = {
    {"data_inlining_row_limit", "Maximum amount of rows to inline in a single insert"},
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
    {"per_thread_output", "Whether to create separate output files per thread during parallel insertion"},
    {"write_deletion_vectors", "[EXPERIMENTAL - do not use outside testing] Whether to write Iceberg V3 deletion "
                               "vectors (puffin) instead of positional delete files (parquet)"},
    {"sort_on_insert", "Whether to sort data on INSERT according to SET SORTED BY (default: true)"},
    {"skip_stats_columns", "Columns for which min/max bounds are not recorded (counts are still recorded)"},
};

struct DuckLakeOptionsData : public TableFunctionData {
	explicit DuckLakeOptionsData(Catalog &catalog) : catalog(catalog) {
	}

	Catalog &catalog;
};

struct DuckLakeOptionsState : public GlobalTableFunctionState {
	vector<vector<Value>> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckLakeOptionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);

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

static vector<Value> GetOptionRow(const DuckLakeTag &tag, const string &scope, const string &scope_entry) {
	return {Value(tag.key), GetOptionDescription(tag.key), Value(tag.value), Value(scope),
	        scope_entry.empty() ? Value() : Value(scope_entry)};
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
		result->rows.push_back(GetOptionRow(tag, "GLOBAL", string()));
	}

	auto snapshot = transaction.GetSnapshot();

	// Schema options
	for (auto &schema_setting : metadata.schema_settings) {
		string scope_entry;
		auto schema_entry = ducklake_catalog.GetEntryById(transaction, snapshot, schema_setting.schema_id);
		if (schema_entry) {
			scope_entry = schema_entry->Cast<SchemaCatalogEntry>().GetSchemaName();
		}
		result->rows.push_back(GetOptionRow(schema_setting.tag, "SCHEMA", scope_entry));
	}

	// Table options
	for (auto &table_setting : metadata.table_settings) {
		string scope_entry;
		auto table_entry = ducklake_catalog.GetEntryById(transaction, snapshot, table_setting.table_id);
		if (table_entry) {
			auto &table_catalog_entry = table_entry->Cast<TableCatalogEntry>();
			scope_entry =
			    QualifiedName(table_catalog_entry.ParentSchema().GetSchemaPath(), table_entry->name).ToString();
		}
		result->rows.push_back(GetOptionRow(table_setting.tag, "TABLE", scope_entry));
	}

	std::sort(result->rows.begin(), result->rows.end(), [](const vector<Value> &a, const vector<Value> &b) {
		return StringValue::Get(a[0]) < StringValue::Get(b[0]);
	});
	return std::move(result);
}

void DuckLakeOptionsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeOptionsState>();
	DuckLakeBaseMetadataFunction::ScanRows(state.rows, state.offset, output);
}

DuckLakeOptionsFunction::DuckLakeOptionsFunction()
    : DuckLakeBaseMetadataFunction("ducklake_options", DuckLakeOptionsBind) {
	init_global = DuckLakeOptionsInit;
	function = DuckLakeOptionsExecute;
}

} // namespace duckdb
