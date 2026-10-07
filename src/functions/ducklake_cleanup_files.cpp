#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct CleanupBindData : public MetadataBindData {
	explicit CleanupBindData(DuckLakeCatalog &catalog, CleanupType type) : catalog(catalog), type(type) {
	}

	string GetFilter() const {
		if (timestamp_filter.empty()) {
			return "";
		}
		switch (type) {
		case CleanupType::OLD_FILES:
			return StringUtil::Format("WHERE schedule_start::TIMESTAMPTZ < '%s'", timestamp_filter);
		case CleanupType::ORPHANED_FILES:
			return StringUtil::Format(" AND last_modified::TIMESTAMPTZ < '%s'", timestamp_filter);
		default:
			throw InternalException("Unknown Cleanup type for GetFilter()");
		}
	}

	string GetFunctionName() const {
		switch (type) {
		case CleanupType::OLD_FILES:
			return "ducklake_cleanup_old_files";
		case CleanupType::ORPHANED_FILES:
			return "ducklake_delete_orphaned_files";
		default:
			throw InternalException("Unknown Cleanup type for GetFunctionName()");
		}
	}

	DuckLakeCatalog &catalog;
	vector<DuckLakeFileForCleanup> files;
	//! If we are going to delete the files for real or not
	bool dry_run = false;

	CleanupType type;
	string timestamp_filter;
};

static unique_ptr<FunctionData> CleanupBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names,
                                            CleanupType type) {
	auto &ducklake_catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto result = make_uniq<CleanupBindData>(ducklake_catalog, type);

	const auto older_than_default = ducklake_catalog.GetConfigOption<string>("delete_older_than", {}, {}, "2 days");

	Value dry_run;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "dry_run", "boolean", dry_run)) {
		result->dry_run = BooleanValue::Get(dry_run);
	}
	bool cleanup_all = false;
	Value cleanup_all_value;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "cleanup_all", "boolean", cleanup_all_value)) {
		cleanup_all = BooleanValue::Get(cleanup_all_value);
	}
	Value older_than;
	bool has_timestamp = DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "older_than", "timestamp", older_than);
	if ((cleanup_all == has_timestamp && cleanup_all == true) ||
	    (cleanup_all == has_timestamp && cleanup_all == false && older_than_default.empty())) {
		throw InvalidInputException(
		    "%s: either cleanup_all OR older_than must be specified.\nYou can also set a default value for file "
		    "deletion via e.g., CALL ducklake.set_option('delete_older_than', '1 week');",
		    result->GetFunctionName());
	}

	if (has_timestamp) {
		auto from_timestamp = older_than.GetValue<timestamp_tz_t>();
		result->timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampISO8601(timestamp_t(from_timestamp.value));
	} else if (!cleanup_all && !older_than_default.empty()) {
		result->timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampOlderThan(older_than_default);
	}

	// orphans belong to no table, so only the global auto_compact option can turn their cleanup off
	bool skip_cleanup = type == CleanupType::ORPHANED_FILES && !ducklake_catalog.AutoCompactEnabled();
	if (!skip_cleanup) {
		auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);
		auto &metadata_manager = transaction.GetMetadataManager();
		result->files = metadata_manager.GetFilesForCleanup(result->GetFilter(), type);
		for (auto &file : result->files) {
			result->rows.push_back({Value(file.path)});
		}
	}

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("path");

	return std::move(result);
}
static unique_ptr<FunctionData> DuckLakeCleanupOldFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	return CleanupBind(context, input, return_types, names, CleanupType::OLD_FILES);
}

static unique_ptr<FunctionData> DuckLakeCleanupOrphanedFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<Identifier> &names) {
	return CleanupBind(context, input, return_types, names, CleanupType::ORPHANED_FILES);
}

void DuckLakeCleanupExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<CleanupBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	if (state.offset >= data.files.size()) {
		return;
	}
	if (!state.executed && !data.dry_run) {
		// delete the files
		auto &fs = FileSystem::GetFileSystem(context);
		vector<string> paths;
		paths.reserve(data.files.size());
		for (const auto &file : data.files) {
			paths.push_back(file.path);
		}
		fs.RemoveFiles(paths);
		if (data.type == CleanupType::OLD_FILES) {
			// If we are removing old files, we need to remove them from the catalog
			auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
			auto &metadata_manager = transaction.GetMetadataManager();
			metadata_manager.RemoveFilesScheduledForCleanup(data.files);
		}
		state.executed = true;
	}
	DuckLakeBaseMetadataFunction::ScanRows(data.rows, state.offset, output);
}

static FunctionSignature CleanupSignature() {
	auto signature = FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR);
	signature.WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("older_than", LogicalType::TIMESTAMP_TZ)
		    .Add("cleanup_all", LogicalType::BOOLEAN)
		    .Add("dry_run", LogicalType::BOOLEAN);
	});
	return signature;
}

DuckLakeCleanupOldFilesFunction::DuckLakeCleanupOldFilesFunction()
    : TableFunction("ducklake_cleanup_old_files", CleanupSignature(), DuckLakeCleanupExecute,
                    DuckLakeCleanupOldFilesBind, DuckLakeRunOnceState::Init) {
}

DuckLakeCleanupOrphanedFilesFunction::DuckLakeCleanupOrphanedFilesFunction()
    : TableFunction("ducklake_delete_orphaned_files", CleanupSignature(), DuckLakeCleanupExecute,
                    DuckLakeCleanupOrphanedFilesBind, DuckLakeRunOnceState::Init) {
}

} // namespace duckdb
