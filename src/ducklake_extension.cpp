#include "ducklake_extension.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage/ducklake_storage.hpp"
#include "common/ducklake_version.hpp"
#include "storage/ducklake_scan.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_secret.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "storage/ducklake_log_type.hpp"
#include "rest/ducklake_rest_server.hpp"

namespace duckdb {

ScalarFunction DuckLakeMurmur3Function();
ScalarFunction DuckLakeUnreadableMacroDefaultFunction();

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for DuckLake, SQL as a Lakehouse Format");

	auto &instance = loader.GetDatabaseInstance();
	instance.GetLogManager().RegisterLogType(make_uniq<DuckLakeMetadataLogType>());

	auto &config = DBConfig::GetConfig(instance);
	StorageExtension::Register(config, "ducklake", make_shared_ptr<DuckLakeStorageExtension>());

	config.AddExtensionOption("ducklake_max_retry_count",
	                          "The maximum amount of retry attempts for a ducklake transaction", LogicalType::UBIGINT,
	                          Value::UBIGINT(10), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_retry_wait_ms", "Time between retries", LogicalType::UBIGINT,
	                          Value::UBIGINT(100), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_retry_backoff", "Backoff factor for exponentially increasing retry wait time",
	                          LogicalType::DOUBLE, Value::DOUBLE(1.5), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_default_data_inlining_row_limit",
	                          "Default row limit for data inlining (0 disables inlining)", LogicalType::UBIGINT,
	                          Value::UBIGINT(10), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_default_version", "Default DuckLake version for new catalogs",
	                          LogicalType::VARCHAR, Value(), nullptr, SetScope::GLOBAL);
	auto set_target_file_size = [](ClientContext &, SetScope, Value &parameter) {
		if (!parameter.IsNull() && !parameter.ToString().empty()) {
			DBConfig::ParseMemoryLimit(parameter.ToString());
		}
	};
	config.AddExtensionOption("ducklake_target_file_size", "Target file size for insertion and compaction",
	                          LogicalType::VARCHAR, Value(), set_target_file_size, SetScope::GLOBAL);
	config.AddExtensionOption(
	    "ducklake_write_deletion_vectors",
	    "[EXPERIMENTAL] Write Iceberg V3 deletion vectors (puffin) instead of positional delete files (parquet)",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false), nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("ducklake_rest_group_commit",
	                          "Commit through the group commit of a DuckLake REST server, where transactions that only "
	                          "insert data can share a snapshot with other transactions. SET changes it for all "
	                          "connections of the database, SET SESSION for the connection only",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("ducklake_debug_rest_commit_hold",
	                          "DEBUG SETTING: the number of commits that have to wait in the queue of a DuckLake REST "
	                          "server before they run, for tests that need their commits to wait together",
	                          LogicalType::UBIGINT, Value::UBIGINT(0), nullptr, SetScope::GLOBAL);

	loader.RegisterFunction(DuckLakeRESTServeFunction());
	loader.RegisterFunction(DuckLakeRESTStopFunction());
	loader.RegisterFunction(DuckLakeRESTRequestFunction());
	loader.RegisterFunction(DuckLakeRESTRowsFunction());
	loader.RegisterFunction(DuckLakeSnapshotsFunction());
	loader.RegisterFunction(DuckLakeTableInfoFunction());
	loader.RegisterFunction(DuckLakeTableInsertionsFunction::GetFunctions());
	loader.RegisterFunction(DuckLakeTableDeletionsFunction::GetFunctions());
	loader.RegisterFunction(DuckLakeMergeAdjacentFilesFunction::GetFunctions());
	loader.RegisterFunction(DuckLakeRewriteDataFilesFunction::GetFunctions());
	loader.RegisterFunction(DuckLakeCleanupOldFilesFunction());
	loader.RegisterFunction(DuckLakeCleanupOrphanedFilesFunction());
	loader.RegisterFunction(DuckLakeExpireSnapshotsFunction());
	loader.RegisterFunction(DuckLakeFlushInlinedDataFunction());
	loader.RegisterFunction(DuckLakeSetOptionFunction());
	loader.RegisterFunction(DuckLakeOptionsFunction());
	loader.RegisterFunction(DuckLakeSetCommitMessage());
	loader.RegisterFunction(DuckLakeCreateTagFunction());
	loader.RegisterFunction(DuckLakeReplaceTagFunction());
	loader.RegisterFunction(DuckLakeDropTagFunction());
	loader.RegisterFunction(DuckLakeSetTagExpiryFunction());
	loader.RegisterFunction(DuckLakeTagsFunction());
	auto table_changes = DuckLakeTableInsertionsFunction::GetDuckLakeTableChanges();
	loader.RegisterFunction(*table_changes);
	loader.RegisterFunction(DuckLakeListFilesFunction());
	loader.RegisterFunction(DuckLakeAddDataFilesFunction::GetFunctions());
	loader.RegisterFunction(DuckLakeRemoveDataFilesFunction::GetFunctions());
	loader.RegisterFunction(DuckLakeCurrentSnapshotFunction());
	loader.RegisterFunction(DuckLakeMetadataTableFunction());
	loader.RegisterFunction(DuckLakeLastCommittedSnapshotFunction());
	loader.RegisterFunction(DuckLakeSettingsFunction());
	loader.RegisterFunction(DuckLakeCommitFunction());
	loader.RegisterFunction(DuckLakeRegisterMetadataProfileFunction());

	// Register ducklake_scan so it can be found during deserialization
	loader.RegisterFunction(DuckLakeFunctions::GetDuckLakeScanFunction(loader.GetDatabaseInstance()));

	// secrets
	loader.RegisterSecretType(DuckLakeSecret::GetSecretType());
	loader.RegisterFunction(DuckLakeSecret::GetFunction());

	// Register murmur3_32 scalar function for Iceberg-compatible bucket partitioning
	loader.RegisterFunction(DuckLakeMurmur3Function());
	loader.RegisterFunction(DuckLakeUnreadableMacroDefaultFunction());
}

// LCOV_EXCL_START
void DucklakeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DucklakeExtension::Name() {
	return "ducklake";
}

std::string DucklakeExtension::Version() const {
#ifdef EXT_VERSION_DUCKLAKE
	return EXT_VERSION_DUCKLAKE;
// LCOV_EXCL_STOP
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ducklake, loader) {
	LoadInternal(loader);
}
}
