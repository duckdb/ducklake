#include "ducklake_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage/ducklake_storage.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_secret.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for DuckLake, SQL as a Lakehouse Format");

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["ducklake"] = make_uniq<DuckLakeStorageExtension>();

	config.AddExtensionOption("ducklake_max_retry_count",
	                          "The maximum amount of retry attempts for a ducklake transaction", LogicalType::UBIGINT,
	                          Value::UBIGINT(10), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_retry_wait_ms", "Time between retries", LogicalType::UBIGINT,
	                          Value::UBIGINT(100), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_retry_backoff", "Backoff factor for exponentially increasing retry wait time",
	                          LogicalType::DOUBLE, Value::DOUBLE(1.5), nullptr, SetScope::GLOBAL);

	DuckLakeSnapshotsFunction snapshots;
	loader.RegisterFunction(snapshots);

	DuckLakeTableInfoFunction table_info;
	loader.RegisterFunction(table_info);

	auto table_insertions = DuckLakeTableInsertionsFunction::GetFunctions();
	loader.RegisterFunction(table_insertions);

	auto table_deletions = DuckLakeTableDeletionsFunction::GetFunctions();
	loader.RegisterFunction(table_deletions);

	DuckLakeMergeAdjacentFilesFunction merge_adjacent_files;
	loader.RegisterFunction(merge_adjacent_files);

	DuckLakeCleanupOldFilesFunction cleanup_old_files;
	loader.RegisterFunction(cleanup_old_files);

	DuckLakeExpireSnapshotsFunction expire_snapshots;
	loader.RegisterFunction(expire_snapshots);

	DuckLakeFlushInlinedDataFunction flush_inlined_data;
	loader.RegisterFunction(flush_inlined_data);

	DuckLakeSetOptionFunction set_options;
	loader.RegisterFunction(set_options);

	DuckLakeOptionsFunction options;
	loader.RegisterFunction(options);

	DuckLakeSetCommitMessage set_commit_message;
	loader.RegisterFunction(set_commit_message);

	auto table_changes = DuckLakeTableInsertionsFunction::GetDuckLakeTableChanges();
	loader.RegisterFunction(*table_changes);

	DuckLakeListFilesFunction list_files;
	loader.RegisterFunction(list_files);

	DuckLakeAddDataFilesFunction add_files;
	loader.RegisterFunction(add_files);

	DuckLakeCurrentSnapshotFunction current_snapshot;
	loader.RegisterFunction(current_snapshot);

	DuckLakeLastCommittedSnapshotFunction last_committed;
	loader.RegisterFunction(last_committed);

	// secrets
	auto secret_type = DuckLakeSecret::GetSecretType();
	loader.RegisterSecretType(secret_type);

	auto ducklake_secret_function = DuckLakeSecret::GetFunction();
	loader.RegisterFunction(ducklake_secret_function);
}

void DucklakeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DucklakeExtension::Name() {
	return "ducklake";
}

std::string DucklakeExtension::Version() const {
#ifdef EXT_VERSION_DUCKLAKE
	return EXT_VERSION_DUCKLAKE;
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
