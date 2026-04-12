#include "functions/ducklake_table_functions.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct BackupBindData : public TableFunctionData {

	explicit BackupBindData(Catalog &catalog) : catalog(catalog) {
	}

	Catalog &catalog;
	string backup_location;
};

static unique_ptr<FunctionData> DuckLakeStaticBackupBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto result = make_uniq<BackupBindData>(catalog);

	auto &ducklake_catalog = reinterpret_cast<DuckLakeCatalog &>(catalog);
	string backup_location = ducklake_catalog.GetStaticBackup();

	if (backup_location.empty()) {
		throw InvalidInputException("static_backup not specified as attach option");
	}

	result->backup_location = backup_location;

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("errors");

	return std::move(result);
}

struct DuckLakeBackupData : public GlobalTableFunctionState {
	DuckLakeBackupData() : offset(0), executed(false) {
	}

	idx_t offset;
	bool executed;
};

unique_ptr<GlobalTableFunctionState> DuckLakeStaticBackupInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckLakeBackupData>();
	return std::move(result);
}

void DuckLakeStaticBackupExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<BackupBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeBackupData>();

	if (!state.executed) {
		auto &transaction = DuckLakeTransaction::Get(context, data.catalog);

		auto tmp_uuid = "ducklake_backup_file." + UUID::ToString(UUID::GenerateRandomUUID());

		auto &fs = FileSystem::GetFileSystem(context);

		if (fs.FileExists(tmp_uuid) || fs.FileExists(tmp_uuid + ".wal")) {
			throw BinderException(
			    "Temporary file \"%s\" is already in use, please cleanup files in the form \"ducklake_backup_file.*\"",
			    tmp_uuid);
		}

		auto result = transaction.Query(
		    string("") + "ATTACH IF NOT EXISTS '" + tmp_uuid +
		    "' AS {METADATA_CATALOG_NAME_IDENTIFIER_BACKUP} (STORAGE_VERSION 'v1.4.0');" +
		    "COPY FROM DATABASE {METADATA_CATALOG_NAME_IDENTIFIER} TO {METADATA_CATALOG_NAME_IDENTIFIER_BACKUP};" +
		    "DETACH {METADATA_CATALOG_NAME_IDENTIFIER_BACKUP};" + "COPY (SELECT content FROM read_blob('" + tmp_uuid +
		    "')) TO '" + data.backup_location + "' (FORMAT BLOB);" + "COPY (SELECT content FROM read_blob('" +
		    tmp_uuid + ".wal')) TO '" + data.backup_location + ".wal' (FORMAT BLOB);" + "");

		fs.TryRemoveFile(tmp_uuid);
		fs.TryRemoveFile(tmp_uuid + ".wal");

		if (result->HasError()) {
			auto &error_obj = result->GetErrorObject();
			error_obj.Throw("Failed to attach temp backup");
		}
		state.executed = true;
	}
	idx_t count = 0;
	output.SetCardinality(count);
}

DuckLakeStaticBackupFunction::DuckLakeStaticBackupFunction()
    : TableFunction("ducklake_static_backup", {LogicalType::VARCHAR}, DuckLakeStaticBackupExecute,
                    DuckLakeStaticBackupBind, DuckLakeStaticBackupInit) {
}

} // namespace duckdb
