#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "storage/ducklake_initializer.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "common/ducklake_version.hpp"
#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"

namespace duckdb {

DuckLakeInitializer::DuckLakeInitializer(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeOptions &options_p)
    : context(context), catalog(catalog), options(options_p) {
	InitializeDataPath();
}

string DuckLakeInitializer::GetAttachOptions() {
	vector<string> attach_options;
	if (options.access_mode != AccessMode::AUTOMATIC) {
		switch (options.access_mode) {
		case AccessMode::READ_ONLY:
			attach_options.push_back("READ_ONLY");
			break;
		case AccessMode::READ_WRITE:
			attach_options.push_back("READ_WRITE");
			break;
		default:
			throw InternalException("Unsupported access mode in DuckLake attach");
		}
	}
	bool has_isolation_level_override = false;
	for (auto &option : options.metadata_parameters) {
		attach_options.push_back(option.first + " " + option.second.ToSQLString());
		if (StringUtil::Lower(option.first) == "isolation_level") {
			has_isolation_level_override = true;
		}
	}
	const string metadata_type = catalog.MetadataType();
	if (metadata_type.empty() || metadata_type == "duckdb") {
		// this is duckdb, we always do latest storage
		attach_options.push_back(StringUtil::Format("STORAGE_VERSION '%s'", "latest"));
	} else if ((metadata_type == "postgres" || metadata_type == "postgres_scanner") &&
	           !has_isolation_level_override) {
		// REPEATABLE READ pins one pg snapshot per DuckLake tx, hiding concurrent
		// committers from CheckForConflicts. Override via meta_isolation_level.
		attach_options.push_back("isolation_level 'read committed'");
	}

	if (attach_options.empty()) {
		return string();
	}
	string result;
	for (auto &option : attach_options) {
		if (!result.empty()) {
			result += ", ";
		}
		result += option;
	}
	return " (" + result + ")";
}

void DuckLakeInitializer::Initialize() {
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	// attach the metadata database
	auto result = transaction.Query("ATTACH OR REPLACE {METADATA_PATH} AS {METADATA_CATALOG_NAME_IDENTIFIER}" +
	                                GetAttachOptions());
	if (result->HasError()) {
		auto &error_obj = result->GetErrorObject();
		error_obj.Throw("Failed to attach DuckLake MetaData \"" + catalog.MetadataDatabaseName() + "\" at path + \"" +
		                catalog.MetadataPath() + "\"");
	}
	// explicitly load all secrets - work-around to secret initialization bug
	transaction.Query("FROM duckdb_secrets()");

	bool has_explicit_schema = !options.metadata_schema.empty();
	if (options.metadata_schema.empty()) {
		// if the schema is not explicitly set by the user - set it to the default schema in the catalog
		options.metadata_schema = transaction.GetDefaultSchemaName();
	}
	// if no explicit ducklake_version was set via ATTACH, check the global setting
	if (options.ducklake_version == DuckLakeVersion::UNSET) {
		Value setting_val;
		if (context.TryGetCurrentSetting("ducklake_default_version", setting_val) && !setting_val.IsNull()) {
			auto version = DuckLakeVersionFromString(setting_val.ToString());
			if (version < DuckLakeVersion::V1_0) {
				throw InvalidInputException("ducklake_default_version must be >= '1.0', got '%s'",
				                            setting_val.ToString());
			}
			options.ducklake_version = version;
		}
	}

	// after the metadata database is attached initialize the ducklake
	// check if we are loading an existing DuckLake or creating a new one
	// directly query a known ducklake metadata table to avoid scanning all attached catalogs via duckdb_tables()
	// this prevents a corrupted ducklake catalog from blocking initialization of unrelated ducklake databases
	// FIXME: verify that all ducklake tables are in the correct format
	result = transaction.Query("SELECT NULL FROM {METADATA_CATALOG}.ducklake_metadata LIMIT 1");
	if (result->HasError()) {
		auto &error_obj = result->GetErrorObject();
		if (error_obj.Type() == ExceptionType::CATALOG) {
			// table does not exist - this is a new ducklake
			if (!options.create_if_not_exists) {
				throw InvalidInputException(
				    "Existing DuckLake at metadata catalog \"%s\" does not exist - and creating a "
				    "new DuckLake is explicitly disabled",
				    options.metadata_path);
			}
			InitializeNewDuckLake(transaction, has_explicit_schema);
		} else {
			error_obj.Throw("Failed to load DuckLake table data");
		}
	} else {
		LoadExistingDuckLake(transaction);
	}
	if (options.at_clause) {
		// if the user specified a snapshot try to load it to trigger an error if it does not exist
		transaction.GetSnapshot();
	}
}

void DuckLakeInitializer::InitializeDataPath() {
	auto &data_path = options.data_path;
	if (data_path.empty()) {
		return;
	}

	// This functions will:
	//	1. Check if a known extension pattern matches the start of the data_path
	//	2. If so, either load the required extension or throw a relevant error message
	CheckAndAutoloadedRequiredExtension(data_path);

	auto &fs = FileSystem::GetFileSystem(context);
	auto separator = fs.PathSeparator(data_path);
	// pop trailing path separators
	while (!data_path.empty() && (data_path.back() == '/' || data_path.back() == '\\')) {
		data_path.pop_back();
	}
	// ensure the paths we store always end in a path separator
	data_path += separator;
	catalog.Separator() = separator;
}

void DuckLakeInitializer::InitializeNewDuckLake(DuckLakeTransaction &transaction, bool has_explicit_schema) {
	if (options.data_path.empty()) {
		auto &metadata_catalog = Catalog::GetCatalog(*transaction.GetConnection().context, options.metadata_database);
		if (!metadata_catalog.IsDuckCatalog()) {
			throw InvalidInputException(
			    "Attempting to create a new ducklake instance but data_path is not set - set the "
			    "DATA_PATH parameter to the desired location of the data files");
		}
		// for DuckDB instances - use a default data path
		auto path = metadata_catalog.GetAttached().GetStorageManager().GetDBPath();
		options.data_path = path + ".files";
		InitializeDataPath();
	}
	// default to the latest version when creating a new DuckLake
	auto version =
	    options.ducklake_version == DuckLakeVersion::UNSET ? DUCKLAKE_LATEST_VERSION : options.ducklake_version;
	SetVersionedMetadataManager(transaction, version);
	auto &metadata_manager = transaction.GetMetadataManager();
	metadata_manager.InitializeDuckLake(has_explicit_schema, catalog.Encryption());
	if (auto *pg_mgr = dynamic_cast<PostgresMetadataManager *>(&metadata_manager)) {
		pg_mgr->EnsureIdSequences();
	}
	if (catalog.Encryption() == DuckLakeEncryption::AUTOMATIC) {
		// default to unencrypted
		catalog.SetEncryption(DuckLakeEncryption::UNENCRYPTED);
	}
}

void DuckLakeInitializer::LoadExistingDuckLake(DuckLakeTransaction &transaction) {
	// load the data path from the existing duck lake
	auto &metadata_manager = transaction.GetMetadataManager();
	auto metadata = metadata_manager.LoadDuckLake();
	DuckLakeVersion resolved_version = DuckLakeVersion::UNSET;
	for (auto &tag : metadata.tags) {
		if (tag.key == "version") {
			auto catalog_version = DuckLakeVersionFromString(tag.value);
			auto target_version = ResolveTargetVersion(catalog_version, tag.value);
			if (catalog_version > target_version) {
				// catalog is newer than the requested version, no FWC
				throw InvalidInputException(
				    "DuckLake catalog version is '%s', which is newer than the requested version '%s'. "
				    "Cannot downgrade a DuckLake catalog.",
				    tag.value, DuckLakeVersionToString(target_version));
			}
			if (catalog_version < target_version && !options.automatic_migration) {
				throw InvalidInputException(
				    "DuckLake catalog version mismatch: catalog version is %s, but the extension requires version "
				    "%s. To automatically migrate, set AUTOMATIC_MIGRATION to TRUE when attaching.",
				    tag.value, DuckLakeVersionToString(target_version));
			}
			if (catalog_version == DuckLakeVersion::V0_1) {
				metadata_manager.MigrateV01();
				catalog_version = DuckLakeVersion::V0_2;
			}
			if (catalog_version == DuckLakeVersion::V0_2) {
				metadata_manager.MigrateV02();
				catalog_version = DuckLakeVersion::V0_3;
			}
			if (catalog_version == DuckLakeVersion::V0_3_DEV1) {
				metadata_manager.MigrateV02(true);
				catalog_version = DuckLakeVersion::V0_3;
			}
			if (catalog_version == DuckLakeVersion::V0_3) {
				metadata_manager.MigrateV03();
				catalog_version = DuckLakeVersion::V0_4;
			}
			if (catalog_version == DuckLakeVersion::V0_4_DEV1) {
				metadata_manager.MigrateV03(true);
				catalog_version = DuckLakeVersion::V0_4;
			}
			if (catalog_version == DuckLakeVersion::V0_4) {
				metadata_manager.MigrateV04();
				catalog_version = DuckLakeVersion::V1_0;
			}
			if (catalog_version >= target_version) {
				resolved_version = catalog_version;
				continue;
			}
			if (catalog_version == DuckLakeVersion::V1_0) {
				metadata_manager.MigrateV10();
				catalog_version = DuckLakeVersion::V1_1_DEV_1;
			}
			if (catalog_version != DUCKLAKE_LATEST_VERSION) {
				throw NotImplementedException("Unsupported DuckLake version '%s'",
				                              DuckLakeVersionToString(catalog_version));
			}
			resolved_version = catalog_version;
		}
		if (tag.key == "data_path") {
			if (options.data_path.empty()) {
				options.data_path = metadata_manager.LoadPath(tag.value);
				InitializeDataPath();
			} else {
				// verify that they match if override_data_path is not set to true
				if (metadata_manager.StorePath(options.data_path) != tag.value && !options.override_data_path) {
					throw InvalidConfigurationException(
					    "DATA_PATH parameter \"%s\" does not match existing data path in the catalog \"%s\".\nYou can "
					    "override the DATA_PATH by setting OVERRIDE_DATA_PATH to True.",
					    options.data_path, tag.value);
				}
			}
		}
		if (tag.key == "encrypted") {
			if (tag.value == "true") {
				catalog.SetEncryption(DuckLakeEncryption::ENCRYPTED);
			} else if (tag.value == "false") {
				catalog.SetEncryption(DuckLakeEncryption::UNENCRYPTED);
			} else {
				throw NotImplementedException("Encrypted should be either true or false");
			}
		}
		options.config_options[tag.key] = tag.value;
	}
	for (auto &entry : metadata.schema_settings) {
		options.schema_options[entry.schema_id][entry.tag.key] = entry.tag.value;
	}
	for (auto &entry : metadata.table_settings) {
		options.table_options[entry.table_id][entry.tag.key] = entry.tag.value;
	}
	if (options.access_mode != AccessMode::READ_ONLY) {
		if (auto *pg_mgr = dynamic_cast<PostgresMetadataManager *>(&transaction.GetMetadataManager())) {
			pg_mgr->EnsureIdSequences();
		}
	}
	if (resolved_version != DuckLakeVersion::UNSET) {
		SetVersionedMetadataManager(transaction, resolved_version);
	}
}

DuckLakeVersion DuckLakeInitializer::ResolveTargetVersion(DuckLakeVersion catalog_version,
                                                          const string &catalog_version_str) {
	if (options.ducklake_version != DuckLakeVersion::UNSET) {
		// If the user pinned a version, we use that
		return options.ducklake_version;
	}
	if (options.automatic_migration) {
		// If automatic_migration is on, use to latest
		return DUCKLAKE_LATEST_VERSION;
	}
	if (catalog_version >= DuckLakeVersion::V1_0) {
		// otherwise, use the catalog's current version (must be >= V1_0)
		return catalog_version;
	}
	// pre-1.0 catalogs always require migration
	throw InvalidInputException("DuckLake catalog version mismatch: catalog version is %s, but the extension requires "
	                            "version %s. To automatically migrate, set AUTOMATIC_MIGRATION to TRUE when attaching.",
	                            catalog_version_str, DuckLakeVersionToString(DUCKLAKE_LATEST_VERSION));
}

void DuckLakeInitializer::SetVersionedMetadataManager(DuckLakeTransaction &transaction, DuckLakeVersion version) {
	if (version == DuckLakeVersion::V1_0) {
		// base metadata managers are already V1.0, nop
		return;
	}
	auto &current = transaction.GetMetadataManager();
	unique_ptr<DuckLakeMetadataManager> new_manager;
	if (version == DuckLakeVersion::V1_1_DEV_1) {
		if (dynamic_cast<PostgresMetadataManager *>(&current)) {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<PostgresMetadataManager>>(transaction);
		} else if (dynamic_cast<SQLiteMetadataManager *>(&current)) {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<SQLiteMetadataManager>>(transaction);
		} else {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<DuckLakeMetadataManager>>(transaction);
		}
	} else {
		throw InternalException("SetVersionedMetadataManager: unsupported version");
	}
	transaction.SetMetadataManager(std::move(new_manager));
}

} // namespace duckdb
