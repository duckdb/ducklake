#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "storage/ducklake_initializer.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "common/ducklake_version.hpp"
#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/quack_metadata_manager.hpp"

namespace duckdb {

DuckLakeInitializer::DuckLakeInitializer(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeOptions &options_p)
    : context(context), catalog(catalog), options(options_p) {
	InitializeDataPath();
}

string DuckLakeInitializer::GetAttachOptions() {
	vector<string> attach_options;
	if (options.access_mode != AccessMode::AUTOMATIC) {
		attach_options.push_back(EnumUtil::ToString(options.access_mode));
	}
	for (auto &option : options.metadata_parameters) {
		attach_options.push_back(option.first + " " + option.second.ToSQLString());
	}
	const string metadata_type = catalog.MetadataType();
	if (metadata_type.empty() || metadata_type == "duckdb") {
		// this is duckdb, we always do latest storage
		attach_options.push_back(StringUtil::Format("STORAGE_VERSION '%s'", "latest"));
	}
	// scope the underlying Postgres attach to the metadata schema - otherwise the postgres extension reflects
	// every schema in the database on attach, which is very slow on large or multi-tenant catalogs.
	// only done when the user explicitly set METADATA_SCHEMA - the default schema is not known until after attach.
	// an explicit META_SCHEMA (or metadata_parameters 'schema') takes precedence over this.
	bool is_postgres = metadata_type == "postgres" || metadata_type == "postgres_scanner";
	bool user_set_schema = options.metadata_parameters.find("schema") != options.metadata_parameters.end();
	if (is_postgres && !user_set_schema && !options.metadata_schema.empty()) {
		attach_options.push_back("SCHEMA " + SQLString::ToString(options.metadata_schema.GetIdentifierName()));
	}
	if (options.hide_metadata_catalog) {
		attach_options.push_back("HIDDEN true");
	}

	if (attach_options.empty()) {
		return string();
	}
	return " (" + StringUtil::Join(attach_options, ", ") + ")";
}

void DuckLakeInitializer::Initialize() {
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	// attach the metadata database
	AttachMetadata(transaction);
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
			options.ducklake_version = ParseWritableDuckLakeVersion(setting_val.ToString(), "ducklake_default_version");
		}
	}

	// after the metadata database is attached initialize the ducklake
	// check if we are loading an existing DuckLake or creating a new one
	// directly query a known ducklake metadata table to avoid scanning all attached catalogs via duckdb_tables()
	// this prevents a corrupted ducklake catalog from blocking initialization of unrelated ducklake databases
	// FIXME: verify that all ducklake tables are in the correct format
	if (transaction.GetMetadataManager().MetadataExists()) {
		LoadExistingDuckLake(transaction);
	} else {
		if (!options.create_if_not_exists) {
			throw InvalidInputException("Existing DuckLake at metadata catalog \"%s\" does not exist - and creating a "
			                            "new DuckLake is explicitly disabled",
			                            options.metadata_path);
		}
		try {
			InitializeNewDuckLake(transaction, has_explicit_schema);
		} catch (std::exception &ex) {
			// another attach may have created the same DuckLake at the same time
			ErrorData error(ex);
			RestartMetadataTransaction(transaction);
			if (!DuckLakeIsInitialized(transaction)) {
				error.Throw();
			}
			LoadExistingDuckLake(transaction);
		}
	}
	auto &current_metadata_manager = transaction.GetMetadataManager();
	// probe the metadata server for optional capabilities (e.g. server-side commit retries) once per attach
	current_metadata_manager.ProbeServerCapabilities();
	current_metadata_manager.ClearCache();
	if (options.at_clause) {
		// if the user specified a snapshot try to load it to trigger an error if it does not exist
		transaction.GetSnapshot();
	}
}

void DuckLakeInitializer::RestartMetadataTransaction(DuckLakeTransaction &transaction) {
	transaction.Rollback();
	AttachMetadata(transaction);
}

void DuckLakeInitializer::AttachMetadata(DuckLakeTransaction &transaction) {
	const string attach_query =
	    "ATTACH OR REPLACE {METADATA_PATH} AS {METADATA_CATALOG_NAME_IDENTIFIER}" + GetAttachOptions();
	auto result = transaction.GetMetadataManager().AttachMetadata(attach_query);
	result->ThrowIfError("Failed to attach DuckLake MetaData \"" + catalog.MetadataDatabaseName() + "\" at path + \"" +
	                     catalog.MetadataPath() + "\"");
}

bool DuckLakeInitializer::DuckLakeIsInitialized(DuckLakeTransaction &transaction) {
	auto &metadata_manager = transaction.GetMetadataManager();
	if (!metadata_manager.MetadataExists()) {
		return false;
	}
	for (auto &tag : metadata_manager.LoadDuckLake().tags) {
		if (tag.key == "version") {
			return true;
		}
	}
	return false;
}

void DuckLakeInitializer::InitializeDataPath() {
	auto &data_path = options.data_path;
	if (data_path.empty()) {
		return;
	}
	ExtensionHelper::AutoLoadExtensionForPath(*context.db, data_path, "Data path");

	auto &fs = FileSystem::GetFileSystem(context);
	auto separator = fs.PathSeparator(data_path);
	StringUtil::RTrim(data_path, "/\\");
	// ensure the paths we store always end in a path separator
	data_path += separator;
	catalog.Separator() = separator;
}

void DuckLakeInitializer::InitializeNewDuckLake(DuckLakeTransaction &transaction, bool has_explicit_schema) {
	if (options.data_path.empty()) {
		auto &metadata_catalog =
		    Catalog::GetCatalog(*transaction.GetConnection().context, Identifier(options.metadata_database));
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
	if (catalog.Encryption() == DuckLakeEncryption::AUTOMATIC) {
		// default to unencrypted
		catalog.SetEncryption(DuckLakeEncryption::UNENCRYPTED);
	}
}

void DuckLakeInitializer::LoadExistingDuckLake(DuckLakeTransaction &transaction, bool skip_dev_migration) {
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
			if (catalog_version == DuckLakeVersion::V1_1_DEV_1) {
				// dev schemas evolve in place, re-run the v1.1 migration so older dev catalogs get every addition
				if (options.automatic_migration) {
					// an explicitly requested migration fails loudly
					metadata_manager.MigrateV10(true);
				} else if (options.access_mode != AccessMode::READ_ONLY && !skip_dev_migration) {
					metadata_manager.MigrateV10Dev();
					if (ValidChecker::IsInvalidated(transaction.GetConnection().context->ActiveTransaction())) {
						RestartMetadataTransaction(transaction);
						LoadExistingDuckLake(transaction, true);
						return;
					}
				}
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
	// set correct version metadata manager
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
	catalog.SetDuckLakeVersion(version);
	if (version == DuckLakeVersion::V1_0) {
		// base metadata managers are already V1.0, nop
		return;
	}
	auto &current = transaction.GetMetadataManager();
	unique_ptr<DuckLakeMetadataManager> new_manager;
	if (version == DuckLakeVersion::V1_1_DEV_1) {
		if (dynamic_cast<QuackMetadataManager *>(&current)) {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<QuackMetadataManager>>(transaction);
		} else if (dynamic_cast<PostgresMetadataManager *>(&current)) {
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
