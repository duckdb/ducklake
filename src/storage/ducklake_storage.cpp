#include "duckdb.hpp"

#include "storage/ducklake_storage.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "storage/ducklake_secret.hpp"

namespace duckdb {

static void HandleDuckLakeOption(DuckLakeOptions &options, const string &option, const Value &value) {
	auto lcase = StringUtil::Lower(option);
	if (lcase == "data_path") {
		options.data_path = value.ToString();
	} else if (lcase == "override_data_path") {
		options.override_data_path = value.GetValue<bool>();
	} else if (lcase == "metadata_schema") {
		options.metadata_schema = value.ToString();
	} else if (lcase == "metadata_catalog") {
		options.metadata_database = value.ToString();
	} else if (lcase == "metadata_path") {
		options.metadata_path = value.ToString();
	} else if (lcase == "metadata_parameters") {
		auto &children = MapValue::GetChildren(value);
		for (auto &child : children) {
			auto &key_value = StructValue::GetChildren(child);
			auto &parameter_name = StringValue::Get(key_value[0]);
			options.metadata_parameters[parameter_name] = key_value[1];
		}
	} else if (lcase == "encrypted") {
		if (value.GetValue<bool>()) {
			options.encryption = DuckLakeEncryption::ENCRYPTED;
		} else {
			options.encryption = DuckLakeEncryption::UNENCRYPTED;
		}
	} else if (lcase == "data_inlining_row_limit") {
		options.config_options["data_inlining_row_limit"] = value.DefaultCastAs(LogicalType::UBIGINT).ToString();
	} else if (lcase == "snapshot_version") {
		if (options.at_clause) {
			throw InvalidInputException("Cannot specify both VERSION and TIMESTAMP");
		}
		options.at_clause = make_uniq<BoundAtClause>("version", value.DefaultCastAs(LogicalType::BIGINT));
	} else if (lcase == "snapshot_time") {
		if (options.at_clause) {
			throw InvalidInputException("Cannot specify both VERSION and TIMESTAMP");
		}
		options.at_clause = make_uniq<BoundAtClause>("timestamp", value.DefaultCastAs(LogicalType::TIMESTAMP_TZ));
	} else if (StringUtil::StartsWith(lcase, "meta_")) {
		auto parameter_name = lcase.substr(5);
		options.metadata_parameters[parameter_name] = value;
	} else if (lcase == "create_if_not_exists") {
		options.create_if_not_exists = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
	} else if (lcase == "migrate_if_required") {
		options.migrate_if_required = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
	} else {
		throw NotImplementedException("Unsupported option %s for DuckLake", option);
	}
}

static unique_ptr<Catalog> DuckLakeAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &attach_options) {
	DuckLakeOptions options;
	unique_ptr<SecretEntry> secret;
	if (info.path.empty()) {
		// no path specified - load the default secret
		secret = DuckLakeSecret::GetSecret(context, DuckLakeSecret::DEFAULT_SECRET);
		if (!secret) {
			throw InvalidInputException(
			    "Default secret was not found - either specify a path to attach to directly, or create the secret");
		}
	} else if (DuckLakeSecret::PathIsSecret(info.path)) {
		// if the path is a plain name - load the secret name
		secret = DuckLakeSecret::GetSecret(context, info.path);
		if (!secret) {
			throw InvalidInputException(
			    "Secret \"%s\" was not found - if this was meant to be a path to a DuckDB file, use duckdb:%s instead",
			    info.path, info.path);
		}
	} else {
		// otherwise set the remainder of the path as the metadata path
		options.metadata_path = info.path;
	}
	if (secret) {
		// if we have a secret - handle the options
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret->secret);
		for (auto &entry : kv_secret.secret_map) {
			HandleDuckLakeOption(options, entry.first, entry.second);
		}
	}
	options.access_mode = attach_options.access_mode;
	bool is_create_if_not_exists_set = false;
	for (auto &entry : attach_options.options) {
		if (StringUtil::Lower(entry.first) == "create_if_not_exists") {
			is_create_if_not_exists_set = true;
		}
		HandleDuckLakeOption(options, entry.first, entry.second);
	}
	if (options.access_mode == AccessMode::READ_ONLY && !is_create_if_not_exists_set) {
		options.create_if_not_exists = false;
	}
	if (options.metadata_database.empty()) {
		options.metadata_database = "__ducklake_metadata_" + name;
	}
	if (options.at_clause) {
		if (attach_options.access_mode == AccessMode::READ_WRITE) {
			throw InvalidInputException("SNAPSHOT_VERSION / SNAPSHOT_TIME can only be used in read-only mode");
		}
		attach_options.access_mode = AccessMode::READ_ONLY;
		db.SetReadOnlyDatabase();
	}
	return make_uniq<DuckLakeCatalog>(db, std::move(options));
}

static unique_ptr<TransactionManager> DuckLakeCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	return make_uniq<DuckLakeTransactionManager>(db, ducklake_catalog);
}

DuckLakeStorageExtension::DuckLakeStorageExtension() {
	attach = DuckLakeAttach;
	create_transaction_manager = DuckLakeCreateTransactionManager;
}

} // namespace duckdb
