#include "common/ducklake_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_encryption_provider.hpp"
#include "storage/ducklake_transaction.hpp"

#include <limits>

namespace duckdb {

//! One row of the sweep's report. Carries no key material, old or new.
struct RewrapReportRow {
	string file_kind;
	int64_t table_id = 0;
	int64_t file_id = 0;
	string stored_path;
	bool rewrapped = false;
};

struct RewrapKeysBindData : public TableFunctionData {
	explicit RewrapKeysBindData(Catalog &catalog) : catalog(catalog) {
	}

	Catalog &catalog;
	bool dry_run = false;
	idx_t batch_size = 128;
};

struct RewrapKeysGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	bool executed = false;
	vector<RewrapReportRow> report;
};

//! The catalog tables that carry an encryption_key. Both must be swept: a delete file whose key is
//! left on the retired version breaks every read of the rows it deletes.
struct RewrapTarget {
	const char *table_name;
	const char *id_column;
	bool is_delete_file;
};

static const RewrapTarget REWRAP_TARGETS[] = {
    {"ducklake_data_file", "data_file_id", false},
    {"ducklake_delete_file", "delete_file_id", true},
};

static unique_ptr<FunctionData> DuckLakeRewrapKeysBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto result = make_uniq<RewrapKeysBindData>(catalog);

	for (auto &entry : input.named_parameters) {
		if (entry.first == "dry_run") {
			result->dry_run = BooleanValue::Get(entry.second);
		} else if (entry.first == "batch_size") {
			auto requested = entry.second.GetValue<int64_t>();
			if (requested <= 0) {
				throw InvalidInputException("ducklake_rewrap_keys: batch_size must be positive, got %lld",
				                            static_cast<long long>(requested));
			}
			result->batch_size = NumericCast<idx_t>(requested);
		} else {
			throw InternalException("Unsupported named parameter for ducklake_rewrap_keys");
		}
	}

	// Refused at bind rather than reported as an empty sweep: an empty sweep reads as "nothing left to
	// do", which is the reading under which the outgoing key gets retired.
	if (!ducklake_catalog.EncryptionProvider()) {
		throw InvalidInputException("ducklake_rewrap_keys: this DuckLake was attached without encryption_socket, so "
		                            "its encryption keys are unwrapped and there is nothing to rewrap");
	}

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file_kind");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("table_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("file_id");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("rewrapped");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeRewrapKeysInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_uniq<RewrapKeysGlobalState>();
}

//! `"catalog"."schema"`, spelled out because the sweep runs on its own connection and so never goes
//! through DuckLakeMetadataManager::Query, which is what expands {METADATA_CATALOG}.
static string MetadataCatalogPrefix(DuckLakeCatalog &catalog) {
	return DuckLakeUtil::SQLIdentifierToString(catalog.MetadataDatabaseName()) + "." +
	       DuckLakeUtil::SQLIdentifierToString(catalog.MetadataSchemaName());
}

static void RequireSuccess(QueryResult &result, const string &what) {
	if (result.HasError()) {
		throw IOException("ducklake_rewrap_keys: %s failed: %s", what, result.GetError());
	}
}

//! Sweeps one catalog table, appending to `report`. `connection` is the sweep's own connection in
//! autocommit, so an interrupted sweep leaves a committed prefix rather than nothing.
static void SweepTable(Connection &connection, DuckLakeCatalog &catalog, DuckLakeEncryptionProvider &provider,
                       const RewrapTarget &target, const RewrapKeysBindData &bind_data,
                       vector<RewrapReportRow> &report) {
	auto prefix = MetadataCatalogPrefix(catalog);
	// Keyset pagination, not OFFSET: the sweep only ever replaces encryption_key, so neither the
	// predicate nor the ordering key moves under the walk.
	int64_t last_id = std::numeric_limits<int64_t>::min();
	while (true) {
		auto select = StringUtil::Format("SELECT %s, table_id, path, encryption_key FROM %s.%s WHERE "
		                                 "encryption_key IS NOT NULL AND "
		                                 "encryption_key <> '' AND %s > %lld ORDER BY %s LIMIT %llu",
		                                 target.id_column, prefix, target.table_name, target.id_column,
		                                 static_cast<long long>(last_id), target.id_column,
		                                 static_cast<uint64_t>(bind_data.batch_size));
		auto page = connection.Query(select);
		RequireSuccess(*page, StringUtil::Format("reading a page of %s", target.table_name));

		vector<int64_t> file_ids;
		vector<int64_t> table_ids;
		vector<string> stored_paths;
		vector<string> blobs;
		vector<DuckLakeFileIdentity> identities;
		for (auto &row : *page) {
			auto file_id = row.GetValue<int64_t>(0);
			auto table_id = row.GetValue<int64_t>(1);
			auto stored_path = row.GetValue<string>(2);
			auto blob = row.GetValue<string>(3);
			// An unwrapped key belongs to no key version, so the sweep cannot move it. Walking past it
			// would report the sweep complete and strand the row when the outgoing version is retired.
			if (!DuckLakeEncryptionProvider::LooksWrapped(blob)) {
				throw IOException("ducklake_rewrap_keys: %s file %s (table %lld) carries an unwrapped encryption key, "
				                  "so there is no key version to move it from. Resolve the row, then re-run the sweep",
				                  target.is_delete_file ? "delete" : "data", stored_path,
				                  static_cast<long long>(table_id));
			}
			// LooksWrapped only discriminates wrapped from unwrapped; a value outside the base64 alphabet
			// can never be a wrapped key and is not worth sending to the key service.
			if (!DuckLakeEncryptionProvider::IsBase64(blob)) {
				throw IOException("ducklake_rewrap_keys: %s file %s (table %lld) carries an encryption key that is not "
				                  "base64, so it is corrupt rather than wrapped",
				                  target.is_delete_file ? "delete" : "data", stored_path,
				                  static_cast<long long>(table_id));
			}
			file_ids.push_back(file_id);
			table_ids.push_back(table_id);
			stored_paths.push_back(stored_path);
			blobs.push_back(blob);
			// Built from the same row the blob came from, using the stored path rather than a resolved
			// one, because that is what the key was bound to when it was minted.
			identities.push_back(catalog.BuildEncryptionIdentity(TableIndex(NumericCast<idx_t>(table_id)), stored_path,
			                                                     target.is_delete_file));
			last_id = file_id;
		}
		if (identities.empty()) {
			return;
		}

		auto results = provider.RewrapKeys(identities, blobs);
		if (results.size() != identities.size()) {
			throw IOException("ducklake_rewrap_keys: the encryption provider answered %llu items for %llu files",
			                  static_cast<uint64_t>(results.size()), static_cast<uint64_t>(identities.size()));
		}

		for (idx_t i = 0; i < results.size(); i++) {
			RewrapReportRow entry;
			entry.file_kind = target.is_delete_file ? "delete" : "data";
			entry.table_id = table_ids[i];
			entry.file_id = file_ids[i];
			entry.stored_path = stored_paths[i];
			entry.rewrapped = results[i].rewrapped;
			if (!results[i].rewrapped || bind_data.dry_run) {
				report.push_back(entry);
				continue;
			}
			if (results[i].wrapped.empty()) {
				throw IOException("ducklake_rewrap_keys: the encryption provider returned an empty wrapped key for %s, "
				                  "which would discard the key the file was written with",
				                  stored_paths[i]);
			}
			// Compare-and-swap: written by primary key, and only while encryption_key still holds the
			// value that was sent. A row another writer changed is left alone for the next pass.
			auto update = StringUtil::Format(
			    "UPDATE %s.%s SET encryption_key = %s WHERE %s = %lld AND encryption_key = %s", prefix,
			    target.table_name, DuckLakeUtil::SQLLiteralToString(results[i].wrapped), target.id_column,
			    static_cast<long long>(file_ids[i]), DuckLakeUtil::SQLLiteralToString(blobs[i]));
			auto written = connection.Query(update);
			RequireSuccess(*written, StringUtil::Format("rewriting a key in %s", target.table_name));
			report.push_back(entry);
		}
	}
}

static void DuckLakeRewrapKeysExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<RewrapKeysBindData>();
	auto &state = data_p.global_state->Cast<RewrapKeysGlobalState>();

	if (!state.executed) {
		auto &ducklake_catalog = data.catalog.Cast<DuckLakeCatalog>();
		auto provider = ducklake_catalog.EncryptionProvider();
		if (!provider) {
			throw InvalidInputException("ducklake_rewrap_keys: this DuckLake has no encryption provider");
		}
		// The sweep's own connection, in autocommit, so each batch is durable when it returns.
		auto &db = DatabaseInstance::GetDatabase(context);
		Connection connection(db);
		for (auto &target : REWRAP_TARGETS) {
			SweepTable(connection, ducklake_catalog, *provider, target, data, state.report);
		}
		state.executed = true;
	}

	idx_t count = 0;
	while (state.offset < state.report.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.report[state.offset++];
		output.data[0].Append(Value(entry.file_kind));
		output.data[1].Append(Value::BIGINT(entry.table_id));
		output.data[2].Append(Value::BIGINT(entry.file_id));
		output.data[3].Append(Value(entry.stored_path));
		output.data[4].Append(Value::BOOLEAN(entry.rewrapped));
		count++;
	}
	output.SetChildCardinality(count);
}

DuckLakeRewrapKeysFunction::DuckLakeRewrapKeysFunction()
    : TableFunction("ducklake_rewrap_keys", {LogicalType::VARCHAR}, DuckLakeRewrapKeysExecute, DuckLakeRewrapKeysBind,
                    DuckLakeRewrapKeysInit) {
	named_parameters["dry_run"] = LogicalType::BOOLEAN;
	named_parameters["batch_size"] = LogicalType::BIGINT;
}

} // namespace duckdb
