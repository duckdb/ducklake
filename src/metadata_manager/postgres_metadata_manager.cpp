#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
	// If we knew that the Postgres installation has PostGIS installed, we could support GEOMETRY in the future.
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

bool PostgresMetadataManager::SupportsInlining(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return false;
	}
	return DuckLakeMetadataManager::SupportsInlining(type);
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(DuckLakeSnapshot snapshot, string &query,
                                                              string command) {
	auto &commit_info = transaction.GetCommitInfo();

	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);

	return connection.Query(StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query)));
}
unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_query");
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		 );')
	)";
}

idx_t PostgresMetadataManager::FetchScalarSequenceValue(const string &seq_name) {
	DuckLakeSnapshot dummy {0, 0, 0, 0};
	string query = "SELECT nextval('{METADATA_SCHEMA_ESCAPED}." + seq_name + "')";
	auto result = Query(dummy, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to allocate next value from " + seq_name + ": ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw InternalException("ducklake: %s returned no value from nextval()", seq_name);
	}
	auto v = chunk->data[0].GetValue(0).GetValue<int64_t>();
	if (v < 0) {
		throw InternalException("ducklake: %s returned negative value: %lld", seq_name, (long long)v);
	}
	return static_cast<idx_t>(v);
}

idx_t PostgresMetadataManager::AllocateNextSnapshotId(idx_t /*advisory*/) {
	return FetchScalarSequenceValue("ducklake_snapshot_id_seq");
}

idx_t PostgresMetadataManager::AllocateNextCatalogId(idx_t /*advisory*/) {
	return FetchScalarSequenceValue("ducklake_catalog_id_seq");
}

idx_t PostgresMetadataManager::AllocateNextFileId(idx_t /*advisory*/) {
	return FetchScalarSequenceValue("ducklake_file_id_seq");
}

idx_t PostgresMetadataManager::AllocateNextSchemaVersion(idx_t /*advisory*/) {
	// Sequence-allocated: catalog cache keys on schema_version; collisions from
	// concurrent commits cause stale-cache reuse across transactions.
	return FetchScalarSequenceValue("ducklake_schema_version_seq");
}

void PostgresMetadataManager::AcquireCommitLock() {
	DuckLakeSnapshot dummy {};
	if (!commit_lock_classid.IsValid()) {
		string probe = "SELECT hashtext({METADATA_CATALOG_NAME_LITERAL})::int4";
		auto probe_result = Query(dummy, probe);
		if (probe_result->HasError()) {
			probe_result->GetErrorObject().Throw(
			    "concurrent: failed to compute DuckLake commit lock classid: ");
		}
		auto chunk = probe_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			throw InternalException("ducklake: hashtext probe returned no row");
		}
		commit_lock_classid = static_cast<idx_t>(
		    static_cast<uint32_t>(chunk->data[0].GetValue(0).GetValue<int32_t>()));
	}

	string set_timeout = "SET LOCAL lock_timeout = '30s'";
	auto timeout_res = Execute(dummy, set_timeout);
	if (timeout_res->HasError()) {
		timeout_res->GetErrorObject().Throw("concurrent: failed to set lock_timeout: ");
	}

	// "concurrent:" prefix -> RetryOnError matches, transient pg error retries.
	string query = "SELECT pg_advisory_xact_lock(" +
	               std::to_string(static_cast<int32_t>(commit_lock_classid.GetIndex())) + ", " +
	               std::to_string(DUCKLAKE_COMMIT_ADVISORY_SUBKEY) + ")";
	auto result = Execute(dummy, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("concurrent: DuckLake commit serialisation lock failed: ");
	}
}

void PostgresMetadataManager::EnsureIdSequences() {
	// One statement per call: postgres_execute drops all but the first of a batch.
	DuckLakeSnapshot dummy {0, 0, 0, 0};

	auto run = [&](string query) {
		auto result = Execute(dummy, query);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to ensure DuckLake id sequences: ");
		}
	};

	// file_id_seq needs MINVALUE 0: bootstrap next_file_id=0 and first allocation must return 0.
	// CACHE > 1 breaks MAX(snapshot_id)-as-horizon (pre-backend-cached nextval).
	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot_id_seq CACHE 1");
	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_catalog_id_seq CACHE 1");
	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_file_id_seq MINVALUE 0 START WITH 0 CACHE 1");
	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_schema_version_seq CACHE 1");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_snapshot_id_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot_id_seq),
    GREATEST(1, COALESCE((SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 1) >= 1
))");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_catalog_id_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_catalog_id_seq),
    GREATEST(1, COALESCE((SELECT MAX(next_catalog_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(next_catalog_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0) >= 1
))");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_file_id_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_id_seq),
    GREATEST(0, COALESCE((SELECT MAX(next_file_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(next_file_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0) >= 1
))");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_schema_version_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_schema_version_seq),
    GREATEST(1, COALESCE((SELECT MAX(schema_version) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(schema_version) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 1) >= 1
))");

	// Replace the PK-violation signal the sequence allocator eliminates.
	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_schema_name_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_schema (schema_name) "
	    "WHERE end_snapshot IS NULL");
	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_table_name_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_table (schema_id, table_name) "
	    "WHERE end_snapshot IS NULL");
	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_view_name_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_view (schema_id, view_name) "
	    "WHERE end_snapshot IS NULL");
	// One live delete_file per data_file: without it, concurrent DELETEs
	// double-count rows by applying both delete masks as independent variants.
	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_delete_file_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_delete_file (data_file_id) "
	    "WHERE end_snapshot IS NULL");
}

// We need a specialized function here to do a reinterpret for postgres from BLOB to VARCHAR
shared_ptr<DuckLakeInlinedData>
PostgresMetadataManager::TransformInlinedData(QueryResult &result, const vector<LogicalType> &expected_types) {
	bool needs_reinterpret = false;
	if (!expected_types.empty()) {
		D_ASSERT(expected_types.size() == result.types.size());
		for (idx_t i = 0; i < expected_types.size(); i++) {
			if (result.types[i] != expected_types[i]) {
				D_ASSERT(result.types[i].id() == LogicalTypeId::BLOB &&
				         expected_types[i].id() == LogicalTypeId::VARCHAR);
				needs_reinterpret = true;
			}
		}
	}
	if (!needs_reinterpret) {
		return DuckLakeMetadataManager::TransformInlinedData(result, expected_types);
	}

	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to read inlined data from DuckLake: ");
	}
	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, expected_types);
	DataChunk reinterpret_chunk;
	reinterpret_chunk.Initialize(*context, expected_types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}
		for (idx_t i = 0; i < expected_types.size(); i++) {
			reinterpret_chunk.data[i].Reinterpret(chunk->data[i]);
		}
		reinterpret_chunk.SetCardinality(chunk->size());
		data->Append(reinterpret_chunk);
	}
	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

} // namespace duckdb
