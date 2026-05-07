#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

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

// ---------------------------------------------------------------------------
// Postgres-flavored list aggregation: emit jsonb_agg(jsonb_build_object(...))
// and parse the resulting JSON text in the Load* helpers.
// ---------------------------------------------------------------------------

string PostgresMetadataManager::MetadataExistsQuery() const {
	// Runs server-side via `CALL postgres_query(catalog, sql)`; postgres exposes
	// its own information_schema.tables natively. No catalog prefix needed.
	return "SELECT COUNT(*) FROM information_schema.tables "
	       "WHERE table_name = 'ducklake_metadata' AND table_schema = {METADATA_SCHEMA_NAME_LITERAL}";
}

bool PostgresMetadataManager::MetadataExists() {
	auto query = MetadataExistsQuery();
	auto result = DuckLakeMetadataManager::Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to probe DuckLake metadata: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return false;
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>() > 0;
}

string PostgresMetadataManager::ListAggregation(const vector<pair<string, string>> &fields) const {
	string parts;
	for (auto const &f : fields) {
		if (!parts.empty()) {
			parts += ", ";
		}
		parts += "'" + f.first + "', " + f.second;
	}
	return "jsonb_agg(jsonb_build_object(" + parts + "))::TEXT";
}

namespace {

string YyjsonValToString(yyjson_val *val) {
	if (!val || yyjson_is_null(val)) {
		return string();
	}
	if (yyjson_is_str(val)) {
		auto raw = yyjson_get_str(val);
		return raw ? string(raw) : string();
	}
	if (yyjson_is_bool(val)) {
		return yyjson_get_bool(val) ? "true" : "false";
	}
	if (yyjson_is_uint(val)) {
		return std::to_string(yyjson_get_uint(val));
	}
	if (yyjson_is_int(val)) {
		return std::to_string(yyjson_get_sint(val));
	}
	if (yyjson_is_real(val)) {
		return std::to_string(yyjson_get_real(val));
	}
	size_t len = 0;
	auto str = yyjson_val_write(val, 0, &len);
	if (!str) {
		return string();
	}
	string out(str, len);
	free(str);
	return out;
}

yyjson_doc *ParseJsonAggregate(const Value &value) {
	if (value.IsNull()) {
		return nullptr;
	}
	auto str = StringValue::Get(value);
	auto doc = yyjson_read(str.c_str(), str.size(), 0);
	if (!doc) {
		throw InvalidInputException(
		    "PostgresMetadataManager: failed to parse JSON aggregate from metadata: %s", str);
	}
	return doc;
}

} // namespace

vector<DuckLakeTag> PostgresMetadataManager::LoadTags(const Value &tag_map) const {
	vector<DuckLakeTag> result;
	auto doc = ParseJsonAggregate(tag_map);
	if (!doc) {
		return result;
	}
	auto root = yyjson_doc_get_root(doc);
	if (yyjson_is_arr(root)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(root, idx, max, item) {
			if (!yyjson_is_obj(item)) {
				continue;
			}
			auto value_v = yyjson_obj_get(item, "value");
			if (!value_v || yyjson_is_null(value_v)) {
				// matches base behavior: skip tags whose value is NULL
				continue;
			}
			DuckLakeTag tag;
			tag.key = YyjsonValToString(yyjson_obj_get(item, "key"));
			tag.value = YyjsonValToString(value_v);
			result.push_back(std::move(tag));
		}
	}
	yyjson_doc_free(doc);
	return result;
}

vector<DuckLakeInlinedTableInfo>
PostgresMetadataManager::LoadInlinedDataTables(const Value &list) const {
	vector<DuckLakeInlinedTableInfo> result;
	auto doc = ParseJsonAggregate(list);
	if (!doc) {
		return result;
	}
	auto root = yyjson_doc_get_root(doc);
	if (yyjson_is_arr(root)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(root, idx, max, item) {
			if (!yyjson_is_obj(item)) {
				continue;
			}
			DuckLakeInlinedTableInfo info;
			info.table_name = YyjsonValToString(yyjson_obj_get(item, "name"));
			auto sv = yyjson_obj_get(item, "schema_version");
			if (yyjson_is_uint(sv)) {
				info.schema_version = static_cast<idx_t>(yyjson_get_uint(sv));
			} else if (yyjson_is_int(sv)) {
				info.schema_version = static_cast<idx_t>(yyjson_get_sint(sv));
			} else {
				auto str = YyjsonValToString(sv);
				info.schema_version = str.empty() ? 0 : static_cast<idx_t>(std::stoull(str));
			}
			result.push_back(std::move(info));
		}
	}
	yyjson_doc_free(doc);
	return result;
}

vector<DuckLakeMacroImplementation>
PostgresMetadataManager::LoadMacroImplementations(const Value &list) const {
	vector<DuckLakeMacroImplementation> result;
	auto doc = ParseJsonAggregate(list);
	if (!doc) {
		return result;
	}
	auto root = yyjson_doc_get_root(doc);
	if (yyjson_is_arr(root)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(root, idx, max, item) {
			if (!yyjson_is_obj(item)) {
				continue;
			}
			DuckLakeMacroImplementation impl;
			impl.dialect = YyjsonValToString(yyjson_obj_get(item, "dialect"));
			impl.sql = YyjsonValToString(yyjson_obj_get(item, "sql"));
			impl.type = YyjsonValToString(yyjson_obj_get(item, "type"));
			auto params = yyjson_obj_get(item, "params");
			if (yyjson_is_arr(params)) {
				size_t pidx, pmax;
				yyjson_val *pitem;
				yyjson_arr_foreach(params, pidx, pmax, pitem) {
					if (!yyjson_is_obj(pitem)) {
						continue;
					}
					DuckLakeMacroParameters p;
					p.parameter_name =
					    YyjsonValToString(yyjson_obj_get(pitem, "parameter_name"));
					p.parameter_type =
					    YyjsonValToString(yyjson_obj_get(pitem, "parameter_type"));
					auto dv = yyjson_obj_get(pitem, "default_value");
					if (dv && !yyjson_is_null(dv)) {
						p.default_value = Value(YyjsonValToString(dv));
					}
					p.default_value_type =
					    YyjsonValToString(yyjson_obj_get(pitem, "default_value_type"));
					impl.parameters.push_back(std::move(p));
				}
			}
			result.push_back(std::move(impl));
		}
	}
	yyjson_doc_free(doc);
	return result;
}

} // namespace duckdb
