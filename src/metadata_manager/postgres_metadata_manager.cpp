#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "yyjson.hpp"

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
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return false;
	default:
		return true;
	}
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
	case LogicalTypeId::BLOB:
		return "BYTEA";
	default:
		return column_type.ToString();
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(string &query, string command) {
	auto &commit_info = transaction.GetCommitInfo();

	DuckLakeMetadataManager::FillSnapshotCommitArgs(query, commit_info);

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());

	DuckLakeMetadataManager::FillCatalogArgs(query, ducklake_catalog);

	return connection.Query(
	    StringUtil::Format("CALL %s(%s, %s)", std::move(command), catalog_literal, SQLString(query)));
}

unique_ptr<QueryResult> PostgresMetadataManager::Execute(string query) {
	return ExecuteQuery(query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(string query) {
	return ExecuteQuery(query, "postgres_query");
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
		SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		    SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		);
	)";
}

string PostgresMetadataManager::WrapWithListAggregation(const unordered_map<string, string> &fields) const {
	string fields_part;
	for (auto const &entry : fields) {
		if (!fields_part.empty()) {
			fields_part += ", ";
		}
		fields_part += "'" + entry.first + "', " + entry.second;
	}
	return "json_agg(json_build_object(" + fields_part + "))";
}

string PostgresMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type) {
	// PostgreSQL doesn't have TRY_CAST, use regular CAST with :: operator
	// For numeric types, we cast directly; stats should be valid numeric values
	if (type.IsNumeric()) {
		return "(" + stats + " :: " + GetColumnTypeInternal(type) + ")";
	}
	return stats;
}

vector<DuckLakeTag> PostgresMetadataManager::LoadTags(const Value &tag_map) const {
	using namespace duckdb_yyjson; // NOLINT

	// PostgreSQL returns jsonb which doesn't map cleanly to DuckDB LIST type
	// TODO: Implement proper JSON parsing for tags
	// For now, return empty tags to avoid crashing
	vector<DuckLakeTag> result;

	auto tag = tag_map.GetValue<string>();

	auto doc = yyjson_read(tag.c_str(), tag.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse tags JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid tags JSON");
	}

	idx_t idx, n_tag;

	yyjson_arr_foreach(root, idx, n_tag, val)
	{

	}
	auto bbox_json = yyjson_obj_get(root, "bbox");
	if (yyjson_is_obj(bbox_json)) {
		auto xmin_json = yyjson_obj_get(bbox_json, "xmin");
		if (yyjson_is_num(xmin_json)) {
			xmin = yyjson_get_real(xmin_json);
		}
		auto xmax_json = yyjson_obj_get(bbox_json, "xmax");
		if (yyjson_is_num(xmax_json)) {
			xmax = yyjson_get_real(xmax_json);
		}
		auto ymin_json = yyjson_obj_get(bbox_json, "ymin");
		if (yyjson_is_num(ymin_json)) {
			ymin = yyjson_get_real(ymin_json);
		}
		auto ymax_json = yyjson_obj_get(bbox_json, "ymax");
		if (yyjson_is_num(ymax_json)) {
			ymax = yyjson_get_real(ymax_json);
		}
		auto zmin_json = yyjson_obj_get(bbox_json, "zmin");
		if (yyjson_is_num(zmin_json)) {
			zmin = yyjson_get_real(zmin_json);
		}
		auto zmax_json = yyjson_obj_get(bbox_json, "zmax");
		if (yyjson_is_num(zmax_json)) {
			zmax = yyjson_get_real(zmax_json);
		}
		auto mmin_json = yyjson_obj_get(bbox_json, "mmin");
		if (yyjson_is_num(mmin_json)) {
			mmin = yyjson_get_real(mmin_json);
		}
		auto mmax_json = yyjson_obj_get(bbox_json, "mmax");
		if (yyjson_is_num(mmax_json)) {
			mmax = yyjson_get_real(mmax_json);
		}
	}

	auto types_json = yyjson_obj_get(root, "types");
	if (yyjson_is_arr(types_json)) {
		yyjson_arr_iter iter;
		yyjson_arr_iter_init(types_json, &iter);
		yyjson_val *type_json;
		while ((type_json = yyjson_arr_iter_next(&iter))) {
			if (yyjson_is_str(type_json)) {
				geo_types.insert(yyjson_get_str(type_json));
			}
		}
	}
	yyjson_doc_free(doc);
	return result;
}

vector<DuckLakeInlinedTableInfo> PostgresMetadataManager::LoadInlinedDataTables(const Value &list) const {
	// PostgreSQL returns jsonb which doesn't map cleanly to DuckDB LIST type
	// TODO: Implement proper JSON parsing
	// For now, return empty list to avoid crashing
	vector<DuckLakeInlinedTableInfo> result;
	return result;
}

vector<DuckLakeMacroImplementation> PostgresMetadataManager::LoadMacroImplementations(const Value &list) const {
	// PostgreSQL returns jsonb which doesn't map cleanly to DuckDB LIST type
	// TODO: Implement proper JSON parsing
	// For now, return empty list to avoid crashing
	vector<DuckLakeMacroImplementation> result;
	return result;
}

vector<DuckLakeFileForCleanup> PostgresMetadataManager::GetOrphanFilesForCleanup(const string &filter,
                                                                                 const string &separator) {
	// PostgreSQL doesn't support filesystem listing like DuckDB's read_blob
	// Return empty list - orphan files will need to be cleaned up differently
	vector<DuckLakeFileForCleanup> result;
	return result;
}

} // namespace duckdb
