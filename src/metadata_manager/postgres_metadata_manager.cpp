#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_info.hpp"
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

string PostgresMetadataManager::WrapWithListAggregation(const vector<pair<string, string>> &fields) const {
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

string PostgresMetadataManager::CastColumnToTarget(const string &stats, const LogicalType &type) {
	// Should not happen, but to pass unittest
	if (!TypeIsNativelySupported(type)) {
		return stats;
	}
	// Also for nested types
	if (type.id() == LogicalTypeId ::ARRAY) {
		LogicalType child_type = ArrayType::GetChildType(type);

		while (child_type.id() == LogicalTypeId::ARRAY) {
			child_type = ArrayType::GetChildType(child_type);
		}
		if (!TypeIsNativelySupported(child_type)) {
			return stats;
		}
	}
	return stats + "::" + GetColumnTypeInternal(type);
}

vector<DuckLakeTag> PostgresMetadataManager::LoadTags(const Value &tag_map) const {
	using namespace duckdb_yyjson; // NOLINT

	vector<DuckLakeTag> result;

	auto tags_json = tag_map.GetValue<string>();
	auto doc = yyjson_read(tags_json.c_str(), tags_json.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse tags JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid tags JSON: expected array");
	}

	idx_t idx, n_tag;
	yyjson_val *tag_json;
	yyjson_arr_foreach(root, idx, n_tag, tag_json) {
		if (!yyjson_is_obj(tag_json)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid tags JSON: expected object");
		}

		DuckLakeTag tag;

		auto key = yyjson_obj_get(tag_json, "key");
		auto value = yyjson_obj_get(tag_json, "value");
		if (!yyjson_is_str(key)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid tags JSON: missing required fields");
		}

		tag.key = yyjson_get_str(key);

		// Value can be null or string - skip null values
		if (yyjson_is_null(value)) {
			continue;
		}
		if (!yyjson_is_str(value)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid tags JSON: value must be string or null");
		}
		tag.value = yyjson_get_str(value);

		result.push_back(std::move(tag));
	}
	yyjson_doc_free(doc);
	return result;
}

vector<DuckLakeInlinedTableInfo> PostgresMetadataManager::LoadInlinedDataTables(const Value &list) const {
	using namespace duckdb_yyjson; // NOLINT

	vector<DuckLakeInlinedTableInfo> result;

	auto json_str = list.GetValue<string>();
	auto doc = yyjson_read(json_str.c_str(), json_str.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse inlined data tables JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid inlined data tables JSON: expected array");
	}

	idx_t idx, n_tables;
	yyjson_val *table_json;
	yyjson_arr_foreach(root, idx, n_tables, table_json) {
		if (!yyjson_is_obj(table_json)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid inlined data tables JSON: expected object");
		}

		auto table_name = yyjson_obj_get(table_json, "name");
		auto schema_version = yyjson_obj_get(table_json, "schema_version");
		if (!yyjson_is_str(table_name) || !yyjson_is_num(schema_version)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid inlined data tables JSON: missing required fields");
		}

		DuckLakeInlinedTableInfo info;
		info.table_name = yyjson_get_str(table_name);
		info.schema_version = yyjson_get_uint(schema_version);
		result.push_back(std::move(info));
	}
	yyjson_doc_free(doc);
	return result;
}

vector<DuckLakeMacroImplementation> PostgresMetadataManager::LoadMacroImplementations(const Value &list) const {
	using namespace duckdb_yyjson; // NOLINT

	vector<DuckLakeMacroImplementation> result;

	auto json_str = list.GetValue<string>();
	auto doc = yyjson_read(json_str.c_str(), json_str.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse macro implementations JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid macro implementations JSON: expected array");
	}

	idx_t idx, n_impls;
	yyjson_val *impl_json;
	yyjson_arr_foreach(root, idx, n_impls, impl_json) {
		if (!yyjson_is_obj(impl_json)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid macro implementations JSON: expected object");
		}

		auto dialect = yyjson_obj_get(impl_json, "dialect");
		auto sql = yyjson_obj_get(impl_json, "sql");
		auto type = yyjson_obj_get(impl_json, "type");
		auto params = yyjson_obj_get(impl_json, "params");

		if (!yyjson_is_str(dialect) || !yyjson_is_str(sql) || !yyjson_is_str(type)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid macro implementations JSON: missing required fields");
		}

		DuckLakeMacroImplementation impl_info;
		impl_info.dialect = yyjson_get_str(dialect);
		impl_info.sql = yyjson_get_str(sql);
		impl_info.type = yyjson_get_str(type);

		// Parse parameters if present
		if (yyjson_is_arr(params)) {
			idx_t p_idx, n_params;
			yyjson_val *param_json;
			yyjson_arr_foreach(params, p_idx, n_params, param_json) {
				if (!yyjson_is_obj(param_json)) {
					yyjson_doc_free(doc);
					throw InvalidInputException("Invalid macro parameter JSON: expected object");
				}

				auto param_name = yyjson_obj_get(param_json, "parameter_name");
				auto param_type = yyjson_obj_get(param_json, "parameter_type");
				auto default_value = yyjson_obj_get(param_json, "default_value");
				auto default_value_type = yyjson_obj_get(param_json, "default_value_type");

				if (!yyjson_is_str(param_name) || !yyjson_is_str(param_type)) {
					yyjson_doc_free(doc);
					throw InvalidInputException("Invalid macro parameter JSON: missing required fields");
				}

				DuckLakeMacroParameters param;
				param.parameter_name = yyjson_get_str(param_name);
				param.parameter_type = yyjson_get_str(param_type);

				// default_value and default_value_type may be null
				if (yyjson_is_str(default_value)) {
					param.default_value = Value(yyjson_get_str(default_value));
				} else {
					param.default_value = Value();
				}
				if (yyjson_is_str(default_value_type)) {
					param.default_value_type = yyjson_get_str(default_value_type);
				}

				impl_info.parameters.push_back(std::move(param));
			}
		}

		result.push_back(std::move(impl_info));
	}
	yyjson_doc_free(doc);
	return result;
}

} // namespace duckdb
