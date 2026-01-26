#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
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
	// Postgres syntax: jsonb_agg(jsonb_build_object('key1', val1, 'key2', val2, ...))
	string fields_part;
	for (auto const &entry : fields) {
		if (!fields_part.empty()) {
			fields_part += ", ";
		}
		fields_part += "'" + entry.first + "', " + entry.second;
	}
	return "jsonb_agg(jsonb_build_object(" + fields_part + "))";
}

vector<DuckLakeTag> PostgresMetadataManager::LoadTags(const Value &tag_map) const {
	vector<DuckLakeTag> result;
	if (tag_map.IsNull()) {
		return result;
	}

	// Try to parse as DuckDB LIST/STRUCT first (postgres scanner might convert JSONB)
	try {
		auto &children = ListValue::GetChildren(tag_map);
		for (auto &child : children) {
			auto &struct_children = StructValue::GetChildren(child);
			if (struct_children[1].IsNull()) {
				continue;
			}
			DuckLakeTag tag;
			tag.key = struct_children[0].ToString();
			tag.value = struct_children[1].ToString();
			result.push_back(std::move(tag));
		}
		return result;
	} catch (std::exception &) {
		// Fall back to JSON string parsing
	}

	// If value is a string, parse as JSON
	if (tag_map.type().id() == LogicalTypeId::VARCHAR) {
		auto json_str = tag_map.ToString();
		// TODO: Parse JSON string - for now return empty
		// This would require a JSON parser or using DuckDB's JSON functions
	}
	return result;
}

vector<DuckLakeInlinedTableInfo> PostgresMetadataManager::LoadInlinedDataTables(const Value &list) const {
	vector<DuckLakeInlinedTableInfo> result;
	if (list.IsNull()) {
		return result;
	}

	// Try to parse as DuckDB LIST/STRUCT first
	try {
		auto &children = ListValue::GetChildren(list);
		for (auto &child : children) {
			auto &struct_children = StructValue::GetChildren(child);
			DuckLakeInlinedTableInfo info;
			info.table_name = StringValue::Get(struct_children[0]);
			info.schema_version = struct_children[1].GetValue<idx_t>();
			result.push_back(std::move(info));
		}
		return result;
	} catch (std::exception &) {
		// Fall back to JSON string parsing
	}

	// If value is a string, parse as JSON
	if (list.type().id() == LogicalTypeId::VARCHAR) {
		auto json_str = list.ToString();
		// TODO: Parse JSON string - for now return empty
	}
	return result;
}

vector<DuckLakeMacroImplementation> PostgresMetadataManager::LoadMacroImplementations(const Value &list) const {
	vector<DuckLakeMacroImplementation> result;
	if (list.IsNull()) {
		return result;
	}

	// Try to parse as DuckDB LIST/STRUCT first
	try {
		auto &children = ListValue::GetChildren(list);
		for (auto &child : children) {
			auto &struct_children = StructValue::GetChildren(child);
			DuckLakeMacroImplementation impl;
			impl.dialect = StringValue::Get(struct_children[0]);
			impl.sql = StringValue::Get(struct_children[1]);
			impl.type = StringValue::Get(struct_children[2]);

			auto param_list = struct_children[3].GetValue<Value>();
			if (!param_list.IsNull()) {
				for (auto &param_value : ListValue::GetChildren(param_list)) {
					auto &param_struct_children = StructValue::GetChildren(param_value);
					DuckLakeMacroParameters param;
					param.parameter_name = StringValue::Get(param_struct_children[0]);
					param.parameter_type = StringValue::Get(param_struct_children[1]);
					param.default_value = StringValue::Get(param_struct_children[2]);
					param.default_value_type = StringValue::Get(param_struct_children[3]);
					impl.parameters.push_back(std::move(param));
				}
			}
			result.push_back(std::move(impl));
		}
		return result;
	} catch (std::exception &) {
		// Fall back to JSON string parsing
	}

	// If value is a string, parse as JSON
	if (list.type().id() == LogicalTypeId::VARCHAR) {
		auto json_str = list.ToString();
		// TODO: Parse JSON string - for now return empty
	}
	return result;
}

} // namespace duckdb
