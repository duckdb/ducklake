#include "metadata_manager/postgres_metadata_manager.hpp"

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
		return false;
	default:
		return true;
	}
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

string PostgresMetadataManager::GenerateCTESectionFromRequirements(
    const unordered_map<idx_t, CTERequirement> &requirements, TableIndex table_id) {
	if (requirements.empty()) {
		return "";
	}

	// For PostgreSQL backends, use postgres_query() to execute the column stats lookup
	// directly on the PostgreSQL server. This allows PostgreSQL to use its indexes on
	// (table_id, column_id) instead of DuckDB's postgres scanner bulk-copying the
	// entire ducklake_file_column_stats table via COPY with ctid range batches.
	string cte_section = "WITH ";
	bool first_cte = true;

	for (const auto &entry : requirements) {
		const auto &req = entry.second;

		if (!first_cte) {
			cte_section += ",\n";
		}
		first_cte = false;

		string select_list = "data_file_id";
		for (const auto &stat : req.referenced_stats) {
			select_list += ", " + stat;
		}

		string materialized_hint = (req.reference_count > 1) ? " AS MATERIALIZED" : " AS NOT MATERIALIZED";

		cte_section +=
		    StringUtil::Format("col_%d_stats%s (\n", req.column_field_index, materialized_hint.c_str());
		cte_section += StringUtil::Format(
		    "  SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},\n"
		    "    'SELECT %s\n"
		    "     FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats\n"
		    "     WHERE column_id = %d AND table_id = %d')\n",
		    select_list.c_str(), req.column_field_index, table_id.index);
		cte_section += ")";
	}

	return cte_section + "\n";
}

} // namespace duckdb
