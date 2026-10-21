#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

#include <algorithm>

namespace duckdb {

SQLiteMetadataManager::SQLiteMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

bool SQLiteMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	// SQLite converts IEEE 754 NaN to NULL when storing double values,
	// so FLOAT/DOUBLE must be stored as VARCHAR to preserve NaN through the round-trip
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::VARIANT:
		return false;
	default:
		return true;
	}
}

static bool IsLockedCommit(const string &lower_message) {
	return StringUtil::Contains(lower_message, "failed to execute query \"commit\": database is locked");
}

bool SQLiteMetadataManager::IsRetryableCommitError(const string &message) const {
	auto lower_message = StringUtil::Lower(message);
	StringUtil::Trim(lower_message);
	return StringUtil::EndsWith(lower_message, "database is locked") && !IsLockedCommit(lower_message);
}

string SQLiteMetadataManager::CommitErrorHint(const string &message) const {
	if (!IsLockedCommit(StringUtil::Lower(message))) {
		return string();
	}
	auto &parameters = transaction.GetCatalog().MetadataParameters();
	auto journal_mode = parameters.find("journal_mode");
	if (journal_mode != parameters.end() && StringUtil::CIEquals(journal_mode->second.ToString(), "wal")) {
		return string();
	}
	return "In its rollback journal mode, a SQLite catalog cannot commit while another transaction is open on it. "
	       "Attach the catalog with META_JOURNAL_MODE 'WAL' to let open transactions and a commit run at the same "
	       "time.\n";
}

string SQLiteMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return "VARCHAR";
	case LogicalTypeId::SQLNULL:
		return "INTEGER";
	default:
		return column_type.ToString();
	}
}

void SQLiteMetadataManager::RenameInlinedColumns(const map<string, vector<pair<string, string>>> &renames) {
	// SQLite parses its whole schema for every renamed column, so the tables are copied with the new names instead
	string query;
	for (auto &table : renames) {
		auto table_name = SQLIdentifier(table.first);
		vector<string> renamed_columns;
		for (auto &column : table.second) {
			renamed_columns.push_back(StringUtil::Format("%s AS %s", column.first, column.second));
		}
		query += StringUtil::Format(
		    "CREATE TEMP TABLE ducklake_renamed_inlined AS SELECT * FROM {METADATA_CATALOG}.%s;", table_name);
		query += StringUtil::Format("DROP TABLE {METADATA_CATALOG}.%s;", table_name);
		query += StringUtil::Format(
		    "CREATE TABLE {METADATA_CATALOG}.%s AS SELECT * RENAME (%s) FROM temp.ducklake_renamed_inlined;",
		    table_name, StringUtil::Join(renamed_columns, ", "));
		query += "DROP TABLE temp.ducklake_renamed_inlined;";
	}
	auto result = Execute(query);
	result->ThrowIfError("Failed to rename inlined-data metadata columns while migrating to v1.1: ");
}

//! The declared type of a stats column, since sqlite_query returns every value as VARCHAR
static string StatsColumnType(const string &stat) {
	if (stat == "value_count" || stat == "null_count" || stat == "column_size_bytes") {
		return "BIGINT";
	}
	if (stat == "contains_nan" || stat == "min_is_exact" || stat == "max_is_exact") {
		return "BOOLEAN";
	}
	if (stat == "min_value" || stat == "max_value" || stat == "extra_stats") {
		return "VARCHAR";
	}
	return string();
}

//! Reads the stats through sqlite_query, since sqlite_scan pushes no filter into SQLite
string SQLiteMetadataManager::GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id,
                                                             const string &metadata_table_prefix) {
	auto unmapped = std::any_of(req.referenced_stats.begin(), req.referenced_stats.end(),
	                            [](const string &stat) { return StatsColumnType(stat).empty(); });
	if (metadata_table_prefix != "{METADATA_CATALOG}" || unmapped) {
		return DuckLakeMetadataManager::GenerateFileColumnStatsCTEBody(req, table_id, metadata_table_prefix);
	}
	// TEXT values, so parallel scans on the shared connection read them without converting
	string native_list = "CAST(data_file_id AS TEXT) AS data_file_id";
	string select_list = "data_file_id::BIGINT AS data_file_id";
	for (const auto &stat : req.referenced_stats) {
		native_list += StringUtil::Format(", CAST(%s AS TEXT) AS %s", stat, stat);
		select_list += StringUtil::Format(", %s::%s AS %s", stat, StatsColumnType(stat), stat);
	}
	auto native_query =
	    StringUtil::Format("SELECT %s FROM (%s)", native_list,
	                       DuckLakeMetadataManager::GenerateFileColumnStatsCTEBody(req, table_id, "main"));
	return StringUtil::Format("  SELECT %s\n"
	                          "  FROM sqlite_query({METADATA_CATALOG_NAME_LITERAL}, %s)\n",
	                          select_list, SQLString(native_query));
}

} // namespace duckdb
