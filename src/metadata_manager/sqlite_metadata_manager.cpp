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

//! The type the stats table declares for a column, since sqlite_query returns every value as VARCHAR. Empty for a
//! stat this list does not name, which keeps the base CTE rather than guessing its type.
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

//! sqlite_scan pushes no filter into SQLite, so the base CTE copies every table's stats rows into DuckDB on each
//! filtered read. sqlite_query runs the WHERE inside SQLite, in the same transaction.
string SQLiteMetadataManager::GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id,
                                                             const string &metadata_table_prefix) {
	auto unmapped = std::any_of(req.referenced_stats.begin(), req.referenced_stats.end(),
	                            [](const string &stat) { return StatsColumnType(stat).empty(); });
	if (metadata_table_prefix != "{METADATA_CATALOG}" || unmapped) {
		return DuckLakeMetadataManager::GenerateFileColumnStatsCTEBody(req, table_id, metadata_table_prefix);
	}
	string native_list = "data_file_id";
	string select_list = "data_file_id::BIGINT AS data_file_id";
	for (const auto &stat : req.referenced_stats) {
		if (stat == "value_count" || stat == "null_count") {
			// every row of a file counts as a value or a NULL, so zero counts of both are unknown
			native_list += StringUtil::Format(
			    ", CASE WHEN value_count = 0 AND null_count = 0 THEN NULL ELSE %s END AS %s", stat, stat);
		} else {
			native_list += ", " + stat;
		}
		select_list += StringUtil::Format(", %s::%s AS %s", stat, StatsColumnType(stat), stat);
	}
	auto native_query =
	    StringUtil::Format("SELECT %s FROM ducklake_file_column_stats WHERE column_id = %d AND table_id = %d",
	                       native_list, req.column_field_index, table_id.index);
	return StringUtil::Format("  SELECT %s\n"
	                          "  FROM sqlite_query({METADATA_CATALOG_NAME_LITERAL}, %s)\n",
	                          select_list, SQLString(native_query));
}

//! Covering, so the per-column stats lookups behind file pruning read only the index. IF NOT EXISTS takes no
//! write lock once the index exists; building it holds the lock for as long as the stats table is large.
static string LookupIndexStatement() {
	return "SELECT * FROM sqlite_query({METADATA_CATALOG_NAME_LITERAL}, 'CREATE INDEX IF NOT EXISTS "
	       "ducklake_file_column_stats_lookup ON ducklake_file_column_stats(table_id, column_id, data_file_id, "
	       "value_count, null_count, contains_nan, min_value, max_value)');";
}

string SQLiteMetadataManager::GetCreateTableStatements() {
	return DuckLakeMetadataManager::GetCreateTableStatements() + LookupIndexStatement();
}

void SQLiteMetadataManager::CreateLookupIndexes() {
	auto result = Execute(LookupIndexStatement());
	result->ThrowIfError("Failed to create the DuckLake file column stats index:");
}

} // namespace duckdb
