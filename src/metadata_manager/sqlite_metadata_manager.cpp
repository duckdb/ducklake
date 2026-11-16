#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

SQLiteMetadataManager::SQLiteMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

bool SQLiteMetadataManager::Manages(const string &metadata_type) {
	return metadata_type == "sqlite" || metadata_type == "sqlite_scanner";
}

//! Whether the table function declares the named option, the shipped scanner lists them as typed kwargs
static bool AcceptsOption(const TableFunctionCatalogEntry &entry, const Identifier &option) {
	auto options = entry.functions.functions[0]->GetSignature().GetTypedKwargs();
	return options && options->Find(option);
}

bool SQLiteMetadataManager::NativeLoadAvailable() const {
	if (!native_load_available.IsValid()) {
		// an untyped sqlite_query returns every column as text, the parser needs the types of the scanner
		auto &context = *transaction.GetConnection().context;
		auto function = Catalog::GetEntry<TableFunctionCatalogEntry>(
		    context, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "sqlite_query"), OnEntryNotFound::RETURN_NULL);
		native_load_available = function && AcceptsOption(*function, "column_types") ? 1 : 0;
	}
	return native_load_available.GetIndex() == 1;
}

//! The list aggregate of SQLite, a json array of objects that is NULL without rows like LIST
static string SQLiteJsonAggregation(const vector<pair<string, string>> &fields, const string &order) {
	if (!order.empty()) {
		// SQLite 3.38 orders no aggregate, an ordered aggregate renders through SQLiteOrderedListSubquery
		throw InternalException("SQLite catalog load cannot order a list aggregate");
	}
	auto fields_part = StringUtil::Join(fields, fields.size(), ", ", [](const pair<string, string> &entry) {
		// a nested aggregate nests as json, as a plain value it would be stored as its text
		auto value =
		    StringUtil::Contains(entry.second, "json_group_array(") ? "json(" + entry.second + ")" : entry.second;
		return "'" + entry.first + "', " + value;
	});
	return "NULLIF(json_group_array(json_object(" + fields_part + ")), '[]')";
}

//! The ordered aggregate as the aggregate of an ordered derived table, which SQLite reads in order
static string SQLiteOrderedListSubquery(const vector<pair<string, string>> &fields, const string &order,
                                        const string &from, const string &filter) {
	return StringUtil::Format(R"((
		SELECT %s
		FROM (
			SELECT *
			FROM %s
			WHERE %s
			ORDER BY %s
		)
		))",
	                          SQLiteJsonAggregation(fields, string()), from, filter, order);
}

//! The type sqlite_query reads a column of the load as, the types of the scanner for the parser
static string SQLiteResultType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		// the scanner reads the BOOLEAN columns of the metadata tables as BIGINT
		return "BIGINT";
	case LogicalTypeId::LIST:
		// the json text of a list aggregate, cast back outside sqlite_query
		return "VARCHAR";
	default:
		return type.ToString();
	}
}

string SQLiteMetadataManager::NativeStatement(const string &statement, const vector<CatalogLoadColumn> &columns) const {
	// SQLite runs the statement as a whole, so only its rows reach the scanner
	auto remote = statement;
	SubstituteCatalogPlaceholders(remote, SQLQuotedIdentifier::ToString(transaction.GetCatalog().MetadataSchemaName()));
	// the scanner path compares the companion keys in DuckDB, SQLite compares their bytes under its own name
	remote = StringUtil::Replace(remote, "COLLATE \"C\"", "COLLATE BINARY");
	vector<string> types;
	vector<string> names;
	for (auto &column : columns) {
		types.push_back(SQLString::ToString(SQLiteResultType(column.type)));
		names.push_back(SQLString::ToString(column.name));
	}
	return StringUtil::Format("SELECT %s FROM sqlite_query({METADATA_CATALOG_NAME_LITERAL}, %s, column_types := "
	                          "[%s], column_names := [%s])",
	                          ListColumnsFromJson(columns), SQLString(remote), StringUtil::Join(types, ", "),
	                          StringUtil::Join(names, ", "));
}

//! Whether the statement is one NativeStatement rendered, which SQLite runs as a whole
static bool IsNativeStatement(const string &query) {
	return StringUtil::StartsWith(query, "SELECT ") && StringUtil::Contains(query, " FROM sqlite_query(");
}

//! Separates the error of a failing native statement from the statement that follows it
static constexpr const char *STATEMENT_MARKER = "\nStatement: ";

unique_ptr<QueryResult> SQLiteMetadataManager::Query(string &query) {
	auto result = DuckLakeMetadataManager::Query(query);
	if (result->HasError() && IsNativeStatement(query)) {
		// SQLite names the failing column or table, the statement shows where it stands
		auto &error = result->GetErrorObject();
		result->SetError(ErrorData(error.Type(), error.RawMessage() + STATEMENT_MARKER + query));
	}
	return result;
}

string SQLiteMetadataManager::CommitErrorWithoutQuery(const string &message) const {
	// the end anchored rules of the profile match the error, which Query puts the statement after
	// the suffix starts at the last marker a native statement follows, a value or the statement may hold the text
	const string marker = STATEMENT_MARKER;
	auto statement = message.rfind(marker);
	while (statement != string::npos) {
		if (IsNativeStatement(message.substr(statement + marker.size()))) {
			return message.substr(0, statement);
		}
		if (statement == 0) {
			break;
		}
		statement = message.rfind(marker, statement - 1);
	}
	return message;
}

DuckLakeCatalogInfo SQLiteMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto &catalog = transaction.GetCatalog();
	if (!NativeLoadAvailable() || !JsonExtensionAvailable(catalog.GetDatabase())) {
		// LCOV_EXCL_START
		return DuckLakeMetadataManager::GetCatalogForSnapshot(snapshot);
		// LCOV_EXCL_STOP
	}
	CatalogLoadDialect dialect;
	dialect.render_list_aggregation = SQLiteJsonAggregation;
	dialect.render_ordered_list_subquery = SQLiteOrderedListSubquery;
	dialect.wrap_statement = [&](const string &statement, const vector<CatalogLoadColumn> &columns) {
		// the snapshot of the load is rendered into the statement before SQLite runs it
		auto remote = statement;
		SubstituteTransactionPlaceholders(snapshot, remote);
		return NativeStatement(remote, columns);
	};
	auto executor = [this](DuckLakeSnapshot s, string q) {
		return Query(s, q);
	};
	return BuildCatalogForSnapshot(snapshot, executor, catalog.DataPath(), catalog.Separator(),
	                               catalog.SupportsV1_1Metadata(), catalog.SupportsBranching(), catalog.AttachedChain(),
	                               dialect);
}

string SQLiteMetadataManager::GetLatestSnapshotQuery() const {
	if (!NativeLoadAvailable()) {
		// LCOV_EXCL_START
		return DuckLakeMetadataManager::GetLatestSnapshotQuery();
		// LCOV_EXCL_STOP
	}
	auto &catalog = transaction.GetCatalog();
	vector<CatalogLoadColumn> columns {{"snapshot_id", LogicalType::BIGINT},
	                                   {"schema_version", LogicalType::BIGINT},
	                                   {"next_catalog_id", LogicalType::BIGINT},
	                                   {"next_file_id", LogicalType::BIGINT},
	                                   {"version", LogicalType::VARCHAR}};
	if (catalog.SupportsBranching()) {
		columns.push_back({"branch_id", LogicalType::BIGINT});
		// the newest snapshot of every parent branch of the chain follows the head
		auto &segments = catalog.AttachedChain().Segments();
		for (idx_t position = 0; position + 1 < segments.size(); position++) {
			columns.push_back({StringUtil::Format("parent_head_%llu", position), LogicalType::BIGINT});
		}
	}
	return NativeStatement(DuckLakeMetadataManager::GetLatestSnapshotQuery(), columns);
}

string SQLiteMetadataManager::GetGlobalTableStatsQuery(const vector<TableIndex> &table_ids) const {
	if (!NativeLoadAvailable()) {
		// LCOV_EXCL_START
		return DuckLakeMetadataManager::GetGlobalTableStatsQuery(table_ids);
		// LCOV_EXCL_STOP
	}
	vector<CatalogLoadColumn> columns {
	    {"table_id", LogicalType::BIGINT},        {"column_id", LogicalType::BIGINT},
	    {"record_count", LogicalType::BIGINT},    {"next_row_id", LogicalType::BIGINT},
	    {"file_size_bytes", LogicalType::BIGINT}, {"contains_null", LogicalType::BOOLEAN},
	    {"contains_nan", LogicalType::BOOLEAN},   {"min_value", LogicalType::VARCHAR},
	    {"max_value", LogicalType::VARCHAR},      {"extra_stats", LogicalType::VARCHAR}};
	if (transaction.GetCatalog().SupportsV1_1Metadata()) {
		columns.push_back({"min_is_exact", LogicalType::BOOLEAN});
		columns.push_back({"max_is_exact", LogicalType::BOOLEAN});
	}
	return NativeStatement(DuckLakeMetadataManager::GetGlobalTableStatsQuery(table_ids), columns);
}

} // namespace duckdb
