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

bool SQLiteMetadataManager::NativeLoadAvailable() {
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

DuckLakeCatalogInfo SQLiteMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto &catalog = transaction.GetCatalog();
	if (!NativeLoadAvailable() || !JsonExtensionAvailable(catalog.GetDatabase())) {
		// LCOV_EXCL_START
		return DuckLakeMetadataManager::GetCatalogForSnapshot(snapshot);
		// LCOV_EXCL_STOP
	}
	auto schema = SQLQuotedIdentifier::ToString(catalog.MetadataSchemaName());
	CatalogLoadDialect dialect;
	dialect.render_list_aggregation = SQLiteJsonAggregation;
	dialect.render_ordered_list_subquery = SQLiteOrderedListSubquery;
	dialect.wrap_statement = [&](const string &statement, const vector<CatalogLoadColumn> &columns) {
		// SQLite runs the statement as a whole, so only the live rows reach the scanner
		auto remote = statement;
		SubstituteTransactionPlaceholders(snapshot, remote);
		SubstituteCatalogPlaceholders(remote, schema);
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
	};
	auto executor = [this](DuckLakeSnapshot s, string q) {
		auto result = Query(s, q);
		if (result->HasError()) {
			// SQLite names the failing column or table, the statement shows where it stands
			auto &error = result->GetErrorObject();
			result->SetError(ErrorData(error.Type(), StringUtil::Format("%s\nStatement: %s", error.RawMessage(), q)));
		}
		return result;
	};
	return BuildCatalogForSnapshot(snapshot, executor, catalog.DataPath(), catalog.Separator(),
	                               catalog.SupportsV1_1Metadata(), catalog.SupportsBranching(), catalog.AttachedChain(),
	                               dialect);
}

} // namespace duckdb
