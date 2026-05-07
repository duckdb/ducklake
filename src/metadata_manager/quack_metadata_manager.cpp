#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

QuackMetadataManager::QuackMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

unique_ptr<QueryResult> QuackMetadataManager::Query(string &query) {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	SubstituteCatalogPlaceholders(query);

	auto metadata_path = ducklake_catalog.MetadataPath();
	if (StringUtil::StartsWith(metadata_path, "quack:quack:")) {
		metadata_path = metadata_path.substr(strlen("quack:"));
	}
	auto metadata_path_literal = DuckLakeUtil::SQLLiteralToString(metadata_path);
	auto wrapper =
	    StringUtil::Format("CALL system.main.quack_query(%s, %s)", metadata_path_literal, SQLString(query));
	auto result = transaction.ExecuteRaw(std::move(wrapper));
	if (result->HasError()) {
		//cleanup
		string reset = "ROLLBACK; BEGIN TRANSACTION;";
		transaction.ExecuteRaw(reset);
	}
	return result;
}

unique_ptr<QueryResult> QuackMetadataManager::AttachMetadata(const string &attach_query) {
	auto query = attach_query;
	SubstituteCatalogPlaceholders(query);
	Connection fresh_conn(transaction.GetCatalog().GetDatabase());
	return fresh_conn.Query(query);
}

unique_ptr<QueryResult> QuackMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	SubstituteSnapshotPlaceholders(snapshot, query);
	return Query(query);
}

unique_ptr<QueryResult> QuackMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return Query(snapshot, query);
}

string QuackMetadataManager::MetadataExistsQuery() const {
	return "SELECT COUNT(*) FROM information_schema.tables "
	       "WHERE table_name = 'ducklake_metadata' AND table_schema = {METADATA_SCHEMA_NAME_LITERAL}";
}

} // namespace duckdb
