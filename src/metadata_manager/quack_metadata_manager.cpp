#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

QuackMetadataManager::QuackMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

unique_ptr<QueryResult> QuackMetadataManager::Query(string &query) {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	SubstituteCatalogPlaceholders(query);

	auto metadata_catalog_name_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto wrapper = StringUtil::Format("CALL system.main.quack_query_by_name(%s, %s)", metadata_catalog_name_literal,
	                                  SQLString(query));
	auto result = transaction.ExecuteRaw(std::move(wrapper));
	if (result->HasError()) {
		// cleanup
		string reset = "ROLLBACK; BEGIN TRANSACTION;";
		transaction.ExecuteRaw(reset);
	}
	return result;
}

unique_ptr<QueryResult> QuackMetadataManager::AttachMetadata(const string &attach_query) {
	auto query = attach_query;
	SubstituteCatalogPlaceholders(query);
	Connection fresh_conn(transaction.GetCatalog().GetDatabase());
	auto result = fresh_conn.Query(query);

	for (idx_t attempt = 0; attempt < 5 && result->HasError(); attempt++) {
		auto raw_message = result->GetErrorObject().RawMessage();
		const bool retryable = StringUtil::Contains(raw_message, "Invalid connection id") ||
		                       StringUtil::Contains(raw_message, "Couldn't connect to server") ||
		                       StringUtil::Contains(raw_message, "Failed to send message");
		if (!retryable) {
			break;
		}
		result = fresh_conn.Query(query);
	}
	return result;
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

void QuackMetadataManager::ClearCache() {
	string clear = "CALL quack_clear_cache();";
	transaction.ExecuteRaw(clear);
}

bool QuackMetadataManager::MetadataExists() {
	auto query = MetadataExistsQuery();
	auto result = Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to probe DuckLake metadata: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return false;
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>() > 0;
}

} // namespace duckdb
