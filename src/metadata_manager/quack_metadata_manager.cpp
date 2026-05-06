#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
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

	auto metadata_path_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto wrapper =
	    StringUtil::Format("CALL system.main.quack_query(%s, %s)", metadata_path_literal, SQLString(query));
	auto result = transaction.ExecuteRaw(std::move(wrapper));
	if (result->HasError()) {
		auto &error = result->GetErrorObject();
		auto raw_message = error.RawMessage();
		bool is_catalog_error = StringUtil::Contains(raw_message, "Catalog Error:");
		if (is_catalog_error) {
			string reset = "ROLLBACK; BEGIN TRANSACTION;";
			transaction.ExecuteRaw(reset);
			return make_uniq<MaterializedQueryResult>(ErrorData(ExceptionType::CATALOG, raw_message));
		}
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

} // namespace duckdb
