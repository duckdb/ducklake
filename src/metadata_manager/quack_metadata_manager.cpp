#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_staged_commit.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

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
	return std::move(result);
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

void QuackMetadataManager::ProbeServerCapabilities() {
	// Check whether the quack server has the ducklake_commit function loaded (i.e. the ducklake
	// extension is available server-side).
	string probe = "SELECT 1 FROM duckdb_functions() WHERE function_name = 'ducklake_commit' LIMIT 1";
	auto result = Query(probe);
	if (!result || result->HasError()) {
		return;
	}
	auto chunk = result->Fetch();
	if (chunk && chunk->size() > 0) {
		transaction.GetCatalog().SetRetrialsServerSide(true);
	}
}

static bool IsDataOnlyCommit(const TransactionChangeInformation &c) {
	return c.created_schemas.empty() && c.dropped_schemas.empty() && c.created_tables.empty() &&
	       c.created_scalar_macros.empty() && c.created_table_macros.empty() && c.altered_tables.empty() &&
	       c.altered_tables_with_schema_version_changes.empty() && c.altered_views.empty() &&
	       c.dropped_tables.empty() && c.dropped_views.empty() && c.dropped_scalar_macros.empty() &&
	       c.dropped_table_macros.empty();
}

bool QuackMetadataManager::CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const {
	if (transaction.GetRequiresNewInlinedTable()) {
		// the server-side commit cannot create the inlined-data table, take the client-side path instead
		return false;
	}
	return ExecuteRetrialsServerSide() && IsDataOnlyCommit(changes);
}

void QuackMetadataManager::FlushChangesServerSide(DuckLakeTransaction &flush_transaction,
                                                  DuckLakeSnapshot transaction_snapshot,
                                                  const TransactionChangeInformation &transaction_changes,
                                                  const DuckLakeRetryConfig &retry_config) {
	if (!IsDataOnlyCommit(transaction_changes) || flush_transaction.GetRequiresNewInlinedTable()) {
		flush_transaction.RunCommitLoop(transaction_snapshot, transaction_changes, retry_config);
		return;
	}
	transaction.GetCatalog().EnsureCommitInfoProvided(flush_transaction.GetCommitInfo());
	DuckLakeStagedCommit staged;
	string batch = staged.Build(flush_transaction, transaction_snapshot, retry_config);
	auto result = Query(batch);
	if (!result || result->HasError()) {
		if (result) {
			result->GetErrorObject().Throw("Failed to invoke server-side ducklake_commit: ");
		}
		throw IOException("Failed to invoke server-side ducklake_commit: empty result");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("Server-side ducklake_commit returned no rows");
	}
	auto committed_snapshot_id = chunk->GetValue(0, 0).GetValue<int64_t>();
	auto committed_schema_version = chunk->GetValue(1, 0).GetValue<int64_t>();
	auto had_flushes = !chunk->GetValue(2, 0).IsNull() && chunk->GetValue(2, 0).GetValue<bool>();
	flush_transaction.GetCatalog().SetCommittedSnapshotId(static_cast<idx_t>(committed_snapshot_id));
	flush_transaction.ApplyServerSideCommit(static_cast<idx_t>(committed_schema_version));
	if (had_flushes) {
		// With quack we need to clear up superseded inlines tables on the client side to avoid dangling caching
		// references
		flush_transaction.DropEmptySupersededInlinedTablesClientSide();
	}
	// We got clear the cache, if this creates inlined tables (e.g., `ducklake_inlined_data_<id>_<v>` or
	// `ducklake_inlined_delete_<id>`)
	ClearCache();
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
