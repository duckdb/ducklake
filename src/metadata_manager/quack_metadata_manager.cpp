#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_row_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_staged_commit.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

QuackMetadataManager::QuackMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

unique_ptr<QueryResult> QuackMetadataManager::Query(string &query) {
	auto &ducklake_catalog = transaction.GetCatalog();
	lock_guard<std::recursive_mutex> guard(ducklake_catalog.GetMetadataQueryLock());
	auto schema_identifier = SQLQuotedIdentifier::ToString(ducklake_catalog.MetadataSchemaName());
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	SubstituteCatalogPlaceholders(query);

	auto metadata_catalog_name_literal = SQLString::ToString(ducklake_catalog.MetadataDatabaseName());
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

unique_ptr<QueryResult> QuackMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	// the server commits each statement on its own, so the statements run in a server transaction
	SubstituteTransactionPlaceholders(snapshot, query);
	return ExecuteInTransaction(query);
}

unique_ptr<QueryResult> QuackMetadataManager::ExecuteInTransaction(string &query) {
	// hold the lock through the rollback
	lock_guard<std::recursive_mutex> guard(transaction.GetCatalog().GetMetadataQueryLock());
	return DuckLakeMetadataManager::ExecuteInTransaction(query);
}

string QuackMetadataManager::MetadataExistsQuery() const {
	return "SELECT COUNT(*) FROM information_schema.tables "
	       "WHERE table_name = 'ducklake_metadata' AND table_schema = {METADATA_SCHEMA_NAME_LITERAL}";
}

bool QuackMetadataManager::InlinedDeletionTableExists(const string &table_name) {
	auto query = StringUtil::Format("SELECT 1 FROM duckdb_tables() WHERE database_name = current_database() "
	                                "AND schema_name = {METADATA_SCHEMA_NAME_LITERAL} AND table_name = %s",
	                                SQLString::ToString(table_name));
	auto result = Query(query);
	result->ThrowIfError("Failed to probe for DuckLake inlined-deletion table: ");
	return result->Fetch() != nullptr;
}

void QuackMetadataManager::ClearCache() {
	lock_guard<std::recursive_mutex> guard(transaction.GetCatalog().GetMetadataQueryLock());
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
	if (result->RowCount() > 0) {
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

//! Whether the commit has to take the client-side path
static bool RequiresClientSideCommit(DuckLakeTransaction &transaction) {
	// the server-side commit cannot create inlined-data tables, delete flushed inlined data or rewrite delete files
	return transaction.GetRequiresNewInlinedTable() || !transaction.GetFlushedInlinedTables().empty() ||
	       !transaction.GetFlushedInlinedFileDeletions().empty() || transaction.GetLocalChanges().HasDatedNewDeletes();
}

bool QuackMetadataManager::CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const {
	if (RequiresClientSideCommit(transaction)) {
		return false;
	}
	return ExecuteRetrialsServerSide() && IsDataOnlyCommit(changes);
}

void QuackMetadataManager::FlushChangesServerSide(DuckLakeTransaction &flush_transaction,
                                                  DuckLakeSnapshot transaction_snapshot,
                                                  const TransactionChangeInformation &transaction_changes,
                                                  const DuckLakeRetryConfig &retry_config) {
	if (!IsDataOnlyCommit(transaction_changes) || RequiresClientSideCommit(flush_transaction)) {
		flush_transaction.RunCommitLoop(transaction_snapshot, transaction_changes, retry_config);
		return;
	}
	transaction.GetCatalog().EnsureCommitInfoProvided(flush_transaction.GetCommitInfo());
	DuckLakeStagedCommit staged;
	string batch = staged.Build(flush_transaction, transaction_snapshot, retry_config);
	auto result = Query(batch);
	if (!result) {
		throw IOException("Failed to invoke server-side ducklake_commit: empty result");
	}
	result->ThrowIfError("Failed to invoke server-side ducklake_commit: ");
	auto row = result->begin();
	if (row == result->end()) {
		throw IOException("Server-side ducklake_commit returned no rows");
	}
	flush_transaction.GetCatalog().SetCommittedSnapshotId(AsIdx(*row, 0));
	flush_transaction.ApplyServerSideCommit(AsIdx(*row, 1));
	if (OptBoolFalse(*row, 2)) {
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
	result->ThrowIfError("Failed to probe DuckLake metadata: ");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return false;
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>() > 0;
}

} // namespace duckdb
