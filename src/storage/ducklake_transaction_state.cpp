#include "storage/ducklake_transaction_state.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_commit_state.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/random_engine.hpp"

#include <chrono>
#include <cmath>
#include <sstream>
#include <thread>

namespace duckdb {

DuckLakeTransactionState::DuckLakeTransactionState(DuckLakeTransaction &transaction) : transaction(transaction) {
}

DuckLakeTransactionState::~DuckLakeTransactionState() {
}

void DuckLakeTransactionState::Commit(DuckLakeSnapshot transaction_snapshot,
                                      const TransactionChangeInformation &transaction_changes,
                                      const DuckLakeRetryConfig &retry_config) {
	SnapshotAndStats commit_stats_snapshot;
	auto &commit_snapshot = commit_stats_snapshot.snapshot;
	optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats;
	for (idx_t i = 0; i < retry_config.max_retry_count + 1; i++) {
		bool can_retry;
		auto attempt_changes = transaction_changes;
		try {
			can_retry = false;
			if (i > 0) {
				// we failed our first commit due to another transaction committing
				// retry - but first check for conflicts
				commit_stats_snapshot = transaction.CheckForConflicts(transaction_snapshot, attempt_changes);
				stats = &commit_stats_snapshot.stats;
			} else {
				commit_stats_snapshot.snapshot = transaction.GetSnapshot();
			}
			commit_snapshot.snapshot_id++;
			if (transaction.SchemaChangesMade()) {
				// we changed the schema - need to get a new schema version
				commit_snapshot.schema_version++;
			}
			can_retry = true;
			DuckLakeCommitState commit_state(commit_snapshot);
			// write the new snapshot
			string batch_queries = transaction.metadata_manager->InsertSnapshot();
			batch_queries += transaction.CommitChanges(commit_state, attempt_changes, stats);

			batch_queries += transaction.WriteSnapshotChanges(commit_state, attempt_changes);
			auto res = transaction.metadata_manager->Execute(commit_snapshot, batch_queries);
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to flush changes into DuckLake: ");
			}
			bool flushed_inlined = !flushed_inlined_tables.empty();
			if (transaction.metadata_manager->TakePendingCacheClear()) {
				transaction.metadata_manager->ClearCache();
			}
			transaction.connection->Commit();
			if (flushed_inlined) {
				transaction.metadata_manager->DropEmptySupersededInlinedTables();
			}
			transaction.catalog_version = commit_snapshot.schema_version;

			// finished writing
			break;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			// rollback if there is an active transaction
			auto has_active_transaction = transaction.connection->context->transaction.HasActiveTransaction();
			if (has_active_transaction) {
				transaction.connection->Rollback();
			}
			bool retry_on_error = DuckLakeTransaction::RetryOnError(error.Message());
			bool finished_retrying = i + 1 >= retry_config.max_retry_count;
			if (!can_retry || !retry_on_error || finished_retrying) {
				// we abort after the max retry count
				transaction.CleanupFiles();
				// Add additional information on the number of retries and suggest to increase it
				std::ostringstream error_message;
				error_message << "Failed to commit DuckLake transaction." << '\n';
				if (finished_retrying) {
					error_message << "Exceeded the maximum retry count of " << retry_config.max_retry_count
					              << " set by the ducklake_max_retry_count setting." << '\n'
					              << ". Consider increasing the value with: e.g., \"SET ducklake_max_retry_count = "
					              << retry_config.max_retry_count * 10 << ";\"" << '\n';
				}
				error.Throw(error_message.str());
			}

#ifndef DUCKDB_NO_THREADS
			RandomEngine random;
			// random multiplier between 0.5 - 1.0
			double random_multiplier = (random.NextRandom() + 1.0) / 2.0;
			uint64_t sleep_amount = (uint64_t)((double)retry_config.retry_wait_ms * random_multiplier *
			                                   pow(retry_config.retry_backoff, static_cast<double>(i)));
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
#endif

			// retry the transaction (with a new snapshot id)
			// clear the inlined table caches - the rollback undid any table creation from the previous attempt
			transaction.metadata_manager->ClearInlinedTableCaches();
			transaction.connection->BeginTransaction();
			transaction.snapshot.reset();
		}
	}
	// If we got here, this snapshot was successful
	transaction.ducklake_catalog.SetCommittedSnapshotId(commit_snapshot.snapshot_id);
}

} // namespace duckdb
