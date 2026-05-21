#include "storage/ducklake_transaction_state.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_commit_state.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <cmath>
#include <sstream>
#include <thread>

namespace duckdb {

DuckLakeTransactionState::DuckLakeTransactionState(DuckLakeTransaction &transaction) : transaction(transaction) {
}

DuckLakeTransactionState::~DuckLakeTransactionState() {
}

namespace {

template <class T, class MAP>
void ConflictCheck(T index, const MAP &conflict_map, const char *action, const char *conflict_action) {
	if (conflict_map.find(index) != conflict_map.end()) {
		throw TransactionException("Transaction conflict - attempting to %s with index \"%d\""
		                           " - but another transaction has %s",
		                           action, index.index, conflict_action);
	}
}

template <class MAP>
void ConflictCheck(const string &source_name, const MAP &conflict_map, const char *action,
                   const char *conflict_action) {
	if (conflict_map.find(source_name) != conflict_map.end()) {
		throw TransactionException("Transaction conflict - attempting to %s with name \"%s\""
		                           " - but another transaction has %s",
		                           action, source_name, conflict_action);
	}
}

string GetCatalogType(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return "table";
	case CatalogType::VIEW_ENTRY:
		return "view";
	case CatalogType::MACRO_ENTRY:
		return "scalar";
	case CatalogType::TABLE_MACRO_ENTRY:
		return "table";
	default:
		throw InternalException("Can't handle catalog type in GetCatalogType()");
	}
}

void ConflictCheck(const case_insensitive_map_t<reference_set_t<CatalogEntry>> &created_changes,
                   const set<SchemaIndex> &dropped_schemas,
                   const case_insensitive_map_t<case_insensitive_map_t<string>> other_created_changes) {
	for (auto &entry : created_changes) {
		auto &schema_name = entry.first;
		auto &created_entry = entry.second;
		for (auto &catalog_ref : created_entry) {
			auto &catalog_entry = catalog_ref.get();
			auto &schema = catalog_entry.ParentSchema().Cast<DuckLakeSchemaEntry>();
			auto entry_type = GetCatalogType(catalog_entry.type);
			string action =
			    StringUtil::Format("create %s \"%s\" in schema \"%s\"", entry_type, catalog_entry.name, schema_name);
			ConflictCheck(schema.GetSchemaId(), dropped_schemas, action.c_str(), "dropped this schema");

			auto tbl_entry = other_created_changes.find(schema_name);
			if (tbl_entry != other_created_changes.end()) {
				auto &other_created_tables = tbl_entry->second;
				auto sub_entry = other_created_tables.find(catalog_entry.name);
				if (sub_entry != other_created_tables.end()) {
					// a table with this name in this schema was already created
					throw TransactionException("Transaction conflict - attempting to create %s \"%s\" in schema \"%s\" "
					                           "- but this %s has been created by another transaction already",
					                           entry_type, catalog_entry.name, schema_name, sub_entry->second);
				}
			}
		}
	}
}

} // namespace

void DuckLakeTransactionState::CheckForConflicts(const TransactionChangeInformation &changes,
                                                 const SnapshotChangeInformation &other_changes,
                                                 DuckLakeSnapshot transaction_snapshot) const {
	// check if we are dropping the same table as another transaction
	for (auto &dropped_idx : changes.dropped_tables) {
		ConflictCheck(dropped_idx, other_changes.dropped_tables, "drop table", "dropped it already");
	}
	// check if we are dropping the same view as another transaction
	for (auto &dropped_idx : changes.dropped_views) {
		ConflictCheck(dropped_idx, other_changes.dropped_views, "drop view", "dropped it already");
	}
	// check if we are dropping the same macro as another transaction
	for (auto &dropped_idx : changes.dropped_scalar_macros) {
		ConflictCheck(dropped_idx, other_changes.dropped_scalar_macros, "drop macro", "dropped it already");
	}
	for (auto &dropped_idx : changes.dropped_table_macros) {
		ConflictCheck(dropped_idx, other_changes.dropped_table_macros, "drop macro", "dropped it already");
	}
	// check if we are dropping the same schema as another transaction
	for (auto &entry : changes.dropped_schemas) {
		auto &dropped_schema = entry.second.get();
		auto dropped_idx = entry.first;
		ConflictCheck(dropped_idx, other_changes.dropped_schemas, "drop schema", "dropped it already");

		ConflictCheck(dropped_schema.name, other_changes.created_tables, "drop schema",
		              "created an entry in this schema");
	}
	// check if we are creating the same schema as another transaction
	for (auto &created_schema : changes.created_schemas) {
		ConflictCheck(created_schema, other_changes.created_schemas, "create schema",
		              "created a schema with this name already");
	}
	// check if we are creating the same macro as another transaction
	ConflictCheck(changes.created_table_macros, other_changes.dropped_schemas, other_changes.created_table_macros);
	ConflictCheck(changes.created_scalar_macros, other_changes.dropped_schemas, other_changes.created_scalar_macros);
	ConflictCheck(changes.created_tables, other_changes.dropped_schemas, other_changes.created_tables);
	// check if we are creating the same table as another transaction
	for (auto &entry : changes.created_tables) {
		auto &schema_name = entry.first;
		auto &created_tables = entry.second;
		for (auto &table_ref : created_tables) {
			auto &table = table_ref.get();
			auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
			auto entry_type = table.type == CatalogType::TABLE_ENTRY ? "table" : "view";

			string action =
			    StringUtil::Format("create %s \"%s\" in schema \"%s\"", entry_type, table.name, schema_name);
			ConflictCheck(schema.GetSchemaId(), other_changes.dropped_schemas, action.c_str(), "dropped this schema");

			auto tbl_entry = other_changes.created_tables.find(schema_name);
			if (tbl_entry != other_changes.created_tables.end()) {
				auto &other_created_tables = tbl_entry->second;
				auto sub_entry = other_created_tables.find(table.name);
				if (sub_entry != other_created_tables.end()) {
					// a table with this name in this schema was already created
					throw TransactionException("Transaction conflict - attempting to create %s \"%s\" in schema \"%s\" "
					                           "- but this %s has been created by another transaction already",
					                           entry_type, table.name, schema_name, sub_entry->second);
				}
			}
		}
	}
	for (auto &table_id : changes.tables_inserted_into) {
		ConflictCheck(table_id, other_changes.dropped_tables, "insert into table", "dropped it");
		ConflictCheck(table_id, other_changes.altered_tables, "insert into table", "altered it");
		ConflictCheck(table_id, other_changes.tables_deleted_from, "insert into table", "deleted from it");
		ConflictCheck(table_id, other_changes.tables_deleted_inlined, "insert into table",
		              "deleted inlined data from it");
	}
	for (auto &table_id : changes.tables_inserted_inlined) {
		ConflictCheck(table_id, other_changes.dropped_tables, "insert into table", "dropped it");
		ConflictCheck(table_id, other_changes.altered_tables, "insert into table", "altered it");
		ConflictCheck(table_id, other_changes.tables_deleted_from, "insert into table", "deleted from it");
		ConflictCheck(table_id, other_changes.tables_deleted_inlined, "insert into table",
		              "deleted inlined data from it");
	}
	for (auto &table_id : changes.tables_deleted_from) {
		ConflictCheck(table_id, other_changes.dropped_tables, "delete from table", "dropped it");
		ConflictCheck(table_id, other_changes.altered_tables, "delete from table", "altered it");
		ConflictCheck(table_id, other_changes.tables_merge_adjacent, "delete from table", "compacted it");
		ConflictCheck(table_id, other_changes.tables_rewrite_delete, "delete from table", "compacted it");
		ConflictCheck(table_id, other_changes.inserted_tables, "delete from table", "inserted into it");
		ConflictCheck(table_id, other_changes.tables_inserted_inlined, "delete from table", "inserted into it");
	}
	if (!changes.tables_deleted_from.empty()) {
		bool check_for_matches = false;
		for (auto &table_id : changes.tables_deleted_from) {
			if (other_changes.tables_deleted_from.find(table_id) != other_changes.tables_deleted_from.end()) {
				check_for_matches = true;
				break;
			}
		}
		if (check_for_matches) {
			// If we have deletes on the tables, check for files being deleted
			const auto deleted_files =
			    transaction.metadata_manager->GetFilesDeletedOrDroppedAfterSnapshot(transaction_snapshot);
			for (auto &entry : local_changes.Changes()) {
				auto &table_changes = entry.GetTableChanges();
				for (auto &file_entry : table_changes.new_delete_files) {
					for (auto &file : file_entry.second) {
						ConflictCheck(file.data_file_id, deleted_files.deleted_from_files, "delete from file",
						              "deleted from it");
					}
				}
			}
			for (auto &file : dropped_files) {
				ConflictCheck(file.second, deleted_files.deleted_from_files, "delete from file", "deleted from it");
			}
		}
	}
	for (auto &table_id : changes.tables_deleted_inlined) {
		ConflictCheck(table_id, other_changes.dropped_tables, "delete from table", "dropped it");
		ConflictCheck(table_id, other_changes.altered_tables, "delete from table", "altered it");
		ConflictCheck(table_id, other_changes.tables_deleted_inlined, "delete from table", "deleted from it");
		ConflictCheck(table_id, other_changes.tables_flushed_inlined, "delete from table", "flushed the inlined data");
		ConflictCheck(table_id, other_changes.inserted_tables, "delete from table", "inserted into it");
		ConflictCheck(table_id, other_changes.tables_inserted_inlined, "delete from table", "inserted into it");
	}
	for (auto &table_id : changes.tables_flushed_inlined) {
		ConflictCheck(table_id, other_changes.dropped_tables, "flush inline data", "dropped it");
		ConflictCheck(table_id, other_changes.tables_deleted_inlined, "flush inline data", "deleted from it");
		ConflictCheck(table_id, other_changes.tables_flushed_inlined, "flush inline data", "flushed it");
	}
	for (auto &table_id : changes.tables_merge_adjacent) {
		ConflictCheck(table_id, other_changes.dropped_tables, "compact table", "dropped it");
		ConflictCheck(table_id, other_changes.tables_deleted_from, "compact table", "deleted from it");
		ConflictCheck(table_id, other_changes.tables_merge_adjacent, "compact table", "compacted it");
		ConflictCheck(table_id, other_changes.tables_rewrite_delete, "compact table", "compacted it");
	}
	for (auto &table_id : changes.tables_rewrite_delete) {
		ConflictCheck(table_id, other_changes.dropped_tables, "compact table", "dropped it");
		ConflictCheck(table_id, other_changes.tables_deleted_from, "compact table", "deleted from it");
		ConflictCheck(table_id, other_changes.tables_merge_adjacent, "compact table", "compacted it");
		ConflictCheck(table_id, other_changes.tables_rewrite_delete, "compact table", "compacted it");
	}
	for (auto &table_id : changes.altered_tables) {
		ConflictCheck(table_id, other_changes.dropped_tables, "alter table", "dropped it");
		ConflictCheck(table_id, other_changes.altered_tables, "alter table", "altered it");
	}
	for (auto &view_id : changes.altered_views) {
		ConflictCheck(view_id, other_changes.altered_views, "alter view", "altered it");
	}
}

SnapshotAndStats
DuckLakeTransactionState::CheckForConflicts(DuckLakeSnapshot transaction_snapshot,
                                            const TransactionChangeInformation &changes,
                                            const std::function<unique_ptr<QueryResult>(string)> &executor) {
	SnapshotAndStats snapshot_and_stats;
	// get all changes made to the system after the current snapshot was started
	auto changes_made = DuckLakeMetadataManager::GetSnapshotAndStatsAndChanges(snapshot_and_stats, executor);
	// parse changes made by other transactions
	auto other_changes = SnapshotChangeInformation::ParseChangesMade(changes_made.changes_made);

	// now check for conflicts
	CheckForConflicts(changes, other_changes, transaction_snapshot);

	return snapshot_and_stats;
}

void DuckLakeTransactionState::Commit(DuckLakeSnapshot transaction_snapshot,
                                      const TransactionChangeInformation &transaction_changes,
                                      const DuckLakeRetryConfig &retry_config,
                                      const std::function<unique_ptr<QueryResult>(string)> &conflict_query_executor) {
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
				commit_stats_snapshot =
				    CheckForConflicts(transaction_snapshot, attempt_changes, conflict_query_executor);
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
