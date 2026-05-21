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

DuckLakeTransactionState::DuckLakeTransactionState(DuckLakeTransaction &transaction, DatabaseInstance &db)
    : transaction(transaction), db(db) {
}

DuckLakeTransactionState::~DuckLakeTransactionState() {
}

void DuckLakeTransactionState::CleanupFiles() {
	// remove any files that were written
	local_changes.CleanupFiles(db);
}

bool DuckLakeTransactionState::SchemaChangesMade() const {
	return !new_tables.empty() || !dropped_tables.empty() || new_schemas || !dropped_schemas.empty() ||
	       !dropped_views.empty() || !renamed_views.empty() || !new_scalar_macros.empty() ||
	       !new_table_macros.empty() || !dropped_scalar_macros.empty() || !dropped_table_macros.empty();
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

void DuckLakeTransactionState::CheckForConflicts(
    const TransactionChangeInformation &changes, const SnapshotChangeInformation &other_changes,
    DuckLakeSnapshot transaction_snapshot,
    const std::function<unique_ptr<QueryResult>(string)> &executor) const {
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
			const auto deleted_files = GetFilesDeletedOrDroppedAfterSnapshot(executor);
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

namespace {

template <class T>
void AddChangeInfo(DuckLakeCommitState &commit_state, SnapshotChangeInfo &change_info, const set<T> &changes,
                   const char *change_type) {
	for (auto &entry : changes) {
		if (!change_info.changes_made.empty()) {
			change_info.changes_made += ",";
		}
		auto id = commit_state.GetTableId(entry);
		change_info.changes_made += change_type;
		change_info.changes_made += ":";
		change_info.changes_made += to_string(id.index);
	}
}

} // namespace

string DuckLakeTransactionState::WriteSnapshotChanges(DuckLakeCommitState &commit_state,
                                                      TransactionChangeInformation &changes,
                                                      const DuckLakeSnapshotCommit &commit_info) const {
	SnapshotChangeInfo change_info;

	// re-add all inserted tables - transaction-local table identifiers should have been converted at this stage
	changes.tables_deleted_from = tables_deleted_from;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = commit_state.GetTableId(entry.GetTableIndex());
		auto &table_changes = entry.GetTableChanges();
		DuckLakeTransaction::AddTableChanges(table_id, table_changes, changes);
	}
	for (auto &entry : changes.dropped_schemas) {
		if (!change_info.changes_made.empty()) {
			change_info.changes_made += ",";
		}
		auto schema_id = entry.first.index;
		change_info.changes_made += "dropped_schema:";
		change_info.changes_made += to_string(schema_id);
	}
	AddChangeInfo(commit_state, change_info, changes.dropped_tables, "dropped_table");
	AddChangeInfo(commit_state, change_info, changes.dropped_views, "dropped_view");
	for (auto &created_schema : changes.created_schemas) {
		if (!change_info.changes_made.empty()) {
			change_info.changes_made += ",";
		}
		change_info.changes_made += "created_schema:";
		change_info.changes_made += KeywordHelper::WriteQuoted(created_schema, '"');
	}
	for (auto &entry : changes.created_tables) {
		auto &schema = entry.first;
		auto schema_prefix = KeywordHelper::WriteQuoted(schema, '"') + ".";
		for (auto &created_table : entry.second) {
			if (!change_info.changes_made.empty()) {
				change_info.changes_made += ",";
			}
			auto is_view = created_table.get().type == CatalogType::VIEW_ENTRY;
			change_info.changes_made += is_view ? "created_view:" : "created_table:";
			change_info.changes_made += schema_prefix + KeywordHelper::WriteQuoted(created_table.get().name, '"');
		}
	}

	for (auto &entry : changes.created_scalar_macros) {
		auto &schema = entry.first;
		auto schema_prefix = KeywordHelper::WriteQuoted(schema, '"') + ".";
		for (auto &created_macro : entry.second) {
			if (!change_info.changes_made.empty()) {
				change_info.changes_made += ",";
			}
			change_info.changes_made += "created_scalar_macro:";
			change_info.changes_made += schema_prefix + KeywordHelper::WriteQuoted(created_macro.get().name, '"');
		}
	}
	for (auto &entry : changes.created_table_macros) {
		auto &schema = entry.first;
		auto schema_prefix = KeywordHelper::WriteQuoted(schema, '"') + ".";
		for (auto &created_macro : entry.second) {
			if (!change_info.changes_made.empty()) {
				change_info.changes_made += ",";
			}
			change_info.changes_made += "created_table_macro:";
			change_info.changes_made += schema_prefix + KeywordHelper::WriteQuoted(created_macro.get().name, '"');
		}
	}

	for (auto &entry : changes.dropped_scalar_macros) {
		if (!change_info.changes_made.empty()) {
			change_info.changes_made += ",";
		}
		change_info.changes_made += "dropped_scalar_macro:";
		change_info.changes_made += to_string(entry.index);
	}

	for (auto &entry : changes.dropped_table_macros) {
		if (!change_info.changes_made.empty()) {
			change_info.changes_made += ",";
		}
		change_info.changes_made += "dropped_table_macro:";
		change_info.changes_made += to_string(entry.index);
	}

	AddChangeInfo(commit_state, change_info, changes.tables_inserted_into, "inserted_into_table");
	AddChangeInfo(commit_state, change_info, changes.tables_deleted_from, "deleted_from_table");
	AddChangeInfo(commit_state, change_info, changes.altered_tables, "altered_table");
	AddChangeInfo(commit_state, change_info, changes.altered_views, "altered_view");
	AddChangeInfo(commit_state, change_info, changes.tables_inserted_inlined, "inlined_insert");
	AddChangeInfo(commit_state, change_info, changes.tables_deleted_inlined, "inlined_delete");
	AddChangeInfo(commit_state, change_info, changes.tables_flushed_inlined, "inline_flush");
	bool has_compaction = !changes.tables_merge_adjacent.empty() || !changes.tables_rewrite_delete.empty();
	if (has_compaction && !change_info.changes_made.empty()) {
		throw InvalidInputException("Transactions can either make changes OR perform compaction - not both");
	}
	AddChangeInfo(commit_state, change_info, changes.tables_merge_adjacent, "merge_adjacent");
	AddChangeInfo(commit_state, change_info, changes.tables_rewrite_delete, "rewrite_delete");
	return DuckLakeMetadataManager::WriteSnapshotChangesSql(change_info, commit_info);
}

string DuckLakeTransactionState::CommitChanges(DuckLakeCommitState &commit_state,
                                               TransactionChangeInformation &transaction_changes,
                                               optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats) {
	auto &commit_snapshot = commit_state.commit_snapshot;
	auto &metadata_manager = transaction.metadata_manager;

	transaction.ducklake_catalog.EnsureCommitInfoProvided(transaction.commit_info);

	string batch_queries;
	// drop entries
	if (!dropped_tables.empty()) {
		batch_queries += DuckLakeMetadataManager::DropTables(dropped_tables, false);
	}

	if (!renamed_tables.empty()) {
		batch_queries += DuckLakeMetadataManager::DropTables(renamed_tables, true);
	}

	if (!dropped_views.empty()) {
		batch_queries += DuckLakeMetadataManager::DropViews(dropped_views, false);
	}

	if (!renamed_views.empty()) {
		batch_queries += DuckLakeMetadataManager::DropViews(renamed_views, true);
	}

	if (!dropped_scalar_macros.empty()) {
		batch_queries += DuckLakeMetadataManager::DropMacros(dropped_scalar_macros);
	}

	if (!dropped_table_macros.empty()) {
		batch_queries += DuckLakeMetadataManager::DropMacros(dropped_table_macros);
	}
	if (!dropped_schemas.empty()) {
		set<SchemaIndex> dropped_schema_ids;
		for (auto &entry : dropped_schemas) {
			dropped_schema_ids.insert(entry.first);
		}
		batch_queries += DuckLakeMetadataManager::DropSchemas(dropped_schema_ids);
	}
	// write new schemas
	vector<DuckLakeSchemaInfo> new_schemas_result;
	if (new_schemas) {
		new_schemas_result = transaction.GetNewSchemas(commit_state);
		vector<DuckLakePath> resolved_schema_paths;
		resolved_schema_paths.reserve(new_schemas_result.size());
		for (auto &schema : new_schemas_result) {
			resolved_schema_paths.push_back(metadata_manager->GetRelativePath(schema.path));
		}
		batch_queries += DuckLakeMetadataManager::WriteNewSchemas(new_schemas_result, resolved_schema_paths);
	}

	// write new tables
	vector<DuckLakeTableInfo> new_tables_result;
	vector<DuckLakeTableInfo> new_inlined_data_tables_result;
	if (!new_tables.empty()) {
		auto result = transaction.GetNewTables(commit_state, transaction_changes);
		vector<DuckLakePath> resolved_table_paths;
		resolved_table_paths.reserve(result.new_tables.size());
		for (auto &table : result.new_tables) {
			resolved_table_paths.push_back(
			    metadata_manager->GetRelativePath(table.schema_id, table.path, new_schemas_result));
		}
		batch_queries += DuckLakeMetadataManager::WriteNewTables(result.new_tables, resolved_table_paths);
		auto existing_catalog = metadata_manager->GetCatalogForSnapshot(commit_snapshot);
		batch_queries +=
		    DuckLakeMetadataManager::WriteNewPartitionKeys(existing_catalog.partitions, result.new_partition_keys);
		batch_queries += DuckLakeMetadataManager::WriteNewViews(result.new_views);
		batch_queries += DuckLakeMetadataManager::WriteNewTags(result.new_tags);
		batch_queries += DuckLakeMetadataManager::WriteNewColumnTags(result.new_column_tags);
		batch_queries += DuckLakeMetadataManager::WriteDroppedColumns(result.dropped_columns);
		batch_queries += DuckLakeMetadataManager::WriteNewColumns(result.new_columns);
		batch_queries += metadata_manager->WriteNewInlinedTables(commit_snapshot, result.new_inlined_data_tables);
		batch_queries += DuckLakeMetadataManager::WriteNewSortKeys(existing_catalog.sorts, result.new_sort_keys);
		new_tables_result = result.new_tables;
		new_inlined_data_tables_result = result.new_inlined_data_tables;
	}

	if (!new_scalar_macros.empty() || !new_table_macros.empty()) {
		auto result = transaction.GetNewMacros(commit_state, transaction_changes);
		batch_queries += DuckLakeMetadataManager::WriteNewMacros(result.new_macros);
	}

	// write new name maps
	if (!transaction.new_name_maps.name_maps.empty()) {
		auto result = transaction.GetNewNameMaps(commit_state);
		batch_queries += DuckLakeMetadataManager::WriteNewColumnMappings(result.new_column_mappings);
	}

	// write new data / data files
	bool has_table_data_changes = local_changes.HasChanges();
	if (has_table_data_changes) {
		auto result = transaction.GetNewDataFiles(batch_queries, commit_state, stats);
		batch_queries += metadata_manager->WriteNewDataFiles(commit_snapshot, result.new_files, new_tables_result,
		                                                     new_schemas_result);
		batch_queries += metadata_manager->WriteNewInlinedData(commit_snapshot, result.new_inlined_data,
		                                                       new_tables_result, new_inlined_data_tables_result);
	}

	// in case of a retry, we generate the deletion of inlined data from the tables
	if (!flushed_inlined_tables.empty()) {
		batch_queries += DuckLakeMetadataManager::GenerateDeleteFlushedInlinedData(flushed_inlined_tables);
	}

	// drop data files
	if (!dropped_files.empty()) {
		set<DataFileIndex> dropped_indexes;
		for (auto &entry : dropped_files) {
			dropped_indexes.insert(entry.second);
		}
		batch_queries += DuckLakeMetadataManager::DropDataFiles(dropped_indexes);
	}

	if (has_table_data_changes) {
		// write new delete files
		vector<DuckLakeOverwrittenDeleteFile> overwritten_delete_files;
		auto file_list = transaction.GetNewDeleteFiles(commit_state, overwritten_delete_files);
		vector<DuckLakePath> resolved_overwritten_paths;
		resolved_overwritten_paths.reserve(overwritten_delete_files.size());
		for (auto &file : overwritten_delete_files) {
			resolved_overwritten_paths.push_back(metadata_manager->GetRelativePath(file.path));
		}
		batch_queries +=
		    DuckLakeMetadataManager::DeleteOverwrittenDeleteFiles(overwritten_delete_files, resolved_overwritten_paths);
		vector<DuckLakePath> resolved_delete_paths;
		resolved_delete_paths.reserve(file_list.size());
		for (auto &file : file_list) {
			resolved_delete_paths.push_back(
			    metadata_manager->GetRelativePath(file.table_id, file.path, new_tables_result, new_schemas_result));
		}
		batch_queries += DuckLakeMetadataManager::WriteNewDeleteFiles(file_list, resolved_delete_paths);

		// write new inlined deletes (for inlined data tables)
		auto inlined_deletes = transaction.GetNewInlinedDeletes(commit_state);
		batch_queries += DuckLakeMetadataManager::WriteNewInlinedDeletes(inlined_deletes);

		// write new inlined file deletes (for parquet files)
		auto inlined_file_deletes = transaction.GetNewInlinedFileDeletes(commit_state);
		batch_queries += metadata_manager->WriteNewInlinedFileDeletes(commit_snapshot, inlined_file_deletes);

		// write compactions
		auto compaction_merge_adjacent_changes =
		    transaction.GetCompactionChanges(commit_state, CompactionType::MERGE_ADJACENT_TABLES);
		vector<DuckLakePath> resolved_merge_paths;
		resolved_merge_paths.reserve(compaction_merge_adjacent_changes.compacted_files.size());
		for (auto &compaction : compaction_merge_adjacent_changes.compacted_files) {
			resolved_merge_paths.push_back(metadata_manager->GetRelativePath(compaction.path));
		}
		batch_queries +=
		    DuckLakeMetadataManager::WriteCompactions(compaction_merge_adjacent_changes.compacted_files,
		                                              CompactionType::MERGE_ADJACENT_TABLES, resolved_merge_paths);
		batch_queries += metadata_manager->WriteNewDataFiles(
		    commit_snapshot, compaction_merge_adjacent_changes.new_files, new_tables_result, new_schemas_result);

		auto compaction_rewrite_delete_changes =
		    transaction.GetCompactionChanges(commit_state, CompactionType::REWRITE_DELETES);
		batch_queries += metadata_manager->WriteNewDataFiles(
		    commit_snapshot, compaction_rewrite_delete_changes.new_files, new_tables_result, new_schemas_result);
		// REWRITE_DELETES ignores resolved_paths; pass empty.
		batch_queries += DuckLakeMetadataManager::WriteCompactions(
		    compaction_rewrite_delete_changes.compacted_files, CompactionType::REWRITE_DELETES, vector<DuckLakePath>());
	}

	// Tracking for tables that had schema changes
	set<TableIndex> tables_with_schema_changes;
	for (auto &table_id : transaction_changes.altered_tables) {
		if (!table_id.IsTransactionLocal() &&
		    transaction_changes.altered_tables_with_schema_version_changes.find(table_id) !=
		        transaction_changes.altered_tables_with_schema_version_changes.end()) {
			tables_with_schema_changes.insert(table_id);
		}
	}
	for (auto &new_table : new_tables_result) {
		if (!new_table.id.IsTransactionLocal()) {
			tables_with_schema_changes.insert(new_table.id);
		}
	}
	batch_queries += DuckLakeMetadataManager::InsertNewSchema(commit_snapshot, tables_with_schema_changes);

	return batch_queries;
}

SnapshotDeletedFromFiles DuckLakeTransactionState::GetFilesDeletedOrDroppedAfterSnapshot(
    const std::function<unique_ptr<QueryResult>(string)> &executor) {
	// get all changes made to the system after the snapshot was started
	string sql = R"(
	SELECT data_file_id
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE begin_snapshot > {SNAPSHOT_ID}
	UNION ALL
	SELECT data_file_id
	FROM {METADATA_CATALOG}.ducklake_data_file
	WHERE end_snapshot IS NOT NULL AND end_snapshot > {SNAPSHOT_ID}
	)";
	auto result = executor(sql);
	if (result->HasError()) {
		result->GetErrorObject().Throw(
		    "Failed to commit DuckLake transaction - failed to get files with deletions for conflict resolution:");
	}
	// parse changes made by other transactions
	SnapshotDeletedFromFiles change_info;
	for (auto &row : *result) {
		change_info.deleted_from_files.insert(DataFileIndex(row.GetValue<idx_t>(0)));
	}
	return change_info;
}

void DuckLakeTransactionState::DropEmptySupersededInlinedTables(const DuckLakeCommitContext &context) {
	// Gather the tables that are empty and have a later schema version, as it will be safe to delete their
	// inlined tables.
	string find_targets_sql = R"(
SELECT idt.table_id, idt.schema_version, idt.table_name
FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt
JOIN duckdb_tables() dt
    ON dt.database_name = {METADATA_CATALOG_NAME_LITERAL}
   AND dt.table_name = idt.table_name
WHERE idt.schema_version < (
    SELECT MAX(idt2.schema_version)
    FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt2
    WHERE idt2.table_id = idt.table_id
)
AND dt.estimated_size = 0;)";
	auto targets = context.query_metadata(find_targets_sql);
	if (targets->HasError()) {
		targets->GetErrorObject().Throw("Failed to identify empty superseded inlined-data tables in DuckLake: ");
	}
	string drops_sql;
	for (auto &row : *targets) {
		auto table_id = row.GetValue<idx_t>(0);
		auto schema_version = row.GetValue<idx_t>(1);
		auto table_name = row.GetValue<string>(2);
		drops_sql += StringUtil::Format(
		    "DELETE FROM {METADATA_CATALOG}.ducklake_inlined_data_tables WHERE table_id=%d AND schema_version=%d;"
		    "DROP TABLE IF EXISTS {METADATA_CATALOG}.%s;",
		    table_id, schema_version, SQLIdentifier(table_name));
	}
	if (drops_sql.empty()) {
		return;
	}
	auto res = context.query_metadata(drops_sql);
	if (res->HasError()) {
		res->GetErrorObject().Throw("Failed to drop superseded inlined-data tables in DuckLake: ");
	}
	// We also need to invalidate the existing schema versions in our catalog
	string snapshot_versions_sql = "SELECT DISTINCT schema_version FROM {METADATA_CATALOG}.ducklake_snapshot;";
	auto snapshot_versions = context.query_metadata(snapshot_versions_sql);
	if (snapshot_versions->HasError()) {
		snapshot_versions->GetErrorObject().Throw("Failed to list schema versions for cache invalidation: ");
	}
	for (auto &row : *snapshot_versions) {
		context.invalidate_schema_cache(row.GetValue<idx_t>(0));
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
	CheckForConflicts(changes, other_changes, transaction_snapshot, executor);

	return snapshot_and_stats;
}

void DuckLakeTransactionState::Commit(DuckLakeSnapshot transaction_snapshot,
                                      const TransactionChangeInformation &transaction_changes,
                                      const DuckLakeRetryConfig &retry_config, const DuckLakeCommitContext &context) {
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
				    CheckForConflicts(transaction_snapshot, attempt_changes, context.conflict_query_executor);
				stats = &commit_stats_snapshot.stats;
			} else {
				commit_stats_snapshot.snapshot = context.get_snapshot();
			}
			commit_snapshot.snapshot_id++;
			if (SchemaChangesMade()) {
				// we changed the schema - need to get a new schema version
				commit_snapshot.schema_version++;
			}
			can_retry = true;
			DuckLakeCommitState commit_state(commit_snapshot);
			// write the new snapshot
			string batch_queries = DuckLakeMetadataManager::InsertSnapshotSql();
			batch_queries += CommitChanges(commit_state, attempt_changes, stats);
			batch_queries += WriteSnapshotChanges(commit_state, attempt_changes, context.commit_info);
			auto res = context.execute_commit_batch(commit_snapshot, batch_queries);
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to flush changes into DuckLake: ");
			}
			bool flushed_inlined = !flushed_inlined_tables.empty();
			context.flush_cache_if_pending();
			context.commit_connection();
			if (flushed_inlined) {
				DropEmptySupersededInlinedTables(context);
			}
			context.set_catalog_version(commit_snapshot.schema_version);

			// finished writing
			break;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			// rollback if there is an active transaction
			context.try_rollback();
			bool retry_on_error = DuckLakeTransaction::RetryOnError(error.Message());
			bool finished_retrying = i + 1 >= retry_config.max_retry_count;
			if (!can_retry || !retry_on_error || finished_retrying) {
				// we abort after the max retry count
				CleanupFiles();
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
			context.prepare_retry();
		}
	}
	// If we got here, this snapshot was successful
	context.set_committed_snapshot_id(commit_snapshot.snapshot_id);
}

} // namespace duckdb
