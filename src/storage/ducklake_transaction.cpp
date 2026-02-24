#include "storage/ducklake_transaction.hpp"

#include "common/ducklake_types.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_macro_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "storage/ducklake_view_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_config.hpp"

namespace duckdb {

bool LocalTableDataChanges::IsEmpty() const {
	if (!new_data_files.empty()) {
		return false;
	}
	if (new_inlined_data) {
		return false;
	}
	if (!new_delete_files.empty()) {
		return false;
	}
	if (!new_inlined_data_deletes.empty()) {
		return false;
	}
	if (!compactions.empty()) {
		return false;
	}
	if (new_inlined_file_deletes) {
		return false;
	}
	return true;
}

DuckLakeTransaction::DuckLakeTransaction(DuckLakeCatalog &ducklake_catalog, TransactionManager &manager,
                                         ClientContext &context)
    : Transaction(manager, context), ducklake_catalog(ducklake_catalog), db(*context.db),
      local_catalog_id(DuckLakeConstants::TRANSACTION_LOCAL_ID_START), catalog_version(0) {
	metadata_manager = DuckLakeMetadataManager::Create(*this);
}

DuckLakeTransaction::~DuckLakeTransaction() {
}

void DuckLakeTransaction::Start() {
}

void DuckLakeTransaction::Commit() {
	if (ChangesMade()) {
		FlushChanges();
	} else if (connection) {
		connection->Commit();
	}
	connection.reset();

	table_data_changes.clear();
}

void DuckLakeTransaction::Rollback() {
	if (connection) {
		// rollback any changes made to the metadata catalog
		connection->Rollback();
		connection.reset();
	}
	CleanupFiles();

	table_data_changes.clear();
}

Connection &DuckLakeTransaction::GetConnection() {
	lock_guard<mutex> lock(connection_lock);
	if (!connection) {
		connection = make_uniq<Connection>(db);
		// set the search path to the metadata catalog
		auto &client_data = ClientData::Get(*connection->context);
		// ensure we are only looking in the ducklake catalog schema during querying
		CatalogSearchEntry metadata_entry(ducklake_catalog.MetadataDatabaseName(),
		                                  ducklake_catalog.MetadataSchemaName());
		if (metadata_entry.schema.empty()) {
			metadata_entry.schema = "main";
		}
		client_data.catalog_search_path->Set(metadata_entry, CatalogSetPathType::SET_DIRECTLY);

		// set max error reporting to 0 so that during error reporting we don't traverse other schemas / catalogs
		auto &client_config = ClientConfig::GetConfig(*connection->context);
		client_config.user_settings.SetUserSetting(CatalogErrorMaxSchemasSetting::SettingIndex, Value::UBIGINT(0));
		connection->BeginTransaction();
	}
	return *connection;
}

bool DuckLakeTransaction::SchemaChangesMade() {
	return !new_tables.empty() || !dropped_tables.empty() || new_schemas || !dropped_schemas.empty() ||
	       !dropped_views.empty() || !new_macros.empty() || !dropped_scalar_macros.empty() ||
	       !dropped_table_macros.empty();
}

bool DuckLakeTransaction::ChangesMade() {
	return SchemaChangesMade() || !table_data_changes.empty() || !dropped_files.empty() ||
	       !new_name_maps.name_maps.empty();
}

struct TransactionChangeInformation {
	case_insensitive_set_t created_schemas;
	map<SchemaIndex, reference<DuckLakeSchemaEntry>> dropped_schemas;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_tables;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_scalar_macros;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_table_macros;

	set<TableIndex> altered_tables;
	set<TableIndex> altered_tables_with_schema_version_changes;
	set<TableIndex> altered_views;
	set<TableIndex> dropped_tables;
	set<TableIndex> dropped_views;
	set<MacroIndex> dropped_scalar_macros;
	set<MacroIndex> dropped_table_macros;
	set<TableIndex> tables_inserted_into;
	set<TableIndex> tables_deleted_from;
	set<TableIndex> tables_inserted_inlined;
	set<TableIndex> tables_deleted_inlined;
	set<TableIndex> tables_flushed_inlined;
	set<TableIndex> tables_compacted;
	set<TableIndex> tables_merge_adjacent;
	set<TableIndex> tables_rewrite_delete;
};

void GetTransactionTableChanges(reference<CatalogEntry> table_entry, TransactionChangeInformation &changes) {
	while (true) {
		auto &table = table_entry.get().Cast<DuckLakeTableEntry>();
		switch (table.GetLocalChange().type) {
		case LocalChangeType::SET_PARTITION_KEY:
		case LocalChangeType::SET_NULL:
		case LocalChangeType::DROP_NULL:
		case LocalChangeType::RENAME_COLUMN:
		case LocalChangeType::ADD_COLUMN:
		case LocalChangeType::REMOVE_COLUMN:
		case LocalChangeType::CHANGE_COLUMN_TYPE:
		case LocalChangeType::SET_DEFAULT: {
			// this table was altered in a way that modifies the ducklake_schema_versions
			auto table_id = table.GetTableId();
			// don't report transaction-local tables yet - these will get added later on
			if (!table_id.IsTransactionLocal()) {
				changes.altered_tables.insert(table_id);
				changes.altered_tables_with_schema_version_changes.insert(table_id);
			}
			break;
		}
		case LocalChangeType::SET_COMMENT:
		case LocalChangeType::SET_COLUMN_COMMENT:
		case LocalChangeType::SET_SORT_KEY: {
			// this table was altered, but not in a way that would break the ability to compact across files (same
			// ducklake_schema_versions)
			auto table_id = table.GetTableId();
			// don't report transaction-local tables yet - these will get added later on
			if (!table_id.IsTransactionLocal()) {
				changes.altered_tables.insert(table_id);
			}
			break;
		}
		case LocalChangeType::NONE:
		case LocalChangeType::CREATED:
		case LocalChangeType::RENAMED: {
			// write any new tables that we created
			auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
			changes.created_tables[schema.name].insert(table);
			break;
		}
		default:
			throw NotImplementedException("Unsupported transaction local change in GetTransactionTableChanges");
		}
		if (!table_entry.get().HasChild()) {
			break;
		}
		table_entry = table_entry.get().Child();
	}
}

void GetTransactionViewChanges(reference<CatalogEntry> view_entry, TransactionChangeInformation &changes) {
	while (true) {
		auto &view = view_entry.get().Cast<DuckLakeViewEntry>();
		switch (view.GetLocalChange().type) {
		case LocalChangeType::SET_COMMENT: {
			// this table was altered
			auto view_id = view.GetViewId();
			// don't report transaction-local views yet - these will get added later on
			if (!view_id.IsTransactionLocal()) {
				changes.altered_views.insert(view_id);
			}
			break;
		}
		case LocalChangeType::NONE:
		case LocalChangeType::CREATED:
		case LocalChangeType::RENAMED: {
			// write any new view that we created
			auto &schema = view.ParentSchema().Cast<DuckLakeSchemaEntry>();
			changes.created_tables[schema.name].insert(view);
			break;
		}
		default:
			throw NotImplementedException("Unsupported transaction local change in GetTransactionTableChanges");
		}

		if (!view_entry.get().HasChild()) {
			break;
		}
		view_entry = view_entry.get().Child();
	}
}

TransactionChangeInformation DuckLakeTransaction::GetTransactionChanges() {
	TransactionChangeInformation changes;
	for (auto &dropped_table_idx : dropped_tables) {
		changes.dropped_tables.insert(dropped_table_idx);
	}
	for (auto &dropped_view_idx : dropped_views) {
		changes.dropped_views.insert(dropped_view_idx);
	}
	for (auto &dropped_macro_idx : dropped_scalar_macros) {
		changes.dropped_scalar_macros.insert(dropped_macro_idx);
	}
	for (auto &dropped_macro_idx : dropped_table_macros) {
		changes.dropped_table_macros.insert(dropped_macro_idx);
	}
	for (auto &entry : dropped_schemas) {
		changes.dropped_schemas.insert(entry);
	}
	if (new_schemas) {
		for (auto &entry : new_schemas->GetEntries()) {
			auto &schema_entry = entry.second->Cast<DuckLakeSchemaEntry>();
			changes.created_schemas.insert(schema_entry.name);
		}
	}
	for (auto &schema_entry : new_macros) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			switch (entry.second->type) {
			case CatalogType::MACRO_ENTRY: {
				auto &macro = *entry.second;
				auto &schema = macro.ParentSchema().Cast<DuckLakeSchemaEntry>();
				changes.created_scalar_macros[schema.name].insert(macro);
				break;
			}
			case CatalogType::TABLE_MACRO_ENTRY: {
				auto &macro = *entry.second;
				auto &schema = macro.ParentSchema().Cast<DuckLakeSchemaEntry>();
				changes.created_table_macros[schema.name].insert(macro);
				break;
			}
			default:
				throw InternalException("Unsupported type found in new_macros");
			}
		}
	}
	for (auto &schema_entry : new_tables) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			switch (entry.second->type) {
			case CatalogType::TABLE_ENTRY:
				GetTransactionTableChanges(*entry.second, changes);
				break;
			case CatalogType::VIEW_ENTRY:
				GetTransactionViewChanges(*entry.second, changes);
				break;
			default:
				throw InternalException("Unsupported type found in new_tables");
			}
		}
	}
	changes.tables_deleted_from = tables_deleted_from;
	for (auto &entry : table_data_changes) {
		auto table_id = entry.first;
		if (table_id.IsTransactionLocal()) {
			// don't report transaction-local tables yet - these will get added later on
			continue;
		}
		auto &table_changes = entry.second;
		AddTableChanges(table_id, table_changes, changes);
	}
	return changes;
}

struct DuckLakeCommitState {
	explicit DuckLakeCommitState(DuckLakeSnapshot &snapshot) : commit_snapshot(snapshot) {
	}

	DuckLakeSnapshot &commit_snapshot;
	map<SchemaIndex, SchemaIndex> committed_schemas;
	map<TableIndex, TableIndex> committed_tables;

	void RemapIdentifier(SchemaIndex &schema_id) const {
		auto entry = committed_schemas.find(schema_id);
		if (entry != committed_schemas.end()) {
			schema_id = entry->second;
		}
	}
	void RemapIdentifier(TableIndex &table_id) const {
		auto entry = committed_tables.find(table_id);
		if (entry != committed_tables.end()) {
			table_id = entry->second;
		}
	}

	SchemaIndex GetSchemaId(DuckLakeSchemaEntry &schema) const {
		auto schema_id = schema.GetSchemaId();
		RemapIdentifier(schema_id);
		return schema_id;
	}
	TableIndex GetTableId(DuckLakeTableEntry &table) const {
		auto table_id = table.GetTableId();
		RemapIdentifier(table_id);
		return table_id;
	}
	TableIndex GetTableId(TableIndex table_id) const {
		RemapIdentifier(table_id);
		return table_id;
	}
	TableIndex GetViewId(DuckLakeViewEntry &view) const {
		auto view_id = view.GetViewId();
		RemapIdentifier(view_id);
		return view_id;
	}
};

void DuckLakeTransaction::AddTableChanges(TableIndex table_id, const LocalTableDataChanges &table_changes,
                                          TransactionChangeInformation &changes) {
	bool inserted_data = false;
	bool flushed_inline_data = false;
	for (auto &file : table_changes.new_data_files) {
		if (file.begin_snapshot.IsValid()) {
			flushed_inline_data = true;
		} else {
			inserted_data = true;
		}
	}

	if (inserted_data) {
		changes.tables_inserted_into.insert(table_id);
	}
	if (flushed_inline_data) {
		changes.tables_flushed_inlined.insert(table_id);
	}
	if (table_changes.new_inlined_data) {
		changes.tables_inserted_inlined.insert(table_id);
	}
	if (!table_changes.new_delete_files.empty()) {
		changes.tables_deleted_from.insert(table_id);
	}
	if (!table_changes.new_inlined_data_deletes.empty() || table_changes.new_inlined_file_deletes) {
		changes.tables_deleted_inlined.insert(table_id);
	}
	for (auto &compaction : table_changes.compactions) {
		switch (compaction.type) {
		case CompactionType::MERGE_ADJACENT_TABLES:
			changes.tables_merge_adjacent.insert(table_id);
			break;
		case CompactionType::REWRITE_DELETES:
			changes.tables_rewrite_delete.insert(table_id);
			break;
		default:
			throw InternalException("Unknown compaction type");
		}
	}
}

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

string DuckLakeTransaction::WriteSnapshotChanges(DuckLakeCommitState &commit_state,
                                                 TransactionChangeInformation &changes) {
	SnapshotChangeInfo change_info;

	// re-add all inserted tables - transaction-local table identifiers should have been converted at this stage
	changes.tables_deleted_from = tables_deleted_from;
	for (auto &entry : table_data_changes) {
		auto table_id = commit_state.GetTableId(entry.first);
		auto &table_changes = entry.second;
		AddTableChanges(table_id, table_changes, changes);
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
	return metadata_manager->WriteSnapshotChanges(change_info, commit_info);
}

void DuckLakeTransaction::CleanupFiles() {
	// remove any files that were written
	auto context_ref = context.lock();
	auto &fs = FileSystem::GetFileSystem(db);
	for (auto &entry : table_data_changes) {
		auto &table_changes = entry.second;
		for (auto &file : table_changes.new_data_files) {
			if (file.created_by_ducklake) {
				fs.TryRemoveFile(file.file_name);
			}
			for (auto &del_file : file.delete_files) {
				fs.TryRemoveFile(del_file.file_name);
			}
		}
		for (auto &file : table_changes.new_delete_files) {
			for (auto &delete_files : file.second) {
				fs.TryRemoveFile(delete_files.file_name);
			}
		}
		table_changes.new_data_files.clear();
		table_changes.new_delete_files.clear();
	}
}

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

void DuckLakeTransaction::CheckForConflicts(const TransactionChangeInformation &changes,
                                            const SnapshotChangeInformation &other_changes,
                                            DuckLakeSnapshot transaction_snapshot) {
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
	}
	for (auto &table_id : changes.tables_inserted_inlined) {
		ConflictCheck(table_id, other_changes.dropped_tables, "insert into table", "dropped it");
		ConflictCheck(table_id, other_changes.altered_tables, "insert into table", "altered it");
	}
	for (auto &table_id : changes.tables_deleted_from) {
		ConflictCheck(table_id, other_changes.dropped_tables, "delete from table", "dropped it");
		ConflictCheck(table_id, other_changes.altered_tables, "delete from table", "altered it");
		ConflictCheck(table_id, other_changes.tables_merge_adjacent, "delete from table", "compacted it");
		ConflictCheck(table_id, other_changes.tables_rewrite_delete, "delete from table", "compacted it");
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
			const auto deleted_files = metadata_manager->GetFilesDeletedOrDroppedAfterSnapshot(transaction_snapshot);
			for (auto &entry : table_data_changes) {
				auto &table_changes = entry.second;
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

SnapshotAndStats DuckLakeTransaction::CheckForConflicts(DuckLakeSnapshot transaction_snapshot,
                                                        const TransactionChangeInformation &changes) {
	SnapshotAndStats snapshot_and_stats;
	// get all changes made to the system after the current snapshot was started
	auto changes_made = metadata_manager->GetSnapshotAndStatsAndChanges(transaction_snapshot, snapshot_and_stats);
	// parse changes made by other transactions
	auto other_changes = SnapshotChangeInformation::ParseChangesMade(changes_made.changes_made);

	// now check for conflicts
	CheckForConflicts(changes, other_changes, transaction_snapshot);

	return snapshot_and_stats;
}

vector<DuckLakeSchemaInfo> DuckLakeTransaction::GetNewSchemas(DuckLakeCommitState &commit_state) {
	vector<DuckLakeSchemaInfo> schemas;
	for (auto &entry : new_schemas->GetEntries()) {
		auto &schema_entry = entry.second->Cast<DuckLakeSchemaEntry>();
		auto old_id = schema_entry.GetSchemaId();
		DuckLakeSchemaInfo schema_info;
		schema_info.id = SchemaIndex(commit_state.commit_snapshot.next_catalog_id++);
		schema_info.uuid = schema_entry.GetSchemaUUID();
		schema_info.name = schema_entry.name;
		schema_info.path = schema_entry.DataPath();

		// add this schema id to the schema id map
		commit_state.committed_schemas.emplace(old_id, schema_info.id);

		// add the schema to the list
		schemas.push_back(std::move(schema_info));
	}
	return schemas;
}

DuckLakePartitionInfo DuckLakeTransaction::GetNewPartitionKey(DuckLakeCommitState &commit_state,
                                                              DuckLakeTableEntry &table) {
	DuckLakePartitionInfo partition_key;
	partition_key.table_id = commit_state.GetTableId(table);
	if (partition_key.table_id.IsTransactionLocal()) {
		throw InternalException("Trying to write partition with transaction local table-id");
	}
	// insert the new partition data
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		// dropping partition data - insert the empty partition key data for this table
		return partition_key;
	}
	auto local_partition_id = partition_data->partition_id;
	auto partition_id = commit_state.commit_snapshot.next_catalog_id++;
	partition_key.id = partition_id;
	partition_data->partition_id = partition_id;
	for (auto &field : partition_data->fields) {
		DuckLakePartitionFieldInfo partition_field;
		partition_field.partition_key_index = field.partition_key_index;
		partition_field.field_id = field.field_id;
		switch (field.transform.type) {
		case DuckLakeTransformType::IDENTITY:
			partition_field.transform = "identity";
			break;
		case DuckLakeTransformType::YEAR:
			partition_field.transform = "year";
			break;
		case DuckLakeTransformType::MONTH:
			partition_field.transform = "month";
			break;
		case DuckLakeTransformType::DAY:
			partition_field.transform = "day";
			break;
		case DuckLakeTransformType::HOUR:
			partition_field.transform = "hour";
			break;
		default:
			throw NotImplementedException("Unimplemented transform type for partition");
		}
		partition_key.fields.push_back(std::move(partition_field));
	}

	// if we wrote any data with this partition id - rewrite it to the latest partition id
	for (auto &entry : table_data_changes) {
		auto &table_changes = entry.second;
		for (auto &file : table_changes.new_data_files) {
			if (file.partition_id.IsValid() && file.partition_id.GetIndex() == local_partition_id) {
				file.partition_id = partition_id;
			}
		}
	}
	return partition_key;
}

DuckLakeSortInfo DuckLakeTransaction::GetNewSortKey(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table) {
	DuckLakeSortInfo sort_key;
	sort_key.table_id = commit_state.GetTableId(table);
	if (sort_key.table_id.IsTransactionLocal()) {
		throw InternalException("Trying to write sort with transaction local table-id");
	}

	// insert the new sort data
	auto sort_data = table.GetSortData();
	if (!sort_data) {
		// dropping sort data - insert the empty sort key data for this table
		return sort_key;
	}

	auto sort_id = commit_state.commit_snapshot.next_catalog_id++;
	sort_key.id = sort_id;
	sort_data->sort_id = sort_id;
	for (auto &field : sort_data->fields) {
		DuckLakeSortFieldInfo sort_field;
		sort_field.sort_key_index = field.sort_key_index;
		sort_field.expression = field.expression;
		sort_field.dialect = field.dialect;
		sort_field.sort_direction = field.sort_direction;
		sort_field.null_order = field.null_order;

		sort_key.fields.push_back(std::move(sort_field));
	}

	return sort_key;
}

vector<DuckLakeColumnInfo> DuckLakeTableEntry::GetTableColumns() const {
	vector<DuckLakeColumnInfo> result;
	auto not_null_fields = GetNotNullFields();
	for (auto &col : GetColumns().Logical()) {
		auto col_info = DuckLakeTableEntry::ConvertColumn(col.GetName(), col.GetType(), GetFieldId(col.Physical()));
		if (not_null_fields.count(col.GetName())) {
			// no null values allowed in this field
			col_info.nulls_allowed = false;
		}
		result.push_back(std::move(col_info));
	}
	return result;
}

DuckLakeTableInfo DuckLakeTableEntry::GetTableInfo() const {
	auto &schema = ParentSchema().Cast<DuckLakeSchemaEntry>();
	DuckLakeTableInfo table_entry;
	table_entry.id = GetTableId();
	table_entry.uuid = GetTableUUID();
	table_entry.schema_id = schema.GetSchemaId();
	table_entry.name = name;
	table_entry.path = DataPath();
	return table_entry;
}

DuckLakeTableInfo DuckLakeTransaction::GetNewTable(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table) {
	auto table_entry = table.GetTableInfo();
	auto original_id = table_entry.id;
	bool is_new_table;
	if (original_id.IsTransactionLocal()) {
		table_entry.id = TableIndex(commit_state.commit_snapshot.next_catalog_id++);
		is_new_table = true;
	} else {
		// this table already has an id - keep it
		// this happens if e.g. this table is renamed
		table_entry.id = original_id;
		is_new_table = false;
	}
	commit_state.RemapIdentifier(table_entry.schema_id);
	if (is_new_table) {
		// if this is a new table - write the columns
		table_entry.columns = table.GetTableColumns();
	}
	return table_entry;
}

struct NewTableInfo {
	vector<DuckLakeTableInfo> new_tables;
	vector<DuckLakeViewInfo> new_views;
	vector<DuckLakePartitionInfo> new_partition_keys;
	vector<DuckLakeTagInfo> new_tags;
	vector<DuckLakeColumnTagInfo> new_column_tags;
	vector<DuckLakeDroppedColumn> dropped_columns;
	vector<DuckLakeNewColumn> new_columns;
	vector<DuckLakeTableInfo> new_inlined_data_tables;
	vector<DuckLakeSortInfo> new_sort_keys;
};

struct NewMacroInfo {
	vector<DuckLakeMacroInfo> new_macros;
};

void HandleChangedFields(TableIndex table_id, const ColumnChangeInfo &change_info, NewTableInfo &result,
                         const set<FieldIndex> &columns_handled_by_later_ops) {
	for (auto &new_col_info : change_info.new_fields) {
		// Skip adding columns that will be handled by a later operation (e.g., SET_DEFAULT after CHANGE_COLUMN_TYPE)
		if (columns_handled_by_later_ops.find(new_col_info.column_info.id) != columns_handled_by_later_ops.end()) {
			continue;
		}
		DuckLakeNewColumn new_column;
		new_column.table_id = table_id;
		new_column.column_info = new_col_info.column_info;
		new_column.parent_idx = new_col_info.parent_idx;
		result.new_columns.push_back(std::move(new_column));
	}
	for (auto &dropped_field_id : change_info.dropped_fields) {
		DuckLakeDroppedColumn dropped_col;
		dropped_col.table_id = table_id;
		dropped_col.field_id = dropped_field_id;
		result.dropped_columns.push_back(dropped_col);
	}
}

void DuckLakeTransaction::GetNewTableInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
                                          reference<CatalogEntry> table_entry, NewTableInfo &result,
                                          TransactionChangeInformation &transaction_changes) {
	// iterate over the table chain in reverse order when committing
	// the latest entry is the root entry - but we need to commit starting from the first entry written
	// gather all tables
	vector<reference<DuckLakeTableEntry>> tables;
	while (true) {
		tables.push_back(table_entry.get().Cast<DuckLakeTableEntry>());
		if (!table_entry.get().HasChild()) {
			break;
		}
		table_entry = table_entry.get().Child();
	}

	set<FieldIndex> columns_handled_by_later_ops;
	for (idx_t table_idx = 0; table_idx < tables.size(); table_idx++) {
		auto &table = tables[table_idx].get();
		auto local_change = table.GetLocalChange();
		switch (local_change.type) {
		case LocalChangeType::SET_NULL:
		case LocalChangeType::DROP_NULL:
		case LocalChangeType::RENAME_COLUMN:
		case LocalChangeType::SET_DEFAULT:
			columns_handled_by_later_ops.insert(local_change.field_index);
			break;
		default:
			break;
		}
	}

	// traverse in reverse order
	bool column_schema_change = false;
	for (idx_t table_idx = tables.size(); table_idx > 0; table_idx--) {
		auto &table = tables[table_idx - 1].get();
		auto local_change = table.GetLocalChange();
		auto table_id = table.GetTableId();
		switch (local_change.type) {
		case LocalChangeType::SET_PARTITION_KEY: {
			auto partition_key = GetNewPartitionKey(commit_state, table);
			result.new_partition_keys.push_back(std::move(partition_key));

			transaction_changes.altered_tables.insert(table_id);
			transaction_changes.altered_tables_with_schema_version_changes.insert(table_id);
			column_schema_change = true;
			break;
		}
		case LocalChangeType::SET_SORT_KEY: {
			auto sort_key = GetNewSortKey(commit_state, table);
			result.new_sort_keys.push_back(std::move(sort_key));
			transaction_changes.altered_tables.insert(table_id);
			break;
		}
		case LocalChangeType::SET_COMMENT: {
			DuckLakeTagInfo comment_info;
			comment_info.id = commit_state.GetTableId(table).index;
			comment_info.key = "comment";
			comment_info.value = table.comment;
			result.new_tags.push_back(std::move(comment_info));

			transaction_changes.altered_tables.insert(table_id);
			break;
		}
		case LocalChangeType::SET_COLUMN_COMMENT: {
			DuckLakeColumnTagInfo comment_info;
			comment_info.table_id = commit_state.GetTableId(table);
			comment_info.field_index = local_change.field_index;
			comment_info.key = "comment";
			comment_info.value = table.GetColumnByFieldId(local_change.field_index).Comment();
			result.new_column_tags.push_back(std::move(comment_info));

			transaction_changes.altered_tables.insert(table_id);
			break;
		}
		case LocalChangeType::SET_NULL:
		case LocalChangeType::DROP_NULL:
		case LocalChangeType::RENAME_COLUMN:
		case LocalChangeType::SET_DEFAULT: {
			// drop the previous column
			DuckLakeDroppedColumn dropped_col;
			dropped_col.table_id = commit_state.GetTableId(table);
			dropped_col.field_id = local_change.field_index;
			result.dropped_columns.push_back(dropped_col);

			// insert the new column with the new info
			DuckLakeNewColumn new_col;
			new_col.table_id = commit_state.GetTableId(table);
			new_col.column_info = table.GetColumnInfo(local_change.field_index);
			result.new_columns.push_back(std::move(new_col));

			transaction_changes.altered_tables.insert(table_id);
			transaction_changes.altered_tables_with_schema_version_changes.insert(table_id);
			if (local_change.type == LocalChangeType::RENAME_COLUMN) {
				column_schema_change = true;
			}
			break;
		}
		case LocalChangeType::REMOVE_COLUMN:
		case LocalChangeType::CHANGE_COLUMN_TYPE: {
			// drop the indicated column
			// note that in case of nested types we might be dropping multiple columns here
			HandleChangedFields(commit_state.GetTableId(table), table.GetChangedFields(), result,
			                    columns_handled_by_later_ops);
			transaction_changes.altered_tables_with_schema_version_changes.insert(table_id);
			column_schema_change = true;
			break;
		}
		case LocalChangeType::ADD_COLUMN: {
			// insert the new column
			DuckLakeNewColumn new_col;
			new_col.table_id = commit_state.GetTableId(table);
			new_col.column_info = table.GetAddColumnInfo();
			result.new_columns.push_back(std::move(new_col));

			transaction_changes.altered_tables.insert(table.GetTableId());
			transaction_changes.altered_tables_with_schema_version_changes.insert(table_id);
			column_schema_change = true;
			break;
		}
		case LocalChangeType::NONE:
		case LocalChangeType::CREATED:
		case LocalChangeType::RENAMED: {
			auto old_table_id = table.GetTableId();
			auto new_table = GetNewTable(commit_state, table);
			auto new_table_id = new_table.id;
			result.new_tables.push_back(std::move(new_table));

			// remap the table in the commit state
			commit_state.committed_tables.emplace(old_table_id, new_table_id);

			// create an inlined data table entry with the latest columns
			// (uses tables.front() to get the most up-to-date schema including any ALTER TABLE changes)
			auto &latest_table = tables.front().get();
			DuckLakeTableInfo inlined_entry;
			inlined_entry.id = new_table_id;
			inlined_entry.schema_id = latest_table.ParentSchema().Cast<DuckLakeSchemaEntry>().GetSchemaId();
			inlined_entry.uuid = latest_table.GetTableUUID();
			inlined_entry.columns = latest_table.GetTableColumns();
			result.new_inlined_data_tables.push_back(std::move(inlined_entry));
			break;
		}
		default:
			throw NotImplementedException("Unsupported transaction local change");
		}
	}
	if (column_schema_change) {
		// we changed the column definitions of an existing table - we need to create a new inlined data table
		// (if data inlining is enabled)
		// for newly created tables this is already handled in the CREATED case above
		auto &table = tables.front().get();
		auto committed_id = commit_state.GetTableId(table);
		bool already_added = false;
		for (auto &entry : result.new_inlined_data_tables) {
			if (entry.id == committed_id) {
				already_added = true;
				break;
			}
		}
		if (!already_added) {
			DuckLakeTableInfo table_entry;
			table_entry.id = committed_id;
			table_entry.schema_id = table.ParentSchema().Cast<DuckLakeSchemaEntry>().GetSchemaId();
			table_entry.uuid = table.GetTableUUID();
			table_entry.columns = table.GetTableColumns();
			result.new_inlined_data_tables.push_back(std::move(table_entry));
		}
	}
}

DuckLakeViewInfo DuckLakeTransaction::GetNewView(DuckLakeCommitState &commit_state, DuckLakeViewEntry &view) {
	auto &schema = view.ParentSchema().Cast<DuckLakeSchemaEntry>();
	DuckLakeViewInfo view_entry;
	auto original_id = view.GetViewId();
	if (original_id.IsTransactionLocal()) {
		view_entry.id = TableIndex(commit_state.commit_snapshot.next_catalog_id++);
	} else {
		// this view already has an id - keep it
		// this happens if e.g. this view is renamed
		view_entry.id = original_id;
	}
	view_entry.uuid = view.GetViewUUID();
	view_entry.schema_id = commit_state.GetSchemaId(schema);
	view_entry.name = view.name;
	view_entry.dialect = "duckdb";
	view_entry.sql = view.GetQuerySQL();
	view_entry.column_aliases = view.aliases;
	return view_entry;
}

void DuckLakeTransaction::GetNewMacroInfo(DuckLakeCommitState &commit_state, reference<CatalogEntry> entry,
                                          NewMacroInfo &result) {
	DuckLakeMacroInfo new_macro_info;
	auto &macro_entry = entry.get().Cast<MacroCatalogEntry>();
	auto &ducklake_schema = macro_entry.schema.Cast<DuckLakeSchemaEntry>();

	new_macro_info.macro_id = MacroIndex(commit_state.commit_snapshot.next_catalog_id++);
	new_macro_info.macro_name = macro_entry.name;
	new_macro_info.schema_id = ducklake_schema.GetSchemaId();
	// Let's do the implementations
	for (const auto &impl : macro_entry.macros) {
		DuckLakeMacroImplementation macro_impl;
		macro_impl.dialect = "duckdb";
		switch (impl->type) {
		case MacroType::SCALAR_MACRO: {
			macro_impl.type = "scalar";
			auto &scalar_macro = impl->Cast<ScalarMacroFunction>();
			macro_impl.sql = scalar_macro.expression->ToString();
			break;
		}
		case MacroType::TABLE_MACRO: {
			macro_impl.type = "table";
			auto &table_macro = impl->Cast<TableMacroFunction>();
			macro_impl.sql = table_macro.query_node->ToString();
			break;
		}
		default:
			throw NotImplementedException("Unsupported macro type");
		}
		macro_impl.sql = StringUtil::Replace(macro_impl.sql, "'", "''");
		// Let's do the parameters
		for (idx_t i = 0; i < impl->parameters.size(); i++) {
			DuckLakeMacroParameters parameter;
			parameter.parameter_name = impl->parameters[i]->GetName();
			parameter.parameter_type = DuckLakeTypes::ToString(impl->types[i]);
			if (impl->default_parameters.find(parameter.parameter_name) != impl->default_parameters.end()) {
				auto value = impl->default_parameters[parameter.parameter_name]->ToString();
				if (StringUtil::StartsWith(value, "'")) {
					value = value.substr(1, value.size() - 2);
				}
				value = StringUtil::Replace(value, "'", "''");

				parameter.default_value = value;

				parameter.default_value_type = DuckLakeTypes::ToString(
				    impl->default_parameters[parameter.parameter_name]->Cast<ConstantExpression>().value.type());
			} else {
				parameter.default_value_type = "unknown";
			}

			macro_impl.parameters.push_back(std::move(parameter));
		}
		new_macro_info.implementations.push_back(std::move(macro_impl));
	}
	result.new_macros.push_back(std::move(new_macro_info));
}

void DuckLakeTransaction::GetNewViewInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
                                         reference<CatalogEntry> view_entry, NewTableInfo &result,
                                         TransactionChangeInformation &transaction_changes) {
	// iterate over the view chain in reverse order when committing
	// the latest entry is the root entry - but we need to commit starting from the first entry written
	// gather all views
	vector<reference<DuckLakeViewEntry>> views;
	while (true) {
		views.push_back(view_entry.get().Cast<DuckLakeViewEntry>());
		if (!view_entry.get().HasChild()) {
			break;
		}
		view_entry = view_entry.get().Child();
	}
	// traverse in reverse order
	for (idx_t view_idx = views.size(); view_idx > 0; view_idx--) {
		auto &view = views[view_idx - 1].get();
		switch (view.GetLocalChange().type) {
		case LocalChangeType::SET_COMMENT: {
			DuckLakeTagInfo comment_info;
			comment_info.id = commit_state.GetViewId(view).index;
			comment_info.key = "comment";
			comment_info.value = view.comment;
			result.new_tags.push_back(std::move(comment_info));

			transaction_changes.altered_views.insert(view.GetViewId());
			break;
		}
		case LocalChangeType::NONE:
		case LocalChangeType::CREATED:
		case LocalChangeType::RENAMED: {
			auto old_view_id = view.GetViewId();
			auto new_view = GetNewView(commit_state, view);
			auto new_view_id = new_view.id;
			result.new_views.push_back(std::move(new_view));

			// remap the view in the commit state
			commit_state.committed_tables.emplace(old_view_id, new_view_id);
			break;
		}
		default:
			throw NotImplementedException("Unsupported transaction local change");
		}
	}
}

NewTableInfo DuckLakeTransaction::GetNewTables(DuckLakeCommitState &commit_state,
                                               TransactionChangeInformation &transaction_changes) {
	NewTableInfo result;
	for (auto &schema_entry : new_tables) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			switch (entry.second->type) {
			case CatalogType::TABLE_ENTRY:
				GetNewTableInfo(commit_state, *schema_entry.second, *entry.second, result, transaction_changes);
				break;
			case CatalogType::VIEW_ENTRY:
				GetNewViewInfo(commit_state, *schema_entry.second, *entry.second, result, transaction_changes);
				break;
			default:
				throw InternalException("Unknown type in new_tables");
			}
		}
	}
	return result;
}

NewMacroInfo DuckLakeTransaction::GetNewMacros(DuckLakeCommitState &commit_state,
                                               TransactionChangeInformation &transaction_changes) {
	NewMacroInfo result;
	for (auto &schema_entry : new_macros) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			switch (entry.second->type) {
			case CatalogType::MACRO_ENTRY:
			case CatalogType::TABLE_MACRO_ENTRY:
				GetNewMacroInfo(commit_state, *entry.second, result);
				break;
			default:
				throw InternalException("Unknown type in GetNewMacros");
			}
		}
	}
	return result;
}

struct DuckLakeNewGlobalStats {
	DuckLakeTableStats stats;
	bool initialized = false;
};

string DuckLakeTransaction::UpdateGlobalTableStats(TableIndex table_id,
                                                   const DuckLakeNewGlobalStats &new_global_stats) {
	DuckLakeGlobalStatsInfo stats;
	stats.table_id = table_id;

	stats.initialized = new_global_stats.initialized;
	auto &new_stats = new_global_stats.stats;
	for (auto &entry : new_stats.column_stats) {
		DuckLakeGlobalColumnStatsInfo col_stats;
		col_stats.column_id = entry.first;
		auto &column_stats = entry.second;
		col_stats.has_contains_null = column_stats.has_null_count;
		if (column_stats.has_null_count) {
			col_stats.contains_null = column_stats.null_count > 0;
		}
		col_stats.has_contains_nan = column_stats.has_contains_nan;
		if (column_stats.has_contains_nan) {
			col_stats.contains_nan = column_stats.contains_nan;
		}
		col_stats.has_min = column_stats.has_min;
		if (column_stats.has_min) {
			col_stats.min_val = column_stats.min;
		}
		col_stats.has_max = column_stats.has_max;
		if (column_stats.has_max) {
			col_stats.max_val = column_stats.max;
		}
		if (column_stats.extra_stats) {
			col_stats.has_extra_stats = column_stats.extra_stats->TrySerialize(col_stats.extra_stats);
		} else {
			col_stats.has_extra_stats = false;
		}
		stats.column_stats.push_back(std::move(col_stats));
	}
	stats.record_count = new_stats.record_count;
	stats.next_row_id = new_stats.next_row_id;
	stats.table_size_bytes = new_stats.table_size_bytes;
	// finally update the stats in the tables
	return metadata_manager->UpdateGlobalTableStats(stats);
}

DuckLakeColumnStatsInfo DuckLakeColumnStatsInfo::FromColumnStats(FieldIndex field_id,
                                                                 const DuckLakeColumnStats &stats) {
	DuckLakeColumnStatsInfo column_stats;
	column_stats.column_id = field_id;
	column_stats.min_val = stats.has_min ? DuckLakeUtil::StatsToString(stats.min) : "NULL";
	column_stats.max_val = stats.has_max ? DuckLakeUtil::StatsToString(stats.max) : "NULL";
	column_stats.column_size_bytes = to_string(stats.column_size_bytes);
	if (stats.has_null_count && stats.has_num_values) {
		// value_count should be the count of non-null values: num_values - null_count
		// Validate that null_count doesn't exceed num_values to prevent underflow
		if (stats.null_count > stats.num_values) {
			// Invalid stats - null_count can't exceed total values
			column_stats.value_count = "NULL";
			column_stats.null_count = "NULL";
		} else {
			column_stats.value_count = to_string(stats.num_values - stats.null_count);
			column_stats.null_count = to_string(stats.null_count);
		}
	} else {
		column_stats.value_count = "NULL";
		column_stats.null_count = "NULL";
	}
	if (stats.has_contains_nan) {
		column_stats.contains_nan = stats.contains_nan ? "true" : "false";
	} else {
		column_stats.contains_nan = "NULL";
	}
	column_stats.extra_stats = "NULL";
	if (stats.extra_stats) {
		stats.extra_stats->Serialize(column_stats);
	}
	return column_stats;
}

DuckLakeFileInfo DuckLakeTransaction::GetNewDataFile(DuckLakeDataFile &file, DuckLakeSnapshot &commit_snapshot,
                                                     TableIndex table_id, optional_idx row_id_start) {
	DuckLakeFileInfo data_file;
	data_file.id = DataFileIndex(commit_snapshot.next_file_id++);
	data_file.table_id = table_id;
	data_file.file_name = file.file_name;
	data_file.row_count = file.row_count;
	data_file.file_size_bytes = file.file_size_bytes;
	data_file.footer_size = file.footer_size;
	data_file.partition_id = file.partition_id;
	data_file.encryption_key = file.encryption_key;
	data_file.row_id_start = row_id_start;
	data_file.mapping_id = file.mapping_id;
	data_file.begin_snapshot = file.begin_snapshot;
	data_file.max_partial_file_snapshot = file.max_partial_file_snapshot;
	// gather the column statistics for this file
	for (auto &column_stats_entry : file.column_stats) {
		auto column_stats =
		    DuckLakeColumnStatsInfo::FromColumnStats(column_stats_entry.first, column_stats_entry.second);
		data_file.column_stats.push_back(std::move(column_stats));
	}
	for (auto &partition_entry : file.partition_values) {
		DuckLakeFilePartitionInfo partition_info;
		partition_info.partition_column_idx = partition_entry.partition_column_idx;
		partition_info.partition_value = partition_entry.partition_value;
		data_file.partition_values.push_back(std::move(partition_info));
	}
	return data_file;
}

struct NewDataInfo {
	vector<DuckLakeFileInfo> new_files;
	vector<DuckLakeInlinedDataInfo> new_inlined_data;
};

NewDataInfo DuckLakeTransaction::GetNewDataFiles(string &batch_query, DuckLakeCommitState &commit_state,
                                                 optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats) {
	NewDataInfo result;
	// get the global table stats
	DuckLakeNewGlobalStats new_globals;
	unique_ptr<DuckLakeStats> dl_stats;
	if (stats) {
		auto &schema = ducklake_catalog.GetSchemaForSnapshot(*this, GetSnapshot());
		dl_stats = ducklake_catalog.ConstructStatsMap(*stats, schema);
	}
	for (auto &entry : table_data_changes) {
		auto table_id = commit_state.GetTableId(entry.first);
		if (table_id.IsTransactionLocal()) {
			throw InternalException("Cannot commit transaction local files - these should have been cleaned up before");
		}
		auto &table_changes = entry.second;
		if (table_changes.new_data_files.empty() && !table_changes.new_inlined_data) {
			// no new data - skip this entry
			continue;
		}
		// get the global table stats
		DuckLakeNewGlobalStats new_globals;
		optional_ptr<DuckLakeTableStats> current_stats;
		if (dl_stats) {
			auto dl_stats_entry = dl_stats->table_stats.find(table_id);
			if (dl_stats_entry != dl_stats->table_stats.end()) {
				current_stats = dl_stats_entry->second.get();
			}
		} else {
			current_stats = ducklake_catalog.GetTableStats(*this, table_id);
		}

		if (current_stats) {
			new_globals.stats = *current_stats;
			new_globals.initialized = true;
		}
		auto &new_stats = new_globals.stats;
		vector<DuckLakeDeleteFile> delete_files;
		for (auto &file : table_changes.new_data_files) {
			// flushed files (with max_partial_file_snapshot) have embedded row_ids, we gotta use the original
			// row_id_start
			auto row_id_start =
			    file.flush_row_id_start.IsValid() ? file.flush_row_id_start.GetIndex() : new_stats.next_row_id;
			auto data_file = GetNewDataFile(file, commit_state.commit_snapshot, table_id, row_id_start);
			for (auto &del_file : file.delete_files) {
				// this transaction-local file already has deletes - write them out
				DuckLakeDeleteFile delete_file = del_file;
				delete_file.data_file_id = data_file.id;
				delete_files.push_back(std::move(delete_file));
			}

			// merge the stats into the new global states
			// files with max_partial_file_snapshot set are flushed from inlined data - don't count them again
			if (!file.max_partial_file_snapshot.IsValid()) {
				new_stats.record_count += file.row_count;
				new_stats.next_row_id += file.row_count;
			}
			new_stats.table_size_bytes += file.file_size_bytes;
			for (auto &entry : file.column_stats) {
				new_stats.MergeStats(entry.first, entry.second);
			}
			result.new_files.push_back(std::move(data_file));
		}
		// write any deletes that were made on top of these transaction-local files
		AddDeletes(table_id, std::move(delete_files));

		if (table_changes.new_inlined_data) {
			auto &inlined_data = *table_changes.new_inlined_data;

			idx_t record_count = inlined_data.data->Count();

			DuckLakeInlinedDataInfo new_inlined_data;
			new_inlined_data.table_id = table_id;
			new_inlined_data.row_id_start = new_stats.next_row_id;

			// merge column stats
			for (auto &entry : inlined_data.column_stats) {
				new_stats.MergeStats(entry.first, entry.second);
			}

			// update global stats
			new_stats.record_count += record_count;
			new_stats.next_row_id += record_count;

			// add the file to the to-be-written inlined data list
			new_inlined_data.data = table_changes.new_inlined_data.get();
			result.new_inlined_data.push_back(new_inlined_data);

			if (table_changes.new_data_files.empty()) {
				// force an increment of file_id to signal a data change if we have only inlined data changes
				commit_state.commit_snapshot.next_file_id++;
			}
		}
		// update the global stats for this table based on the newly written data
		batch_query += UpdateGlobalTableStats(table_id, new_globals);
	}
	return result;
}

vector<DuckLakeDeleteFileInfo>
DuckLakeTransaction::GetNewDeleteFiles(const DuckLakeCommitState &commit_state,
                                       vector<DuckLakeOverwrittenDeleteFile> &overwritten_delete_files) const {
	vector<DuckLakeDeleteFileInfo> result;
	for (auto &entry : table_data_changes) {
		auto table_id = commit_state.GetTableId(entry.first);
		auto &table_changes = entry.second;
		for (auto &file_entry : table_changes.new_delete_files) {
			for (auto &file : file_entry.second) {
				if (file.overwritten_delete_file.delete_file_id.IsValid()) {
					// track the old delete file for deletion from metadata and disk
					overwritten_delete_files.push_back(file.overwritten_delete_file);
				}
				DuckLakeDeleteFileInfo delete_file;
				delete_file.id = DataFileIndex(commit_state.commit_snapshot.next_file_id++);
				delete_file.table_id = table_id;
				delete_file.data_file_id = file.data_file_id;
				delete_file.path = file.file_name;
				delete_file.delete_count = file.delete_count;
				delete_file.file_size_bytes = file.file_size_bytes;
				delete_file.footer_size = file.footer_size;
				delete_file.encryption_key = file.encryption_key;
				delete_file.begin_snapshot = file.begin_snapshot;
				delete_file.max_snapshot = file.max_snapshot;
				result.push_back(std::move(delete_file));
			}
		}
	}
	return result;
}

struct NewNameMapInfo {
	vector<DuckLakeColumnMappingInfo> new_column_mappings;
};

void ConvertNameMapColumn(const DuckLakeNameMapEntry &name_map_entry, MappingIndex map_id, idx_t &column_idx,
                          DuckLakeColumnMappingInfo &result, optional_idx parent_idx = optional_idx()) {
	auto column_id = column_idx++;

	DuckLakeNameMapColumnInfo column_info;
	column_info.column_id = column_id;
	column_info.source_name = name_map_entry.source_name;
	column_info.target_field_id = name_map_entry.target_field_id;
	column_info.hive_partition = name_map_entry.hive_partition;
	column_info.parent_column = parent_idx;
	result.map_columns.push_back(std::move(column_info));

	// recurse into children
	for (auto &child_column : name_map_entry.child_entries) {
		ConvertNameMapColumn(*child_column, map_id, column_idx, result, column_id);
	}
}

NewNameMapInfo DuckLakeTransaction::GetNewNameMaps(DuckLakeCommitState &commit_state) {
	NewNameMapInfo result;
	map<MappingIndex, MappingIndex> remap_mapping_index;
	for (auto &entry : new_name_maps.name_maps) {
		// generate a new mapping id
		auto local_map_id = entry.first;
		auto &mapping = *entry.second;
		MappingIndex new_map_id(commit_state.commit_snapshot.next_file_id++);

		DuckLakeColumnMappingInfo map_info;
		map_info.table_id = commit_state.GetTableId(mapping.table_id);
		map_info.mapping_id = new_map_id;
		map_info.map_type = "map_by_name";
		if (map_info.table_id.IsTransactionLocal()) {
			throw InternalException("table_id should be rewritten to non-transaction local before");
		}

		// iterate over the columns to generate the new name map columns
		idx_t column_idx = 0;
		for (auto &name_map_column : mapping.column_maps) {
			ConvertNameMapColumn(*name_map_column, new_map_id, column_idx, map_info);
		}
		result.new_column_mappings.push_back(std::move(map_info));

		remap_mapping_index[local_map_id] = new_map_id;
	}
	// iterate over the data files to point them towards any new mapping ids
	for (auto &entry : table_data_changes) {
		auto &table_changes = entry.second;
		for (auto &data_file : table_changes.new_data_files) {
			if (!data_file.mapping_id.IsValid()) {
				continue;
			}
			auto entry = remap_mapping_index.find(data_file.mapping_id);
			if (entry != remap_mapping_index.end()) {
				data_file.mapping_id = entry->second;
			}
		}
	}
	return result;
}

vector<DuckLakeDeletedInlinedDataInfo> DuckLakeTransaction::GetNewInlinedDeletes(DuckLakeCommitState &commit_state) {
	vector<DuckLakeDeletedInlinedDataInfo> result;
	for (auto &entry : table_data_changes) {
		auto table_id = commit_state.GetTableId(entry.first);
		auto &table_changes = entry.second;
		for (auto &delete_entry : table_changes.new_inlined_data_deletes) {
			DuckLakeDeletedInlinedDataInfo info;
			info.table_id = table_id;
			info.table_name = delete_entry.first;
			for (auto &row_id : delete_entry.second->rows) {
				info.deleted_row_ids.push_back(row_id);
			}
			result.push_back(std::move(info));
		}
	}
	return result;
}

struct CompactionInformation {
	vector<DuckLakeCompactedFileInfo> compacted_files;
	vector<DuckLakeFileInfo> new_files;
};

string DuckLakeTransaction::CommitChanges(DuckLakeCommitState &commit_state,
                                          TransactionChangeInformation &transaction_changes,
                                          optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats) {
	auto &commit_snapshot = commit_state.commit_snapshot;

	if (ducklake_catalog.IsCommitInfoRequired() && !commit_info.is_commit_info_set) {
		throw InvalidConfigurationException(
		    "Commit Information for the snapshot is required but has not been provided. \n * Provide the information "
		    "with \"CALL ducklake.set_commit_message('author_name', 'commit_message'); \n * Set the required commit "
		    "message to false with \"CALL ducklake.set_option('require_commit_message', False)\" '\"");
	}
	string batch_queries;
	// drop entries
	if (!dropped_tables.empty()) {
		batch_queries += metadata_manager->DropTables(dropped_tables, false);
	}

	if (!renamed_tables.empty()) {
		batch_queries += metadata_manager->DropTables(renamed_tables, true);
	}

	if (!dropped_views.empty()) {
		batch_queries += metadata_manager->DropViews(dropped_views);
	}

	if (!dropped_scalar_macros.empty()) {
		batch_queries += metadata_manager->DropMacros(dropped_scalar_macros);
	}

	if (!dropped_table_macros.empty()) {
		batch_queries += metadata_manager->DropMacros(dropped_table_macros);
	}
	if (!dropped_schemas.empty()) {
		set<SchemaIndex> dropped_schema_ids;
		for (auto &entry : dropped_schemas) {
			dropped_schema_ids.insert(entry.first);
		}
		batch_queries += metadata_manager->DropSchemas(dropped_schema_ids);
	}
	// write new schemas
	vector<DuckLakeSchemaInfo> new_schemas_result;
	if (new_schemas) {
		new_schemas_result = GetNewSchemas(commit_state);
		batch_queries += metadata_manager->WriteNewSchemas(new_schemas_result);
	}

	// write new tables
	vector<DuckLakeTableInfo> new_tables_result;
	vector<DuckLakeTableInfo> new_inlined_data_tables_result;
	if (!new_tables.empty()) {
		auto result = GetNewTables(commit_state, transaction_changes);
		batch_queries += metadata_manager->WriteNewTables(commit_snapshot, result.new_tables, new_schemas_result);
		batch_queries += metadata_manager->WriteNewPartitionKeys(commit_snapshot, result.new_partition_keys);
		batch_queries += metadata_manager->WriteNewViews(result.new_views);
		batch_queries += metadata_manager->WriteNewTags(result.new_tags);
		batch_queries += metadata_manager->WriteNewColumnTags(result.new_column_tags);
		batch_queries += metadata_manager->WriteDroppedColumns(result.dropped_columns);
		batch_queries += metadata_manager->WriteNewColumns(result.new_columns);
		batch_queries += metadata_manager->WriteNewInlinedTables(commit_snapshot, result.new_inlined_data_tables);
		batch_queries += metadata_manager->WriteNewSortKeys(commit_snapshot, result.new_sort_keys);
		new_tables_result = result.new_tables;
		new_inlined_data_tables_result = result.new_inlined_data_tables;
	}

	if (!new_macros.empty()) {
		auto result = GetNewMacros(commit_state, transaction_changes);
		batch_queries += metadata_manager->WriteNewMacros(result.new_macros);
	}

	// write new name maps
	if (!new_name_maps.name_maps.empty()) {
		auto result = GetNewNameMaps(commit_state);
		batch_queries += metadata_manager->WriteNewColumnMappings(result.new_column_mappings);
	}

	// write new data / data files
	if (!table_data_changes.empty()) {
		auto result = GetNewDataFiles(batch_queries, commit_state, stats);
		batch_queries += metadata_manager->WriteNewDataFiles(result.new_files, new_tables_result, new_schemas_result);
		batch_queries += metadata_manager->WriteNewInlinedData(commit_snapshot, result.new_inlined_data,
		                                                       new_tables_result, new_inlined_data_tables_result);
	}

	// drop data files
	if (!dropped_files.empty()) {
		set<DataFileIndex> dropped_indexes;
		for (auto &entry : dropped_files) {
			dropped_indexes.insert(entry.second);
		}
		batch_queries += metadata_manager->DropDataFiles(dropped_indexes);
	}

	if (!table_data_changes.empty()) {
		// write new delete files
		vector<DuckLakeOverwrittenDeleteFile> overwritten_delete_files;
		auto file_list = GetNewDeleteFiles(commit_state, overwritten_delete_files);
		batch_queries += metadata_manager->DeleteOverwrittenDeleteFiles(overwritten_delete_files);
		batch_queries += metadata_manager->WriteNewDeleteFiles(file_list, new_tables_result, new_schemas_result);

		// write new inlined deletes (for inlined data tables)
		auto inlined_deletes = GetNewInlinedDeletes(commit_state);
		batch_queries += metadata_manager->WriteNewInlinedDeletes(inlined_deletes);

		// write new inlined file deletes (for parquet files)
		auto inlined_file_deletes = GetNewInlinedFileDeletes(commit_state);
		batch_queries += metadata_manager->WriteNewInlinedFileDeletes(commit_snapshot, inlined_file_deletes);

		// write compactions
		auto compaction_merge_adjacent_changes =
		    GetCompactionChanges(commit_snapshot, CompactionType::MERGE_ADJACENT_TABLES);
		batch_queries += metadata_manager->WriteCompactions(compaction_merge_adjacent_changes.compacted_files,
		                                                    CompactionType::MERGE_ADJACENT_TABLES);
		batch_queries += metadata_manager->WriteNewDataFiles(compaction_merge_adjacent_changes.new_files,
		                                                     new_tables_result, new_schemas_result);

		auto compaction_rewrite_delete_changes = GetCompactionChanges(commit_snapshot, CompactionType::REWRITE_DELETES);
		batch_queries += metadata_manager->WriteNewDataFiles(compaction_rewrite_delete_changes.new_files,
		                                                     new_tables_result, new_schemas_result);
		batch_queries += metadata_manager->WriteCompactions(compaction_rewrite_delete_changes.compacted_files,
		                                                    CompactionType::REWRITE_DELETES);
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
	batch_queries += metadata_manager->InsertNewSchema(commit_snapshot, tables_with_schema_changes);

	return batch_queries;
}

CompactionInformation DuckLakeTransaction::GetCompactionChanges(DuckLakeSnapshot &commit_snapshot,
                                                                CompactionType type) {
	CompactionInformation result;
	for (auto &entry : table_data_changes) {
		auto table_id = entry.first;
		auto &table_changes = entry.second;
		for (auto &compaction : table_changes.compactions) {
			if (type != compaction.type) {
				continue;
			}
			auto new_file = GetNewDataFile(compaction.written_file, commit_snapshot, table_id, compaction.row_id_start);
			switch (type) {
			case CompactionType::REWRITE_DELETES:
				new_file.begin_snapshot = commit_snapshot.snapshot_id;
				break;
			case CompactionType::MERGE_ADJACENT_TABLES: {
				// For MERGE_ADJACENT_TABLES, track the max partial snapshot across all source files
				optional_idx merged_max_partial_snapshot;
				idx_t first_begin_snapshot = compaction.source_files[0].file.begin_snapshot;
				for (auto &compacted_file : compaction.source_files) {
					idx_t file_max_snapshot = compacted_file.max_partial_file_snapshot.IsValid()
					                              ? compacted_file.max_partial_file_snapshot.GetIndex()
					                              : compacted_file.file.begin_snapshot;
					if (!merged_max_partial_snapshot.IsValid() ||
					    file_max_snapshot > merged_max_partial_snapshot.GetIndex()) {
						merged_max_partial_snapshot = file_max_snapshot;
					}
				}
				// Use the first source file's begin_snapshot for proper time travel support
				new_file.begin_snapshot = first_begin_snapshot;
				if (compaction.source_files.size() > 1) {
					new_file.max_partial_file_snapshot = merged_max_partial_snapshot;
				}
				break;
			}
			default:
				throw InternalException("DuckLakeTransaction::GetCompactionChanges Compaction type is invalid");
			}

			idx_t row_id_limit = 0;
			for (auto &compacted_file : compaction.source_files) {
				row_id_limit += compacted_file.file.row_count;
				if (!compacted_file.delete_files.empty()) {
					row_id_limit -= compacted_file.delete_files.back().row_count;
				}
				DuckLakeCompactedFileInfo file_info;
				file_info.path = compacted_file.file.data.path;
				file_info.source_id = compacted_file.file.id;
				file_info.new_id = new_file.id;

				if (!compacted_file.delete_files.empty()) {
					file_info.delete_file_path = compacted_file.delete_files.back().data.path;
					file_info.delete_file_id = compacted_file.delete_files.back().delete_file_id;
					file_info.start_snapshot = compacted_file.file.begin_snapshot;
					file_info.table_index = entry.first;
					file_info.delete_file_start_snapshot = commit_snapshot.snapshot_id;
					file_info.delete_file_end_snapshot = compacted_file.delete_files.back().end_snapshot;
				}
				if (row_id_limit > new_file.row_count) {
					throw InternalException("Compaction error - row id limit is larger than the row count of the file");
				}
				result.compacted_files.push_back(std::move(file_info));
			}
			result.new_files.push_back(std::move(new_file));
		}
	}
	return result;
}

bool RetryOnError(const string &original_message) {
	auto message = StringUtil::Lower(original_message);
	// retry on primary key errors
	if (StringUtil::Contains(message, "primary key") || StringUtil::Contains(message, "unique")) {
		return true;
	}
	// retry on conflicts
	if (StringUtil::Contains(message, "conflict")) {
		return true;
	}
	// retry on concurrent access
	if (StringUtil::Contains(message, "concurrent")) {
		return true;
	}
	return false;
}

void DuckLakeTransaction::FlushChanges() {
	if (!ChangesMade()) {
		// read-only transactions don't need to do anything
		return;
	}
	idx_t max_retry_count = 10;
	idx_t retry_wait_ms = 100;
	double retry_backoff = 1.5;
	Value setting_val;
	auto context_ref = context.lock();
	if (context_ref->TryGetCurrentSetting("ducklake_max_retry_count", setting_val)) {
		max_retry_count = setting_val.GetValue<idx_t>();
	}
	if (context_ref->TryGetCurrentSetting("ducklake_retry_wait_ms", setting_val)) {
		retry_wait_ms = setting_val.GetValue<idx_t>();
	}
	if (context_ref->TryGetCurrentSetting("ducklake_retry_backoff", setting_val)) {
		retry_backoff = setting_val.GetValue<double>();
	}

	auto transaction_snapshot = GetSnapshot();
	auto transaction_changes = GetTransactionChanges();
	SnapshotAndStats commit_stats_snapshot;
	auto &commit_snapshot = commit_stats_snapshot.snapshot;
	optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats;
	for (idx_t i = 0; i < max_retry_count + 1; i++) {
		bool can_retry;
		try {
			can_retry = false;
			if (i > 0) {
				// we failed our first commit due to another transaction committing
				// retry - but first check for conflicts
				commit_stats_snapshot = CheckForConflicts(transaction_snapshot, transaction_changes);
				stats = &commit_stats_snapshot.stats;
			} else {
				commit_stats_snapshot.snapshot = GetSnapshot();
			}
			commit_snapshot.snapshot_id++;
			if (SchemaChangesMade()) {
				// we changed the schema - need to get a new schema version
				commit_snapshot.schema_version++;
			}
			can_retry = true;
			DuckLakeCommitState commit_state(commit_snapshot);
			// write the new snapshot
			string batch_queries = metadata_manager->InsertSnapshot();
			batch_queries += CommitChanges(commit_state, transaction_changes, stats);

			batch_queries += WriteSnapshotChanges(commit_state, transaction_changes);
			auto res = metadata_manager->Execute(commit_snapshot, batch_queries);
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to flush changes into DuckLake: ");
			}
			connection->Commit();
			catalog_version = commit_snapshot.schema_version;

			// finished writing
			break;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			// rollback if there is an active transaction
			auto has_active_transaction = connection->context->transaction.HasActiveTransaction();
			if (has_active_transaction) {
				connection->Rollback();
			}
			bool retry_on_error = RetryOnError(error.Message());
			bool finished_retrying = i + 1 >= max_retry_count;
			if (!can_retry || !retry_on_error || finished_retrying) {
				// we abort after the max retry count
				CleanupFiles();
				// Add additional information on the number of retries and suggest to increase it
				std::ostringstream error_message;
				error_message << "Failed to commit DuckLake transaction." << '\n';
				if (finished_retrying) {
					error_message << "Exceeded the maximum retry count of " << max_retry_count
					              << " set by the ducklake_max_retry_count setting." << '\n'
					              << ". Consider increasing the value with: e.g., \"SET ducklake_max_retry_count = "
					              << max_retry_count * 10 << ";\"" << '\n';
				}
				error.Throw(error_message.str());
			}

#ifndef DUCKDB_NO_THREADS
			RandomEngine random;
			// random multiplier between 0.5 - 1.0
			double random_multiplier = (random.NextRandom() + 1.0) / 2.0;
			uint64_t sleep_amount =
			    (uint64_t)((double)retry_wait_ms * random_multiplier * pow(retry_backoff, static_cast<double>(i)));
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
#endif

			// retry the transaction (with a new snapshot id)
			connection->BeginTransaction();
			snapshot.reset();
		}
	}
	// If we got here, this snapshot was successful
	ducklake_catalog.SetCommittedSnapshotId(commit_snapshot.snapshot_id);
}

void DuckLakeTransaction::SetConfigOption(const DuckLakeConfigOption &option) {
	// write the config option to the metadata
	metadata_manager->SetConfigOption(option);
	// set the option in the catalog
	ducklake_catalog.SetConfigOption(option);
}

void DuckLakeTransaction::SetCommitMessage(const DuckLakeSnapshotCommit &option) {
	commit_info = option;
}

void DuckLakeTransaction::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	auto &metadata_manager = GetMetadataManager();
	metadata_manager.DeleteSnapshots(snapshots);
}

void DuckLakeTransaction::DeleteInlinedData(const DuckLakeInlinedTableInfo &inlined_table) {
	auto &metadata_manager = GetMetadataManager();
	metadata_manager.DeleteInlinedData(inlined_table);
}

unique_ptr<QueryResult> DuckLakeTransaction::Query(string query) {
	auto &connection = GetConnection();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", catalog_identifier + "." + schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);
	return connection.Query(query);
}

unique_ptr<QueryResult> DuckLakeTransaction::Query(DuckLakeSnapshot snapshot, string query) {
	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	return Query(std::move(query));
}

string DuckLakeTransaction::GetDefaultSchemaName() {
	auto &metadata_context = *connection->context;
	auto &db_manager = DatabaseManager::Get(metadata_context);
	auto metadb = db_manager.GetDatabase(metadata_context, ducklake_catalog.MetadataDatabaseName());
	return metadb->GetCatalog().GetDefaultSchema();
}

DuckLakeSnapshot DuckLakeTransaction::GetSnapshot() {
	auto catalog_snapshot = ducklake_catalog.CatalogSnapshot();
	if (catalog_snapshot) {
		// the catalog was opened at a specific snapshot - load that snapshot
		return GetSnapshot(catalog_snapshot);
	}
	lock_guard<mutex> guard(snapshot_lock);
	if (!snapshot) {
		// no snapshot loaded yet for this transaction - load it
		snapshot = metadata_manager->GetSnapshot();
	}
	return *snapshot;
}

DuckLakeSnapshot DuckLakeTransaction::GetSnapshot(optional_ptr<BoundAtClause> at_clause, SnapshotBound bound) {
	if (!at_clause) {
		// no AT-clause - get the latest snapshot
		return GetSnapshot();
	}
	// construct a struct value from the AT clause in the form of {"unit": value} (e.g. {"version": 2}
	// this is used as a caching key for the snapshot
	child_list_t<Value> values;
	values.push_back(make_pair(at_clause->Unit(), at_clause->GetValue()));
	auto snapshot_value = Value::STRUCT(std::move(values));

	lock_guard<mutex> guard(snapshot_lock);
	auto entry = snapshot_cache.find(snapshot_value);
	if (entry != snapshot_cache.end()) {
		// we already found this snapshot - return it
		return entry->second;
	}
	// find the snapshot and cache it
	auto result_snapshot = *metadata_manager->GetSnapshot(*at_clause, bound);
	snapshot_cache.insert(make_pair(std::move(snapshot_value), result_snapshot));
	return result_snapshot;
}

idx_t DuckLakeTransaction::GetLocalCatalogId() {
	return local_catalog_id++;
}

bool DuckLakeTransaction::HasTransactionLocalInserts(TableIndex table_id) const {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return false;
	}
	auto &table_changes = entry->second;
	return !table_changes.new_data_files.empty() || table_changes.new_inlined_data;
}

bool DuckLakeTransaction::HasTransactionInlinedData(TableIndex table_id) const {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return false;
	}
	auto &table_changes = entry->second;
	return table_changes.new_inlined_data != nullptr;
}

vector<DuckLakeDataFile> DuckLakeTransaction::GetTransactionLocalFiles(TableIndex table_id) {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return vector<DuckLakeDataFile>();
	}
	return entry->second.new_data_files;
}

shared_ptr<DuckLakeInlinedData> DuckLakeTransaction::GetTransactionLocalInlinedData(TableIndex table_id) {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return nullptr;
	}
	auto &table_changes = entry->second;
	if (!table_changes.new_inlined_data) {
		return nullptr;
	}
	auto &local_changes = *table_changes.new_inlined_data;
	auto context_ref = context.lock();
	auto result = make_shared_ptr<DuckLakeInlinedData>();
	result->data = make_uniq<ColumnDataCollection>(*context_ref, local_changes.data->Types());
	for (auto &chunk : local_changes.data->Chunks()) {
		result->data->Append(chunk);
	}
	return result;
}

void DuckLakeTransaction::DropTransactionLocalFile(TableIndex table_id, const string &path) {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		throw InternalException(
		    "DropTransactionLocalFile called for a table for which no transaction-local files exist");
	}
	auto &table_changes = entry->second;
	auto &table_files = table_changes.new_data_files;
	auto context_ref = context.lock();
	auto &fs = FileSystem::GetFileSystem(*context_ref);
	for (idx_t i = 0; i < table_files.size(); i++) {
		auto &file = table_files[i];
		if (file.file_name == path) {
			for (auto &del_file : file.delete_files) {
				fs.RemoveFile(del_file.file_name);
			}
			file.delete_files.clear();
			// found the file - delete it from the table list and from disk
			table_files.erase_at(i);
			fs.RemoveFile(path);
			if (table_changes.IsEmpty()) {
				// no more files remaining
				table_data_changes.erase(entry);
			}
			return;
		}
	}
	throw InternalException("Failed to find matching transaction-local file for DropTransactionLocalFile");
}

void DuckLakeTransaction::AppendFiles(TableIndex table_id, vector<DuckLakeDataFile> files) {
	if (files.empty()) {
		return;
	}
	lock_guard<mutex> guard(table_data_changes_lock);
	auto &table_changes = table_data_changes[table_id];
	for (auto &file : files) {
		table_changes.new_data_files.push_back(std::move(file));
	}
}

void DuckLakeTransaction::AppendInlinedData(TableIndex table_id, unique_ptr<DuckLakeInlinedData> new_data) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto &table_changes = table_data_changes[table_id];
	if (table_changes.new_inlined_data) {
		// already exists - append
		auto &existing_data = *table_changes.new_inlined_data;
		auto &existing_types = existing_data.data->Types();
		auto &new_types = new_data->data->Types();
		// check if types changed (e.g. due to ALTER COLUMN TYPE)
		if (existing_types != new_types) {
			// if types differ we gotta add a cast.
			auto context_ref = context.lock();
			auto casted_data = make_uniq<ColumnDataCollection>(*context_ref, new_types);
			ColumnDataAppendState append_state;
			casted_data->InitializeAppend(append_state);
			for (auto &chunk : existing_data.data->Chunks()) {
				DataChunk casted_chunk;
				casted_chunk.Initialize(*context_ref, new_types);
				for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
					if (existing_types[col_idx] != new_types[col_idx]) {
						VectorOperations::Cast(*context_ref, chunk.data[col_idx], casted_chunk.data[col_idx],
						                       chunk.size());
					} else {
						casted_chunk.data[col_idx].Reference(chunk.data[col_idx]);
					}
				}
				casted_chunk.SetCardinality(chunk.size());
				casted_data->Append(append_state, casted_chunk);
			}
			existing_data.data = std::move(casted_data);
		}
		ColumnDataAppendState append_state;
		existing_data.data->InitializeAppend(append_state);
		for (auto &chunk : new_data->data->Chunks()) {
			existing_data.data->Append(chunk);
		}
		for (auto &entry : new_data->column_stats) {
			auto stats_entry = existing_data.column_stats.find(entry.first);
			if (stats_entry == existing_data.column_stats.end()) {
				throw InternalException("Missing stats when merging inlined data");
			}
			stats_entry->second.MergeStats(entry.second);
		}
	} else {
		// does not exist yet - set it
		table_changes.new_inlined_data = std::move(new_data);
	}
}

void DuckLakeTransaction::AddNewInlinedDeletes(TableIndex table_id, const string &table_name, set<idx_t> new_deletes) {
	if (new_deletes.empty()) {
		return;
	}
	lock_guard<mutex> guard(table_data_changes_lock);
	auto &table_changes = table_data_changes[table_id];
	auto &table_deletes = table_changes.new_inlined_data_deletes;
	auto entry = table_deletes.find(table_name);
	if (entry != table_deletes.end()) {
		// merge deletes
		auto &existing_rows = entry->second->rows;
		for (auto &row_idx : new_deletes) {
			existing_rows.insert(row_idx);
		}
	} else {
		auto new_data = make_uniq<DuckLakeInlinedDataDeletes>();
		new_data->rows = std::move(new_deletes);
		table_deletes.emplace(table_name, std::move(new_data));
	}
}

void DuckLakeTransaction::DeleteFromLocalInlinedData(TableIndex table_id, set<idx_t> new_deletes) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		throw InternalException("DeleteFromLocalInlinedData called but no transaction-local data exists for table");
	}
	auto &table_changes = entry->second;
	auto &existing = *table_changes.new_inlined_data->data;
	// construct a new collection from the existing data minus the deletes
	auto context_ref = context.lock();
	auto new_data = make_uniq<ColumnDataCollection>(*context_ref, existing.Types());

	idx_t base_row_id = 0;
	ColumnDataAppendState append_state;
	new_data->InitializeAppend(append_state);
	for (auto &chunk : existing.Chunks()) {
		// slice out non-deleted rows
		SelectionVector sel(chunk.size());
		idx_t selected_rows = 0;

		for (idx_t r = 0; r < chunk.size(); r++) {
			auto row_id = base_row_id + r;
			if (new_deletes.find(row_id) != new_deletes.end()) {
				// deleted - skip
				continue;
			}
			sel.set_index(selected_rows++, r);
		}
		base_row_id += chunk.size();
		if (selected_rows == 0) {
			continue;
		}
		chunk.Slice(sel, selected_rows);
		new_data->Append(append_state, chunk);
	}

	// override the existing collection
	table_changes.new_inlined_data->data = std::move(new_data);
}

void DuckLakeTransaction::AddColumnToLocalInlinedData(TableIndex table_id, const LogicalType &new_column_type,
                                                      FieldIndex new_field_index, const Value &default_value) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		throw InternalException("AddColumnToLocalInlinedData called but no transaction-local data exists");
	}
	auto &table_changes = entry->second;
	if (!table_changes.new_inlined_data) {
		throw InternalException("AddColumnToLocalInlinedData called but no inlined data exists");
	}

	auto &existing = *table_changes.new_inlined_data->data;

	// New types: existing + new column
	auto new_types = existing.Types();
	new_types.push_back(new_column_type);

	auto context_ref = context.lock();
	auto new_data = make_uniq<ColumnDataCollection>(*context_ref, new_types);

	ColumnDataAppendState append_state;
	new_data->InitializeAppend(append_state);

	bool has_default = !default_value.IsNull();

	for (auto &chunk : existing.Chunks()) {
		DataChunk new_chunk;
		new_chunk.Initialize(*context_ref, new_types);

		// Copy existing columns
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			new_chunk.data[col_idx].Reference(chunk.data[col_idx]);
		}

		// New column: use default value or NULL
		auto &new_col_vector = new_chunk.data[chunk.ColumnCount()];
		if (has_default) {
			new_col_vector.Reference(default_value);
		} else {
			new_col_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(new_col_vector, true);
		}

		new_chunk.SetCardinality(chunk.size());
		new_data->Append(append_state, new_chunk);
	}

	// Add stats for new column
	idx_t total_rows = existing.Count();
	DuckLakeColumnStats new_col_stats(new_column_type);
	new_col_stats.num_values = total_rows;
	new_col_stats.has_num_values = true;
	if (has_default) {
		new_col_stats.null_count = 0;
		new_col_stats.has_null_count = true;
		new_col_stats.any_valid = true;
	} else {
		new_col_stats.null_count = total_rows;
		new_col_stats.has_null_count = true;
		new_col_stats.any_valid = false;
	}

	table_changes.new_inlined_data->column_stats.emplace(new_field_index, std::move(new_col_stats));
	table_changes.new_inlined_data->data = std::move(new_data);
}

static void RemoveFieldStats(map<FieldIndex, DuckLakeColumnStats> &column_stats, const DuckLakeFieldId &field_id) {
	column_stats.erase(field_id.GetFieldIndex());
	for (auto &child_id : field_id.Children()) {
		RemoveFieldStats(column_stats, *child_id);
	}
}

void DuckLakeTransaction::RemoveColumnFromLocalInlinedData(TableIndex table_id, LogicalIndex removed_column_index,
                                                           const DuckLakeFieldId &field_id) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		throw InternalException("RemoveColumnFromLocalInlinedData called but no transaction-local data exists");
	}
	auto &table_changes = entry->second;
	if (!table_changes.new_inlined_data) {
		throw InternalException("RemoveColumnFromLocalInlinedData called but no inlined data exists");
	}

	auto &existing = *table_changes.new_inlined_data->data;

	// New types: existing minus the removed column
	vector<LogicalType> new_types;
	for (idx_t col_idx = 0; col_idx < existing.Types().size(); col_idx++) {
		if (col_idx == removed_column_index.index) {
			continue;
		}
		new_types.push_back(existing.Types()[col_idx]);
	}

	auto context_ref = context.lock();
	auto new_data = make_uniq<ColumnDataCollection>(*context_ref, new_types);

	ColumnDataAppendState append_state;
	new_data->InitializeAppend(append_state);

	for (auto &chunk : existing.Chunks()) {
		DataChunk new_chunk;
		new_chunk.Initialize(*context_ref, new_types);

		idx_t new_col_idx = 0;
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			if (col_idx == removed_column_index.index) {
				continue;
			}
			new_chunk.data[new_col_idx].Reference(chunk.data[col_idx]);
			new_col_idx++;
		}

		new_chunk.SetCardinality(chunk.size());
		new_data->Append(append_state, new_chunk);
	}

	// Remove stats for the dropped field and all its children
	RemoveFieldStats(table_changes.new_inlined_data->column_stats, field_id);

	table_changes.new_inlined_data->data = std::move(new_data);
}

optional_ptr<DuckLakeInlinedDataDeletes> DuckLakeTransaction::GetInlinedDeletes(TableIndex table_id,
                                                                                const string &table_name) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return nullptr;
	}
	auto &table_changes = entry->second;
	auto delete_entry = table_changes.new_inlined_data_deletes.find(table_name);
	if (delete_entry == table_changes.new_inlined_data_deletes.end()) {
		return nullptr;
	}
	return delete_entry->second.get();
}

void DuckLakeTransaction::AddNewInlinedFileDeletes(TableIndex table_id, idx_t file_id, set<idx_t> new_deletes) {
	if (new_deletes.empty()) {
		return;
	}
	lock_guard<mutex> guard(table_data_changes_lock);
	auto &table_changes = table_data_changes[table_id];
	if (!table_changes.new_inlined_file_deletes) {
		table_changes.new_inlined_file_deletes = make_uniq<DuckLakeInlinedFileDeletes>();
	}
	auto &file_deletes = table_changes.new_inlined_file_deletes->file_deletes[file_id];
	for (auto &row_id : new_deletes) {
		file_deletes.insert(row_id);
	}
}

vector<DuckLakeInlinedFileDeletionInfo>
DuckLakeTransaction::GetNewInlinedFileDeletes(DuckLakeCommitState &commit_state) {
	vector<DuckLakeInlinedFileDeletionInfo> result;
	for (auto &entry : table_data_changes) {
		auto table_id = commit_state.GetTableId(entry.first);
		auto &table_changes = entry.second;
		if (!table_changes.new_inlined_file_deletes) {
			continue;
		}
		if (table_changes.new_inlined_file_deletes->file_deletes.empty()) {
			continue;
		}
		DuckLakeInlinedFileDeletionInfo info;
		info.table_id = table_id;
		info.file_deletions.file_deletes = std::move(table_changes.new_inlined_file_deletes->file_deletes);
		result.push_back(std::move(info));
	}
	return result;
}

void DuckLakeTransaction::AddDeletes(TableIndex table_id, vector<DuckLakeDeleteFile> files) {
	if (files.empty()) {
		return;
	}
	lock_guard<mutex> guard(table_data_changes_lock);
	auto &table_changes = table_data_changes[table_id];
	auto &table_delete_map = table_changes.new_delete_files;
	for (auto &file : files) {
		auto &data_file_path = file.data_file_path;
		if (data_file_path.empty()) {
			throw InternalException("Data file path needs to be set in delete");
		}
		if (file.source == DeleteFileSource::FLUSH) {
			// If we have a snapshot, this is a flushed delete file, we add it to the rooster
			table_delete_map[data_file_path].push_back(std::move(file));
		} else {
			// If not is a regular deletion
			auto existing_entry = table_delete_map.find(data_file_path);
			if (existing_entry != table_delete_map.end() && !existing_entry->second.empty()) {
				// If a file already exists we remove it
				auto context_ref = context.lock();
				auto &fs = FileSystem::GetFileSystem(*context_ref);
				for (auto &old_file : existing_entry->second) {
					fs.RemoveFile(old_file.file_name);
				}
				existing_entry->second.clear();
			}
			// We add the new file in
			table_delete_map[data_file_path].push_back(std::move(file));
		}
	}
}

void DuckLakeTransaction::AddCompaction(TableIndex table_id, DuckLakeCompactionEntry entry) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto &table_changes = table_data_changes[table_id];
	table_changes.compactions.push_back(std::move(entry));
}

bool DuckLakeTransaction::HasLocalDeletes(TableIndex table_id) {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return false;
	}
	return !entry->second.new_delete_files.empty();
}

bool DuckLakeTransaction::HasAnyLocalChanges(TableIndex table_id) {
	auto entry = table_data_changes.find(table_id);
	if (entry != table_data_changes.end() && !entry->second.IsEmpty()) {
		return true;
	}
	return tables_deleted_from.find(table_id) != tables_deleted_from.end();
}

void DuckLakeTransaction::GetLocalDeleteForFile(TableIndex table_id, const string &path, DuckLakeFileData &result) {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return;
	}
	auto &table_changes = entry->second;
	auto file_entry = table_changes.new_delete_files.find(path);
	if (file_entry == table_changes.new_delete_files.end() || file_entry->second.empty()) {
		return;
	}
	auto &delete_file = file_entry->second.back();
	result.path = delete_file.file_name;
	result.file_size_bytes = delete_file.file_size_bytes;
	result.footer_size = delete_file.footer_size;
	result.encryption_key = delete_file.encryption_key;
}

bool DuckLakeTransaction::HasLocalInlinedFileDeletes(TableIndex table_id) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return false;
	}
	auto &table_changes = entry->second;
	if (!table_changes.new_inlined_file_deletes) {
		return false;
	}
	return !table_changes.new_inlined_file_deletes->file_deletes.empty();
}

void DuckLakeTransaction::GetLocalInlinedFileDeletesForFile(TableIndex table_id, idx_t file_id, set<idx_t> &result) {
	lock_guard<mutex> guard(table_data_changes_lock);
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		return;
	}
	auto &table_changes = entry->second;
	if (!table_changes.new_inlined_file_deletes) {
		return;
	}
	auto file_entry = table_changes.new_inlined_file_deletes->file_deletes.find(file_id);
	if (file_entry == table_changes.new_inlined_file_deletes->file_deletes.end()) {
		return;
	}
	// Merge the inlined deletes into the result set
	for (auto &row_id : file_entry->second) {
		result.insert(row_id);
	}
}

void DuckLakeTransaction::TransactionLocalDelete(TableIndex table_id, const string &data_file_path,
                                                 DuckLakeDeleteFile delete_file) {
	auto entry = table_data_changes.find(table_id);
	if (entry == table_data_changes.end()) {
		throw InternalException(
		    "Transaction local delete called for table which does not have transaction local insertions");
	}
	auto &table_changes = entry->second;
	for (auto &file : table_changes.new_data_files) {
		if (file.file_name == data_file_path) {
			if (!file.delete_files.empty()) {
				auto context_ref = context.lock();
				auto &fs = FileSystem::GetFileSystem(*context_ref);
				vector<string> files_to_delete;
				files_to_delete.reserve(file.delete_files.size());
				for (auto &old_file : file.delete_files) {
					files_to_delete.push_back(old_file.file_name);
				}
				fs.RemoveFiles(files_to_delete);
				file.delete_files.clear();
			}
			file.delete_files.push_back(std::move(delete_file));
			return;
		}
	}
	throw InternalException("Failed to find matching transaction-local file for written delete file");
}

DuckLakeTransaction &DuckLakeTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<DuckLakeTransaction>();
}

void DuckLakeTransaction::CreateEntry(unique_ptr<CatalogEntry> entry) {
	catalog_version = ducklake_catalog.GetNewUncommittedCatalogVersion();
	auto &set = GetOrCreateTransactionLocalEntries(*entry);
	set.CreateEntry(std::move(entry));
}

void DuckLakeTransaction::DropSchema(DuckLakeSchemaEntry &schema) {
	auto schema_id = schema.GetSchemaId();
	if (schema_id.IsTransactionLocal()) {
		// schema is transaction-local - drop it from the transaction local changes
		if (!new_schemas) {
			throw InternalException("Dropping a transaction local table that does not exist?");
		}
		new_schemas->DropEntry(schema.name);
		if (new_schemas->GetEntries().empty()) {
			// we have dropped all schemas created in this transaction - clear it
			new_schemas.reset();
		}
	} else {
		dropped_schemas.insert(make_pair(schema.GetSchemaId(), reference<DuckLakeSchemaEntry>(schema)));
	}
}

void DuckLakeTransaction::DropTable(DuckLakeTableEntry &table) {
	catalog_version = ducklake_catalog.GetNewUncommittedCatalogVersion();
	if (table.IsTransactionLocal()) {
		// table is transaction-local - drop it from the transaction local changes
		auto schema_entry = new_tables.find(table.ParentSchema().name);
		if (schema_entry == new_tables.end()) {
			throw InternalException("Dropping a transaction local table that does not exist?");
		}
		auto table_id = table.GetTableId();
		schema_entry->second->DropEntry(table.name);
		// if we have written any files for this table - clean them up
		auto table_entry = table_data_changes.find(table_id);
		if (table_entry != table_data_changes.end()) {
			auto &table_changes = table_entry->second;
			auto context_ref = context.lock();
			auto &fs = FileSystem::GetFileSystem(*context_ref);
			for (auto &file : table_changes.new_data_files) {
				fs.RemoveFile(file.file_name);
			}
			table_data_changes.erase(table_entry);
		}
		new_tables.erase(schema_entry);
	} else {
		auto table_id = table.GetTableId();
		dropped_tables.insert(table_id);
	}
}

void DuckLakeTransaction::DropView(DuckLakeViewEntry &view) {
	if (view.IsTransactionLocal()) {
		// table is transaction-local - drop it from the transaction local changes
		auto schema_entry = new_tables.find(view.ParentSchema().name);
		if (schema_entry == new_tables.end()) {
			throw InternalException("Dropping a transaction local view that does not exist?");
		}
		schema_entry->second->DropEntry(view.name);
		new_tables.erase(schema_entry);
	} else {
		auto view_id = view.GetViewId();
		dropped_views.insert(view_id);
	}
}

void DuckLakeTransaction::DropScalarMacro(DuckLakeScalarMacroEntry &macro) {
	dropped_scalar_macros.insert(macro.GetIndex());
}

void DuckLakeTransaction::DropTableMacro(DuckLakeTableMacroEntry &macro) {
	dropped_table_macros.insert(macro.GetIndex());
}

void DuckLakeTransaction::DropFile(TableIndex table_id, DataFileIndex data_file_id, string path) {
	tables_deleted_from.insert(table_id);
	dropped_files.emplace(std::move(path), data_file_id);
}

bool DuckLakeTransaction::HasDroppedFiles() const {
	return !dropped_files.empty();
}

bool DuckLakeTransaction::FileIsDropped(const string &path) const {
	return dropped_files.find(path) != dropped_files.end();
}

void DuckLakeTransaction::DropEntry(CatalogEntry &entry) {
	catalog_version = ducklake_catalog.GetNewUncommittedCatalogVersion();
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY:
		DropTable(entry.Cast<DuckLakeTableEntry>());
		break;
	case CatalogType::VIEW_ENTRY:
		DropView(entry.Cast<DuckLakeViewEntry>());
		break;
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto local_entry = GetTransactionLocalEntry(entry.type, entry.ParentSchema().name, entry.name);
		if (local_entry) {
			auto schema_entry = new_macros.find(entry.ParentSchema().name);
			if (schema_entry == new_macros.end()) {
				throw InternalException("Dropping a transaction local macro that does not exist.");
			}
			schema_entry->second->DropEntry(entry.name);
		} else if (entry.type == CatalogType::MACRO_ENTRY) {
			DropScalarMacro(entry.Cast<DuckLakeScalarMacroEntry>());
		} else {
			DropTableMacro(entry.Cast<DuckLakeTableMacroEntry>());
		}
		break;
	}
	case CatalogType::SCHEMA_ENTRY:
		DropSchema(entry.Cast<DuckLakeSchemaEntry>());
		break;
	default:
		throw InternalException("Unsupported type for drop");
	}
}

bool DuckLakeTransaction::IsDeleted(CatalogEntry &entry) {
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table_entry = entry.Cast<DuckLakeTableEntry>();
		return dropped_tables.find(table_entry.GetTableId()) != dropped_tables.end();
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view_entry = entry.Cast<DuckLakeViewEntry>();
		return dropped_views.find(view_entry.GetViewId()) != dropped_views.end();
	}
	case CatalogType::MACRO_ENTRY: {
		auto &macro_entry = entry.Cast<DuckLakeScalarMacroEntry>();
		return dropped_scalar_macros.find(macro_entry.GetIndex()) != dropped_scalar_macros.end();
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &macro_entry = entry.Cast<DuckLakeTableMacroEntry>();
		return dropped_table_macros.find(macro_entry.GetIndex()) != dropped_table_macros.end();
	}
	case CatalogType::SCHEMA_ENTRY: {
		auto &schema_entry = entry.Cast<DuckLakeSchemaEntry>();
		return dropped_schemas.find(schema_entry.GetSchemaId()) != dropped_schemas.end();
	}
	default:
		throw InternalException("Catalog type not supported for IsDeleted");
	}
}

bool DuckLakeTransaction::IsRenamed(CatalogEntry &entry) {
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table_entry = entry.Cast<DuckLakeTableEntry>();
		return renamed_tables.find(table_entry.GetTableId()) != renamed_tables.end();
	}
	case CatalogType::VIEW_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::SCHEMA_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY: {
		return false;
	}
	default:
		throw InternalException("Catalog type not supported for IsRenamed");
	}
}

void DuckLakeTransaction::AlterEntry(CatalogEntry &entry, unique_ptr<CatalogEntry> new_entry) {
	catalog_version = ducklake_catalog.GetNewUncommittedCatalogVersion();
	if (!new_entry) {
		return;
	}
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY:
		AlterEntryInternal(entry.Cast<DuckLakeTableEntry>(), std::move(new_entry));
		break;
	case CatalogType::VIEW_ENTRY:
		AlterEntryInternal(entry.Cast<DuckLakeViewEntry>(), std::move(new_entry));
		break;
	default:
		throw NotImplementedException("Unsupported catalog type for AlterEntry");
	}
}

void DuckLakeTransaction::AlterEntryInternal(DuckLakeTableEntry &table, unique_ptr<CatalogEntry> new_entry) {
	auto &new_table = new_entry->Cast<DuckLakeTableEntry>();
	auto &entries = GetOrCreateTransactionLocalEntries(table);
	entries.CreateEntry(std::move(new_entry));
	switch (new_table.GetLocalChange().type) {
	case LocalChangeType::RENAMED: {
		// rename - take care of the old table
		if (table.IsTransactionLocal()) {
			// table is transaction local - delete the old table from there
			entries.DropEntry(table.name);
		} else {
			// table is not transaction local - add to drop list
			auto table_id = table.GetTableId();
			renamed_tables.insert(table_id);
		}
		break;
	}
	case LocalChangeType::ADD_COLUMN:
	case LocalChangeType::SET_PARTITION_KEY:
	case LocalChangeType::SET_COMMENT:
	case LocalChangeType::SET_COLUMN_COMMENT:
	case LocalChangeType::SET_NULL:
	case LocalChangeType::DROP_NULL:
	case LocalChangeType::RENAME_COLUMN:
	case LocalChangeType::REMOVE_COLUMN:
	case LocalChangeType::CHANGE_COLUMN_TYPE:
	case LocalChangeType::SET_DEFAULT:
	case LocalChangeType::SET_SORT_KEY:
		break;
	default:
		throw NotImplementedException("Alter type not supported in DuckLakeTransaction::AlterEntry");
	}
}

void DuckLakeTransaction::AlterEntryInternal(DuckLakeViewEntry &view, unique_ptr<CatalogEntry> new_entry) {
	auto &new_view = new_entry->Cast<DuckLakeViewEntry>();
	auto &entries = GetOrCreateTransactionLocalEntries(view);
	entries.CreateEntry(std::move(new_entry));
	switch (new_view.GetLocalChange().type) {
	case LocalChangeType::RENAMED: {
		// rename - take care of the old table
		if (view.IsTransactionLocal()) {
			// view is transaction local - delete the old table from there
			entries.DropEntry(view.name);
		} else {
			// view is not transaction local - add to drop list
			auto table_id = view.GetViewId();
			dropped_views.insert(table_id);
		}
		break;
	}
	case LocalChangeType::SET_COMMENT:
		break;
	default:
		throw NotImplementedException("Alter type not supported in DuckLakeTransaction::AlterEntry");
	}
}

DuckLakeCatalogSet &DuckLakeTransaction::GetOrCreateTransactionLocalEntries(CatalogEntry &entry) {
	auto catalog_type = entry.type;
	if (catalog_type == CatalogType::SCHEMA_ENTRY) {
		if (!new_schemas) {
			new_schemas = make_uniq<DuckLakeCatalogSet>();
		}
		return *new_schemas;
	}
	auto &schema_name = entry.ParentSchema().name;
	auto local_entry = GetTransactionLocalEntries(catalog_type, schema_name);
	if (local_entry) {
		return *local_entry;
	}
	switch (catalog_type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY: {
		auto new_table_list = make_uniq<DuckLakeCatalogSet>();
		auto &result = *new_table_list;
		new_tables.insert(make_pair(schema_name, std::move(new_table_list)));
		return result;
	}
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto new_macro_list = make_uniq<DuckLakeCatalogSet>();
		auto &result = *new_macro_list;
		new_macros.insert(make_pair(schema_name, std::move(new_macro_list)));
		return result;
	}
	default:
		throw InternalException("Catalog type not supported for transaction local storage");
	}
}

optional_ptr<DuckLakeCatalogSet> DuckLakeTransaction::GetTransactionLocalSchemas() {
	return new_schemas;
}

optional_ptr<CatalogEntry> DuckLakeTransaction::GetTransactionLocalEntry(CatalogType catalog_type,
                                                                         const string &schema_name,
                                                                         const string &entry_name) {
	auto set = GetTransactionLocalEntries(catalog_type, schema_name);
	if (!set) {
		return nullptr;
	}
	return set->GetEntry(entry_name);
}

optional_ptr<DuckLakeCatalogSet> DuckLakeTransaction::GetTransactionLocalEntries(CatalogType catalog_type,
                                                                                 const string &schema_name) {
	switch (catalog_type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY: {
		auto entry = new_tables.find(schema_name);
		if (entry == new_tables.end()) {
			return nullptr;
		}
		return entry->second;
	}
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::TABLE_FUNCTION_ENTRY: {
		auto entry = new_macros.find(schema_name);
		if (entry == new_macros.end()) {
			return nullptr;
		}
		return entry->second;
	}
	default:
		return nullptr;
	}
}

optional_ptr<CatalogEntry> DuckLakeTransaction::GetLocalEntryById(SchemaIndex schema_id) {
	if (!new_schemas) {
		return nullptr;
	}
	return new_schemas->GetEntryById(schema_id);
}

optional_ptr<CatalogEntry> DuckLakeTransaction::GetLocalEntryById(TableIndex table_id) {
	for (auto &schema_entry : new_tables) {
		auto entry = schema_entry.second->GetEntryById(table_id);
		if (entry) {
			return entry;
		}
	}
	return nullptr;
}

MappingIndex DuckLakeTransaction::AddNameMap(unique_ptr<DuckLakeNameMap> name_map) {
	// check if we can re-use a previously added name map
	auto map_index = ducklake_catalog.TryGetCompatibleNameMap(*this, *name_map);
	if (map_index.IsValid()) {
		return map_index;
	}
	map_index = new_name_maps.TryGetCompatibleNameMap(*name_map);
	if (map_index.IsValid()) {
		// found a compatible map already - return it
		return map_index;
	}
	// no compatible map found - generate a new index
	MappingIndex new_index(GetLocalCatalogId());
	name_map->id = new_index;
	new_name_maps.Add(std::move(name_map));
	return new_index;
}

const DuckLakeNameMap &DuckLakeTransaction::GetMappingById(MappingIndex mapping_id) {
	// search the transaction-local name maps
	auto entry = new_name_maps.name_maps.find(mapping_id);
	if (entry != new_name_maps.name_maps.end()) {
		return *entry->second;
	}
	// search the catalog name maps
	auto name_map = ducklake_catalog.TryGetMappingById(*this, mapping_id);
	if (name_map) {
		return *name_map;
	}
	throw InvalidInputException("Unknown name map id %d when trying to map file", mapping_id.index);
}

string DuckLakeTransaction::GenerateUUIDv7() {
	return UUID::ToString(UUIDv7::GenerateRandomUUID());
}

string DuckLakeTransaction::GenerateUUID() const {
	return GenerateUUIDv7();
}

idx_t DuckLakeTransaction::GetCatalogVersion() {
	if (catalog_version > 0) {
		return catalog_version;
	}
	return GetSnapshot().schema_version;
}

} // namespace duckdb
