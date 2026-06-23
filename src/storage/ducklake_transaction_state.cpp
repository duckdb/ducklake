#include "storage/ducklake_transaction_state.hpp"

#include "common/ducklake_types.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_commit_state.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "common/ducklake_util.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <cmath>
#include <sstream>
#include <thread>

namespace duckdb {

DuckLakeTransactionState::DuckLakeTransactionState(DatabaseInstance &db, bool require_commit_message,
                                                   DuckLakeNameMapSet &new_name_maps, string data_path,
                                                   string separator)
    : db(db), require_commit_message(require_commit_message), new_name_maps(new_name_maps),
      data_path(std::move(data_path)), separator(std::move(separator)) {
}

DuckLakePath DuckLakeTransactionState::GetRelativePath(const string &path) const {
	DuckLakePath result;
	if (StringUtil::StartsWith(path, data_path)) {
		result.path = path.substr(data_path.size());
		result.path_is_relative = true;
	} else {
		result.path = path;
		result.path_is_relative = false;
	}
	if (!separator.empty() && separator != "/") {
		result.path = StringUtil::Replace(result.path, separator, "/");
	}
	return result;
}

void DuckLakeTransactionState::EnsureCommitInfoProvided(const DuckLakeSnapshotCommit &commit_info) const {
	if (!require_commit_message || commit_info.is_commit_info_set) {
		return;
	}
	throw InvalidConfigurationException(
	    "Commit Information for the snapshot is required but has not been provided. \n * Provide the information "
	    "with \"CALL ducklake.set_commit_message('author_name', 'commit_message'); \n * Set the required commit "
	    "message to false with \"CALL ducklake.set_option('require_commit_message', False)\" '\"");
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
			string action = StringUtil::Format("create %s \"%s\" in schema \"%s\"", entry_type,
			                                   catalog_entry.name.GetIdentifierName(), schema_name);
			ConflictCheck(schema.GetSchemaId(), dropped_schemas, action.c_str(), "dropped this schema");

			auto tbl_entry = other_created_changes.find(schema_name);
			if (tbl_entry != other_created_changes.end()) {
				auto &other_created_tables = tbl_entry->second;
				auto sub_entry = other_created_tables.find(catalog_entry.name.GetIdentifierName());
				if (sub_entry != other_created_tables.end()) {
					// a table with this name in this schema was already created
					throw TransactionException("Transaction conflict - attempting to create %s \"%s\" in schema \"%s\" "
					                           "- but this %s has been created by another transaction already",
					                           entry_type, catalog_entry.name.GetIdentifierName(), schema_name,
					                           sub_entry->second);
				}
			}
		}
	}
}

} // namespace

void DuckLakeTransactionState::CheckForConflicts(const TransactionChangeInformation &changes,
                                                 const SnapshotChangeInformation &other_changes,
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

		ConflictCheck(dropped_schema.name.GetIdentifierName(), other_changes.created_tables, "drop schema",
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

			string action = StringUtil::Format("create %s \"%s\" in schema \"%s\"", entry_type,
			                                   table.name.GetIdentifierName(), schema_name);
			ConflictCheck(schema.GetSchemaId(), other_changes.dropped_schemas, action.c_str(), "dropped this schema");

			auto tbl_entry = other_changes.created_tables.find(schema_name);
			if (tbl_entry != other_changes.created_tables.end()) {
				auto &other_created_tables = tbl_entry->second;
				auto sub_entry = other_created_tables.find(table.name.GetIdentifierName());
				if (sub_entry != other_created_tables.end()) {
					// a table with this name in this schema was already created
					throw TransactionException("Transaction conflict - attempting to create %s \"%s\" in schema \"%s\" "
					                           "- but this %s has been created by another transaction already",
					                           entry_type, table.name.GetIdentifierName(), schema_name,
					                           sub_entry->second);
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
		change_info.changes_made += DuckLakeUtil::SQLIdentifierToString(created_schema);
	}
	for (auto &entry : changes.created_tables) {
		auto &schema = entry.first;
		auto schema_prefix = DuckLakeUtil::SQLIdentifierToString(schema) + ".";
		for (auto &created_table : entry.second) {
			if (!change_info.changes_made.empty()) {
				change_info.changes_made += ",";
			}
			auto is_view = created_table.get().type == CatalogType::VIEW_ENTRY;
			change_info.changes_made += is_view ? "created_view:" : "created_table:";
			change_info.changes_made +=
			    schema_prefix + DuckLakeUtil::SQLIdentifierToString(created_table.get().name.GetIdentifierName());
		}
	}

	for (auto &entry : changes.created_scalar_macros) {
		auto &schema = entry.first;
		auto schema_prefix = DuckLakeUtil::SQLIdentifierToString(schema) + ".";
		for (auto &created_macro : entry.second) {
			if (!change_info.changes_made.empty()) {
				change_info.changes_made += ",";
			}
			change_info.changes_made += "created_scalar_macro:";
			change_info.changes_made +=
			    schema_prefix + DuckLakeUtil::SQLIdentifierToString(created_macro.get().name.GetIdentifierName());
		}
	}
	for (auto &entry : changes.created_table_macros) {
		auto &schema = entry.first;
		auto schema_prefix = DuckLakeUtil::SQLIdentifierToString(schema) + ".";
		for (auto &created_macro : entry.second) {
			if (!change_info.changes_made.empty()) {
				change_info.changes_made += ",";
			}
			change_info.changes_made += "created_table_macro:";
			change_info.changes_made +=
			    schema_prefix + DuckLakeUtil::SQLIdentifierToString(created_macro.get().name.GetIdentifierName());
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

void CancelOrDropField(TableIndex table_id, FieldIndex field_id, NewTableInfo &result,
                       unordered_map<idx_t, idx_t> &txn_added_fields) {
	auto it = txn_added_fields.find(field_id.index);
	if (it != txn_added_fields.end()) {
		auto erased_idx = it->second;
		result.new_columns.erase(result.new_columns.begin() + erased_idx);
		txn_added_fields.erase(it);
		for (auto &entry : txn_added_fields) {
			if (entry.second > erased_idx) {
				entry.second--;
			}
		}
		return;
	}
	// Column was not added in the same transaction, we emit a drop.
	DuckLakeDroppedColumn dropped_col;
	dropped_col.table_id = table_id;
	dropped_col.field_id = field_id;
	result.dropped_columns.push_back(dropped_col);
}

template <typename T>
void RenameEmittedEntry(vector<T> &entries, TableIndex remapped_id, const string &new_name) {
	for (auto &entry : entries) {
		if (entry.id == remapped_id) {
			entry.name = new_name;
			return;
		}
	}
}

void HandleChangedFields(TableIndex table_id, const ColumnChangeInfo &change_info, NewTableInfo &result,
                         unordered_map<idx_t, idx_t> &txn_added_fields) {
	for (auto &dropped_field_id : change_info.dropped_fields) {
		CancelOrDropField(table_id, dropped_field_id, result, txn_added_fields);
	}
	for (auto &new_col_info : change_info.new_fields) {
		DuckLakeNewColumn new_column;
		new_column.table_id = table_id;
		new_column.column_info = new_col_info.column_info;
		new_column.parent_idx = new_col_info.parent_idx;
		txn_added_fields[new_col_info.column_info.id.index] = result.new_columns.size();
		result.new_columns.push_back(std::move(new_column));
	}
}

void GetNewMacroInfo(DuckLakeCommitState &commit_state, reference<CatalogEntry> entry, NewMacroInfo &result) {
	DuckLakeMacroInfo new_macro_info;
	auto &macro_entry = entry.get().Cast<MacroCatalogEntry>();
	auto &ducklake_schema = macro_entry.schema.Cast<DuckLakeSchemaEntry>();

	new_macro_info.macro_id = MacroIndex(commit_state.commit_snapshot.next_catalog_id++);
	new_macro_info.macro_name = macro_entry.name.GetIdentifierName();
	new_macro_info.schema_id = commit_state.GetSchemaId(ducklake_schema);
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
		// Let's do the parameters
		for (idx_t i = 0; i < impl->parameters.size(); i++) {
			DuckLakeMacroParameters parameter;
			parameter.parameter_name = impl->parameters[i]->GetName().GetIdentifierName();
			parameter.parameter_type = DuckLakeTypes::ToString(impl->types[i]);
			auto default_it = impl->default_parameters.find(Identifier(parameter.parameter_name));
			if (default_it != impl->default_parameters.end()) {
				auto &const_expr = default_it->second->Cast<ConstantExpression>();
				parameter.default_value = const_expr.GetValue().ToString();
				parameter.default_value_type = DuckLakeTypes::ToString(const_expr.GetValue().type());
			} else {
				parameter.default_value_type = "unknown";
			}

			macro_impl.parameters.push_back(std::move(parameter));
		}
		new_macro_info.implementations.push_back(std::move(macro_impl));
	}
	result.new_macros.push_back(std::move(new_macro_info));
}

static void ConvertNameMapColumn(const DuckLakeNameMapEntry &name_map_entry, MappingIndex map_id, idx_t &column_idx,
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

DuckLakeFileInfo DuckLakeTransactionState::GetNewDataFile(const DuckLakeDataFile &file,
                                                          DuckLakeCommitState &commit_state, TableIndex table_id,
                                                          optional_idx row_id_start) {
	auto data_file = DuckLakeTransaction::BuildDataFileInfo(file, commit_state.commit_snapshot, table_id, row_id_start);
	commit_state.RemapPartitionId(data_file.partition_id);
	if (data_file.partition_id.IsValid() &&
	    data_file.partition_id.GetIndex() >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START) {
		throw InternalException("Cannot commit data file with transaction-local partition id");
	}
	commit_state.RemapMappingIndex(data_file.mapping_id);
	return data_file;
}

NewNameMapInfo DuckLakeTransactionState::GetNewNameMaps(DuckLakeCommitState &commit_state) {
	NewNameMapInfo result;
	auto &committed_mapping_indexes = commit_state.committed_mapping_indexes;
	for (auto &entry : new_name_maps.name_maps) {
		// generate a new mapping id
		auto local_map_id = entry.first;
		auto &mapping = *entry.second;
		MappingIndex new_map_id(commit_state.commit_snapshot.next_file_id++);

		DuckLakeColumnMappingInfo map_info;
		map_info.table_id = commit_state.GetTableId(mapping.table_id);
		map_info.mapping_id = new_map_id;
		map_info.map_type = "map_by_name";
		if (IsTransactionLocal(map_info.table_id)) {
			throw InternalException("table_id should be rewritten to non-transaction local before");
		}

		// iterate over the columns to generate the new name map columns
		idx_t column_idx = 0;
		for (auto &name_map_column : mapping.column_maps) {
			ConvertNameMapColumn(*name_map_column, new_map_id, column_idx, map_info);
		}
		result.new_column_mappings.push_back(std::move(map_info));

		committed_mapping_indexes[local_map_id] = new_map_id;
	}
	return result;
}

vector<DuckLakeDeleteFileInfo>
DuckLakeTransactionState::GetNewDeleteFiles(const DuckLakeCommitState &commit_state,
                                            vector<DuckLakeOverwrittenDeleteFile> &overwritten_delete_files) const {
	vector<DuckLakeDeleteFileInfo> result;
	// handle delete files made to existing files
	for (auto &entry : local_changes.Changes()) {
		auto table_id = commit_state.GetTableId(entry.GetTableIndex());
		auto &table_changes = entry.GetTableChanges();
		for (auto &file_entry : table_changes.new_delete_files) {
			for (auto &file : file_entry.second) {
				if (file.overwritten_delete_file.delete_file_id.IsValid()) {
					// track the old delete file for deletion from metadata and disk
					overwritten_delete_files.push_back(file.overwritten_delete_file);
				}
				auto delete_file = DuckLakeTransaction::GetNewDeleteFile(table_id, commit_state, file);
				result.push_back(std::move(delete_file));
			}
		}
	}
	// handle any delete files that were added to data files that are ALSO added in this transaction
	for (auto &entry : commit_state.local_delete_files) {
		auto table_id = commit_state.GetTableId(entry.first);
		for (auto &file : entry.second) {
			if (file.overwritten_delete_file.delete_file_id.IsValid()) {
				throw InternalException("Local delete files should not overwrite files");
				// track the old delete file for deletion from metadata and disk
				overwritten_delete_files.push_back(file.overwritten_delete_file);
			}
			auto delete_file = DuckLakeTransaction::GetNewDeleteFile(table_id, commit_state, file);
			result.push_back(std::move(delete_file));
		}
	}
	return result;
}

vector<DuckLakeDeletedInlinedDataInfo>
DuckLakeTransactionState::GetNewInlinedDeletes(DuckLakeCommitState &commit_state) const {
	vector<DuckLakeDeletedInlinedDataInfo> result;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = commit_state.GetTableId(entry.GetTableIndex());
		auto &table_changes = entry.GetTableChanges();
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

vector<DuckLakeInlinedFileDeletionInfo>
DuckLakeTransactionState::GetNewInlinedFileDeletes(DuckLakeCommitState &commit_state) {
	vector<DuckLakeInlinedFileDeletionInfo> result;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = commit_state.GetTableId(entry.GetTableIndex());
		auto &table_changes = entry.GetTableChanges();
		if (!table_changes.new_inlined_file_deletes) {
			continue;
		}
		if (table_changes.new_inlined_file_deletes->file_deletes.empty()) {
			continue;
		}
		DuckLakeInlinedFileDeletionInfo info;
		info.table_id = table_id;
		// copy, not move - data must survive commit retries in FlushChanges
		info.file_deletions.file_deletes = table_changes.new_inlined_file_deletes->file_deletes;
		result.push_back(std::move(info));
	}
	return result;
}

CompactionInformation DuckLakeTransactionState::GetCompactionChanges(DuckLakeCommitState &commit_state,
                                                                     CompactionType type) {
	auto &commit_snapshot = commit_state.commit_snapshot;
	CompactionInformation result;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		for (auto &compaction : table_changes.compactions) {
			if (type != compaction.type) {
				continue;
			}
			bool has_new_file = !compaction.written_file.file_name.empty();
			DuckLakeFileInfo new_file;

			if (has_new_file) {
				new_file = GetNewDataFile(compaction.written_file, commit_state, table_id, compaction.row_id_start);
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
					throw InternalException(
					    "DuckLakeTransactionState::GetCompactionChanges Compaction type is invalid");
				}
			}

			idx_t row_id_limit = 0;
			for (auto &compacted_file : compaction.source_files) {
				row_id_limit += compacted_file.file.row_count;
				if (!compacted_file.delete_files.empty()) {
					row_id_limit -= compacted_file.delete_files.back().row_count;
				}
				row_id_limit -= compacted_file.inlined_file_deletions.size();
				DuckLakeCompactedFileInfo file_info;
				file_info.path = compacted_file.file.data.path;
				file_info.source_id = compacted_file.file.id;
				file_info.table_index = entry.GetTableIndex();
				file_info.rewrite_snapshot = commit_snapshot.snapshot_id;
				if (has_new_file) {
					file_info.new_id = new_file.id;
				}

				if (!compacted_file.delete_files.empty()) {
					file_info.delete_file_path = compacted_file.delete_files.back().data.path;
					file_info.delete_file_id = compacted_file.delete_files.back().delete_file_id;
					file_info.start_snapshot = compacted_file.file.begin_snapshot;
					file_info.delete_file_start_snapshot = commit_snapshot.snapshot_id;
					file_info.delete_file_end_snapshot = compacted_file.delete_files.back().end_snapshot;
				}
				if (has_new_file && row_id_limit > new_file.row_count) {
					throw InternalException("Compaction error - row id limit is larger than the row count of the file");
				}
				result.compacted_files.push_back(std::move(file_info));
			}
			if (!has_new_file && row_id_limit != 0) {
				throw InternalException(
				    "Compaction error - compaction without output file must have zero live source rows (got %llu)",
				    row_id_limit);
			}
			if (has_new_file) {
				result.new_files.push_back(std::move(new_file));
			}
		}
	}
	return result;
}

static bool IsFoldableScalarType(const LogicalType &type) {
	if (type.IsNumeric() || type.IsTemporal()) {
		return true;
	}
	auto id = type.id();
	return id == LogicalTypeId::VARCHAR || id == LogicalTypeId::BOOLEAN;
}

bool DuckLakeTransactionState::TryMergeInlinedStats(const vector<DuckLakeColumnSchemaEntry> &columns,
                                                    const vector<string> &inlined_table_names,
                                                    DuckLakeSnapshot snapshot, DuckLakeTableStats &target,
                                                    const DuckLakeCommitContext &context) {
	// We can only compute exact inlined min/max for top-level foldable scalar columns. If any column is a
	// non-scalar type we cannot account for the inlined rows exactly - bail (caller keeps the scan fallback).
	for (auto &col : columns) {
		if (!IsFoldableScalarType(col.column_type)) {
			return false;
		}
	}
	for (auto &inlined_table_name : inlined_table_names) {
		// Build one aggregate query: COUNT(*) followed by (MIN, MAX, COUNT(col), nan-flag) per column.
		string select_list = "COUNT(*)";
		for (auto &col : columns) {
			auto col_ident = DuckLakeUtil::SQLIdentifierToString(col.column_name);
			bool is_float =
			    col.column_type.id() == LogicalTypeId::FLOAT || col.column_type.id() == LogicalTypeId::DOUBLE;
			string nan_expr =
			    is_float ? StringUtil::Format("COALESCE(BOOL_OR(isnan(%s)), false)", col_ident) : string("false");
			select_list += StringUtil::Format(", MIN(%s)::VARCHAR, MAX(%s)::VARCHAR, COUNT(%s), %s", col_ident,
			                                  col_ident, col_ident, nan_expr);
		}
		auto sql = DuckLakeMetadataManager::ReadInlinedDataAggregatesSql(
		    DuckLakeUtil::SQLIdentifierToString(inlined_table_name), select_list);
		auto result = context.query_metadata_with_snapshot(snapshot, sql);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to read inlined-data aggregates from DuckLake: ");
		}
		for (auto &row : *result) {
			auto total = static_cast<idx_t>(row.template GetValue<int64_t>(0));
			if (total == 0) {
				break;
			}
			idx_t col_offset = 1;
			for (auto &col : columns) {
				bool is_float =
				    col.column_type.id() == LogicalTypeId::FLOAT || col.column_type.id() == LogicalTypeId::DOUBLE;
				bool contains_nan = !row.IsNull(col_offset + 3) && row.template GetValue<bool>(col_offset + 3);
				DuckLakeColumnStats col_stats(col.column_type);
				auto non_null = static_cast<idx_t>(row.template GetValue<int64_t>(col_offset + 2));
				col_stats.has_num_values = true;
				col_stats.num_values = total;
				col_stats.has_null_count = true;
				col_stats.null_count = total - non_null;
				if (is_float) {
					col_stats.has_contains_nan = true;
					col_stats.contains_nan = contains_nan;
				}
				// do not record float min/max if NaN is present (matches the parquet stats behaviour)
				if (!(is_float && contains_nan)) {
					if (!row.IsNull(col_offset + 0)) {
						col_stats.has_min = true;
						col_stats.min = row.template GetValue<string>(col_offset + 0);
					}
					if (!row.IsNull(col_offset + 1)) {
						col_stats.has_max = true;
						col_stats.max = row.template GetValue<string>(col_offset + 1);
					}
				}
				target.MergeStats(col.field_index, col_stats);
				col_offset += 4;
			}
			break;
		}
	}
	return true;
}

void DuckLakeTransactionState::RecomputeGlobalStatsAfterRewrite(string &batch_query, TableIndex table_id,
                                                                DuckLakeSnapshot snapshot,
                                                                const CompactionInformation &rewrite_changes,
                                                                const set<DataFileIndex> &removed_source_ids,
                                                                const DuckLakeCommitContext &context) {
	auto columns = context.get_table_column_schema(table_id);
	if (columns.empty()) {
		return; // no schema visible at the commit snapshot
	}
	auto current_stats = context.get_table_stats(table_id);
	if (!current_stats) {
		// no committed stats row exists yet - the UPDATE path of UpdateGlobalTableStats only refreshes existing rows
		return;
	}

	// Build a field_index -> column_type map for the per-file stats merge below. `columns` is the full flattened
	// schema (roots + nested leaves), so per-file stats keyed by a nested-leaf FieldIndex resolve here too - without
	// the leaves, an un-rewritten file's nested-leaf stats would be dropped and the global min/max corrupted.
	map<FieldIndex, LogicalType> type_by_field;
	for (auto &col : columns) {
		type_by_field.insert(make_pair(col.field_index, col.column_type));
	}

	DuckLakeTableStats new_stats;
	new_stats.next_row_id = current_stats->next_row_id; // monotonic - carried forward, never recomputed
	idx_t parquet_gross_rows = 0;

	// 1. Merge the per-file stats of the post-rewrite parquet files = (pre-commit visible files - removed) + new files.
	auto result = context.query_metadata_with_snapshot(
	    snapshot, DuckLakeMetadataManager::ReadFileColumnStatsForTableSql(table_id));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to read per-file column stats for rewrite from DuckLake: ");
	}
	bool have_file = false;
	idx_t last_file_id = 0;
	for (auto &row : *result) {
		auto data_file_id = static_cast<idx_t>(row.GetValue<int64_t>(0));
		if (removed_source_ids.find(DataFileIndex(data_file_id)) != removed_source_ids.end()) {
			continue; // this file is being rewritten away
		}
		if (!have_file || data_file_id != last_file_id) {
			have_file = true;
			last_file_id = data_file_id;
			new_stats.record_count += static_cast<idx_t>(row.GetValue<int64_t>(1));
			new_stats.table_size_bytes += static_cast<idx_t>(row.GetValue<int64_t>(2));
			parquet_gross_rows += static_cast<idx_t>(row.GetValue<int64_t>(1));
		}
		if (row.IsNull(3)) {
			continue; // file without column stats
		}
		FieldIndex field_idx(static_cast<idx_t>(row.GetValue<int64_t>(3)));
		auto type_it = type_by_field.find(field_idx);
		if (type_it == type_by_field.end()) {
			continue; // column no longer exists or is nested
		}
		DuckLakeColumnStats col_stats(type_it->second);
		if (!row.IsNull(4) && !row.IsNull(5)) {
			auto value_count = static_cast<idx_t>(row.GetValue<int64_t>(4));
			auto null_count = static_cast<idx_t>(row.GetValue<int64_t>(5));
			col_stats.has_num_values = true;
			col_stats.num_values = value_count + null_count;
			col_stats.has_null_count = true;
			col_stats.null_count = null_count;
		}
		if (!row.IsNull(6)) {
			col_stats.has_min = true;
			col_stats.min = row.GetValue<string>(6);
		}
		if (!row.IsNull(7)) {
			col_stats.has_max = true;
			col_stats.max = row.GetValue<string>(7);
		}
		if (!row.IsNull(8)) {
			col_stats.has_contains_nan = true;
			col_stats.contains_nan = row.GetValue<bool>(8);
		}
		if (!row.IsNull(9) && col_stats.extra_stats) {
			col_stats.extra_stats->Deserialize(row.GetValue<string>(9));
		}
		new_stats.MergeStats(field_idx, col_stats);
	}

	// add the new (rewritten, delete-free) files - their stats are available in memory
	for (auto &file : rewrite_changes.new_files) {
		if (file.table_id != table_id) {
			continue;
		}
		new_stats.record_count += file.row_count;
		new_stats.table_size_bytes += file.file_size_bytes;
		parquet_gross_rows += file.row_count;
		for (auto &col_entry : file.column_stats) {
			new_stats.MergeStats(col_entry.first, col_entry.second);
		}
	}

	// 2. Delete-free gate: the merged parquet stats are exact only if no deletions remain on the table's data files.
	//    That holds iff the gross row count of the post-rewrite files equals the net (delete-adjusted) data-file count.
	//    (Covers partial rewrites with delete_threshold > 0, leftover delete files, and inlined file deletions.)
	if (parquet_gross_rows != context.get_net_data_file_row_count(table_id)) {
		return;
	}

	// 3. Fold in committed inlined data (REWRITE_DELETES does not rewrite inlined data). Pass only the top-level
	//    roots: TryMergeInlinedStats references each column by name against the inlined table and bails on any
	//    non-scalar root; feeding it nested leaves would emit MIN("<leaf>") against a column that does not exist.
	idx_t net_inlined = context.get_net_inlined_row_count(table_id);
	if (net_inlined > 0) {
		vector<DuckLakeColumnSchemaEntry> root_columns;
		for (auto &col : columns) {
			if (col.is_root) {
				root_columns.push_back(col);
			}
		}
		auto inlined_table_names = context.get_inlined_table_names(table_id);
		if (!TryMergeInlinedStats(root_columns, inlined_table_names, snapshot, new_stats, context)) {
			return; // cannot account for inlined data exactly - keep the existing stats and the scan fallback
		}
		new_stats.record_count += net_inlined;
	}

	// 4. Make sure every committed column (root or nested leaf) appears so its global row is refreshed (a column with
	//    no live data becomes "unknown" -> it is scanned at query time, which is correct). UpdateGlobalTableStatsSql
	//    only UPDATEs existing rows, so entries with no committed stats row (e.g. struct containers) are harmless
	//    no-ops. Use the snapshot schema instead of current_stats->column_stats: the server-side stats cache is only
	//    populated for tables with staged inserts, so a pure-rewrite commit can see an empty
	//    current_stats.column_stats.
	for (auto &col : columns) {
		if (new_stats.column_stats.find(col.field_index) == new_stats.column_stats.end()) {
			new_stats.column_stats.insert(make_pair(col.field_index, DuckLakeColumnStats(col.column_type)));
		}
	}

	DuckLakeNewGlobalStats new_globals;
	new_globals.initialized = true;
	new_globals.stats = std::move(new_stats);
	batch_query += DuckLakeMetadataManager::UpdateGlobalTableStatsSql(
	    DuckLakeTransaction::ConvertNewGlobalStats(table_id, new_globals));
}

NewDataInfo DuckLakeTransactionState::GetNewDataFiles(string &batch_query, DuckLakeCommitState &commit_state,
                                                      optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats,
                                                      const DuckLakeCommitContext &context) {
	NewDataInfo result;
	// get the global table stats
	DuckLakeNewGlobalStats new_globals;
	unique_ptr<DuckLakeStats> dl_stats;
	if (stats) {
		dl_stats = context.build_stats_map(*stats);
	}
	for (auto &entry : local_changes.Changes()) {
		auto table_id = commit_state.GetTableId(entry.GetTableIndex());
		if (IsTransactionLocal(table_id)) {
			throw InternalException("Cannot commit transaction local files - these should have been cleaned up before");
		}
		auto &table_changes = entry.GetTableChanges();
		if (table_changes.new_data_files.empty() && !table_changes.new_inlined_data) {
			// no new data - skip this entry
			continue;
		}
		// get the global table stats
		DuckLakeNewGlobalStats new_globals;
		optional_ptr<DuckLakeTableStats> current_stats;
		shared_ptr<DuckLakeTableStats> current_stats_pin;
		if (dl_stats) {
			auto dl_stats_entry = dl_stats->table_stats.find(table_id);
			if (dl_stats_entry != dl_stats->table_stats.end()) {
				current_stats = dl_stats_entry->second.get();
			}
		} else {
			current_stats_pin = context.get_table_stats(table_id);
			current_stats = current_stats_pin.get();
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
			auto data_file = GetNewDataFile(file, commit_state, table_id, row_id_start);
			for (auto &del_file : file.delete_files) {
				// this transaction-local file already has deletes - write them out
				DuckLakeDeleteFile delete_file = del_file;
				delete_file.data_file_id = data_file.id;
				delete_files.push_back(std::move(delete_file));
			}

			// merge the stats into the new global state
			new_stats.MergeFileStats(file);
			result.new_files.push_back(std::move(data_file));
		}
		// add any delete files that were made on top of these transaction-local files
		commit_state.local_delete_files[table_id] = std::move(delete_files);

		if (table_changes.new_inlined_data) {
			auto &inlined_data = *table_changes.new_inlined_data;

			idx_t record_count = inlined_data.Count();

			DuckLakeInlinedDataInfo new_inlined_data;
			new_inlined_data.table_id = table_id;
			new_inlined_data.row_id_start = new_stats.next_row_id;

			// merge column stats
			for (auto &entry : inlined_data.column_stats) {
				new_stats.MergeStats(entry.first, entry.second);
			}

			// update global stats
			new_stats.record_count += record_count;
			if (!inlined_data.HasPreservedRowIds()) {
				// regular insert, we advance next_row_id
				new_stats.next_row_id += record_count;
			} else {
				// mixed insert and updates, we only advance inserted row_ids
				for (auto &rid : inlined_data.row_ids) {
					if (DuckLakeConstants::IsTransactionLocalRowId(rid)) {
						new_stats.next_row_id++;
					}
				}
			}
			// add the file to the to-be-written inlined data list
			new_inlined_data.data = table_changes.new_inlined_data.get();
			result.new_inlined_data.push_back(new_inlined_data);

			if (table_changes.new_data_files.empty()) {
				// force an increment of file_id to signal a data change if we have only inlined data changes
				commit_state.commit_snapshot.next_file_id++;
			}
		}
		// update the global stats for this table based on the newly written data
		batch_query += DuckLakeMetadataManager::UpdateGlobalTableStatsSql(
		    DuckLakeTransaction::ConvertNewGlobalStats(table_id, new_globals));
	}
	return result;
}

NewMacroInfo DuckLakeTransactionState::GetNewMacros(DuckLakeCommitState &commit_state,
                                                    TransactionChangeInformation &transaction_changes) {
	NewMacroInfo result;
	for (auto &schema_entry : new_scalar_macros) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			GetNewMacroInfo(commit_state, *entry.second, result);
		}
	}
	for (auto &schema_entry : new_table_macros) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			GetNewMacroInfo(commit_state, *entry.second, result);
		}
	}
	return result;
}

void DuckLakeTransactionState::GetNewTableInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
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

	// Maps from field index to the number of alter operations for that field.
	map<FieldIndex, idx_t> field_alter_count;
	// Number of table comment operations.
	idx_t comment_count = 0;
	// Maps from field index to the number of column comment operations for that field.
	map<FieldIndex, idx_t> column_comment_count;
	for (idx_t table_idx = 0; table_idx < tables.size(); table_idx++) {
		auto &table = tables[table_idx].get();
		auto local_change = table.GetLocalChange();
		switch (local_change.type) {
		case LocalChangeType::SET_NULL:
		case LocalChangeType::DROP_NULL:
		case LocalChangeType::RENAME_COLUMN:
		case LocalChangeType::SET_DEFAULT:
			field_alter_count[local_change.field_index]++;
			break;
		case LocalChangeType::SET_COMMENT:
			comment_count++;
			break;
		case LocalChangeType::SET_COLUMN_COMMENT:
			column_comment_count[local_change.field_index]++;
			break;
		default:
			break;
		}
	}

	// Maps from field index to the number of alter operations remaining for that field.
	map<FieldIndex, idx_t> field_alter_remaining(std::move(field_alter_count));
	// Number of table comment operations remaining.
	idx_t comment_remaining = comment_count;
	// Maps from field index to the number of column comment operations remaining for that field.
	map<FieldIndex, idx_t> column_comment_remaining(std::move(column_comment_count));

	// Used to decide whether a column is a newly added column in this transaction
	// Maps from field_index.index to the index of the column in result.new_columns.
	unordered_map<idx_t, idx_t> txn_added_fields;

	// traverse in reverse order
	bool column_schema_change = false;
	for (idx_t table_idx = tables.size(); table_idx > 0; table_idx--) {
		auto &table = tables[table_idx - 1].get();
		auto local_change = table.GetLocalChange();
		auto table_id = table.GetTableId();
		switch (local_change.type) {
		case LocalChangeType::SET_PARTITION_KEY: {
			auto partition_key = DuckLakeTransaction::GetNewPartitionKey(commit_state, table);
			result.new_partition_keys.push_back(std::move(partition_key));

			transaction_changes.altered_tables.insert(table_id);
			transaction_changes.altered_tables_with_schema_version_changes.insert(table_id);
			column_schema_change = true;
			break;
		}
		case LocalChangeType::SET_SORT_KEY: {
			auto sort_key = DuckLakeTransaction::GetNewSortKey(commit_state, table);
			result.new_sort_keys.push_back(std::move(sort_key));
			transaction_changes.altered_tables.insert(table_id);
			break;
		}
		case LocalChangeType::SET_COMMENT: {
			comment_remaining--;
			if (comment_remaining > 0) {
				break;
			}
			DuckLakeTagInfo comment_info;
			comment_info.id = commit_state.GetTableId(table).index;
			comment_info.key = "comment";
			comment_info.value = table.comment;
			result.new_tags.push_back(std::move(comment_info));

			transaction_changes.altered_tables.insert(table_id);
			break;
		}
		case LocalChangeType::SET_COLUMN_COMMENT: {
			auto &col_comment_rem = column_comment_remaining[local_change.field_index];
			col_comment_rem--;
			if (col_comment_rem > 0) {
				break;
			}
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
			auto &remaining = field_alter_remaining[local_change.field_index];
			remaining--;
			// This is an older thus superseded entry for a field that is altered for multiple times in this
			// transaction, so we skip it; the newest entry will emit the definitive value.
			if (remaining > 0) {
				break;
			}

			auto committed_table_id = commit_state.GetTableId(table);

			// Cancel the previous txn-local add for this field, or drop the committed column.
			CancelOrDropField(committed_table_id, local_change.field_index, result, txn_added_fields);

			// Insert the new column with the updated info and register it.
			DuckLakeNewColumn new_col;
			new_col.table_id = committed_table_id;
			new_col.column_info = table.GetColumnInfo(local_change.field_index);
			txn_added_fields[local_change.field_index.index] = result.new_columns.size();
			result.new_columns.push_back(std::move(new_col));

			transaction_changes.altered_tables.insert(table_id);
			transaction_changes.altered_tables_with_schema_version_changes.insert(table_id);
			if (local_change.type == LocalChangeType::RENAME_COLUMN) {
				column_schema_change = true;
				// persist updated sort expressions (column name was updated in the table entry)
				if (table.GetSortData()) {
					auto sort_key = DuckLakeTransaction::GetNewSortKey(commit_state, table);
					result.new_sort_keys.push_back(std::move(sort_key));
				}
			}
			break;
		}
		case LocalChangeType::REMOVE_COLUMN:
		case LocalChangeType::CHANGE_COLUMN_TYPE: {
			// drop the indicated column
			// note that in case of nested types we might be dropping multiple columns here
			HandleChangedFields(commit_state.GetTableId(table), table.GetChangedFields(), result, txn_added_fields);
			transaction_changes.altered_tables_with_schema_version_changes.insert(table_id);
			column_schema_change = true;
			break;
		}
		case LocalChangeType::ADD_COLUMN: {
			// insert the new column
			DuckLakeNewColumn new_col;
			new_col.table_id = commit_state.GetTableId(table);
			new_col.column_info = table.GetAddColumnInfo();
			txn_added_fields[new_col.column_info.id.index] = result.new_columns.size();
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
			if (local_change.type == LocalChangeType::RENAMED && IsTransactionLocal(old_table_id)) {
				auto existing = commit_state.committed_tables.find(old_table_id);
				if (existing != commit_state.committed_tables.end()) {
					// If we are rename a table in the same transaction it was created, we need to patch it
					RenameEmittedEntry(result.new_tables, existing->second, table.name.GetIdentifierName());
					break;
				}
			}
			auto new_table = DuckLakeTransaction::GetNewTable(commit_state, table);
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

void DuckLakeTransactionState::GetNewViewInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
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
	// count comment operations for deduplication
	idx_t view_comment_count = 0;
	for (idx_t view_idx = 0; view_idx < views.size(); view_idx++) {
		if (views[view_idx].get().GetLocalChange().type == LocalChangeType::SET_COMMENT) {
			view_comment_count++;
		}
	}
	idx_t view_comment_remaining = view_comment_count;
	// traverse in reverse order
	for (idx_t view_idx = views.size(); view_idx > 0; view_idx--) {
		auto &view = views[view_idx - 1].get();
		switch (view.GetLocalChange().type) {
		case LocalChangeType::SET_COMMENT: {
			view_comment_remaining--;
			if (view_comment_remaining > 0) {
				break;
			}
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
			if (view.GetLocalChange().type == LocalChangeType::RENAMED && IsTransactionLocal(old_view_id)) {
				auto existing = commit_state.committed_tables.find(old_view_id);
				if (existing != commit_state.committed_tables.end()) {
					// renaming a view in the same transaction it was created - patch the name on the existing row
					RenameEmittedEntry(result.new_views, existing->second, view.name.GetIdentifierName());
					break;
				}
			}
			auto new_view = DuckLakeTransaction::GetNewView(commit_state, view);
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

NewTableInfo DuckLakeTransactionState::GetNewTables(DuckLakeCommitState &commit_state,
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

vector<DuckLakeSchemaInfo> DuckLakeTransactionState::GetNewSchemas(DuckLakeCommitState &commit_state) {
	vector<DuckLakeSchemaInfo> schemas;
	for (auto &entry : new_schemas->GetEntries()) {
		auto &schema_entry = entry.second->Cast<DuckLakeSchemaEntry>();
		auto old_id = schema_entry.GetSchemaId();
		DuckLakeSchemaInfo schema_info;
		schema_info.id = SchemaIndex(commit_state.commit_snapshot.next_catalog_id++);
		schema_info.uuid = schema_entry.GetSchemaUUID();
		schema_info.name = schema_entry.name.GetIdentifierName();
		schema_info.path = schema_entry.DataPath();

		// add this schema id to the schema id map
		commit_state.committed_schemas.emplace(old_id, schema_info.id);

		// add the schema to the list
		schemas.push_back(std::move(schema_info));
	}
	return schemas;
}

string DuckLakeTransactionState::CommitChanges(DuckLakeCommitState &commit_state,
                                               TransactionChangeInformation &transaction_changes,
                                               optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats,
                                               const DuckLakeCommitContext &context) {
	auto &commit_snapshot = commit_state.commit_snapshot;

	EnsureCommitInfoProvided(commit_info);

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
		new_schemas_result = GetNewSchemas(commit_state);
		vector<DuckLakePath> resolved_schema_paths;
		resolved_schema_paths.reserve(new_schemas_result.size());
		for (auto &schema : new_schemas_result) {
			resolved_schema_paths.push_back(GetRelativePath(schema.path));
		}
		batch_queries += DuckLakeMetadataManager::WriteNewSchemas(new_schemas_result, resolved_schema_paths);
	}

	// write new tables
	vector<DuckLakeTableInfo> new_tables_result;
	vector<DuckLakeTableInfo> new_inlined_data_tables_result;
	if (!new_tables.empty()) {
		auto result = GetNewTables(commit_state, transaction_changes);
		vector<DuckLakePath> resolved_table_paths;
		resolved_table_paths.reserve(result.new_tables.size());
		for (auto &table : result.new_tables) {
			resolved_table_paths.push_back(DuckLakeMetadataManager::GetRelativePath(
			    table.schema_id, table.path, new_schemas_result, context.query_metadata, data_path, separator));
		}
		batch_queries += DuckLakeMetadataManager::WriteNewTables(result.new_tables, resolved_table_paths);
		auto existing_catalog = DuckLakeMetadataManager::BuildCatalogForSnapshot(
		    commit_snapshot, context.query_metadata_with_snapshot, data_path, separator);
		batch_queries +=
		    DuckLakeMetadataManager::WriteNewPartitionKeys(existing_catalog.partitions, result.new_partition_keys);
		batch_queries += DuckLakeMetadataManager::WriteNewViews(result.new_views);
		batch_queries += DuckLakeMetadataManager::WriteNewTags(result.new_tags);
		batch_queries += DuckLakeMetadataManager::WriteNewColumnTags(result.new_column_tags);
		batch_queries += DuckLakeMetadataManager::WriteDroppedColumns(result.dropped_columns);
		batch_queries += DuckLakeMetadataManager::WriteNewColumns(result.new_columns);
		batch_queries += context.write_inlined_tables(commit_snapshot, result.new_inlined_data_tables);
		batch_queries += DuckLakeMetadataManager::WriteNewSortKeys(existing_catalog.sorts, result.new_sort_keys);
		new_tables_result = result.new_tables;
		new_inlined_data_tables_result = result.new_inlined_data_tables;
	}

	if (!new_scalar_macros.empty() || !new_table_macros.empty()) {
		auto result = GetNewMacros(commit_state, transaction_changes);
		batch_queries += DuckLakeMetadataManager::WriteNewMacros(result.new_macros);
	}

	// write new name maps
	if (!new_name_maps.name_maps.empty()) {
		auto result = GetNewNameMaps(commit_state);
		batch_queries += DuckLakeMetadataManager::WriteNewColumnMappings(result.new_column_mappings);
	}

	auto write_data_files_sql = [&](const vector<DuckLakeFileInfo> &files) {
		if (files.empty()) {
			return string();
		}
		if (context.try_append_data_files &&
		    context.try_append_data_files(commit_snapshot, files, new_tables_result, new_schemas_result)) {
			// fast-path: files were written directly via Appender, skip SQL emission
			return string();
		}
		vector<DuckLakePath> resolved_paths;
		resolved_paths.reserve(files.size());
		for (auto &file : files) {
			resolved_paths.push_back(DuckLakeMetadataManager::GetRelativePath(
			    file.table_id, file.file_name, new_tables_result, new_schemas_result, context.query_metadata, data_path,
			    separator));
		}
		return DuckLakeMetadataManager::WriteNewDataFilesSqlBatch(files, resolved_paths, context.write_row_group_count);
	};

	// write new data / data files
	bool has_table_data_changes = local_changes.HasChanges();
	if (has_table_data_changes) {
		auto result = GetNewDataFiles(batch_queries, commit_state, stats, context);
		batch_queries += write_data_files_sql(result.new_files);
		batch_queries += context.write_inlined_data(commit_snapshot, result.new_inlined_data, new_tables_result,
		                                            new_inlined_data_tables_result);
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
		auto file_list = GetNewDeleteFiles(commit_state, overwritten_delete_files);
		vector<DuckLakePath> resolved_overwritten_paths;
		resolved_overwritten_paths.reserve(overwritten_delete_files.size());
		for (auto &file : overwritten_delete_files) {
			resolved_overwritten_paths.push_back(GetRelativePath(file.path));
		}
		batch_queries +=
		    DuckLakeMetadataManager::DeleteOverwrittenDeleteFiles(overwritten_delete_files, resolved_overwritten_paths);
		vector<DuckLakePath> resolved_delete_paths;
		resolved_delete_paths.reserve(file_list.size());
		for (auto &file : file_list) {
			resolved_delete_paths.push_back(DuckLakeMetadataManager::GetRelativePath(
			    file.table_id, file.path, new_tables_result, new_schemas_result, context.query_metadata, data_path,
			    separator));
		}
		batch_queries += DuckLakeMetadataManager::WriteNewDeleteFiles(file_list, resolved_delete_paths,
		                                                              context.write_row_group_count);

		// write new inlined deletes (for inlined data tables)
		auto inlined_deletes = GetNewInlinedDeletes(commit_state);
		batch_queries += DuckLakeMetadataManager::WriteNewInlinedDeletes(inlined_deletes);

		// write new inlined file deletes (for parquet files)
		auto inlined_file_deletes = GetNewInlinedFileDeletes(commit_state);
		batch_queries += context.write_inlined_file_deletes(inlined_file_deletes);

		// write compactions
		auto compaction_merge_adjacent_changes =
		    GetCompactionChanges(commit_state, CompactionType::MERGE_ADJACENT_TABLES);
		vector<DuckLakePath> resolved_merge_paths;
		resolved_merge_paths.reserve(compaction_merge_adjacent_changes.compacted_files.size());
		for (auto &compaction : compaction_merge_adjacent_changes.compacted_files) {
			resolved_merge_paths.push_back(GetRelativePath(compaction.path));
		}
		batch_queries +=
		    DuckLakeMetadataManager::WriteCompactions(compaction_merge_adjacent_changes.compacted_files,
		                                              CompactionType::MERGE_ADJACENT_TABLES, resolved_merge_paths);
		batch_queries += write_data_files_sql(compaction_merge_adjacent_changes.new_files);

		auto compaction_rewrite_delete_changes = GetCompactionChanges(commit_state, CompactionType::REWRITE_DELETES);
		batch_queries += write_data_files_sql(compaction_rewrite_delete_changes.new_files);
		// REWRITE_DELETES ignores resolved_paths; pass empty.
		batch_queries += DuckLakeMetadataManager::WriteCompactions(
		    compaction_rewrite_delete_changes.compacted_files, CompactionType::REWRITE_DELETES, vector<DuckLakePath>());

		// Rewriting deletes physically removes deleted rows, so the affected tables can become delete-free
		// again. Recompute their EXACT global stats from the post-rewrite file set so MIN/MAX can once more be
		// answered from metadata (see DuckLakeGetPartitionStats). Safe no-op when a table is not fully delete-free.
		if (!compaction_rewrite_delete_changes.compacted_files.empty()) {
			map<TableIndex, set<DataFileIndex>> removed_source_ids_by_table;
			for (auto &compacted : compaction_rewrite_delete_changes.compacted_files) {
				removed_source_ids_by_table[compacted.table_index].insert(compacted.source_id);
			}
			auto recompute_snapshot = context.get_snapshot();
			for (auto &table_entry : removed_source_ids_by_table) {
				RecomputeGlobalStatsAfterRewrite(batch_queries, table_entry.first, recompute_snapshot,
				                                 compaction_rewrite_delete_changes, table_entry.second, context);
			}
		}
	}

	// Tracking for tables that had schema changes
	set<TableIndex> tables_with_schema_changes;
	for (auto &table_id : transaction_changes.altered_tables) {
		if (!IsTransactionLocal(table_id) &&
		    transaction_changes.altered_tables_with_schema_version_changes.find(table_id) !=
		        transaction_changes.altered_tables_with_schema_version_changes.end()) {
			tables_with_schema_changes.insert(table_id);
		}
	}
	for (auto &new_table : new_tables_result) {
		if (!IsTransactionLocal(new_table.id)) {
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
	// Superseded inlined tables.
	string find_targets_sql = R"(
SELECT idt.table_id, idt.schema_version, idt.table_name
FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt
WHERE idt.schema_version < (
    SELECT MAX(idt2.schema_version)
    FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt2
    WHERE idt2.table_id = idt.table_id
);)";
	auto targets = context.query_metadata(find_targets_sql);
	if (targets->HasError()) {
		targets->GetErrorObject().Throw("Failed to identify superseded inlined-data tables in DuckLake: ");
	}
	// Collect candidates before issuing the per-table emptiness queries on the same connection.
	struct SupersededInlinedTable {
		idx_t table_id;
		idx_t schema_version;
		string table_name;
	};
	vector<SupersededInlinedTable> candidates;
	for (auto &row : *targets) {
		candidates.push_back({row.GetValue<idx_t>(0), row.GetValue<idx_t>(1), row.GetValue<string>(2)});
	}
	string drops_sql;
	for (auto &candidate : candidates) {
		auto count_result = context.query_metadata(
		    StringUtil::Format("SELECT COUNT(*) FROM {METADATA_CATALOG}.%s;", SQLIdentifier(candidate.table_name)));
		if (count_result->HasError()) {
			count_result->GetErrorObject().Throw(
			    "Failed to check emptiness of superseded inlined-data table in DuckLake: ");
		}
		idx_t row_count = 0;
		for (auto &row : *count_result) {
			row_count = row.GetValue<idx_t>(0);
		}
		if (row_count != 0) {
			continue;
		}
		drops_sql += StringUtil::Format(
		    "DELETE FROM {METADATA_CATALOG}.ducklake_inlined_data_tables WHERE table_id=%d AND schema_version=%d;"
		    "DROP TABLE IF EXISTS {METADATA_CATALOG}.%s;",
		    candidate.table_id, candidate.schema_version, SQLIdentifier(candidate.table_name));
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
			batch_queries += CommitChanges(commit_state, attempt_changes, stats, context);
			batch_queries += WriteSnapshotChanges(commit_state, attempt_changes, context.commit_info);
			auto res = context.execute_commit_batch(commit_snapshot, batch_queries);
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to flush changes into DuckLake: ");
			}
			bool flushed_inlined = !flushed_inlined_tables.empty();
			context.flush_cache_if_pending();
			context.commit_connection();
			if (flushed_inlined && !context.skip_drop_empty_inlined) {
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
			// We perform one initial attempt plus up to max_retry_count retries. Since i is the
			// zero-based attempt index, we are done retrying once i reaches max_retry_count.
			bool finished_retrying = i >= retry_config.max_retry_count;
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
