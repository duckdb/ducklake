#include "storage/ducklake_transaction.hpp"

#include "storage/ducklake_commit_state.hpp"
#include "storage/ducklake_transaction_state.hpp"
#include "common/ducklake_types.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/sql_identifier.hpp"
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
#include "duckdb/logging/logger.hpp"
#include "storage/ducklake_log_type.hpp"
#include "duckdb/main/client_context.hpp"
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

void LocalTableChanges::Clear() {
	lock_guard<mutex> guard(lock);
	changes.clear();
}

bool LocalTableChanges::HasChanges() const {
	lock_guard<mutex> guard(lock);
	return !changes.empty();
}

void LocalTableChanges::CleanupFiles(DatabaseInstance &db) {
	auto &fs = FileSystem::GetFileSystem(db);
	lock_guard<mutex> guard(lock);
	for (auto &entry : changes) {
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

bool LocalTableChanges::HasTransactionLocalInserts(TableIndex table_id) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return false;
	}
	auto &table_changes = entry->second;
	return !table_changes.new_data_files.empty() || table_changes.new_inlined_data;
}

bool LocalTableChanges::HasTransactionInlinedData(TableIndex table_id) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return false;
	}
	auto &table_changes = entry->second;
	return table_changes.new_inlined_data != nullptr;
}

vector<DuckLakeDataFile> LocalTableChanges::GetTransactionLocalFiles(TableIndex table_id) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return vector<DuckLakeDataFile>();
	}
	return entry->second.new_data_files;
}

shared_ptr<DuckLakeInlinedData> LocalTableChanges::GetTransactionLocalInlinedData(ClientContext &context,
                                                                                  TableIndex table_id) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return nullptr;
	}
	auto &table_changes = entry->second;
	if (!table_changes.new_inlined_data) {
		return nullptr;
	}
	auto &inlined = *table_changes.new_inlined_data;
	auto result = make_shared_ptr<DuckLakeInlinedData>();
	result->data = make_uniq<ColumnDataCollection>(context, inlined.data->Types());
	for (auto &chunk : inlined.data->Chunks()) {
		result->data->Append(chunk);
	}
	result->row_ids = inlined.row_ids;
	return result;
}

void LocalTableChanges::DropTransactionLocalFile(ClientContext &context, TableIndex table_id, const string &path) {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		throw InternalException(
		    "DropTransactionLocalFile called for a table for which no transaction-local files exist");
	}
	auto &table_changes = entry->second;
	auto &table_files = table_changes.new_data_files;
	auto &fs = FileSystem::GetFileSystem(context);
	for (idx_t i = 0; i < table_files.size(); i++) {
		auto &file = table_files[i];
		if (file.file_name == path) {
			auto created_by_ducklake = file.created_by_ducklake;
			for (auto &del_file : file.delete_files) {
				fs.RemoveFile(del_file.file_name);
			}
			file.delete_files.clear();
			// found the file - delete it from the table list and from disk if DuckLake owns it
			table_files.erase_at(i);
			if (created_by_ducklake) {
				fs.RemoveFile(path);
			}
			if (table_changes.IsEmpty()) {
				// no more files remaining
				changes.erase(entry);
			}
			return;
		}
	}
	throw InternalException("Failed to find matching transaction-local file for DropTransactionLocalFile");
}

void LocalTableChanges::AppendFiles(TableIndex table_id, vector<DuckLakeDataFile> files) {
	lock_guard<mutex> guard(lock);
	auto &table_changes = changes[table_id];
	if (table_changes.new_data_files.empty()) {
		// If empty, just move the entire vector
		table_changes.new_data_files = std::move(files);
	} else {
		// Reserve to avoid reallocations during insertion
		table_changes.new_data_files.reserve(table_changes.new_data_files.size() + files.size());
		// Use move_iterator for efficient batch move
		table_changes.new_data_files.insert(table_changes.new_data_files.end(), std::make_move_iterator(files.begin()),
		                                    std::make_move_iterator(files.end()));
	}
}

void LocalTableChanges::AppendDeleteFiles(TableIndex table_id, const string &data_file_path,
                                          vector<DuckLakeDeleteFile> files) {
	if (files.empty()) {
		return;
	}
	lock_guard<mutex> guard(lock);
	auto &table_changes = changes[table_id];
	auto &entry = table_changes.new_delete_files[data_file_path];
	for (auto &f : files) {
		entry.push_back(std::move(f));
	}
}

void LocalTableChanges::AppendInlinedData(ClientContext &context, TableIndex table_id,
                                          unique_ptr<DuckLakeInlinedData> new_data) {
	lock_guard<mutex> guard(lock);
	auto &table_changes = changes[table_id];
	if (table_changes.new_inlined_data) {
		// already exists - append
		auto &existing_data = *table_changes.new_inlined_data;
		auto &existing_types = existing_data.data->Types();
		auto &new_types = new_data->data->Types();
		// check if types changed (e.g. due to ALTER COLUMN TYPE)
		if (existing_types != new_types) {
			// if types differ we gotta add a cast.
			auto casted_data = make_uniq<ColumnDataCollection>(context, new_types);
			ColumnDataAppendState append_state;
			casted_data->InitializeAppend(append_state);
			for (auto &chunk : existing_data.data->Chunks()) {
				DataChunk casted_chunk;
				casted_chunk.Initialize(context, new_types);
				for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
					if (existing_types[col_idx] != new_types[col_idx]) {
						VectorOperations::Cast(context, chunk.data[col_idx], casted_chunk.data[col_idx], chunk.size());
					} else {
						casted_chunk.data[col_idx].Reference(chunk.data[col_idx]);
					}
				}
				casted_chunk.SetChildCardinality(chunk.size());
				casted_data->Append(append_state, casted_chunk);
			}
			existing_data.data = std::move(casted_data);
		}
		ColumnDataAppendState append_state;
		existing_data.data->InitializeAppend(append_state);
		for (auto &chunk : new_data->data->Chunks()) {
			existing_data.data->Append(chunk);
		}
		// merge preserved row_ids from update inlining
		existing_data.MergeRowIds(*new_data, new_data->data->Count());
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

void LocalTableChanges::AddNewInlinedDeletes(TableIndex table_id, const string &table_name, set<idx_t> new_deletes) {
	lock_guard<mutex> guard(lock);
	auto &table_changes = changes[table_id];
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

void LocalTableChanges::DeleteFromLocalInlinedData(ClientContext &context, TableIndex table_id,
                                                   set<idx_t> new_deletes) {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		throw InternalException("DeleteFromLocalInlinedData called but no transaction-local data exists for table");
	}
	auto &table_changes = entry->second;
	auto &inlined_data = *table_changes.new_inlined_data;
	auto &existing = *inlined_data.data;
	// construct a new collection from the existing data minus the deletes
	auto new_data = make_uniq<ColumnDataCollection>(context, existing.Types());

	idx_t base_row_id = 0;
	vector<int64_t> new_row_ids;
	ColumnDataAppendState append_state;
	new_data->InitializeAppend(append_state);
	for (auto &chunk : existing.Chunks()) {
		// slice out non-deleted rows
		SelectionVector sel(chunk.size());
		idx_t selected_rows = 0;

		for (idx_t r = 0; r < chunk.size(); r++) {
			idx_t position = base_row_id + r;
			auto row_id = inlined_data.GetRowId(position);
			if (new_deletes.find(row_id) != new_deletes.end()) {
				// deleted - skip
				continue;
			}
			sel.set_index(selected_rows++, r);
			new_row_ids.push_back(inlined_data.GetOutputRowId(position));
		}
		base_row_id += chunk.size();
		if (selected_rows == 0) {
			continue;
		}
		chunk.Slice(sel, selected_rows);
		new_data->Append(append_state, chunk);
	}

	// override the existing collection and row_ids
	inlined_data.data = std::move(new_data);
	inlined_data.row_ids = std::move(new_row_ids);
}

static void RemoveFieldStats(map<FieldIndex, DuckLakeColumnStats> &column_stats, const DuckLakeFieldId &field_id) {
	column_stats.erase(field_id.GetFieldIndex());
	for (auto &child_id : field_id.Children()) {
		RemoveFieldStats(column_stats, *child_id);
	}
}

void LocalTableChanges::AddColumnToLocalInlinedData(ClientContext &context, TableIndex table_id,
                                                    const LogicalType &new_column_type, FieldIndex new_field_index,
                                                    const Value &default_value) {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
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

	auto new_data = make_uniq<ColumnDataCollection>(context, new_types);

	ColumnDataAppendState append_state;
	new_data->InitializeAppend(append_state);

	bool has_default = !default_value.IsNull();

	for (auto &chunk : existing.Chunks()) {
		DataChunk new_chunk;
		new_chunk.Initialize(context, new_types);

		// Copy existing columns
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			new_chunk.data[col_idx].Reference(chunk.data[col_idx]);
		}

		// New column: use default value or NULL
		auto &new_col_vector = new_chunk.data[chunk.ColumnCount()];
		if (has_default) {
			new_col_vector.Reference(default_value, count_t(chunk.size()));
		} else {
			new_col_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			FlatVector::SetSize(new_col_vector, chunk.size());
			ConstantVector::SetNull(new_col_vector, true);
		}

		new_chunk.SetChildCardinality(chunk.size());
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
		if (total_rows > 0) {
			new_col_stats.any_valid = true;
			auto default_str = default_value.ToString();
			new_col_stats.has_min = true;
			new_col_stats.min = default_str;
			new_col_stats.has_max = true;
			new_col_stats.max = std::move(default_str);
		} else {
			new_col_stats.any_valid = false;
		}
	} else {
		new_col_stats.null_count = total_rows;
		new_col_stats.has_null_count = true;
		new_col_stats.any_valid = false;
	}

	table_changes.new_inlined_data->column_stats.emplace(new_field_index, std::move(new_col_stats));
	table_changes.new_inlined_data->data = std::move(new_data);
}

void LocalTableChanges::RemoveColumnFromLocalInlinedData(ClientContext &context, TableIndex table_id,
                                                         LogicalIndex removed_column_index,
                                                         const DuckLakeFieldId &field_id) {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
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

	auto new_data = make_uniq<ColumnDataCollection>(context, new_types);

	ColumnDataAppendState append_state;
	new_data->InitializeAppend(append_state);

	for (auto &chunk : existing.Chunks()) {
		DataChunk new_chunk;
		new_chunk.Initialize(context, new_types);

		idx_t new_col_idx = 0;
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			if (col_idx == removed_column_index.index) {
				continue;
			}
			new_chunk.data[new_col_idx].Reference(chunk.data[col_idx]);
			new_col_idx++;
		}

		new_chunk.SetChildCardinality(chunk.size());
		new_data->Append(append_state, new_chunk);
	}

	// Remove stats for the dropped field and all its children
	RemoveFieldStats(table_changes.new_inlined_data->column_stats, field_id);

	table_changes.new_inlined_data->data = std::move(new_data);
}

optional_ptr<DuckLakeInlinedDataDeletes> LocalTableChanges::GetInlinedDeletes(TableIndex table_id,
                                                                              const string &table_name) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return nullptr;
	}
	auto &table_changes = entry->second;
	auto delete_entry = table_changes.new_inlined_data_deletes.find(table_name);
	if (delete_entry == table_changes.new_inlined_data_deletes.end()) {
		return nullptr;
	}
	return delete_entry->second.get();
}

void LocalTableChanges::AddNewInlinedFileDeletes(TableIndex table_id, idx_t file_id, set<idx_t> new_deletes) {
	if (new_deletes.empty()) {
		return;
	}
	lock_guard<mutex> guard(lock);
	auto &table_changes = changes[table_id];
	if (!table_changes.new_inlined_file_deletes) {
		table_changes.new_inlined_file_deletes = make_uniq<DuckLakeInlinedFileDeletes>();
	}
	auto &file_deletes = table_changes.new_inlined_file_deletes->file_deletes[file_id];
	for (auto &row_id : new_deletes) {
		file_deletes.insert(row_id);
	}
}

void LocalTableChanges::AddCompaction(TableIndex table_id, DuckLakeCompactionEntry entry) {
	lock_guard<mutex> guard(lock);
	auto &table_changes = changes[table_id];
	table_changes.compactions.push_back(std::move(entry));
}

bool LocalTableChanges::HasLocalDeletes(TableIndex table_id) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return false;
	}
	return !entry->second.new_delete_files.empty();
}

bool LocalTableChanges::HasAnyLocalChanges(TableIndex table_id) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry != changes.end() && !entry->second.IsEmpty()) {
		return true;
	}
	return false;
}

bool LocalTableChanges::HasLocalDeleteForFile(TableIndex table_id, const string &path) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return false;
	}
	auto &table_changes = entry->second;
	auto file_entry = table_changes.new_delete_files.find(path);
	return file_entry != table_changes.new_delete_files.end() && !file_entry->second.empty();
}

void LocalTableChanges::GetLocalDeleteForFile(TableIndex table_id, const string &path, DuckLakeFileData &result) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
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
	result.format = delete_file.format;
}

bool LocalTableChanges::HasLocalInlinedFileDeletes(TableIndex table_id) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		return false;
	}
	auto &table_changes = entry->second;
	if (!table_changes.new_inlined_file_deletes) {
		return false;
	}
	return !table_changes.new_inlined_file_deletes->file_deletes.empty();
}

void LocalTableChanges::GetLocalInlinedFileDeletesForFile(TableIndex table_id, idx_t file_id,
                                                          set<idx_t> &result) const {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
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

void LocalTableChanges::TransactionLocalDelete(ClientContext &context, TableIndex table_id,
                                               const string &data_file_path, DuckLakeDeleteFile delete_file) {
	lock_guard<mutex> guard(lock);
	auto entry = changes.find(table_id);
	if (entry == changes.end()) {
		throw InternalException(
		    "Transaction local delete called for table which does not have transaction local insertions");
	}
	auto &table_changes = entry->second;
	for (auto &file : table_changes.new_data_files) {
		if (file.file_name == data_file_path) {
			if (!file.delete_files.empty()) {
				auto &fs = FileSystem::GetFileSystem(context);
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

void LocalTableChanges::CleanupFiles(ClientContext &context, TableIndex table_id) {
	lock_guard<mutex> guard(lock);
	auto table_entry = changes.find(table_id);
	if (table_entry != changes.end()) {
		auto &table_changes = table_entry->second;
		auto &fs = FileSystem::GetFileSystem(context);
		for (auto &file : table_changes.new_data_files) {
			if (file.created_by_ducklake) {
				fs.RemoveFile(file.file_name);
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
		changes.erase(table_entry);
	}
}

void LocalTableChanges::AddDeletesToMap(ClientContext &context, vector<DuckLakeDeleteFile> new_deletes,
                                        unordered_map<string, vector<DuckLakeDeleteFile>> &table_delete_map) {
	for (auto &file : new_deletes) {
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
				auto &fs = FileSystem::GetFileSystem(context);
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

void LocalTableChanges::AddDeletes(ClientContext &context, TableIndex table_id, vector<DuckLakeDeleteFile> files) {
	if (files.empty()) {
		return;
	}
	lock_guard<mutex> guard(lock);
	auto &table_changes = changes[table_id];
	auto &table_delete_map = table_changes.new_delete_files;
	LocalTableChanges::AddDeletesToMap(context, std::move(files), table_delete_map);
}

LocalTableChangeIterationHelper::LocalTableChangeIterationHelper(
    mutex &local_changes_lock, const map<TableIndex, LocalTableDataChanges> &changes_p)
    : lock(local_changes_lock), changes(changes_p) {
}

LocalTableChangeIterationHelper::LocalTableChangeIteratorEntry::LocalTableChangeIteratorEntry() {
}

TableIndex LocalTableChangeIterationHelper::LocalTableChangeIteratorEntry::GetTableIndex() const {
	return table_id;
}

const LocalTableDataChanges &LocalTableChangeIterationHelper::LocalTableChangeIteratorEntry::GetTableChanges() const {
	return *changes;
}

LocalTableChangeIterationHelper::LocalTableChangeIterator::LocalTableChangeIterator(
    map<TableIndex, LocalTableDataChanges>::const_iterator it_p,
    map<TableIndex, LocalTableDataChanges>::const_iterator end_it_p)
    : it(std::move(it_p)), end_it(std::move(end_it_p)) {
	if (it != end_it) {
		entry.table_id = it->first;
		entry.changes = it->second;
	}
}

LocalTableChangeIterationHelper::LocalTableChangeIterator &
LocalTableChangeIterationHelper::LocalTableChangeIterator::operator++() {
	it++;
	if (it != end_it) {
		entry.table_id = it->first;
		entry.changes = it->second;
	}
	return *this;
}

bool LocalTableChangeIterationHelper::LocalTableChangeIterator::operator!=(
    const LocalTableChangeIterator &other) const {
	return it != other.it;
}

const LocalTableChangeIterationHelper::LocalTableChangeIteratorEntry &
LocalTableChangeIterationHelper::LocalTableChangeIterator::operator*() const {
	return entry;
}

LocalTableChangeIterationHelper LocalTableChanges::Changes() const {
	return LocalTableChangeIterationHelper(lock, changes);
}

DuckLakeTransaction::DuckLakeTransaction(DuckLakeCatalog &ducklake_catalog, TransactionManager &manager,
                                         ClientContext &context)
    : Transaction(manager, context), ducklake_catalog(ducklake_catalog), db(*context.db),
      local_catalog_id(DuckLakeConstants::TRANSACTION_LOCAL_ID_START), catalog_version(0) {
	metadata_manager = DuckLakeMetadataManager::Create(*this);
	state = make_uniq<DuckLakeTransactionState>(db, ducklake_catalog.IsCommitInfoRequired(), new_name_maps,
	                                            ducklake_catalog.DataPath(), ducklake_catalog.Separator());
}

DuckLakeTransaction::~DuckLakeTransaction() {
}

const LocalTableChanges &DuckLakeTransaction::GetLocalChanges() const {
	return state->local_changes;
}
const set<TableIndex> &DuckLakeTransaction::GetDroppedTables() {
	return state->dropped_tables;
}
const set<TableIndex> &DuckLakeTransaction::GetDroppedViews() {
	return state->dropped_views;
}
const set<MacroIndex> &DuckLakeTransaction::GetDroppedScalarMacros() {
	return state->dropped_scalar_macros;
}
const set<MacroIndex> &DuckLakeTransaction::GetDroppedTableMacros() {
	return state->dropped_table_macros;
}
const set<TableIndex> &DuckLakeTransaction::GetRenamedTables() {
	return state->renamed_tables;
}
const case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> &DuckLakeTransaction::GetNewTables() {
	return state->new_tables;
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
	state->local_changes.Clear();
	SetRequiresNewInlinedTable(false);
}

void DuckLakeTransaction::Rollback() {
	if (connection) {
		// rollback any changes made to the metadata catalog
		connection->Rollback();
		connection.reset();
	}
	state->CleanupFiles();
	state->local_changes.Clear();
	SetRequiresNewInlinedTable(false);
}

Connection &DuckLakeTransaction::GetConnection() {
	lock_guard<mutex> lock(connection_lock);
	if (!connection) {
		connection = make_uniq<Connection>(db);
		auto caller_context = context.lock();
		if (caller_context) {
			DuckLakeUtil::CopyExtensionSettings(*caller_context, *connection->context);
		}
		// set the search path to the metadata catalog
		auto &client_data = ClientData::Get(*connection->context);
		// ensure we are only looking in the ducklake catalog schema during querying
		CatalogSearchEntry metadata_entry(Identifier(ducklake_catalog.MetadataDatabaseName()),
		                                  Identifier(ducklake_catalog.MetadataSchemaName()));
		if (metadata_entry.schema.empty()) {
			metadata_entry.schema = "main";
		}
		client_data.catalog_search_path->Set(metadata_entry, CatalogSetPathType::SET_DIRECTLY);

		// set max error reporting to 0 so that during error reporting we don't traverse other schemas / catalogs
		auto &client_config = ClientConfig::GetConfig(*connection->context);
		client_config.user_settings.SetUserSetting(CatalogErrorMaxSchemasSetting::SettingIndex, Value::UBIGINT(0));
		// FIXME: disable postgres_scanner experimental filter pushdown for metadata queries
		// it does not support all filter types DuckDB may push down (e.g. EXPRESSION_FILTER)
		auto &metadata_type = ducklake_catalog.MetadataType();
		if (metadata_type == "postgres" || metadata_type == "postgres_scanner") {
			connection->Query("SET pg_experimental_filter_pushdown=false");
		}
		connection->BeginTransaction();
		connection->Query("SET current_transaction_invalidation_policy='SYNTACTIC_ERRORS_DO_NOT_INVALIDATE'");
	}
	return *connection;
}

case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> &DuckLakeTransaction::GetNewMacroMap(CatalogType type) {
	switch (type) {
	case CatalogType::MACRO_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return state->new_scalar_macros;
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return state->new_table_macros;
	default:
		throw InternalException("Unsupported catalog type for GetNewMacroMap");
	}
}

bool DuckLakeTransaction::ChangesMade() const {
	return state->SchemaChangesMade() || state->local_changes.HasChanges() || !state->dropped_files.empty() ||
	       !new_name_maps.name_maps.empty();
}

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
			if (!IsTransactionLocal(table_id)) {
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
			if (!IsTransactionLocal(table_id)) {
				changes.altered_tables.insert(table_id);
			}
			break;
		}
		case LocalChangeType::NONE:
		case LocalChangeType::CREATED:
		case LocalChangeType::RENAMED: {
			// write any new tables that we created
			auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
			changes.created_tables[schema.name.GetIdentifierName()].insert(table);
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
			if (!IsTransactionLocal(view_id)) {
				changes.altered_views.insert(view_id);
			}
			break;
		}
		case LocalChangeType::NONE:
		case LocalChangeType::CREATED:
		case LocalChangeType::RENAMED: {
			// write any new view that we created
			auto &schema = view.ParentSchema().Cast<DuckLakeSchemaEntry>();
			changes.created_tables[schema.name.GetIdentifierName()].insert(view);
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

TransactionChangeInformation DuckLakeTransaction::GetTransactionChanges() const {
	auto &dropped_tables = state->dropped_tables;
	auto &dropped_views = state->dropped_views;
	auto &dropped_scalar_macros = state->dropped_scalar_macros;
	auto &dropped_table_macros = state->dropped_table_macros;
	auto &dropped_schemas = state->dropped_schemas;
	auto &new_schemas = state->new_schemas;
	auto &new_scalar_macros = state->new_scalar_macros;
	auto &new_table_macros = state->new_table_macros;
	auto &new_tables = state->new_tables;
	auto &tables_deleted_from = state->tables_deleted_from;
	auto &local_changes = state->local_changes;

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
			changes.created_schemas.insert(schema_entry.name.GetIdentifierName());
		}
	}
	for (auto &schema_entry : new_scalar_macros) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			auto &macro = *entry.second;
			auto &schema = macro.ParentSchema().Cast<DuckLakeSchemaEntry>();
			changes.created_scalar_macros[schema.name.GetIdentifierName()].insert(macro);
		}
	}
	for (auto &schema_entry : new_table_macros) {
		for (auto &entry : schema_entry.second->GetEntries()) {
			auto &macro = *entry.second;
			auto &schema = macro.ParentSchema().Cast<DuckLakeSchemaEntry>();
			changes.created_table_macros[schema.name.GetIdentifierName()].insert(macro);
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
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		if (IsTransactionLocal(table_id.index)) {
			// don't report transaction-local tables yet - these will get added later on
			continue;
		}
		auto &table_changes = entry.GetTableChanges();
		AddTableChanges(table_id, table_changes, changes);
	}
	return changes;
}

// DuckLakeCommitState is defined in storage/ducklake_commit_state.hpp

idx_t DuckLakeCommitState::AllocateCatalogId() const {
	idx_t id = context.allocate_catalog_id(commit_snapshot.next_catalog_id);
	if (id + 1 > commit_snapshot.next_catalog_id) {
		commit_snapshot.next_catalog_id = id + 1;
	}
	return id;
}

idx_t DuckLakeCommitState::AllocateFileId() const {
	idx_t id = context.allocate_file_id(commit_snapshot.next_file_id);
	if (id + 1 > commit_snapshot.next_file_id) {
		commit_snapshot.next_file_id = id + 1;
	}
	return id;
}

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

DuckLakePartitionInfo DuckLakeTransaction::GetNewPartitionKey(DuckLakeCommitState &commit_state,
                                                              DuckLakeTableEntry &table) {
	DuckLakePartitionInfo partition_key;
	partition_key.table_id = commit_state.GetTableId(table);
	if (IsTransactionLocal(partition_key.table_id.index)) {
		throw InternalException("Trying to write partition with transaction local table-id");
	}
	// insert the new partition data
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		// dropping partition data - insert the empty partition key data for this table
		return partition_key;
	}
	// key the remap off the original transaction-local id: a previous failed commit attempt may have
	// already overwritten partition_id, while data files still reference the transaction-local id
	auto local_partition_id = partition_data->local_partition_id.IsValid()
	                              ? partition_data->local_partition_id.GetIndex()
	                              : partition_data->partition_id;
	auto partition_id = commit_state.AllocateCatalogId();
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
		case DuckLakeTransformType::BUCKET:
			partition_field.transform = StringUtil::Format("bucket(%d)", field.transform.bucket_count);
			break;
		default:
			throw NotImplementedException("Unimplemented transform type for partition");
		}
		partition_key.fields.push_back(std::move(partition_field));
	}
	commit_state.committed_partition_ids[local_partition_id] = partition_id;
	return partition_key;
}

DuckLakeSortInfo DuckLakeTransaction::GetNewSortKey(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table) {
	DuckLakeSortInfo sort_key;
	sort_key.table_id = commit_state.GetTableId(table);
	if (IsTransactionLocal(sort_key.table_id.index)) {
		throw InternalException("Trying to write sort with transaction local table-id");
	}

	// insert the new sort data
	auto sort_data = table.GetSortData();
	if (!sort_data) {
		// dropping sort data - insert the empty sort key data for this table
		return sort_key;
	}

	auto sort_id = commit_state.AllocateCatalogId();
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
		auto col_info = DuckLakeTableEntry::ConvertColumn(col.GetName().GetIdentifierName(), col.GetType(),
		                                                  GetFieldId(col.Physical()));
		if (not_null_fields.count(col.GetName().GetIdentifierName())) {
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
	table_entry.name = name.GetIdentifierName();
	table_entry.path = DataPath();
	return table_entry;
}

DuckLakeTableInfo DuckLakeTransaction::GetNewTable(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table) {
	auto table_entry = table.GetTableInfo();
	auto original_id = table_entry.id;
	bool is_new_table;
	if (IsTransactionLocal(original_id.index)) {
		table_entry.id = TableIndex(commit_state.AllocateCatalogId());
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

DuckLakeViewInfo DuckLakeTransaction::GetNewView(DuckLakeCommitState &commit_state, DuckLakeViewEntry &view) {
	auto &schema = view.ParentSchema().Cast<DuckLakeSchemaEntry>();
	DuckLakeViewInfo view_entry;
	auto original_id = view.GetViewId();
	if (IsTransactionLocal(original_id.index)) {
		view_entry.id = TableIndex(commit_state.AllocateCatalogId());
	} else {
		// this view already has an id - keep it
		// this happens if e.g. this view is renamed
		view_entry.id = original_id;
	}
	view_entry.uuid = view.GetViewUUID();
	view_entry.schema_id = commit_state.GetSchemaId(schema);
	view_entry.name = view.name.GetIdentifierName();
	view_entry.dialect = "duckdb";
	view_entry.sql = view.GetQuerySQL();
	view_entry.column_aliases = IdentifiersToStrings(view.aliases);
	return view_entry;
}

DuckLakeGlobalStatsInfo DuckLakeTransaction::ConvertNewGlobalStats(TableIndex table_id,
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
	return stats;
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

DuckLakeFileInfo DuckLakeTransaction::BuildDataFileInfo(const DuckLakeDataFile &file, DuckLakeCommitState &commit_state,
                                                        TableIndex table_id, optional_idx row_id_start) {
	DuckLakeFileInfo data_file;
	data_file.id = DataFileIndex(commit_state.AllocateFileId());
	data_file.table_id = table_id;
	data_file.file_name = file.file_name;
	data_file.row_count = file.row_count;
	data_file.file_size_bytes = file.file_size_bytes;
	data_file.footer_size = file.footer_size;
	data_file.row_group_count = file.row_group_count;
	data_file.partition_id = file.partition_id;
	data_file.encryption_key = file.encryption_key;
	data_file.row_id_start = row_id_start;
	data_file.mapping_id = file.mapping_id;
	data_file.begin_snapshot = file.begin_snapshot;
	data_file.max_partial_file_snapshot = file.max_partial_file_snapshot;
	data_file.column_stats = file.column_stats;
	for (auto &partition_entry : file.partition_values) {
		DuckLakeFilePartitionInfo partition_info;
		partition_info.partition_column_idx = partition_entry.partition_column_idx;
		partition_info.partition_value = partition_entry.partition_value;
		data_file.partition_values.push_back(std::move(partition_info));
	}
	return data_file;
}

DuckLakeDeleteFileInfo DuckLakeTransaction::GetNewDeleteFile(TableIndex table_id,
                                                             const DuckLakeCommitState &commit_state,
                                                             const DuckLakeDeleteFile &file) {
	DuckLakeDeleteFileInfo delete_file;
	delete_file.id = DataFileIndex(commit_state.AllocateFileId());
	delete_file.table_id = table_id;
	delete_file.data_file_id = file.data_file_id;
	delete_file.path = file.file_name;
	delete_file.format = file.format;
	delete_file.delete_count = file.delete_count;
	delete_file.file_size_bytes = file.file_size_bytes;
	delete_file.footer_size = file.footer_size;
	delete_file.row_group_count = file.row_group_count;
	delete_file.encryption_key = file.encryption_key;
	delete_file.begin_snapshot = file.begin_snapshot;
	delete_file.max_snapshot = file.max_snapshot;
	return delete_file;
}

bool DuckLakeTransaction::RetryOnError(const string &original_message) {
	return RetryOnError(original_message, ExceptionType::INVALID);
}

bool DuckLakeTransaction::RetryOnError(const string &original_message, ExceptionType type) {
	// DuckLake's own logical conflicts are thrown as TransactionException; classify those by type,
	// matching DuckDB core's MVCC handling. Backend (postgres) errors cross back untyped, so the
	// checks below fall back to the message string.
	if (type == ExceptionType::TRANSACTION) {
		return true;
	}
	auto message = StringUtil::Lower(original_message);
	if (StringUtil::Contains(message, "primary key") || StringUtil::Contains(message, "unique")) {
		return true;
	}
	if (StringUtil::Contains(message, "conflict")) {
		return true;
	}
	if (StringUtil::Contains(message, "concurrent")) {
		return true;
	}
	// postgres serialization failure (SQLSTATE 40001)
	if (StringUtil::Contains(message, "could not serialize")) {
		return true;
	}
	// deadlock (postgres 40P01 / duckdb): a victim is rolled back, retry can succeed
	if (StringUtil::Contains(message, "deadlock")) {
		return true;
	}
	// connection dropped mid-commit: outcome unknown, re-attempt under the conflict check
	if (StringUtil::Contains(message, "server closed the connection") ||
	    StringUtil::Contains(message, "connection to server") || StringUtil::Contains(message, "connection reset")) {
		return true;
	}
	return false;
}

DuckLakeRetryConfig DuckLakeRetryConfig::FromContext(ClientContext &context) {
	DuckLakeRetryConfig config;
	Value setting_val;
	if (context.TryGetCurrentSetting("ducklake_max_retry_count", setting_val)) {
		config.max_retry_count = setting_val.GetValue<idx_t>();
	}
	if (context.TryGetCurrentSetting("ducklake_retry_wait_ms", setting_val)) {
		config.retry_wait_ms = setting_val.GetValue<idx_t>();
	}
	if (context.TryGetCurrentSetting("ducklake_retry_backoff", setting_val)) {
		config.retry_backoff = setting_val.GetValue<double>();
	}
	return config;
}

void DuckLakeTransaction::FlushChanges() {
	if (!ChangesMade()) {
		return;
	}
	auto retry_config = DuckLakeRetryConfig::FromContext(*context.lock());
	auto transaction_changes = GetTransactionChanges();
	if (metadata_manager->CanSkipSnapshotFetch(transaction_changes)) {
		lock_guard<mutex> guard(snapshot_lock);
		if (snapshot) {
			metadata_manager->FlushChangesServerSide(*this, *snapshot, transaction_changes, retry_config);
		} else {
			DuckLakeSnapshot sentinel;
			metadata_manager->FlushChangesServerSide(*this, sentinel, transaction_changes, retry_config);
		}
	} else {
		auto transaction_snapshot = GetSnapshot();
		if (metadata_manager->ExecuteRetrialsServerSide()) {
			metadata_manager->FlushChangesServerSide(*this, transaction_snapshot, transaction_changes, retry_config);
		} else {
			RunCommitLoop(transaction_snapshot, transaction_changes, retry_config);
		}
	}
}

void DuckLakeTransaction::ApplyServerSideCommit(idx_t schema_version) {
	if (snapshot) {
		for (auto &entry : state->dropped_file_stats) {
			ducklake_catalog.InvalidateTableStatsCache(snapshot->next_file_id, entry.first);
		}
	}
	catalog_version = schema_version;
	if (connection) {
		connection->Commit();
	}
}

void DuckLakeTransaction::DropEmptySupersededInlinedTablesClientSide() {
	DuckLakeCommitContext context;
	context.query_metadata = [&](string q) {
		return metadata_manager->Query(q);
	};
	context.invalidate_schema_cache = [&](idx_t schema_version) {
		ducklake_catalog.InvalidateSchemaCache(schema_version);
	};
	DuckLakeTransactionState::DropEmptySupersededInlinedTables(context);
}

void DuckLakeTransaction::RunCommitLoop(DuckLakeSnapshot transaction_snapshot,
                                        const TransactionChangeInformation &transaction_changes,
                                        const DuckLakeRetryConfig &retry_config) {
	DuckLakeCommitContext context;
	context.conflict_query_executor = [&](string q) -> unique_ptr<QueryResult> {
		auto result = metadata_manager->Query(transaction_snapshot, q);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to commit DuckLake transaction - failed to get snapshot and "
			                               "snapshot changes for conflict resolution:");
		}
		return result;
	};
	context.snapshot_and_stats_query = [&]() {
		return metadata_manager->GetSnapshotAndStatsAndChangesQuery();
	};
	context.get_snapshot = [&]() {
		return GetSnapshot();
	};
	context.allocate_snapshot_id = [&](idx_t current) {
		return metadata_manager->AllocateNextSnapshotId(current);
	};
	context.allocate_catalog_id = [&](idx_t current) {
		return metadata_manager->AllocateNextCatalogId(current);
	};
	context.allocate_file_id = [&](idx_t current) {
		return metadata_manager->AllocateNextFileId(current);
	};
	context.allocate_schema_version = [&](idx_t current) {
		return metadata_manager->AllocateNextSchemaVersion(current);
	};
	context.acquire_commit_lock = [&](const TransactionChangeInformation &changes) {
		metadata_manager->AcquireCommitLock(changes);
	};
	context.execute_commit_batch = [&](DuckLakeSnapshot snapshot, string &query) {
		return metadata_manager->Execute(snapshot, query);
	};
	context.flush_cache_if_pending = [&]() {
		if (metadata_manager->TakePendingCacheClear()) {
			metadata_manager->ClearCache();
		}
	};
	context.commit_connection = [&]() {
		connection->Commit();
	};
	context.try_rollback = [&]() {
		if (connection->context->transaction.HasActiveTransaction()) {
			connection->Rollback();
		}
	};
	context.prepare_retry = [&]() {
		metadata_manager->ClearInlinedTableCaches();
		connection->BeginTransaction();
		snapshot.reset();
	};
	context.query_metadata = [&](string q) {
		return metadata_manager->Query(q);
	};
	context.query_metadata_with_snapshot = [&](DuckLakeSnapshot s, string q) {
		return metadata_manager->Query(s, q);
	};
	context.try_append_data_files = [&](DuckLakeSnapshot &snapshot, const vector<DuckLakeFileInfo> &files,
	                                    const vector<DuckLakeTableInfo> &new_tables,
	                                    vector<DuckLakeSchemaInfo> &new_schemas) {
		return metadata_manager->TryAppendDataFiles(snapshot, files, new_tables, new_schemas);
	};
	context.write_inlined_tables = [&](DuckLakeSnapshot snapshot, const vector<DuckLakeTableInfo> &tables) {
		return metadata_manager->WriteNewInlinedTables(snapshot, tables);
	};
	context.write_inlined_file_deletes = [&](const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes) {
		return metadata_manager->WriteNewInlinedFileDeletesSqlBatch(new_deletes);
	};
	context.write_inlined_data = [&](DuckLakeSnapshot &snapshot, const vector<DuckLakeInlinedDataInfo> &new_data,
	                                 const vector<DuckLakeTableInfo> &new_tables,
	                                 const vector<DuckLakeTableInfo> &new_inlined_data_tables_result) {
		return metadata_manager->WriteNewInlinedData(snapshot, new_data, new_tables, new_inlined_data_tables_result);
	};
	context.get_table_stats = [&](TableIndex table_id) {
		return ducklake_catalog.GetTableStats(*this, table_id);
	};
	context.get_table_column_schema = [&](TableIndex table_id) {
		// The full flattened schema at the commit snapshot: top-level roots (is_root=true) plus every nested
		// struct/list/map/array leaf (is_root=false). Roots carry the inlined-data merge (TryMergeInlinedStats
		// references columns by name and bails on any non-scalar root); leaves carry their own per-file stats keyed
		// by leaf FieldIndex, which the rewrite recompute must merge. Each node uses its authoritative stored
		// FieldIndex (ALTER ADD FIELD makes nested leaf ids non-contiguous, so they cannot be re-derived).
		vector<DuckLakeColumnSchemaEntry> schema;
		auto entry = ducklake_catalog.GetEntryById(*this, transaction_snapshot, table_id);
		if (!entry) {
			return schema;
		}
		std::function<void(const DuckLakeFieldId &, bool)> add_field = [&](const DuckLakeFieldId &field, bool is_root) {
			schema.push_back({field.GetFieldIndex(), field.Name(), field.Type(), is_root});
			for (auto &child : field.Children()) {
				add_field(*child, false);
			}
		};
		for (auto &field : entry->Cast<DuckLakeTableEntry>().GetFieldData().GetFieldIds()) {
			add_field(*field, true);
		}
		return schema;
	};
	context.get_inlined_table_names = [&](TableIndex table_id) {
		vector<string> names;
		auto entry = ducklake_catalog.GetEntryById(*this, transaction_snapshot, table_id);
		if (!entry) {
			return names;
		}
		for (auto &t : entry->Cast<DuckLakeTableEntry>().GetInlinedDataTables()) {
			names.push_back(t.table_name);
		}
		return names;
	};
	context.get_net_data_file_row_count = [&](TableIndex table_id) -> idx_t {
		auto entry = ducklake_catalog.GetEntryById(*this, transaction_snapshot, table_id);
		if (!entry) {
			return 0;
		}
		return entry->Cast<DuckLakeTableEntry>().GetNetDataFileRowCount(*this);
	};
	context.get_net_inlined_row_count = [&](TableIndex table_id) -> idx_t {
		auto entry = ducklake_catalog.GetEntryById(*this, transaction_snapshot, table_id);
		if (!entry) {
			return 0;
		}
		return entry->Cast<DuckLakeTableEntry>().GetNetInlinedRowCount(*this);
	};
	context.build_stats_map = [&](vector<DuckLakeGlobalStatsInfo> &stats) {
		auto &schema = ducklake_catalog.GetSchemaForSnapshot(*this, GetSnapshot());
		return DuckLakeCatalog::ConstructStatsMap(stats, schema);
	};
	context.invalidate_schema_cache = [&](idx_t schema_version) {
		ducklake_catalog.InvalidateSchemaCache(schema_version);
	};
	context.set_catalog_version = [&](idx_t schema_version) {
		catalog_version = schema_version;
	};
	context.set_committed_snapshot_id = [&](idx_t snapshot_id) {
		ducklake_catalog.SetCommittedSnapshotId(snapshot_id);
	};
	context.invalidate_table_stats_cache = [&](idx_t next_file_id, TableIndex table_id) {
		ducklake_catalog.InvalidateTableStatsCache(next_file_id, table_id);
	};
	context.commit_info = state->commit_info;
	context.write_row_group_count = ducklake_catalog.SupportsRowGroupCount();
	state->Commit(transaction_snapshot, transaction_changes, retry_config, context);
}

void DuckLakeTransaction::SetConfigOption(const DuckLakeConfigOption &option) {
	// write the config option to the metadata
	metadata_manager->SetConfigOption(option);
	// set the option in the catalog
	ducklake_catalog.SetConfigOption(option);
}

DuckLakeSnapshotCommit &DuckLakeTransaction::GetCommitInfo() {
	return state->commit_info;
}

void DuckLakeTransaction::SetCommitMessage(const DuckLakeSnapshotCommit &option) {
	state->commit_info = option;
}

void DuckLakeTransaction::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	auto &metadata_manager = GetMetadataManager();
	metadata_manager.DeleteSnapshots(snapshots);
}

void DuckLakeTransaction::DeleteInlinedData(const DuckLakeInlinedTableInfo &inlined_table) {
	auto &metadata_manager = GetMetadataManager();
	metadata_manager.DeleteInlinedData(inlined_table);
}

void DuckLakeTransaction::DeleteFlushedInlinedData(const DuckLakeInlinedTableInfo &inlined_table,
                                                   idx_t flush_snapshot_id) {
	auto &metadata_manager = GetMetadataManager();
	metadata_manager.DeleteFlushedInlinedData(inlined_table, flush_snapshot_id);
}

void DuckLakeTransaction::MarkInlinedDataForDeletion(DuckLakeInlinedTableInfo inlined_table, idx_t flush_snapshot_id) {
	state->flushed_inlined_tables.push_back({std::move(inlined_table), flush_snapshot_id});
}

unique_ptr<QueryResult> DuckLakeTransaction::ExecuteRaw(string query) {
	auto &connection = GetConnection();
	auto start = std::chrono::steady_clock::now();
	auto result = connection.Query(query);
	auto end = std::chrono::steady_clock::now();
	auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

	DUCKDB_LOG(db, DuckLakeMetadataLogType, ducklake_catalog.GetName().GetIdentifierName(), query, elapsed_ms);

	auto &cb = ducklake_catalog.GetQueryCallback();
	if (cb) {
		cb(query, end - start);
	}
	return std::move(result);
}

unique_ptr<QueryResult> DuckLakeTransaction::Query(string query) {
	return metadata_manager->Query(query);
}

unique_ptr<QueryResult> DuckLakeTransaction::Query(DuckLakeSnapshot snapshot, string query) {
	return metadata_manager->Query(snapshot, query);
}

string DuckLakeTransaction::GetDefaultSchemaName() {
	auto &metadata_context = *connection->context;
	auto &db_manager = DatabaseManager::Get(metadata_context);
	auto metadb = db_manager.GetDatabase(metadata_context, Identifier(ducklake_catalog.MetadataDatabaseName()));
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
	values.push_back(make_pair(Identifier(at_clause->Unit()), at_clause->GetValue()));
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
	return state->local_changes.HasTransactionLocalInserts(table_id);
}

bool DuckLakeTransaction::HasTransactionInlinedData(TableIndex table_id) const {
	return state->local_changes.HasTransactionInlinedData(table_id);
}

vector<DuckLakeDataFile> DuckLakeTransaction::GetTransactionLocalFiles(TableIndex table_id) const {
	return state->local_changes.GetTransactionLocalFiles(table_id);
}

shared_ptr<DuckLakeInlinedData> DuckLakeTransaction::GetTransactionLocalInlinedData(TableIndex table_id) const {
	auto context_ref = context.lock();
	return state->local_changes.GetTransactionLocalInlinedData(*context_ref, table_id);
}

void DuckLakeTransaction::DropTransactionLocalFile(TableIndex table_id, const string &path) {
	auto context_ref = context.lock();
	state->local_changes.DropTransactionLocalFile(*context_ref, table_id, path);
}

void DuckLakeTransaction::AppendFiles(TableIndex table_id, vector<DuckLakeDataFile> files) {
	if (files.empty()) {
		return;
	}
	state->local_changes.AppendFiles(table_id, std::move(files));
}

void DuckLakeTransaction::AppendInlinedData(TableIndex table_id, unique_ptr<DuckLakeInlinedData> new_data) {
	auto context_ref = context.lock();
	state->local_changes.AppendInlinedData(*context_ref, table_id, std::move(new_data));
}

void DuckLakeTransaction::SetRequiresNewInlinedTable(bool requires_new) {
	requires_new_inlined_table = requires_new;
}

bool DuckLakeTransaction::GetRequiresNewInlinedTable() const {
	return requires_new_inlined_table;
}

void DuckLakeTransaction::AddNewInlinedDeletes(TableIndex table_id, const string &table_name, set<idx_t> new_deletes) {
	if (new_deletes.empty()) {
		return;
	}
	state->local_changes.AddNewInlinedDeletes(table_id, table_name, std::move(new_deletes));
}

void DuckLakeTransaction::DeleteFromLocalInlinedData(TableIndex table_id, set<idx_t> new_deletes) {
	auto context_ref = context.lock();
	state->local_changes.DeleteFromLocalInlinedData(*context_ref, table_id, std::move(new_deletes));
}

void DuckLakeTransaction::AddColumnToLocalInlinedData(TableIndex table_id, const LogicalType &new_column_type,
                                                      FieldIndex new_field_index, const Value &default_value) {
	auto context_ref = context.lock();
	state->local_changes.AddColumnToLocalInlinedData(*context_ref, table_id, new_column_type, new_field_index,
	                                                 default_value);
}

void DuckLakeTransaction::RemoveColumnFromLocalInlinedData(TableIndex table_id, LogicalIndex removed_column_index,
                                                           const DuckLakeFieldId &field_id) {
	auto context_ref = context.lock();
	state->local_changes.RemoveColumnFromLocalInlinedData(*context_ref, table_id, removed_column_index, field_id);
}

optional_ptr<DuckLakeInlinedDataDeletes> DuckLakeTransaction::GetInlinedDeletes(TableIndex table_id,
                                                                                const string &table_name) const {
	return state->local_changes.GetInlinedDeletes(table_id, table_name);
}

void DuckLakeTransaction::AddNewInlinedFileDeletes(TableIndex table_id, idx_t file_id, set<idx_t> new_deletes) {
	state->local_changes.AddNewInlinedFileDeletes(table_id, file_id, std::move(new_deletes));
}

void DuckLakeTransaction::AddDeletes(TableIndex table_id, vector<DuckLakeDeleteFile> files) {
	auto context_ref = context.lock();
	state->local_changes.AddDeletes(*context_ref, table_id, std::move(files));
}

void DuckLakeTransaction::AddCompaction(TableIndex table_id, DuckLakeCompactionEntry entry) {
	state->local_changes.AddCompaction(table_id, std::move(entry));
}

bool DuckLakeTransaction::HasLocalDeletes(TableIndex table_id) const {
	return state->local_changes.HasLocalDeletes(table_id);
}

bool DuckLakeTransaction::HasLocalDeleteForFile(TableIndex table_id, const string &path) const {
	return state->local_changes.HasLocalDeleteForFile(table_id, path);
}

bool DuckLakeTransaction::HasAnyLocalChanges(TableIndex table_id) const {
	if (state->local_changes.HasAnyLocalChanges(table_id)) {
		return true;
	}
	return state->tables_deleted_from.find(table_id) != state->tables_deleted_from.end();
}

void DuckLakeTransaction::GetLocalDeleteForFile(TableIndex table_id, const string &path,
                                                DuckLakeFileData &result) const {
	state->local_changes.GetLocalDeleteForFile(table_id, path, result);
}

bool DuckLakeTransaction::HasLocalInlinedFileDeletes(TableIndex table_id) const {
	return state->local_changes.HasLocalInlinedFileDeletes(table_id);
}

void DuckLakeTransaction::GetLocalInlinedFileDeletesForFile(TableIndex table_id, idx_t file_id,
                                                            set<idx_t> &result) const {
	state->local_changes.GetLocalInlinedFileDeletesForFile(table_id, file_id, result);
}

void DuckLakeTransaction::TransactionLocalDelete(TableIndex table_id, const string &data_file_path,
                                                 DuckLakeDeleteFile delete_file) {
	auto context_ref = context.lock();
	state->local_changes.TransactionLocalDelete(*context_ref, table_id, data_file_path, std::move(delete_file));
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
	auto &new_schemas = state->new_schemas;
	auto schema_id = schema.GetSchemaId();
	if (schema_id.IsTransactionLocal()) {
		// schema is transaction-local - drop it from the transaction local changes
		if (!new_schemas) {
			throw InternalException("Dropping a transaction local table that does not exist?");
		}
		new_schemas->DropEntry(schema.name.GetIdentifierName());
		if (new_schemas->GetEntries().empty()) {
			// we have dropped all schemas created in this transaction - clear it
			new_schemas.reset();
		}
	} else {
		state->dropped_schemas.insert(make_pair(schema.GetSchemaId(), reference<DuckLakeSchemaEntry>(schema)));
	}
}

void DuckLakeTransaction::DropTable(DuckLakeTableEntry &table) {
	catalog_version = ducklake_catalog.GetNewUncommittedCatalogVersion();
	auto &new_tables = state->new_tables;
	auto table_id = table.GetTableId();
	if (table.IsTransactionLocal()) {
		auto schema_entry = new_tables.find(table.ParentSchema().name.GetIdentifierName());
		if (schema_entry == new_tables.end()) {
			throw InternalException("Dropping a transaction local table %s that does not exist", table.name);
		}
		schema_entry->second->DropEntry(table.name.GetIdentifierName());
		// if we have written any files for this table - clean them up
		auto context_ref = context.lock();
		state->local_changes.CleanupFiles(*context_ref, table_id);
		if (schema_entry->second->GetEntries().empty()) {
			new_tables.erase(schema_entry);
		}
	}

	// The table exists before the transaction, drop the table anyway.
	if (!IsTransactionLocal(table_id.index)) {
		state->renamed_tables.erase(table_id);
		state->dropped_tables.insert(table_id);
	}
}

void DuckLakeTransaction::DropView(DuckLakeViewEntry &view) {
	auto &new_tables = state->new_tables;
	auto view_id = view.GetViewId();
	if (view.IsTransactionLocal()) {
		auto schema_entry = new_tables.find(view.ParentSchema().name.GetIdentifierName());
		if (schema_entry == new_tables.end()) {
			throw InternalException("Dropping a transaction local view that does not exist?");
		}
		schema_entry->second->DropEntry(view.name.GetIdentifierName());
		if (schema_entry->second->GetEntries().empty()) {
			new_tables.erase(schema_entry);
		}
	}

	// The view exists before the transaction, drop the view anyway.
	if (!IsTransactionLocal(view_id.index)) {
		state->renamed_views.erase(view_id);
		state->dropped_views.insert(view_id);
	}
}

void DuckLakeTransaction::DropScalarMacro(DuckLakeScalarMacroEntry &macro) {
	state->dropped_scalar_macros.insert(macro.GetIndex());
}

void DuckLakeTransaction::DropTableMacro(DuckLakeTableMacroEntry &macro) {
	state->dropped_table_macros.insert(macro.GetIndex());
}

void DuckLakeTransaction::DropFile(TableIndex table_id, DataFileIndex data_file_id, string path, idx_t row_count,
                                   idx_t file_size_bytes) {
	state->tables_deleted_from.insert(table_id);
	auto inserted = state->dropped_files.emplace(std::move(path), data_file_id);
	if (!inserted.second) {
		return;
	}
	auto &stats = state->dropped_file_stats[table_id];
	stats.row_count += row_count;
	stats.file_size_bytes += file_size_bytes;
}

bool DuckLakeTransaction::HasDroppedFiles() const {
	return !state->dropped_files.empty();
}

const unordered_map<string, DataFileIndex> &DuckLakeTransaction::GetDroppedFiles() const {
	return state->dropped_files;
}

const set<TableIndex> &DuckLakeTransaction::GetTablesDeletedFrom() const {
	return state->tables_deleted_from;
}

const vector<FlushedInlinedTableInfo> &DuckLakeTransaction::GetFlushedInlinedTables() const {
	return state->flushed_inlined_tables;
}

bool DuckLakeTransaction::FileIsDropped(const string &path) const {
	return state->dropped_files.find(path) != state->dropped_files.end();
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
		auto local_entry = GetTransactionLocalEntry(entry.type, entry.ParentSchema().name.GetIdentifierName(),
		                                            entry.name.GetIdentifierName());
		if (local_entry) {
			auto &macro_map = GetNewMacroMap(entry.type);
			auto schema_entry = macro_map.find(entry.ParentSchema().name.GetIdentifierName());
			if (schema_entry == macro_map.end()) {
				throw InternalException("Dropping a transaction local macro %s that does not exist.", entry.name);
			}
			schema_entry->second->DropEntry(entry.name.GetIdentifierName());
			if (schema_entry->second->GetEntries().empty()) {
				macro_map.erase(schema_entry);
			}
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
	auto &s = *state;
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table_entry = entry.Cast<DuckLakeTableEntry>();
		return s.dropped_tables.find(table_entry.GetTableId()) != s.dropped_tables.end();
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view_entry = entry.Cast<DuckLakeViewEntry>();
		return s.dropped_views.find(view_entry.GetViewId()) != s.dropped_views.end();
	}
	case CatalogType::MACRO_ENTRY: {
		auto &macro_entry = entry.Cast<DuckLakeScalarMacroEntry>();
		return s.dropped_scalar_macros.find(macro_entry.GetIndex()) != s.dropped_scalar_macros.end();
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &macro_entry = entry.Cast<DuckLakeTableMacroEntry>();
		return s.dropped_table_macros.find(macro_entry.GetIndex()) != s.dropped_table_macros.end();
	}
	case CatalogType::SCHEMA_ENTRY: {
		auto &schema_entry = entry.Cast<DuckLakeSchemaEntry>();
		return s.dropped_schemas.find(schema_entry.GetSchemaId()) != s.dropped_schemas.end();
	}
	default:
		throw InternalException("Catalog type not supported for IsDeleted");
	}
}

bool DuckLakeTransaction::IsRenamed(CatalogEntry &entry) {
	auto &s = *state;
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY: {
		auto &table_entry = entry.Cast<DuckLakeTableEntry>();
		return s.renamed_tables.find(table_entry.GetTableId()) != s.renamed_tables.end();
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view_entry = entry.Cast<DuckLakeViewEntry>();
		return s.renamed_views.find(view_entry.GetViewId()) != s.renamed_views.end();
	}
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

static void HandleRenameOldEntry(DuckLakeCatalogSet &entries, const string &old_name, const string &new_name,
                                 TableIndex id, bool entry_is_transaction_local, set<TableIndex> &renamed_set,
                                 const set<TableIndex> &dropped_set) {
	if (IsTransactionLocal(id)) {
		// entry was created in this same transaction
		auto dropped = entries.DropEntry(old_name);
		auto new_entry_ptr = entries.GetEntry(new_name);
		if (new_entry_ptr && dropped) {
			new_entry_ptr->SetChild(std::move(dropped));
		}
	} else if (entry_is_transaction_local) {
		// entry existed before this transaction and has already been renamed earlier in this txn
		entries.DropEntry(old_name);
	} else {
		// first rename of a committed entry
		// Invariant: an id cannot be both renamed and dropped in the same transaction.
		D_ASSERT(dropped_set.find(id) == dropped_set.end());
		renamed_set.insert(id);
	}
}

void DuckLakeTransaction::AlterEntryInternal(DuckLakeTableEntry &table, unique_ptr<CatalogEntry> new_entry) {
	auto &new_table = new_entry->Cast<DuckLakeTableEntry>();
	auto &entries = GetOrCreateTransactionLocalEntries(table);
	entries.CreateEntry(std::move(new_entry));
	switch (new_table.GetLocalChange().type) {
	case LocalChangeType::RENAMED: {
		HandleRenameOldEntry(entries, table.name.GetIdentifierName(), new_table.name.GetIdentifierName(),
		                     table.GetTableId(), table.IsTransactionLocal(), state->renamed_tables,
		                     state->dropped_tables);
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
		HandleRenameOldEntry(entries, view.name.GetIdentifierName(), new_view.name.GetIdentifierName(),
		                     view.GetViewId(), view.IsTransactionLocal(), state->renamed_views, state->dropped_views);
		break;
	}
	case LocalChangeType::SET_COMMENT:
		break;
	default:
		throw NotImplementedException("Alter type not supported in DuckLakeTransaction::AlterEntry");
	}
}

DuckLakeCatalogSet &DuckLakeTransaction::GetOrCreateTransactionLocalEntries(CatalogEntry &entry) {
	auto &new_schemas = state->new_schemas;
	auto &new_tables = state->new_tables;
	auto catalog_type = entry.type;
	if (catalog_type == CatalogType::SCHEMA_ENTRY) {
		if (!new_schemas) {
			new_schemas = make_uniq<DuckLakeCatalogSet>();
		}
		return *new_schemas;
	}
	auto &schema_name = entry.ParentSchema().name.GetIdentifierName();
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
		auto &macro_map = GetNewMacroMap(catalog_type);
		auto new_macro_list = make_uniq<DuckLakeCatalogSet>();
		auto &result = *new_macro_list;
		macro_map.insert(make_pair(schema_name, std::move(new_macro_list)));
		return result;
	}
	default:
		throw InternalException("Catalog type not supported for transaction local storage");
	}
}

optional_ptr<DuckLakeCatalogSet> DuckLakeTransaction::GetTransactionLocalSchemas() {
	return state->new_schemas;
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
	auto &new_tables = state->new_tables;
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
		auto &macro_map = GetNewMacroMap(catalog_type);
		auto entry = macro_map.find(schema_name);
		if (entry == macro_map.end()) {
			return nullptr;
		}
		return entry->second;
	}
	default:
		return nullptr;
	}
}

optional_ptr<CatalogEntry> DuckLakeTransaction::GetLocalEntryById(SchemaIndex schema_id) {
	if (!state->new_schemas) {
		return nullptr;
	}
	return state->new_schemas->GetEntryById(schema_id);
}

optional_ptr<CatalogEntry> DuckLakeTransaction::GetLocalEntryById(TableIndex table_id) {
	for (auto &schema_entry : state->new_tables) {
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
