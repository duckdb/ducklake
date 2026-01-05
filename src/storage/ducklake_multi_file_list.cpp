#include "common/ducklake_util.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_metadata_manager.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeDataFile> transaction_local_files_p,
                                             shared_ptr<DuckLakeInlinedData> transaction_local_data_p,
                                             unique_ptr<FilterPushdownInfo> filter_info_p)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info), read_file_list(false),
      transaction_local_files(std::move(transaction_local_files_p)),
      transaction_local_data(std::move(transaction_local_data_p)), filter_info(std::move(filter_info_p)) {
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeFileListEntry> files_to_scan)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info),
      files(std::move(files_to_scan)), read_file_list(true) {
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             const DuckLakeInlinedTableInfo &inlined_table)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info), read_file_list(true) {
	DuckLakeFileListEntry file_entry;
	file_entry.file.path = inlined_table.table_name;
	file_entry.row_id_start = 0;
	file_entry.data_type = DuckLakeDataType::INLINED_DATA;
	files.push_back(std::move(file_entry));
	inlined_data_tables.push_back(inlined_table);
}

unique_ptr<MultiFileList> DuckLakeMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                       const MultiFileOptions &options,
                                                                       MultiFilePushdownInfo &info,
                                                                       vector<unique_ptr<Expression>> &filters) {
	return nullptr;
}

unique_ptr<MultiFileList>
DuckLakeMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                             const vector<string> &names, const vector<LogicalType> &types,
                                             const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || filters.filters.empty()) {
		// filter pushdown is only supported when scanning full tables
		return nullptr;
	}

	auto pushdown_info = make_uniq<FilterPushdownInfo>();

	for (auto &entry : filters.filters) {
		auto column_index_val = entry.first;
		idx_t column_idx = column_index_val;
		auto column_id = column_ids[column_idx];

		if (IsVirtualColumn(column_id)) {
			continue;
		}

		auto column_index = PhysicalIndex(column_id);
		auto &root_id = read_info.table.GetFieldId(column_index);
		auto field_index = root_id.GetFieldIndex().index;

		auto filter_copy = entry.second->Copy();
		// Get the column type from the table schema, not from the scan types array
		const auto &column_type = read_info.column_types[column_index.index];

		ColumnFilterInfo filter_info(field_index, column_type, std::move(filter_copy));
		pushdown_info->column_filters.emplace(field_index, std::move(filter_info));
	}

	if (pushdown_info->column_filters.empty()) {
		// no pushdown possible
		return nullptr;
	}

	return make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                        std::move(pushdown_info));
}

vector<OpenFileInfo> DuckLakeMultiFileList::GetAllFiles() {
	vector<OpenFileInfo> file_list;
	for (idx_t i = 0; i < GetTotalFileCount(); i++) {
		file_list.push_back(GetFile(i));
	}
	return file_list;
}

FileExpandResult DuckLakeMultiFileList::GetExpandResult() {
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t DuckLakeMultiFileList::GetTotalFileCount() {
	return GetFiles().size();
}

unique_ptr<NodeStatistics> DuckLakeMultiFileList::GetCardinality(ClientContext &context) {
	auto stats = read_info.table.GetTableStats(context);
	if (!stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(stats->record_count);
}

DuckLakeTableEntry &DuckLakeMultiFileList::GetTable() {
	return read_info.table;
}

OpenFileInfo DuckLakeMultiFileList::GetFile(idx_t i) {
	auto &files = GetFiles();
	if (i >= files.size()) {
		return OpenFileInfo();
	}
	auto &file_entry = files[i];
	auto &file = file_entry.file;
	OpenFileInfo result(file.path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	idx_t inlined_data_file_start = files.size() - inlined_data_tables.size();
	if (transaction_local_data) {
		inlined_data_file_start--;
	}
	if (transaction_local_data && i + 1 == files.size()) {
		// scanning transaction local data
		extended_info->options["transaction_local_data"] = Value::BOOLEAN(true);
		extended_info->options["inlined_data"] = Value::BOOLEAN(true);
		if (file_entry.row_id_start.IsValid()) {
			extended_info->options["row_id_start"] = Value::UBIGINT(file_entry.row_id_start.GetIndex());
		}
		extended_info->options["snapshot_id"] = Value(LogicalType::BIGINT);
		if (file_entry.mapping_id.IsValid()) {
			extended_info->options["mapping_id"] = Value::UBIGINT(file_entry.mapping_id.index);
		}
	} else if (i >= inlined_data_file_start) {
		// scanning inlined data
		auto inlined_data_index = i - inlined_data_file_start;
		auto &inlined_data_table = inlined_data_tables[inlined_data_index];
		extended_info->options["table_name"] = inlined_data_table.table_name;
		extended_info->options["inlined_data"] = Value::BOOLEAN(true);
		extended_info->options["schema_version"] =
		    Value::BIGINT(NumericCast<int64_t>(inlined_data_table.schema_version));
	} else {
		extended_info->options["file_size"] = Value::UBIGINT(file.file_size_bytes);
		if (file.footer_size.IsValid()) {
			extended_info->options["footer_size"] = Value::UBIGINT(file.footer_size.GetIndex());
		}
		if (files[i].row_id_start.IsValid()) {
			extended_info->options["row_id_start"] = Value::UBIGINT(files[i].row_id_start.GetIndex());
		}
		Value snapshot_id;
		if (files[i].snapshot_id.IsValid()) {
			snapshot_id = Value::BIGINT(NumericCast<int64_t>(files[i].snapshot_id.GetIndex()));
		} else {
			snapshot_id = Value(LogicalType::BIGINT);
		}
		extended_info->options["snapshot_id"] = std::move(snapshot_id);
		if (!file.encryption_key.empty()) {
			extended_info->options["encryption_key"] = Value::BLOB_RAW(file.encryption_key);
		}
		// files managed by DuckLake are never modified - we can keep them cached
		extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		// etag / last modified time can be set to dummy values
		extended_info->options["etag"] = Value("");
		extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
		if (!file_entry.delete_file.path.empty() || file_entry.max_row_count.IsValid()) {
			extended_info->options["has_deletes"] = Value::BOOLEAN(true);
		}
		if (file_entry.mapping_id.IsValid()) {
			extended_info->options["mapping_id"] = Value::UBIGINT(file_entry.mapping_id.index);
		}
	}
	result.extended_info = std::move(extended_info);
	return result;
}

unique_ptr<MultiFileList> DuckLakeMultiFileList::Copy() {
	unique_ptr<FilterPushdownInfo> filter_copy;
	if (filter_info) {
		filter_copy = filter_info->Copy();
	}

	auto result = make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                               std::move(filter_copy));
	result->files = GetFiles();
	result->read_file_list = read_file_list;
	result->delete_scans = delete_scans;
	result->inlined_data_tables = inlined_data_tables;
	return result;
}

const DuckLakeFileListEntry &DuckLakeMultiFileList::GetFileEntry(idx_t file_idx) {
	auto &files = GetFiles();
	return files[file_idx];
}

DuckLakeFileData GetFileData(const DuckLakeDataFile &file) {
	DuckLakeFileData result;
	result.path = file.file_name;
	result.encryption_key = file.encryption_key;
	result.file_size_bytes = file.file_size_bytes;
	result.footer_size = file.footer_size;
	return result;
}

DuckLakeFileData GetDeleteData(const DuckLakeDataFile &file) {
	DuckLakeFileData result;
	if (!file.delete_file) {
		return result;
	}
	auto &delete_file = *file.delete_file;
	result.path = delete_file.file_name;
	result.encryption_key = delete_file.encryption_key;
	result.file_size_bytes = delete_file.file_size_bytes;
	result.footer_size = delete_file.footer_size;
	return result;
}

vector<DuckLakeFileListExtendedEntry> DuckLakeMultiFileList::GetFilesExtended() {
	lock_guard<mutex> l(file_lock);
	vector<DuckLakeFileListExtendedEntry> result;
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!read_info.table_id.IsTransactionLocal()) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		result = metadata_manager.GetExtendedFilesForTable(read_info.table, read_info.snapshot, filter_info.get());
	}
	if (transaction.HasDroppedFiles()) {
		for (idx_t file_idx = 0; file_idx < result.size(); file_idx++) {
			if (transaction.FileIsDropped(result[file_idx].file.path)) {
				result.erase_at(file_idx);
				file_idx--;
			}
		}
	}
	// if the transaction has any local deletes - apply them to the file list
	if (transaction.HasLocalDeletes(read_info.table_id)) {
		for (auto &file_entry : result) {
			transaction.GetLocalDeleteForFile(read_info.table_id, file_entry.file.path, file_entry.delete_file);
		}
	}
	idx_t transaction_row_start = TRANSACTION_LOCAL_ID_START;
	for (auto &file : transaction_local_files) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = file.row_count;
		file_entry.file = GetFileData(file);
		file_entry.delete_file = GetDeleteData(file);
		file_entry.row_id_start = transaction_row_start;
		transaction_row_start += file.row_count;
		result.push_back(std::move(file_entry));
	}
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = 0;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		result.push_back(std::move(file_entry));
	}
	if (transaction_local_data) {
		// we have transaction local inlined data - create the dummy file entry
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file.path = DUCKLAKE_TRANSACTION_LOCAL_INLINED_FILENAME;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = transaction_local_data->data->Count();
		file_entry.row_id_start = transaction_row_start;
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		result.push_back(std::move(file_entry));
	}
	if (!read_file_list) {
		// we have not read the file list yet - construct it from the extended file list
		for (auto &file : result) {
			DuckLakeFileListEntry file_entry;
			file_entry.file = file.file;
			file_entry.row_id_start = file.row_id_start;
			file_entry.delete_file = file.delete_file;
			files.emplace_back(std::move(file_entry));
		}
		read_file_list = true;
	}
	return result;
}

void DuckLakeMultiFileList::GetFilesForTable() {
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!read_info.table_id.IsTransactionLocal()) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		files = metadata_manager.GetFilesForTable(read_info.table, read_info.snapshot, filter_info.get());
	}
	// If Top-N dynamic filter pushdown is present, sort files so we find a good boundary early.
	if (filter_info && !filter_info->column_filters.empty()) {
		for (auto &entry : filter_info->column_filters) {
			auto &col_filter = entry.second;
			auto *filter = DuckLakeUtil::GetOptionalDynamicFilter(*col_filter.table_filter);
			if (!filter) {
				continue;
			}
			ExpressionType comparison_type;
			{
				lock_guard<mutex> l(filter->filter_data->lock);
				comparison_type = filter->filter_data->filter->comparison_type;
			}
			const bool seeking_high_values = comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
			                                 comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
			const bool seeking_low_values = comparison_type == ExpressionType::COMPARE_LESSTHAN ||
			                                comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
			if (!seeking_high_values && !seeking_low_values) {
				continue;
			}
			// Sort files to prioritize those most likely to satisfy the Top-N filter based on their min/max stats
			std::stable_sort(files.begin(), files.end(),
			                 [&](const DuckLakeFileListEntry &a, const DuckLakeFileListEntry &b) {
				                 auto a_it = a.column_min_max.find(col_filter.column_field_index);
				                 auto b_it = b.column_min_max.find(col_filter.column_field_index);
				                 const bool a_has = a_it != a.column_min_max.end();
				                 const bool b_has = b_it != b.column_min_max.end();
				                 if (a_has != b_has) {
					                 return a_has;
				                 }
				                 if (!a_has) {
					                 return false;
				                 }
				                 const auto &a_min = a_it->second.first;
				                 const auto &a_max = a_it->second.second;
				                 const auto &b_min = b_it->second.first;
				                 const auto &b_max = b_it->second.second;
				                 if (seeking_high_values) {
					                 // Prioritize files with non-empty stats over those with empty stats
					                 if (a_max.empty() || b_max.empty()) {
						                 return !a_max.empty();
					                 }
					                 return Value(a_max).DefaultCastAs(col_filter.column_type) >
					                        Value(b_max).DefaultCastAs(col_filter.column_type);
				                 } else {
					                 // Prioritize files with non-empty stats over those with empty stats
					                 if (a_min.empty() || b_min.empty()) {
						                 return !a_min.empty();
					                 }
					                 return Value(a_min).DefaultCastAs(col_filter.column_type) <
					                        Value(b_min).DefaultCastAs(col_filter.column_type);
				                 }
			                 });
			break;
		}
	}
	if (transaction.HasDroppedFiles()) {
		for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
			if (transaction.FileIsDropped(files[file_idx].file.path)) {
				files.erase_at(file_idx);
				file_idx--;
			}
		}
	}
	// if the transaction has any local deletes - apply them to the file list
	if (transaction.HasLocalDeletes(read_info.table_id)) {
		for (auto &file_entry : files) {
			transaction.GetLocalDeleteForFile(read_info.table_id, file_entry.file.path, file_entry.delete_file);
		}
	}
	idx_t transaction_row_start = TRANSACTION_LOCAL_ID_START;
	for (auto &file : transaction_local_files) {
		DuckLakeFileListEntry file_entry;
		file_entry.file = GetFileData(file);
		file_entry.row_id_start = transaction_row_start;
		file_entry.delete_file = GetDeleteData(file);
		file_entry.mapping_id = file.mapping_id;
		transaction_row_start += file.row_count;
		files.emplace_back(std::move(file_entry));
	}
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
	if (transaction_local_data) {
		// we have transaction local inlined data - create the dummy file entry
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = DUCKLAKE_TRANSACTION_LOCAL_INLINED_FILENAME;
		file_entry.row_id_start = transaction_row_start;
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

void DuckLakeMultiFileList::GetTableInsertions() {
	if (read_info.table_id.IsTransactionLocal()) {
		throw InternalException("Cannot get changes between snapshots for transaction-local files");
	}
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	auto &metadata_manager = transaction.GetMetadataManager();
	files = metadata_manager.GetTableInsertions(read_info.table, *read_info.start_snapshot, read_info.snapshot);
	// add inlined data tables as sources (if any)
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

void DuckLakeMultiFileList::GetTableDeletions() {
	if (read_info.table_id.IsTransactionLocal()) {
		throw InternalException("Cannot get changes between snapshots for transaction-local files");
	}
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	auto &metadata_manager = transaction.GetMetadataManager();
	delete_scans = metadata_manager.GetTableDeletions(read_info.table, *read_info.start_snapshot, read_info.snapshot);
	for (auto &file : delete_scans) {
		DuckLakeFileListEntry file_entry;
		file_entry.file = file.file;
		file_entry.row_id_start = file.row_id_start;
		file_entry.snapshot_id = file.snapshot_id;
		file_entry.mapping_id = file.mapping_id;
		files.emplace_back(std::move(file_entry));
	}
	// add inlined data tables as sources (if any)
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

bool DuckLakeMultiFileList::IsDeleteScan() const {
	return read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS;
}

const DuckLakeDeleteScanEntry &DuckLakeMultiFileList::GetDeleteScanEntry(idx_t file_idx) {
	return delete_scans[file_idx];
}

const vector<DuckLakeFileListEntry> &DuckLakeMultiFileList::GetFiles() {
	lock_guard<mutex> l(file_lock);
	if (!read_file_list) {
		// we have not read the file list yet - read it
		switch (read_info.scan_type) {
		case DuckLakeScanType::SCAN_TABLE:
			GetFilesForTable();
			break;
		case DuckLakeScanType::SCAN_INSERTIONS:
			GetTableInsertions();
			break;
		case DuckLakeScanType::SCAN_DELETIONS:
			GetTableDeletions();
			break;
		default:
			throw InternalException("Unknown DuckLake scan type");
		}
		read_file_list = true;
	}
	return files;
}

} // namespace duckdb
