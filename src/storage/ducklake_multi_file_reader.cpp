#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "common/ducklake_util.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "storage/ducklake_delete.hpp"
#include "duckdb/function/function_binder.hpp"
#include "storage/ducklake_inlined_data_reader.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {

//! Try to find a column in local_columns that matches the given field_id
static bool TryFindColumnByFieldId(const vector<MultiFileColumnDefinition> &local_columns, int32_t field_id,
                                   MultiFileColumnDefinition *fallback_column,
                                   optional_ptr<MultiFileColumnDefinition> &global_column_reference) {
	for (auto &col : local_columns) {
		if (col.identifier.IsNull()) {
			continue;
		}
		if (col.identifier.GetValue<int32_t>() == field_id) {
			global_column_reference = fallback_column;
			return true;
		}
	}
	return false;
}

//! Add a snapshot filter to the reader's filter set
static void AddSnapshotFilter(BaseFileReader &reader, const ColumnIndex &col_idx, const LogicalType &col_type,
                              idx_t snapshot_value, ExpressionType comparison_type) {
	auto constant = Value::UBIGINT(snapshot_value).DefaultCastAs(col_type);
	auto filter = make_uniq<ConstantFilter>(comparison_type, std::move(constant));
	reader.filters->PushFilter(col_idx, std::move(filter));
}

// recursively normalize LIST child names from legacy formats blame legacy Avro/Parquet formats
static void NormalizeListChildNames(vector<MultiFileColumnDefinition> &columns, bool parent_is_list = false) {
	for (auto &col : columns) {
		// basically array, element becomes list
		if (parent_is_list && (col.name == "array" || col.name == "element")) {
			col.name = "list";
		}
		if (!col.children.empty()) {
			bool is_list = col.type.id() == LogicalTypeId::LIST;
			NormalizeListChildNames(col.children, is_list);
		}
	}
}

static bool CanSkipFileByTopNDynamicFilter(const DuckLakeFileListEntry &file_entry,
                                           const FilterPushdownInfo &filter_info, ClientContext &context) {
	if (file_entry.data_type != DuckLakeDataType::DATA_FILE) {
		return false;
	}
	for (auto &it : filter_info.column_filters) {
		auto &col_filter = it.second;
		auto *filter = DuckLakeUtil::GetOptionalDynamicFilter(*col_filter.table_filter);
		if (!filter) {
			continue;
		}
		ExpressionType comparison_type;
		Value constant;
		{
			lock_guard<mutex> l(filter->filter_data->lock);
			if (!filter->filter_data->initialized) {
				return false;
			}
			comparison_type = filter->filter_data->filter->comparison_type;
			constant = filter->filter_data->filter->constant;
		}

		auto mm_it = file_entry.column_min_max.find(col_filter.column_field_index);
		if (mm_it == file_entry.column_min_max.end()) {
			continue;
		}

		// from here we'll try to cast and compare with the dynamic filter values
		// if casts fail, just skip pruning
		Value casted_constant;
		if (!constant.DefaultTryCastAs(col_filter.column_type, casted_constant, nullptr)) {
			continue;
		}

		switch (comparison_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
			const auto &max_str = mm_it->second.second;
			if (max_str.empty()) {
				continue;
			}
			Value file_max;
			if (!Value(max_str).DefaultTryCastAs(col_filter.column_type, file_max, nullptr)) {
				continue;
			}
			if (comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
				return !(file_max > casted_constant);
			}
			return !(file_max >= casted_constant);
		}
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
			const auto &min_str = mm_it->second.first;
			if (min_str.empty()) {
				continue;
			}
			Value file_min;
			if (!Value(min_str).DefaultTryCastAs(col_filter.column_type, file_min, nullptr)) {
				continue;
			}
			if (comparison_type == ExpressionType::COMPARE_LESSTHAN) {
				return !(file_min < casted_constant);
			}
			return !(file_min <= casted_constant);
		}
		default:
			// nothing to prune
			continue;
		}
	}
	return false;
}

DuckLakeMultiFileReader::DuckLakeMultiFileReader(DuckLakeFunctionInfo &read_info) : read_info(read_info) {
	row_id_column = make_uniq<MultiFileColumnDefinition>("_ducklake_internal_row_id", LogicalType::BIGINT);
	row_id_column->identifier = Value::INTEGER(MultiFileReader::ROW_ID_FIELD_ID);
	snapshot_id_column = make_uniq<MultiFileColumnDefinition>("_ducklake_internal_snapshot_id", LogicalType::BIGINT);
	snapshot_id_column->identifier = Value::INTEGER(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID);
}

DuckLakeMultiFileReader::~DuckLakeMultiFileReader() {
}

unique_ptr<MultiFileReader> DuckLakeMultiFileReader::Copy() const {
	auto result = make_uniq<DuckLakeMultiFileReader>(read_info);
	result->transaction_local_data = transaction_local_data;
	return std::move(result);
}

unique_ptr<MultiFileReader> DuckLakeMultiFileReader::CreateInstance(const TableFunction &table_function) {
	auto &function_info = table_function.function_info->Cast<DuckLakeFunctionInfo>();
	auto result = make_uniq<DuckLakeMultiFileReader>(function_info);
	return std::move(result);
}

shared_ptr<MultiFileList> DuckLakeMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                  const FileGlobInput &options) {
	auto &transaction = DuckLakeTransaction::Get(context, read_info.table.ParentCatalog());
	auto transaction_local_files = transaction.GetTransactionLocalFiles(read_info.table_id);
	transaction_local_data = transaction.GetTransactionLocalInlinedData(read_info.table_id);
	auto result =
	    make_shared_ptr<DuckLakeMultiFileList>(read_info, std::move(transaction_local_files), transaction_local_data);
	return std::move(result);
}

MultiFileColumnDefinition CreateColumnFromFieldId(const DuckLakeFieldId &field_id, bool emit_key_value) {
	MultiFileColumnDefinition column(field_id.Name(), field_id.Type());
	auto &column_data = field_id.GetColumnData();
	if (column_data.initial_default.IsNull()) {
		column.default_expression = make_uniq<ConstantExpression>(Value(field_id.Type()));
	} else {
		column.default_expression = make_uniq<ConstantExpression>(column_data.initial_default);
	}
	column.identifier = Value::INTEGER(NumericCast<int32_t>(field_id.GetFieldIndex().index));
	for (auto &child : field_id.Children()) {
		column.children.push_back(CreateColumnFromFieldId(*child, emit_key_value));
	}
	if (field_id.Type().id() == LogicalTypeId::MAP && emit_key_value) {
		// for maps, insert a dummy "key_value" entry
		MultiFileColumnDefinition key_val("key_value", LogicalTypeId::INVALID);
		key_val.children = std::move(column.children);
		column.children.push_back(std::move(key_val));
	}
	return column;
}

// FIXME: emit_key_value is a work-around for an inconsistency in the MultiFileColumnMapper
vector<MultiFileColumnDefinition> DuckLakeMultiFileReader::ColumnsFromFieldData(const DuckLakeFieldData &field_data,
                                                                                bool emit_key_value) {
	vector<MultiFileColumnDefinition> result;
	for (auto &item : field_data.GetFieldIds()) {
		result.push_back(CreateColumnFromFieldId(*item, emit_key_value));
	}
	return result;
}

bool DuckLakeMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                   vector<string> &names, MultiFileReaderBindData &bind_data) {
	auto &field_data = read_info.table.GetFieldData();
	auto &columns = bind_data.schema;
	columns = ColumnsFromFieldData(field_data);
	//	bind_data.file_row_number_idx = names.size();
	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
	names = read_info.column_names;
	return_types = read_info.column_types;
	return true;
}

//! Override the Options bind
void DuckLakeMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                          vector<LogicalType> &return_types, vector<string> &names,
                                          MultiFileReaderBindData &bind_data) {
}

ReaderInitializeType DuckLakeMultiFileReader::InitializeReader(MultiFileReaderData &reader_data,
                                                               const MultiFileBindData &bind_data,
                                                               const vector<MultiFileColumnDefinition> &global_columns,
                                                               const vector<ColumnIndex> &global_column_ids,
                                                               optional_ptr<TableFilterSet> table_filters,
                                                               ClientContext &context, MultiFileGlobalState &gstate) {
	auto &file_list = gstate.file_list.Cast<DuckLakeMultiFileList>();
	auto &reader = *reader_data.reader;
	auto file_idx = reader.file_list_idx.GetIndex();

	auto &file_entry = file_list.GetFileEntry(file_idx);
	if (file_list.GetFilterInfo() && CanSkipFileByTopNDynamicFilter(file_entry, *file_list.GetFilterInfo(), context)) {
		return ReaderInitializeType::SKIP_READING_FILE;
	}
	if (!file_list.IsDeleteScan()) {
		// regular scan - read the deletes from the delete file (if any) and apply the max row count
		if (file_entry.data_type != DuckLakeDataType::DATA_FILE) {
			auto transaction = read_info.GetTransaction();
			auto inlined_deletes = transaction->GetInlinedDeletes(read_info.table.GetTableId(), file_entry.file.path);
			if (inlined_deletes) {
				auto delete_filter = make_uniq<DuckLakeDeleteFilter>();
				delete_filter->Initialize(*inlined_deletes);
				reader.deletion_filter = std::move(delete_filter);
			}
		} else if (!file_entry.delete_file.path.empty() || file_entry.max_row_count.IsValid() ||
		           !file_entry.inlined_file_deletions.empty()) {
			auto delete_filter = make_uniq<DuckLakeDeleteFilter>();
			if (!file_entry.delete_file.path.empty()) {
				delete_filter->Initialize(context, file_entry.delete_file);
			}
			// Apply inlined file deletions (stored in metadata database instead of delete file)
			if (!file_entry.inlined_file_deletions.empty()) {
				DuckLakeInlinedDataDeletes inlined_deletes;
				inlined_deletes.rows = file_entry.inlined_file_deletions;
				delete_filter->Initialize(inlined_deletes);
			}
			if (file_entry.max_row_count.IsValid()) {
				delete_filter->SetMaxRowCount(file_entry.max_row_count.GetIndex());
			}
			// set the snapshot id so we know what to skip from deletion files
			delete_filter->SetSnapshotFilter(read_info.snapshot.snapshot_id);
			// Only add to delete_map if there's an actual delete file and not just inlined deletions
			if (delete_map && !file_entry.delete_file.path.empty()) {
				delete_map->AddDeleteData(reader.GetFileName(), delete_filter->delete_data);
			}
			reader.deletion_filter = std::move(delete_filter);
		}
	} else {
		// delete scan - we need to read ONLY the entries that have been deleted
		if (file_entry.data_type == DuckLakeDataType::DATA_FILE) {
			auto &delete_entry = file_list.GetDeleteScanEntry(file_idx);
			auto delete_filter = make_uniq<DuckLakeDeleteFilter>();
			delete_filter->Initialize(context, delete_entry);
			reader.deletion_filter = std::move(delete_filter);
		}
	}
	auto result = MultiFileReader::InitializeReader(reader_data, bind_data, global_columns, global_column_ids,
	                                                table_filters, context, gstate);
	// Handle snapshot filters for files with multiple snapshots (partial_max set)
	if (file_entry.snapshot_filter_max.IsValid() || file_entry.snapshot_filter_min.IsValid()) {
		// we have a snapshot filter - add it to the filter list
		// find the column we need to filter on
		auto &reader = *reader_data.reader;
		optional_idx snapshot_col;
		LogicalType snapshot_col_type;
		for (idx_t col_idx = 0; col_idx < reader.columns.size(); col_idx++) {
			auto &col = reader.columns[col_idx];
			if (col.identifier.type() == LogicalTypeId::INTEGER &&
			    IntegerValue::Get(col.identifier) == LAST_UPDATED_SEQUENCE_NUMBER_ID) {
				snapshot_col = col_idx;
				snapshot_col_type = col.type;
				break;
			}
		}
		if (!snapshot_col.IsValid()) {
			throw InvalidInputException("Snapshot filter was specified but snapshot column was not present in file");
		}
		idx_t snapshot_col_id = snapshot_col.GetIndex();
		// check if the column is currently projected
		optional_idx snapshot_local_id;
		for (idx_t i = 0; i < reader.column_ids.size(); i++) {
			if (reader.column_indexes[i].GetPrimaryIndex() == snapshot_col_id) {
				snapshot_local_id = i;
				break;
			}
		}
		if (!snapshot_local_id.IsValid()) {
			snapshot_local_id = reader.column_indexes.size();
			reader.column_indexes.emplace_back(snapshot_col_id);
			reader.column_ids.emplace_back(snapshot_col_id);
		}
		if (!reader.filters) {
			reader.filters = make_uniq<TableFilterSet>();
		}
		ColumnIndex snapshot_col_idx(snapshot_local_id.GetIndex());

		// Add _ducklake_internal_snapshot_id <= snapshot_filter_max
		if (file_entry.snapshot_filter_max.IsValid()) {
			AddSnapshotFilter(reader, snapshot_col_idx, snapshot_col_type, file_entry.snapshot_filter_max.GetIndex(),
			                  ExpressionType::COMPARE_LESSTHANOREQUALTO);
		}

		// Add _ducklake_internal_snapshot_id >= snapshot_filter_min
		if (file_entry.snapshot_filter_min.IsValid()) {
			AddSnapshotFilter(reader, snapshot_col_idx, snapshot_col_type, file_entry.snapshot_filter_min.GetIndex(),
			                  ExpressionType::COMPARE_GREATERTHANOREQUALTO);
		}
	}
	return result;
}

shared_ptr<BaseFileReader> DuckLakeMultiFileReader::TryCreateInlinedDataReader(const OpenFileInfo &file) {
	if (!file.extended_info) {
		return nullptr;
	}
	auto entry = file.extended_info->options.find("inlined_data");
	if (entry == file.extended_info->options.end()) {
		return nullptr;
	}
	// this is not a file but inlined data
	entry = file.extended_info->options.find("table_name");
	if (entry == file.extended_info->options.end()) {
		// scanning transaction local inlined data
		if (!transaction_local_data) {
			throw InternalException("No transaction local data");
		}
		auto columns = DuckLakeMultiFileReader::ColumnsFromFieldData(read_info.table.GetFieldData(), true);
		return make_shared_ptr<DuckLakeInlinedDataReader>(read_info, file, transaction_local_data, std::move(columns));
	}
	optional_idx schema_version;
	auto version_entry = file.extended_info->options.find("schema_version");
	if (version_entry != file.extended_info->options.end()) {
		schema_version = version_entry->second.GetValue<idx_t>();
	}
	reference<DuckLakeTableEntry> schema_table = read_info.table;
	if (schema_version.IsValid()) {
		// read the table at the specified version
		auto transaction = read_info.GetTransaction();
		auto &catalog = transaction->GetCatalog();
		DuckLakeSnapshot snapshot(catalog.GetBeginSnapshotForTable(read_info.table.GetTableId(), *transaction),
		                          schema_version.GetIndex(), 0, 0);
		auto entry = catalog.GetEntryById(*transaction, snapshot, read_info.table.GetTableId());
		if (!entry) {
			return nullptr;
		}
		schema_table = entry->Cast<DuckLakeTableEntry>();
	}
	// we are reading from a table - set up the inlined data reader that will read this data when requested
	auto columns = DuckLakeMultiFileReader::ColumnsFromFieldData(schema_table.get().GetFieldData(), true);
	columns.insert(columns.begin(), *snapshot_id_column);
	columns.insert(columns.begin(), *row_id_column);

	auto inlined_table_name = StringValue::Get(entry->second);
	return make_shared_ptr<DuckLakeInlinedDataReader>(read_info, file, std::move(inlined_table_name),
	                                                  std::move(columns));
}

shared_ptr<BaseFileReader> DuckLakeMultiFileReader::CreateReader(ClientContext &context,
                                                                 GlobalTableFunctionState &gstate,
                                                                 const OpenFileInfo &file, idx_t file_idx,
                                                                 const MultiFileBindData &bind_data) {
	auto reader = TryCreateInlinedDataReader(file);
	if (reader) {
		return reader;
	}
	return MultiFileReader::CreateReader(context, gstate, file, file_idx, bind_data);
}

shared_ptr<BaseFileReader> DuckLakeMultiFileReader::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                                 BaseFileReaderOptions &options,
                                                                 const MultiFileOptions &file_options,
                                                                 MultiFileReaderInterface &interface) {
	auto reader = TryCreateInlinedDataReader(file);
	if (reader) {
		return reader;
	}
	return MultiFileReader::CreateReader(context, file, options, file_options, interface);
}

vector<MultiFileColumnDefinition> MapColumns(MultiFileReaderData &reader_data,
                                             const vector<MultiFileColumnDefinition> &global_map,
                                             const vector<unique_ptr<DuckLakeNameMapEntry>> &column_maps,
                                             bool parent_is_list = false) {
	// create a map of field id -> column map index for the mapping at this level
	unordered_map<idx_t, idx_t> field_id_map;
	for (idx_t column_map_idx = 0; column_map_idx < column_maps.size(); column_map_idx++) {
		auto &column_map = *column_maps[column_map_idx];
		field_id_map.emplace(column_map.target_field_id.index, column_map_idx);
	}
	map<string, string> partitions;

	// make a copy of the global column map
	auto result = global_map;
	// now perform the actual remapping for the file
	for (auto &result_col : result) {
		auto field_id = result_col.identifier.GetValue<idx_t>();
		// look up the field id
		auto entry = field_id_map.find(field_id);
		if (entry == field_id_map.end()) {
			// field-id not found - this means the column is not present in the file
			// replace the identifier with a stub name to ensure it is omitted
			result_col.identifier = Value("__ducklake_unknown_identifier");
			continue;
		}
		// field-id found - add the name-based mapping at this level
		auto &column_map = column_maps[entry->second];
		if (column_map->hive_partition) {
			// this column is read from a hive partition - replace the identifier with a stub name
			result_col.identifier = Value("__ducklake_unknown_identifier");
			// replace the default value with the actual partition value
			if (partitions.empty()) {
				partitions = HivePartitioning::Parse(reader_data.reader->file.path);
			}
			auto entry = partitions.find(column_map->source_name);
			if (entry == partitions.end()) {
				throw InvalidInputException("Column \"%s\" should have been read from hive partitions - but it was not "
				                            "found in filename \"%s\"",
				                            column_map->source_name, reader_data.reader->file.path);
			}
			Value partition_val(entry->second);
			result_col.default_expression = make_uniq<ConstantExpression>(partition_val.DefaultCastAs(result_col.type));
			continue;
		}

		auto source_name = column_map->source_name;
		// normalize array element to list, due to old parquet formats
		if (parent_is_list && (source_name == "array" || source_name == "element")) {
			source_name = "list";
		}
		result_col.identifier = Value(source_name);
		// recursively process any child nodes
		if (!column_map->child_entries.empty()) {
			bool is_list = result_col.type.id() == LogicalTypeId::LIST;
			result_col.children = MapColumns(reader_data, result_col.children, column_map->child_entries, is_list);
		}
	}
	return result;
}

vector<MultiFileColumnDefinition> CreateNewMapping(MultiFileReaderData &reader_data,
                                                   const vector<MultiFileColumnDefinition> &global_map,
                                                   const DuckLakeNameMap &name_map) {
	return MapColumns(reader_data, global_map, name_map.column_maps);
}

ReaderInitializeType DuckLakeMultiFileReader::CreateMapping(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &global_columns,
    const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
    const MultiFileReaderBindData &bind_data, const virtual_column_map_t &virtual_columns) {
	NormalizeListChildNames(reader_data.reader->columns);

	// For deletion scans, we may need to internally project row_id for snapshot lookups
	// Check if this is a deletion scan and if row_id is not already in the user's query
	auto &file_list = multi_file_list.Cast<DuckLakeMultiFileList>();
	bool needs_internal_rowid = false;
	if (file_list.IsDeleteScan()) {
		// Check if row_id is already in global_column_ids
		bool has_rowid = false;
		for (auto &col_id : global_column_ids) {
			if (col_id.GetPrimaryIndex() == COLUMN_IDENTIFIER_ROW_ID) {
				has_rowid = true;
				break;
			}
		}
		// We need internal row_id if it's not in the user's query
		needs_internal_rowid = !has_rowid;
	}

	// Create extended column ids if we need to internally project row_id
	vector<ColumnIndex> extended_column_ids;
	internally_projected_rowid = needs_internal_rowid;
	if (needs_internal_rowid) {
		extended_column_ids = global_column_ids;
		extended_column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	const vector<ColumnIndex> &column_ids_to_use = needs_internal_rowid ? extended_column_ids : global_column_ids;

	if (reader_data.reader->file.extended_info) {
		auto &file_options = reader_data.reader->file.extended_info->options;
		auto entry = file_options.find("mapping_id");
		if (entry != file_options.end()) {
			auto mapping_id = MappingIndex(entry->second.GetValue<idx_t>());
			auto transaction = read_info.transaction.lock();
			auto &mapping = transaction->GetMappingById(mapping_id);
			// use the mapping to generate a new set of global columns for this file
			auto mapped_columns = CreateNewMapping(reader_data, global_columns, mapping);
			return MultiFileReader::CreateMapping(context, reader_data, mapped_columns, column_ids_to_use, filters,
			                                      multi_file_list, bind_data, virtual_columns,
			                                      MultiFileColumnMappingMode::BY_NAME);
		}
	}
	return MultiFileReader::CreateMapping(context, reader_data, global_columns, column_ids_to_use, filters,
	                                      multi_file_list, bind_data, virtual_columns);
}

unique_ptr<Expression> DuckLakeMultiFileReader::GetVirtualColumnExpression(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &local_columns,
    idx_t &column_id, const LogicalType &type, MultiFileLocalIndex local_idx,
    optional_ptr<MultiFileColumnDefinition> &global_column_reference) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		deletion_scan_rowid_col = local_idx.GetIndex();
		// row id column
		// this is computed as row_id_start + file_row_number OR read from the file
		// first check if the row id is explicitly defined in this file
		if (TryFindColumnByFieldId(local_columns, MultiFileReader::ROW_ID_FIELD_ID, row_id_column.get(),
		                           global_column_reference)) {
			return nullptr;
		}
		// get the row id start for this file
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for reading row id column");
		}

		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("row_id_start");
		if (entry == options.end()) {
			throw InvalidInputException("File \"%s\" does not have row_id_start defined, and the file does not have a "
			                            "row_id column written either - row id could not be read",
			                            reader_data.file_to_be_opened.path);
		}
		auto row_id_expr = make_uniq<BoundConstantExpression>(entry->second);
		auto file_row_number = make_uniq<BoundReferenceExpression>(type, local_idx.GetIndex());

		// transform this virtual column to file_row_number
		column_id = MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;

		// generate the addition
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(row_id_expr));
		children.push_back(std::move(file_row_number));

		FunctionBinder binder(context);
		ErrorData error;
		auto function_expr = binder.BindScalarFunction(DEFAULT_SCHEMA, "+", std::move(children), error, true, nullptr);
		if (error.HasError()) {
			error.Throw();
		}
		return function_expr;
	}
	if (column_id == COLUMN_IDENTIFIER_SNAPSHOT_ID) {
		deletion_scan_snapshot_col = local_idx.GetIndex();
		if (TryFindColumnByFieldId(local_columns, MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID,
		                           snapshot_id_column.get(), global_column_reference)) {
			return nullptr;
		}
		// get the row id start for this file
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for reading snapshot id column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("snapshot_id");
		if (entry == options.end()) {
			throw InternalException("snapshot_id not found for reading snapshot_id column");
		}
		return make_uniq<BoundConstantExpression>(entry->second);
	}
	return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type, local_idx,
	                                                   global_column_reference);
}

void DuckLakeMultiFileReader::GatherDeletionScanSnapshots(BaseFileReader &reader,
                                                          const MultiFileReaderData &reader_data, DataChunk &chunk,
                                                          optional_idx rowid_col_override) const {
	auto &delete_filter = static_cast<DuckLakeDeleteFilter &>(*reader.deletion_filter);
	optional_idx snapshot_col_idx = deletion_scan_snapshot_col;
	optional_idx rowid_col_idx = rowid_col_override.IsValid() ? rowid_col_override : deletion_scan_rowid_col;

	if (delete_filter.delete_data->scan_snapshot_map.empty() || !snapshot_col_idx.IsValid() ||
	    !rowid_col_idx.IsValid()) {
		// We don't have anything to gather
		return;
	}

	auto &rowid_vector = chunk.data[rowid_col_idx.GetIndex()];
	auto &snapshot_vector = chunk.data[snapshot_col_idx.GetIndex()];

	idx_t count = chunk.size();
	snapshot_vector.Flatten(count);
	auto snapshot_data = FlatVector::GetData<int64_t>(snapshot_vector);

	UnifiedVectorFormat row_id_data;
	rowid_vector.ToUnifiedFormat(count, row_id_data);
	auto row_id_ptr = UnifiedVectorFormat::GetData<int64_t>(row_id_data);

	// Look up the snapshot_id for each row
	for (idx_t i = 0; i < count; i++) {
		auto row_id_idx = row_id_data.sel->get_index(i);
		auto row_id = row_id_ptr[row_id_idx];

		idx_t lookup_key;
		if (delete_filter.delete_data->uses_row_id) {
			// File has embedded row_ids - use global row_id directly
			lookup_key = static_cast<idx_t>(row_id);
		} else {
			optional_idx row_id_start;
			if (reader_data.file_to_be_opened.extended_info) {
				auto entry = reader_data.file_to_be_opened.extended_info->options.find("row_id_start");
				if (entry != reader_data.file_to_be_opened.extended_info->options.end()) {
					row_id_start = entry->second.GetValue<idx_t>();
				}
			}
			if (row_id_start.IsValid()) {
				lookup_key = NumericCast<idx_t>(row_id) - row_id_start.GetIndex();
			} else {
				lookup_key = NumericCast<idx_t>(row_id);
			}
		}

		auto snapshot = delete_filter.delete_data->GetSnapshotForRow(lookup_key);
		if (snapshot.IsValid()) {
			snapshot_data[i] = NumericCast<int64_t>(snapshot.GetIndex());
		}
	}
}

void DuckLakeMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
                                            BaseFileReader &reader, const MultiFileReaderData &reader_data,
                                            DataChunk &input_chunk, DataChunk &output_chunk,
                                            ExpressionExecutor &executor,
                                            optional_ptr<MultiFileReaderGlobalState> global_state) {
	// If we internally projected row_id for deletion scan snapshot lookups,
	// we need to use a temp chunk that includes the row_id column
	if (internally_projected_rowid && read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS) {
		// Create a temp chunk with user columns + internally projected row_id
		vector<LogicalType> temp_types;
		for (idx_t i = 0; i < output_chunk.ColumnCount(); i++) {
			temp_types.push_back(output_chunk.data[i].GetType());
		}
		temp_types.push_back(LogicalType::BIGINT); // row_id

		DataChunk temp_chunk;
		temp_chunk.Initialize(Allocator::DefaultAllocator(), temp_types);

		// Call base FinalizeChunk with the temp chunk
		MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, temp_chunk, executor,
		                               global_state);

		// Gather deletion scan snapshots using the temp chunk (which has row_id)
		// The row_id column is at the last position in temp_chunk (we added it at the end)
		if (reader.deletion_filter) {
			idx_t internal_rowid_col = output_chunk.ColumnCount(); // last column in temp_chunk
			GatherDeletionScanSnapshots(reader, reader_data, temp_chunk, internal_rowid_col);
		}

		// Copy only user columns (excluding internally projected row_id) to output_chunk
		output_chunk.SetCardinality(temp_chunk.size());
		for (idx_t i = 0; i < output_chunk.ColumnCount(); i++) {
			output_chunk.data[i].Reference(temp_chunk.data[i]);
		}
	} else {
		MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, output_chunk, executor,
		                               global_state);

		// We need to gather the snapshot_id information correctly for scan deletions if the files are partial deletion
		// files.
		if (read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS && reader.deletion_filter) {
			GatherDeletionScanSnapshots(reader, reader_data, output_chunk);
		}
	}
}

} // namespace duckdb
