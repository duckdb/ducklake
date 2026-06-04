#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_deletion_vector.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_scan.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "common/ducklake_data_file.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "common/ducklake_util.hpp"

namespace duckdb {

template <typename InputType>
static DuckLakeDeleteFile WriteDeleteFileInternal(ClientContext &context, InputType &input) {
	constexpr bool with_snapshots = std::is_same<InputType, WriteDeleteFileWithSnapshotsInput>::value;

	auto delete_file_uuid = "ducklake-" + input.transaction.GenerateUUID() + "-delete.parquet";
	string delete_file_path = DuckLakeUtil::JoinPath(input.fs, input.data_path, delete_file_uuid);

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// generate the field ids to be written by the parquet writer
	// these field ids follow icebergs' ids and names for the delete files
	child_list_t<Value> values;
	values.emplace_back("file_path", Value::INTEGER(MultiFileReader::FILENAME_FIELD_ID));
	values.emplace_back("pos", Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID));
	if (with_snapshots) {
		// add the snapshot_id column to track when each deletion became valid
		values.emplace_back("_ducklake_internal_snapshot_id",
		                    Value::INTEGER(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID));
	}
	auto field_ids = Value::STRUCT(std::move(values));
	vector<Value> field_input;
	field_input.push_back(std::move(field_ids));
	info->options["field_ids"] = std::move(field_input);

	if (!input.encryption_key.empty()) {
		child_list_t<Value> enc_values;
		enc_values.emplace_back("footer_key_value", Value::BLOB_RAW(input.encryption_key));
		vector<Value> encryption_input;
		encryption_input.push_back(Value::STRUCT(std::move(enc_values)));
		info->options["encryption_config"] = std::move(encryption_input);
	}

	// get the actual copy function and bind it
	auto &copy_fun = DuckLakeFunctions::GetCopyFunction(input.context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	vector<string> names_to_write {"file_path", "pos"};
	vector<LogicalType> types_to_write {LogicalType::VARCHAR, LogicalType::BIGINT};
	if (with_snapshots) {
		names_to_write.push_back("_ducklake_internal_snapshot_id");
		types_to_write.push_back(LogicalType::BIGINT);
	}

	DuckLakeUtil::EnsureDirectoryExists(input.fs, input.data_path);

	auto function_data = copy_fun.function.copy_to_bind(input.context, bind_input, names_to_write, types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	// set up stats to get them from function
	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	DataChunk write_chunk;
	write_chunk.Initialize(input.context, types_to_write);
	// the first vector is constant (the file name)
	Value filename_val(input.data_file_path);
	write_chunk.data[0].Reference(filename_val, count_t(STANDARD_VECTOR_SIZE));

	optional_idx begin_snapshot;
	idx_t row_count = 0;
	auto pos_data = FlatVector::GetDataMutable<int64_t>(write_chunk.data[1]);
	int64_t *snapshot_data = nullptr;
	if (with_snapshots) {
		snapshot_data = FlatVector::GetDataMutable<int64_t>(write_chunk.data[2]);
	}

	for (auto &entry : input.positions) {
		if (with_snapshots) {
			// entry is PositionWithSnapshot
			auto &pos_with_snap = reinterpret_cast<const PositionWithSnapshot &>(entry);
			if (!begin_snapshot.IsValid() || pos_with_snap.snapshot_id < begin_snapshot.GetIndex()) {
				begin_snapshot = pos_with_snap.snapshot_id;
			}
			pos_data[row_count] = NumericCast<int64_t>(pos_with_snap.position);
			snapshot_data[row_count] = NumericCast<int64_t>(pos_with_snap.snapshot_id);
		} else {
			// entry is idx_t
			pos_data[row_count] = NumericCast<int64_t>(reinterpret_cast<const idx_t &>(entry));
		}
		row_count++;
		if (row_count >= STANDARD_VECTOR_SIZE) {
			write_chunk.SetChildCardinality(row_count);
			copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
			                               write_chunk);
			row_count = 0;
		}
	}
	if (row_count > 0) {
		write_chunk.SetChildCardinality(row_count);
		copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
		                               write_chunk);
	}

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	// add to the written files
	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = input.data_file_path;
	delete_file.file_name = delete_file_path;
	delete_file.format = DeleteFileFormat::PARQUET;
	delete_file.delete_count = stats.row_count;
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.footer_size = stats.footer_size_bytes.GetValue<idx_t>();
	delete_file.encryption_key = input.encryption_key;
	delete_file.source = input.source;
	if (with_snapshots) {
		delete_file.begin_snapshot = begin_snapshot;
	}
	return delete_file;
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeleteFile(ClientContext &context, WriteDeleteFileInput &input) {
	return WriteDeleteFileInternal(context, input);
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(ClientContext &context,
                                                                          WriteDeleteFileWithSnapshotsInput &input) {
	return WriteDeleteFileInternal(context, input);
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeletionVectorFile(ClientContext &context,
                                                                     WriteDeleteFileInput &input) {
	auto delete_file_uuid = "ducklake-" + input.transaction.GenerateUUID() + "-delete.puffin";
	string delete_file_path = DuckLakeUtil::JoinPath(input.fs, input.data_path, delete_file_uuid);

	// Serialize positions to puffin blob
	auto blob_data = DuckLakeDeletionVectorData::ToBlob(input.positions);

	// Write blob to file
	auto &fs = FileSystem::GetFileSystem(context);
	DuckLakeUtil::EnsureDirectoryExists(fs, input.data_path);
	auto file_handle =
	    fs.OpenFile(delete_file_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
	file_handle->Write(blob_data.data(), blob_data.size());
	file_handle->Close();

	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = input.data_file_path;
	delete_file.file_name = delete_file_path;
	delete_file.format = DeleteFileFormat::PUFFIN;
	delete_file.delete_count = input.positions.size();
	delete_file.file_size_bytes = blob_data.size();
	delete_file.footer_size = 0;
	delete_file.encryption_key = input.encryption_key;
	delete_file.source = input.source;
	return delete_file;
}

//===--------------------------------------------------------------------===//
// DuckLakeDelete
//===--------------------------------------------------------------------===//
DuckLakeDelete::DuckLakeDelete(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, PhysicalOperator &child,
                               shared_ptr<DuckLakeDeleteMap> delete_map_p, vector<idx_t> row_id_indexes_p,
                               string encryption_key_p, bool allow_duplicates)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      delete_map(std::move(delete_map_p)), row_id_indexes(std::move(row_id_indexes_p)),
      encryption_key(std::move(encryption_key_p)), allow_duplicates(allow_duplicates) {
	children.push_back(child);
}

class DuckLakeMetadataDeleteGlobalState : public GlobalSourceState {
public:
	mutex lock;
	bool finished = false;
};

class DuckLakeMetadataDelete : public PhysicalOperator {
public:
	DuckLakeMetadataDelete(PhysicalPlan &physical_plan, DuckLakeTableEntry &table,
	                       vector<DuckLakeFileListExtendedEntry> files)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
	      files(std::move(files)) {
	}

	DuckLakeTableEntry &table;
	vector<DuckLakeFileListExtendedEntry> files;

public:
	bool IsSource() const override {
		return true;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		return make_uniq<DuckLakeMetadataDeleteGlobalState>();
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &state = input.global_state.Cast<DuckLakeMetadataDeleteGlobalState>();
		lock_guard<mutex> guard(state.lock);
		if (state.finished) {
			return SourceResultType::FINISHED;
		}

		auto &transaction = DuckLakeTransaction::Get(context.client, table.catalog);
		idx_t count = 0;
		for (auto &file : files) {
			if (file.data_type != DuckLakeDataType::DATA_FILE) {
				throw InternalException("DuckLakeMetadataDelete can only drop data files");
			}
			count += file.row_count;
			if (file.file_id.IsValid()) {
				transaction.DropFile(table.GetTableId(), file.file_id, file.file.path, file.row_count,
				                     file.file.file_size_bytes);
			} else {
				transaction.DropTransactionLocalFile(table.GetTableId(), file.file.path);
			}
		}

		chunk.SetCardinality(1);
		chunk.data[0].SetValue(0, Value::BIGINT(NumericCast<int64_t>(count)));
		state.finished = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	string GetName() const override {
		return "DUCKLAKE_METADATA_DELETE";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Table Name"] = table.name;
		result["Files"] = std::to_string(files.size());
		return result;
	}
};

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
struct WrittenColumnInfo {
	WrittenColumnInfo() = default;
	WrittenColumnInfo(LogicalType type_p, int32_t field_id) : type(std::move(type_p)), field_id(field_id) {
	}

	LogicalType type;
	int32_t field_id;
};

class DuckLakeDeleteLocalState : public LocalSinkState {
public:
	optional_idx current_file_index;
	vector<idx_t> file_row_numbers;
	unordered_map<idx_t, string> filenames;
};

class DuckLakeDeleteGlobalState : public GlobalSinkState {
public:
	explicit DuckLakeDeleteGlobalState() {
		written_columns["file_path"] =
		    WrittenColumnInfo(LogicalType::VARCHAR, MultiFileReader::DELETE_FILE_PATH_FIELD_ID);
		written_columns["pos"] = WrittenColumnInfo(LogicalType::BIGINT, MultiFileReader::DELETE_POS_FIELD_ID);
	}

	mutex lock;
	unordered_map<string, DuckLakeDeleteFile> written_files;
	unordered_map<string, WrittenColumnInfo> written_columns;
	idx_t total_deleted_count = 0;
	unordered_map<uint64_t, unique_ptr<ColumnDataCollection>> deleted_rows;
	unordered_map<idx_t, string> filenames;

	void Flush(ClientContext &context, DuckLakeDeleteLocalState &local_state) {
		auto &local_entry = local_state.file_row_numbers;
		if (local_entry.empty()) {
			return;
		}
		lock_guard<mutex> guard(lock);
		auto deleted_row_idx = local_state.current_file_index.GetIndex();
		auto global_entry = deleted_rows.find(deleted_row_idx);
		DataChunk file_row_id_chunk;
		vector<LogicalType> row_id_types {LogicalType::UBIGINT};
		file_row_id_chunk.Initialize(context, row_id_types);

		optional_ptr<ColumnDataCollection> deleted_row_collection;
		if (global_entry == deleted_rows.end()) {
			auto collection = make_uniq<ColumnDataCollection>(context, row_id_types);
			deleted_row_collection = collection.get();
			deleted_rows[deleted_row_idx] = std::move(collection);
		} else {
			deleted_row_collection = global_entry->second.get();
		}
		ColumnDataAppendState append_state;
		deleted_row_collection->InitializeAppend(append_state);
		auto data = FlatVector::GetDataMutable<uint64_t>(file_row_id_chunk.data[0]);
		idx_t chunk_size = 0;
		for (idx_t r = 0; r < local_entry.size(); ++r) {
			data[chunk_size++] = local_entry[r];
			if (chunk_size == STANDARD_VECTOR_SIZE) {
				file_row_id_chunk.SetChildCardinality(chunk_size);
				deleted_row_collection->Append(append_state, file_row_id_chunk);
				chunk_size = 0;
			}
		}
		if (chunk_size > 0) {
			file_row_id_chunk.SetChildCardinality(chunk_size);
			deleted_row_collection->Append(append_state, file_row_id_chunk);
			chunk_size = 0;
		}
		total_deleted_count += local_entry.size();
		local_entry.clear();
	}

	void FinalFlush(ClientContext &context, DuckLakeDeleteLocalState &local_state) {
		Flush(context, local_state);
		// flush the file names to the global state
		lock_guard<mutex> guard(lock);
		for (auto &entry : local_state.filenames) {
			filenames.emplace(entry.first, entry.second);
		}
	}
};

unique_ptr<GlobalSinkState> DuckLakeDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DuckLakeDeleteGlobalState>();
}

unique_ptr<LocalSinkState> DuckLakeDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DuckLakeDeleteLocalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType DuckLakeDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<DuckLakeDeleteLocalState>();

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_index_vector = chunk.data[row_id_indexes[1]];
	auto &file_row_number = chunk.data[row_id_indexes[2]];

	UnifiedVectorFormat row_data;
	file_row_number.ToUnifiedFormat(row_data);
	auto file_row_data = UnifiedVectorFormat::GetData<int64_t>(row_data);

	UnifiedVectorFormat file_name_vdata;
	file_name_vector.ToUnifiedFormat(file_name_vdata);

	UnifiedVectorFormat file_index_vdata;
	file_index_vector.ToUnifiedFormat(file_index_vdata);

	auto file_index_data = UnifiedVectorFormat::GetData<uint64_t>(file_index_vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto file_idx = file_index_vdata.sel->get_index(i);
		auto row_idx = row_data.sel->get_index(i);
		if (!file_index_vdata.validity.RowIsValid(file_idx)) {
			throw InternalException("File index cannot be NULL!");
		}
		auto file_index = file_index_data[file_idx];
		if (!local_state.current_file_index.IsValid() || file_index != local_state.current_file_index.GetIndex()) {
			// file has changed - flush
			global_state.Flush(context.client, local_state);
			local_state.current_file_index = file_index;
			// insert the file name for the file if it has not yet been inserted
			auto entry = local_state.filenames.find(file_index);
			if (entry == local_state.filenames.end()) {
				auto file_name_idx = file_name_vdata.sel->get_index(i);
				auto file_name_data = UnifiedVectorFormat::GetData<string_t>(file_name_vdata);
				if (!file_name_vdata.validity.RowIsValid(file_name_idx)) {
					throw InternalException("Filename cannot be NULL!");
				}
				local_state.filenames.emplace(file_index, file_name_data[file_name_idx].GetString());
			}
		}
		auto row_number = file_row_data[row_idx];
		local_state.file_row_numbers.push_back(row_number);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType DuckLakeDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<DuckLakeDeleteLocalState>();
	global_state.FinalFlush(context.client, local_state);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
bool DuckLakeDelete::TryDropFullyDeletedFile(DuckLakeTransaction &transaction, const DuckLakeDeleteFile &delete_file,
                                             const DuckLakeFileListExtendedEntry &data_file_info,
                                             idx_t delete_count) const {
	if (delete_count != data_file_info.row_count) {
		return false;
	}
	// ALL rows in this file are deleted - drop the file
	if (delete_file.data_file_id.IsValid()) {
		transaction.DropFile(table.GetTableId(), delete_file.data_file_id, data_file_info.file.path,
		                     data_file_info.row_count, data_file_info.file.file_size_bytes);
	} else {
		transaction.DropTransactionLocalFile(table.GetTableId(), data_file_info.file.path);
	}
	return true;
}

void DuckLakeDelete::FlushMergedDeletionVector(DuckLakeTransaction &transaction, ClientContext &context,
                                               DuckLakeDeleteGlobalState &global_state, const string &filename,
                                               const DuckLakeFileListExtendedEntry &data_file_info,
                                               DuckLakeDeleteData &existing_delete_data,
                                               const set<idx_t> &sorted_deletes,
                                               DuckLakeDeleteFile &delete_file) const {
	// Deletion vectors don't support per-position snapshot tracking.
	// Merge all positions (existing + new) into a flat set, like the old pre-snapshot code.
	set<idx_t> all_deletes = sorted_deletes;
	auto &existing_deletes = existing_delete_data.deleted_rows;
	all_deletes.insert(existing_deletes.begin(), existing_deletes.end());

	// clear the deletes from the map
	delete_map->ClearDeletes(filename);

	// set the delete file as overwriting existing deletes
	delete_file.overwrites_existing_delete = true;

	if (TryDropFullyDeletedFile(transaction, delete_file, data_file_info, all_deletes.size())) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	WriteDeleteFileInput input {context,        transaction, fs,          table.DataPath(),
	                            encryption_key, filename,    all_deletes, DeleteFileSource::REGULAR};
	auto written_file = DuckLakeDeleteFileWriter::WriteDeletionVectorFile(context, input);

	written_file.data_file_id = delete_file.data_file_id;
	written_file.overwrites_existing_delete = delete_file.overwrites_existing_delete;
	// track the old delete file for deletion from metadata
	written_file.overwritten_delete_file.delete_file_id = data_file_info.delete_file_id;
	written_file.overwritten_delete_file.path = data_file_info.delete_file.path;

	global_state.written_files.emplace(filename, std::move(written_file));
}

void DuckLakeDelete::FlushDeleteWithSnapshots(DuckLakeTransaction &transaction, ClientContext &context,
                                              DuckLakeDeleteGlobalState &global_state, const string &filename,
                                              const DuckLakeFileListExtendedEntry &data_file_info,
                                              DuckLakeDeleteData &existing_delete_data,
                                              const set<idx_t> &sorted_deletes, DuckLakeDeleteFile &delete_file) const {
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	if (catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId())) {
		FlushMergedDeletionVector(transaction, context, global_state, filename, data_file_info, existing_delete_data,
		                          sorted_deletes, delete_file);
		return;
	}

	auto existing_snapshot = data_file_info.delete_file_begin_snapshot;

	// the commit snapshot for new deletes is current_snapshot + 1
	const auto current_snapshot = transaction.GetSnapshot();
	const idx_t new_delete_snapshot = current_snapshot.snapshot_id + 1;

	set<PositionWithSnapshot> sorted_deletes_with_snapshots;
	// add existing deletes with their snapshot IDs
	MergeDeletesWithSnapshots(existing_delete_data, existing_snapshot.GetIndex(), sorted_deletes_with_snapshots);

	// add new deletes with the commit snapshot
	for (auto &pos : sorted_deletes) {
		PositionWithSnapshot pos_with_snap;
		pos_with_snap.position = static_cast<int64_t>(pos);
		pos_with_snap.snapshot_id = static_cast<int64_t>(new_delete_snapshot);
		sorted_deletes_with_snapshots.insert(pos_with_snap);
	}

	// clear the deletes from the map
	delete_map->ClearDeletes(filename);

	// set the delete file as overwriting existing deletes
	delete_file.overwrites_existing_delete = true;

	if (TryDropFullyDeletedFile(transaction, delete_file, data_file_info, sorted_deletes_with_snapshots.size())) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	WriteDeleteFileWithSnapshotsInput input {context,
	                                         transaction,
	                                         fs,
	                                         table.DataPath(),
	                                         encryption_key,
	                                         filename,
	                                         sorted_deletes_with_snapshots,
	                                         DeleteFileSource::REGULAR};
	auto written_file = DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(context, input);

	written_file.data_file_id = delete_file.data_file_id;
	written_file.overwrites_existing_delete = delete_file.overwrites_existing_delete;
	// track the old delete file for deletion from metadata
	written_file.overwritten_delete_file.delete_file_id = data_file_info.delete_file_id;
	written_file.overwritten_delete_file.path = data_file_info.delete_file.path;

	idx_t max_snapshot = 0;
	for (auto &entry : sorted_deletes_with_snapshots) {
		max_snapshot = MaxValue(max_snapshot, static_cast<idx_t>(entry.snapshot_id));
	}
	written_file.max_snapshot = max_snapshot;

	global_state.written_files.emplace(filename, std::move(written_file));
}

void DuckLakeDelete::FlushDelete(DuckLakeTransaction &transaction, ClientContext &context,
                                 DuckLakeDeleteGlobalState &global_state, const string &filename,
                                 ColumnDataCollection &deleted_rows) const {
	// find the matching data file for the deletion
	auto data_file_info = delete_map->GetExtendedFileInfo(filename);

	// sort and duplicate eliminate the deletes
	set<idx_t> sorted_deletes;
	for (auto &chunk : deleted_rows.Chunks()) {
		auto row_data = FlatVector::GetData<uint64_t>(chunk.data[0]);
		for (idx_t r = 0; r < chunk.size(); r++) {
			sorted_deletes.insert(row_data[r]);
		}
	}
	if (sorted_deletes.size() != deleted_rows.Count() && !allow_duplicates) {
		throw NotImplementedException("The same row was updated multiple times - this is not (yet) supported in "
		                              "DuckLake. Eliminate duplicate matches prior to running the UPDATE");
	}

	if (data_file_info.data_type == DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA) {
		// deletes from transaction-local inlined data are directly deleted from the inlined data
		transaction.DeleteFromLocalInlinedData(table.GetTableId(), std::move(sorted_deletes));
		return;
	}
	if (data_file_info.data_type == DuckLakeDataType::INLINED_DATA) {
		// deletes from inlined data are not written to a file but pushed directly into the metadata manager
		transaction.AddNewInlinedDeletes(table.GetTableId(), data_file_info.file.path, std::move(sorted_deletes));
		return;
	}
	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = filename;
	delete_file.data_file_id = data_file_info.file_id;
	// check if the file already has deletes
	auto existing_delete_data = delete_map->GetDeleteData(filename);

	// check if we should use inlined file deletions instead of creating a delete file
	if (data_file_info.file_id.IsValid()) {
		auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
		auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
		auto threshold = catalog.DataInliningRowLimit(context, schema.GetSchemaId(), table.GetTableId());
		if (threshold > 0 && sorted_deletes.size() <= threshold) {
			// use inlined file deletions
			if (catalog.CheckInlinedDeletionTableCache(table.GetTableId(), transaction.GetSnapshot()) !=
			    InlinedDeletionCacheResult::EXISTS) {
				// this commit may create the inlined-delete table - take the client-side path
				transaction.SetRequiresNewInlinedTable(true);
			}
			transaction.AddNewInlinedFileDeletes(table.GetTableId(), data_file_info.file_id.index,
			                                     std::move(sorted_deletes));
			return;
		}
	}

	if (existing_delete_data) {
		// deletes already exist for this file
		auto &existing_deletes = existing_delete_data->deleted_rows;

		// we check if we need to write the snapshot information into our deletion file
		// that basically happens if the file already has embedded snapshots
		// or if it's a delete file from a different transaction (committed delete file)
		bool write_with_snapshots =
		    existing_delete_data->HasEmbeddedSnapshots() || data_file_info.delete_file_id.IsValid();

		if (write_with_snapshots) {
			FlushDeleteWithSnapshots(transaction, context, global_state, filename, data_file_info,
			                         *existing_delete_data, sorted_deletes, delete_file);
			return;
		}

		// transaction-local deletes without metadata
		sorted_deletes.insert(existing_deletes.begin(), existing_deletes.end());

		// clear the deletes
		delete_map->ClearDeletes(filename);

		// set the delete file as overwriting existing deletes
		delete_file.overwrites_existing_delete = true;
	}
	if (TryDropFullyDeletedFile(transaction, delete_file, data_file_info, sorted_deletes.size())) {
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	bool use_deletion_vectors = catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId());

	WriteDeleteFileInput input {context,
	                            transaction,
	                            fs,
	                            table.DataPath(),
	                            encryption_key,
	                            filename,
	                            sorted_deletes,
	                            DeleteFileSource::REGULAR};
	auto written_file = use_deletion_vectors ? DuckLakeDeleteFileWriter::WriteDeletionVectorFile(context, input)
	                                         : DuckLakeDeleteFileWriter::WriteDeleteFile(context, input);

	written_file.data_file_id = delete_file.data_file_id;
	written_file.overwrites_existing_delete = delete_file.overwrites_existing_delete;

	global_state.written_files.emplace(filename, std::move(written_file));
}

SinkFinalizeType DuckLakeDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeDeleteGlobalState>();
	if (global_state.deleted_rows.empty()) {
		return SinkFinalizeType::READY;
	}

	auto &transaction = DuckLakeTransaction::Get(context, table.catalog);
	// write out the delete rows
	for (auto &entry : global_state.deleted_rows) {
		auto filename_entry = global_state.filenames.find(entry.first);
		if (filename_entry == global_state.filenames.end()) {
			throw InternalException("Filename not found for file index");
		}
		FlushDelete(transaction, context, global_state, filename_entry->second, *entry.second);
	}
	vector<DuckLakeDeleteFile> delete_files;
	for (auto &entry : global_state.written_files) {
		auto &data_file_path = entry.first;
		auto delete_file = std::move(entry.second);
		if (delete_file.data_file_id.IsValid()) {
			// deleting from a committed file - add to delete files directly
			delete_files.push_back(std::move(delete_file));
		} else {
			// deleting from a transaction local file - find the file we are deleting from
			delete_file.overwrites_existing_delete = false;
			transaction.TransactionLocalDelete(table.GetTableId(), data_file_path, std::move(delete_file));
		}
	}
	transaction.AddDeletes(table.GetTableId(), std::move(delete_files));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<DuckLakeDeleteGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count));
	chunk.data[0].Append(value);
	chunk.SetChildCardinality(1);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeDelete::GetName() const {
	return "DUCKLAKE_DELETE";
}

InsertionOrderPreservingMap<string> DuckLakeDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

optional_ptr<PhysicalTableScan> FindDeleteSource(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		// does this emit the virtual columns?
		auto &scan = plan.Cast<PhysicalTableScan>();
		bool found = false;
		for (auto &col : scan.column_ids) {
			if (col.GetPrimaryIndex() == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				found = true;
				break;
			}
		}
		if (!found) {
			return nullptr;
		}
		return scan;
	}
	for (auto &children : plan.children) {
		auto result = FindDeleteSource(children.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

bool IsIdentityPartitionField(DuckLakeTableEntry &table, idx_t field_index);

bool PhysicalFilterReferencesOnlyIdentityPartitionFields(DuckLakeTableEntry &table, const Expression &expr,
                                                         PhysicalTableScan &scan) {
	bool result = true;
	ExpressionIterator::VisitExpression<BoundReferenceExpression>(expr, [&](const BoundReferenceExpression &col_ref) {
		auto column_index = col_ref.Index();
		if (column_index >= scan.column_ids.size()) {
			result = false;
			return;
		}
		auto &column_id = scan.column_ids[column_index];
		if (column_id.IsVirtualColumn()) {
			result = false;
			return;
		}
		auto &field_id = table.GetFieldId(PhysicalIndex(column_id.GetPrimaryIndex()));
		if (!IsIdentityPartitionField(table, field_id.GetFieldIndex().index)) {
			result = false;
		}
	});
	return result;
}

optional_ptr<PhysicalTableScan> FindMetadataDeleteSource(DuckLakeTableEntry &table, PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		return plan.Cast<PhysicalTableScan>();
	}
	// Residual filters are transparent only when they still reference identity partition columns. File-level safety is
	// enforced below by checking recorded partition values for every candidate file.
	if (plan.type == PhysicalOperatorType::PROJECTION && plan.children.size() == 1) {
		return FindMetadataDeleteSource(table, plan.children[0].get());
	}
	if (plan.type == PhysicalOperatorType::FILTER && plan.children.size() == 1) {
		auto scan = FindMetadataDeleteSource(table, plan.children[0].get());
		if (!scan) {
			return nullptr;
		}
		auto &filter = plan.Cast<PhysicalFilter>();
		if (!filter.expression || !PhysicalFilterReferencesOnlyIdentityPartitionFields(table, *filter.expression, *scan)) {
			return nullptr;
		}
		return scan;
	}
	return nullptr;
}

bool IsIdentityPartitionField(DuckLakeTableEntry &table, idx_t field_index) {
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		return false;
	}
	for (auto &field : partition_data->fields) {
		if (field.field_id.index == field_index && field.transform.type == DuckLakeTransformType::IDENTITY) {
			return true;
		}
	}
	return false;
}

struct MetadataDeleteLogicalContext {
	optional_idx table_index;
	vector<optional_idx> output_partition_keys;
};

bool ExpressionReferencesOnlyIdentityPartitionFields(DuckLakeTableEntry &table, const Expression &expr,
                                                     const MetadataDeleteLogicalContext &logical_context) {
	bool result = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &col_ref) {
		auto &binding = col_ref.Binding();
		if (!logical_context.table_index.IsValid() ||
		    binding.table_index != TableIndex(logical_context.table_index.GetIndex())) {
			result = false;
			return;
		}
		auto &column = table.GetColumn(LogicalIndex(binding.column_index));
		auto &field_id = table.GetFieldId(column.Physical());
		if (!IsIdentityPartitionField(table, field_id.GetFieldIndex().index)) {
			result = false;
		}
	});
	ExpressionIterator::VisitExpression<BoundReferenceExpression>(expr, [&](const BoundReferenceExpression &col_ref) {
		auto column_index = col_ref.Index();
		if (column_index >= logical_context.output_partition_keys.size() ||
		    !logical_context.output_partition_keys[column_index].IsValid()) {
			result = false;
		}
	});
	return result;
}

bool GetIdentityPartitionKeyIndex(DuckLakeTableEntry &table, idx_t field_index, optional_idx &partition_key_index) {
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		return false;
	}
	for (auto &field : partition_data->fields) {
		if (field.field_id.index == field_index && field.transform.type == DuckLakeTransformType::IDENTITY) {
			partition_key_index = field.partition_key_index;
			return true;
		}
	}
	return false;
}

bool AddPartitionValue(vector<Value> &values, const Value &value) {
	if (value.IsNull()) {
		return false;
	}
	for (auto &existing_value : values) {
		if (Value::NotDistinctFrom(existing_value, value)) {
			return true;
		}
	}
	values.push_back(value);
	return true;
}

bool IntersectPartitionValues(vector<Value> &left, const vector<Value> &right) {
	vector<Value> result;
	for (auto &left_value : left) {
		for (auto &right_value : right) {
			if (Value::NotDistinctFrom(left_value, right_value)) {
				if (!AddPartitionValue(result, left_value)) {
					return false;
				}
				break;
			}
		}
	}
	if (result.empty()) {
		return false;
	}
	left = std::move(result);
	return true;
}

bool MergePartitionValuesConjunction(unordered_map<idx_t, vector<Value>> &target,
                                     const unordered_map<idx_t, vector<Value>> &source) {
	for (auto &entry : source) {
		if (entry.second.empty()) {
			return false;
		}
		auto result = target.find(entry.first);
		if (result == target.end()) {
			target.emplace(entry.first, entry.second);
		} else if (!IntersectPartitionValues(result->second, entry.second)) {
			return false;
		}
	}
	return true;
}

bool MergePartitionValuesDisjunction(unordered_map<idx_t, vector<Value>> &target,
                                     const unordered_map<idx_t, vector<Value>> &source) {
	if (source.size() != 1) {
		return false;
	}
	if (target.empty()) {
		target = source;
		return true;
	}
	if (target.size() != 1 || target.begin()->first != source.begin()->first) {
		return false;
	}
	for (auto &value : source.begin()->second) {
		if (!AddPartitionValue(target.begin()->second, value)) {
			return false;
		}
	}
	return true;
}

bool ExtractTableFilterValuesFromExpression(const Expression &expr, vector<Value> &values);

bool ExtractTableFilterValuesFromComparison(const BoundFunctionExpression &expr, vector<Value> &values) {
	if (!BoundComparisonExpression::IsComparison(expr) || expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	auto &left = BoundComparisonExpression::Left(expr);
	auto &right = BoundComparisonExpression::Right(expr);
	if (right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return AddPartitionValue(values, right.Cast<BoundConstantExpression>().GetValue());
	}
	if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return AddPartitionValue(values, left.Cast<BoundConstantExpression>().GetValue());
	}
	return false;
}

bool ExtractTableFilterValuesFromComparison(const LegacyBoundComparisonExpression &expr, vector<Value> &values) {
	if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	if (!expr.left.get() || !expr.right.get()) {
		return false;
	}
	if (expr.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return AddPartitionValue(values, expr.right->Cast<BoundConstantExpression>().GetValue());
	}
	if (expr.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return AddPartitionValue(values, expr.left->Cast<BoundConstantExpression>().GetValue());
	}
	return false;
}

bool ExtractTableFilterValuesFromOperator(const BoundOperatorExpression &expr, vector<Value> &values) {
	auto &children = expr.GetChildren();
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		if (children.size() != 2) {
			return false;
		}
		if (!children[0].get() || !children[1].get()) {
			return false;
		}
		if (children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			return AddPartitionValue(values, children[1]->Cast<BoundConstantExpression>().GetValue());
		}
		if (children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			return AddPartitionValue(values, children[0]->Cast<BoundConstantExpression>().GetValue());
		}
		return false;
	case ExpressionType::COMPARE_IN:
		if (children.size() < 2) {
			return false;
		}
		if (!children[0].get()) {
			return false;
		}
		for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
			if (!children[child_idx].get() ||
			    children[child_idx]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
			    !AddPartitionValue(values, children[child_idx]->Cast<BoundConstantExpression>().GetValue())) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

bool ExtractTableFilterValuesFromExpression(const Expression &expr, vector<Value> &values) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::LEGACY_BOUND_COMPARISON:
		return ExtractTableFilterValuesFromComparison(expr.Cast<LegacyBoundComparisonExpression>(), values);
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (BoundComparisonExpression::IsComparison(func)) {
			return ExtractTableFilterValuesFromComparison(func, values);
		}
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractTableFilterValuesFromExpression(*data.child_filter_expr, values) : false;
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractTableFilterValuesFromExpression(*data.child_filter_expr, values) : false;
		}
		return false;
	}
	case ExpressionClass::BOUND_OPERATOR:
		return ExtractTableFilterValuesFromOperator(expr.Cast<BoundOperatorExpression>(), values);
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto &children = conjunction.GetChildren();
		if (children.empty()) {
			return false;
		}
		bool initialized = false;
		for (auto &child : children) {
			if (!child.get()) {
				return false;
			}
			vector<Value> child_values;
			if (!ExtractTableFilterValuesFromExpression(*child, child_values)) {
				return false;
			}
			if (!initialized) {
				values = std::move(child_values);
				initialized = true;
				continue;
			}
			switch (conjunction.GetExpressionType()) {
			case ExpressionType::CONJUNCTION_AND:
				if (!IntersectPartitionValues(values, child_values)) {
					return false;
				}
				break;
			case ExpressionType::CONJUNCTION_OR:
				for (auto &value : child_values) {
					if (!AddPartitionValue(values, value)) {
						return false;
					}
				}
				break;
			default:
				return false;
			}
		}
		return initialized;
	}
	default:
		return false;
	}
}

bool ExtractTableFilterValues(const TableFilter &filter, vector<Value> &values) {
	switch (filter.filter_type) {
	case TableFilterType::LEGACY_CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<LegacyConstantFilter>();
		if (constant_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		return AddPartitionValue(values, constant_filter.constant);
	}
	case TableFilterType::LEGACY_IN_FILTER: {
		auto &in_filter = filter.Cast<LegacyInFilter>();
		if (in_filter.values.empty()) {
			return false;
		}
		for (auto &value : in_filter.values) {
			if (!AddPartitionValue(values, value)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::LEGACY_OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<LegacyOptionalFilter>();
		return optional_filter.child_filter.get() ? ExtractTableFilterValues(*optional_filter.child_filter, values) : false;
	}
	case TableFilterType::LEGACY_CONJUNCTION_OR: {
		auto &conjunction = filter.Cast<LegacyConjunctionOrFilter>();
		if (conjunction.child_filters.empty()) {
			return false;
		}
		for (auto &child_filter : conjunction.child_filters) {
			if (!child_filter.get()) {
				return false;
			}
			if (!ExtractTableFilterValues(*child_filter, values)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::EXPRESSION_FILTER: {
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		return expression_filter.expr.get() ? ExtractTableFilterValuesFromExpression(*expression_filter.expr, values) : false;
	}
	default:
		return false;
	}
}

bool ExtractPartitionValuesFromLogicalGet(DuckLakeTableEntry &table, LogicalGet &get,
                                          unordered_map<idx_t, vector<Value>> &partition_values) {
	if (!get.table_filters.HasFilters()) {
		return true;
	}
	for (auto &entry : get.table_filters) {
		auto column_idx = entry.GetIndex();
		auto &column_id = get.GetColumnIndex(column_idx);
		if (column_id.IsVirtualColumn()) {
			return false;
		}
		auto &field_id = table.GetFieldId(PhysicalIndex(column_id.GetPrimaryIndex()));
		optional_idx partition_key_index;
		if (!GetIdentityPartitionKeyIndex(table, field_id.GetFieldIndex().index, partition_key_index)) {
			return false;
		}
		unordered_map<idx_t, vector<Value>> filter_partition_values;
		if (!ExtractTableFilterValues(entry.Filter(), filter_partition_values[partition_key_index.GetIndex()]) ||
		    !MergePartitionValuesConjunction(partition_values, filter_partition_values)) {
			return false;
		}
	}
	return true;
}

bool GetExpressionIdentityPartitionKeyIndex(DuckLakeTableEntry &table, const Expression &expr,
                                            const MetadataDeleteLogicalContext &logical_context,
                                            optional_idx &partition_key_index) {
	bool result = true;
	bool found_column = false;
	auto set_partition_key_index = [&](optional_idx next_partition_key_index) {
		if (!next_partition_key_index.IsValid()) {
			result = false;
			return;
		}
		if (found_column && partition_key_index != next_partition_key_index) {
			result = false;
			return;
		}
		found_column = true;
		partition_key_index = next_partition_key_index;
	};
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &col_ref) {
		auto &binding = col_ref.Binding();
		if (!logical_context.table_index.IsValid() ||
		    binding.table_index != TableIndex(logical_context.table_index.GetIndex())) {
			result = false;
			return;
		}
		auto &column = table.GetColumn(LogicalIndex(binding.column_index));
		auto &field_id = table.GetFieldId(column.Physical());
		optional_idx next_partition_key_index;
		if (!GetIdentityPartitionKeyIndex(table, field_id.GetFieldIndex().index, next_partition_key_index)) {
			result = false;
			return;
		}
		set_partition_key_index(next_partition_key_index);
	});
	ExpressionIterator::VisitExpression<BoundReferenceExpression>(expr, [&](const BoundReferenceExpression &col_ref) {
		auto column_index = col_ref.Index();
		if (column_index >= logical_context.output_partition_keys.size()) {
			result = false;
			return;
		}
		set_partition_key_index(logical_context.output_partition_keys[column_index]);
	});
	return result && found_column && partition_key_index.IsValid();
}

bool ExtractComparisonPartitionValues(DuckLakeTableEntry &table, const BoundFunctionExpression &expr,
                                      const MetadataDeleteLogicalContext &logical_context,
                                      unordered_map<idx_t, vector<Value>> &partition_values) {
	if (!BoundComparisonExpression::IsComparison(expr) || expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	auto &left = BoundComparisonExpression::Left(expr);
	auto &right = BoundComparisonExpression::Right(expr);
	reference<const Expression> column_expr(left);
	reference<const Expression> constant_expr(right);
	if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column_expr = right;
		constant_expr = left;
	}
	if (constant_expr.get().GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
	    !ExpressionReferencesOnlyIdentityPartitionFields(table, column_expr.get(), logical_context)) {
		return false;
	}
	optional_idx partition_key_index;
	if (!GetExpressionIdentityPartitionKeyIndex(table, column_expr.get(), logical_context, partition_key_index)) {
		return false;
	}
	return AddPartitionValue(partition_values[partition_key_index.GetIndex()],
	                         constant_expr.get().Cast<BoundConstantExpression>().GetValue());
}

bool ExtractComparisonPartitionValues(DuckLakeTableEntry &table, const LegacyBoundComparisonExpression &expr,
                                      const MetadataDeleteLogicalContext &logical_context,
                                      unordered_map<idx_t, vector<Value>> &partition_values) {
	if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	if (!expr.left.get() || !expr.right.get()) {
		return false;
	}
	reference<const Expression> column_expr(*expr.left);
	reference<const Expression> constant_expr(*expr.right);
	if (expr.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column_expr = *expr.right;
		constant_expr = *expr.left;
	}
	if (constant_expr.get().GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
	    !ExpressionReferencesOnlyIdentityPartitionFields(table, column_expr.get(), logical_context)) {
		return false;
	}
	optional_idx partition_key_index;
	if (!GetExpressionIdentityPartitionKeyIndex(table, column_expr.get(), logical_context, partition_key_index)) {
		return false;
	}
	return AddPartitionValue(partition_values[partition_key_index.GetIndex()],
	                         constant_expr.get().Cast<BoundConstantExpression>().GetValue());
}

bool ExtractInPartitionValues(DuckLakeTableEntry &table, const BoundOperatorExpression &expr,
                              const MetadataDeleteLogicalContext &logical_context,
                              unordered_map<idx_t, vector<Value>> &partition_values) {
	auto &children = expr.GetChildren();
	if (expr.GetExpressionType() != ExpressionType::COMPARE_IN || children.size() < 2 || !children[0].get() ||
	    !ExpressionReferencesOnlyIdentityPartitionFields(table, *children[0], logical_context)) {
		return false;
	}
	optional_idx partition_key_index;
	if (!GetExpressionIdentityPartitionKeyIndex(table, *children[0], logical_context, partition_key_index)) {
		return false;
	}
	auto &values = partition_values[partition_key_index.GetIndex()];
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		if (!children[child_idx].get() ||
		    children[child_idx]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
		    !AddPartitionValue(values, children[child_idx]->Cast<BoundConstantExpression>().GetValue())) {
			return false;
		}
	}
	return true;
}

bool ExtractOperatorPartitionValues(DuckLakeTableEntry &table, const BoundOperatorExpression &expr,
                                    const MetadataDeleteLogicalContext &logical_context,
                                    unordered_map<idx_t, vector<Value>> &partition_values) {
	if (expr.GetExpressionType() == ExpressionType::COMPARE_IN) {
		return ExtractInPartitionValues(table, expr, logical_context, partition_values);
	}
	auto &children = expr.GetChildren();
	if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL || children.size() != 2) {
		return false;
	}
	if (!children[0].get() || !children[1].get()) {
		return false;
	}
	reference<const Expression> column_expr(*children[0]);
	reference<const Expression> constant_expr(*children[1]);
	if (children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column_expr = *children[1];
		constant_expr = *children[0];
	}
	if (constant_expr.get().GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
	    !ExpressionReferencesOnlyIdentityPartitionFields(table, column_expr.get(), logical_context)) {
		return false;
	}
	optional_idx partition_key_index;
	if (!GetExpressionIdentityPartitionKeyIndex(table, column_expr.get(), logical_context, partition_key_index)) {
		return false;
	}
	return AddPartitionValue(partition_values[partition_key_index.GetIndex()],
	                         constant_expr.get().Cast<BoundConstantExpression>().GetValue());
}

bool ExtractPartitionValuesFromLogicalExpression(DuckLakeTableEntry &table, const Expression &expr,
                                                 const MetadataDeleteLogicalContext &logical_context,
                                                 unordered_map<idx_t, vector<Value>> &partition_values) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::LEGACY_BOUND_COMPARISON:
		return ExtractComparisonPartitionValues(table, expr.Cast<LegacyBoundComparisonExpression>(), logical_context,
		                                        partition_values);
	case ExpressionClass::BOUND_FUNCTION:
		return ExtractComparisonPartitionValues(table, expr.Cast<BoundFunctionExpression>(), logical_context,
		                                        partition_values);
	case ExpressionClass::BOUND_OPERATOR:
		return ExtractOperatorPartitionValues(table, expr.Cast<BoundOperatorExpression>(), logical_context,
		                                      partition_values);
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto &children = conjunction.GetChildren();
		if (children.empty()) {
			return false;
		}
		unordered_map<idx_t, vector<Value>> conjunction_values;
		bool initialized = false;
		for (auto &child : children) {
			if (!child.get()) {
				return false;
			}
			unordered_map<idx_t, vector<Value>> child_values;
			if (!ExtractPartitionValuesFromLogicalExpression(table, *child, logical_context, child_values)) {
				return false;
			}
			if (!initialized) {
				conjunction_values = std::move(child_values);
				initialized = true;
				continue;
			}
			switch (conjunction.GetExpressionType()) {
			case ExpressionType::CONJUNCTION_AND:
				if (!MergePartitionValuesConjunction(conjunction_values, child_values)) {
					return false;
				}
				break;
			case ExpressionType::CONJUNCTION_OR:
				if (!MergePartitionValuesDisjunction(conjunction_values, child_values)) {
					return false;
				}
				break;
			default:
				return false;
			}
		}
		return initialized && MergePartitionValuesConjunction(partition_values, conjunction_values);
	}
	default:
		return false;
	}
}

bool LogicalPlanGetMetadataDeletePartitionValues(DuckLakeTableEntry &table, LogicalOperator &op,
                                                 MetadataDeleteLogicalContext &logical_context,
                                                 unordered_map<idx_t, vector<Value>> &partition_values) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		logical_context.table_index = get.table_index.index;
		logical_context.output_partition_keys.clear();
		for (auto &column_id : get.GetColumnIds()) {
			optional_idx partition_key_index;
			if (!column_id.IsVirtualColumn()) {
				auto &field_id = table.GetFieldId(PhysicalIndex(column_id.GetPrimaryIndex()));
				GetIdentityPartitionKeyIndex(table, field_id.GetFieldIndex().index, partition_key_index);
			}
			logical_context.output_partition_keys.push_back(partition_key_index);
		}
		return ExtractPartitionValuesFromLogicalGet(table, get, partition_values);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op.children.size() != 1) {
			return false;
		}
		if (!LogicalPlanGetMetadataDeletePartitionValues(table, *op.children[0], logical_context, partition_values)) {
			return false;
		}
		auto &projection = op.Cast<LogicalProjection>();
		vector<optional_idx> projection_partition_keys;
		for (auto &expr : projection.expressions) {
			optional_idx partition_key_index;
			if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
				auto &bound_ref = expr->Cast<BoundReferenceExpression>();
				auto column_index = bound_ref.Index();
				if (column_index < logical_context.output_partition_keys.size()) {
					partition_key_index = logical_context.output_partition_keys[column_index];
				}
			}
			projection_partition_keys.push_back(partition_key_index);
		}
		logical_context.output_partition_keys = std::move(projection_partition_keys);
		return true;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.children.size() != 1 ||
		    !LogicalPlanGetMetadataDeletePartitionValues(table, *filter.children[0], logical_context, partition_values) ||
		    !logical_context.table_index.IsValid()) {
			return false;
		}
		for (auto &expr : filter.expressions) {
			if (!ExpressionReferencesOnlyIdentityPartitionFields(table, *expr, logical_context)) {
				return false;
			}
			unordered_map<idx_t, vector<Value>> expression_partition_values;
			if (!ExtractPartitionValuesFromLogicalExpression(table, *expr, logical_context,
			                                                 expression_partition_values) ||
			    !MergePartitionValuesConjunction(partition_values, expression_partition_values)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

bool ScanHasOnlyPartitionFilters(DuckLakeTableEntry &table, PhysicalTableScan &scan, DuckLakeMultiFileList &file_list) {
	if (scan.dynamic_filters && scan.dynamic_filters->HasFilters()) {
		return false;
	}
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return true;
	}
	auto filter_info = file_list.GetFilterPushdownInfo();
	if (!filter_info || filter_info->column_filters.size() != scan.table_filters->FilterCount()) {
		return false;
	}
	for (auto &entry : filter_info->column_filters) {
		if (!IsIdentityPartitionField(table, entry.first)) {
			return false;
		}
	}
	for (auto &entry : *scan.table_filters) {
		auto column_idx = entry.GetIndex().GetIndex();
		if (column_idx >= scan.column_ids.size()) {
			return false;
		}
		auto &column_id = scan.column_ids[column_idx];
		if (column_id.IsVirtualColumn()) {
			return false;
		}
		auto &field_id = table.GetFieldId(PhysicalIndex(column_id.GetPrimaryIndex()));
		if (!IsIdentityPartitionField(table, field_id.GetFieldIndex().index)) {
			return false;
		}
	}
	return true;
}

bool PartitionValueMatches(const Value &partition_value, const vector<Value> &accepted_values) {
	if (partition_value.IsNull()) {
		return false;
	}
	for (auto &accepted_value : accepted_values) {
		Value cast_value;
		if (!accepted_value.DefaultTryCastAs(partition_value.type(), cast_value, nullptr, true)) {
			continue;
		}
		if (Value::NotDistinctFrom(partition_value, cast_value)) {
			return true;
		}
	}
	return false;
}

bool FileMatchesMetadataDeletePartitionValues(const DuckLakeFileListExtendedEntry &file,
                                              const unordered_map<idx_t, vector<Value>> &partition_values) {
	for (auto &entry : partition_values) {
		bool found = false;
		for (auto &file_partition : file.partition_values) {
			if (file_partition.partition_column_idx != entry.first) {
				continue;
			}
			if (found || !PartitionValueMatches(file_partition.partition_value, entry.second)) {
				return false;
			}
			found = true;
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

bool HasActiveDeleteFilesForDataFiles(DuckLakeTransaction &transaction, TableIndex table_id,
                                      const vector<DuckLakeFileListExtendedEntry> &files) {
	string file_ids;
	for (auto &file : files) {
		if (!file.file_id.IsValid()) {
			continue;
		}
		if (!file_ids.empty()) {
			file_ids += ",";
		}
		file_ids += to_string(file.file_id.index);
	}
	if (file_ids.empty()) {
		return false;
	}
	auto result = transaction.Query(transaction.GetSnapshot(), StringUtil::Format(R"(
SELECT COUNT(*)
FROM {METADATA_CATALOG}.ducklake_delete_file
WHERE table_id=%d AND data_file_id IN (%s)
  AND {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
)",
	                                                                              table_id.index, file_ids));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to check active DuckLake delete files: ");
	}
	for (auto &row : *result) {
		return row.GetValue<idx_t>(0) > 0;
	}
	return false;
}

bool CanUseMetadataDelete(ClientContext &context, DuckLakeTableEntry &table, PhysicalOperator &child_plan,
                          const unordered_map<idx_t, vector<Value>> &partition_values,
                          vector<DuckLakeFileListExtendedEntry> &files) {
	auto scan = FindMetadataDeleteSource(table, child_plan);
	if (!scan || !table.GetPartitionData()) {
		return false;
	}

	auto &bind_data = scan->bind_data->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<DuckLakeMultiFileList>();
	if (!ScanHasOnlyPartitionFilters(table, *scan, file_list)) {
		return false;
	}
	if (partition_values.empty() && table.GetPartitionData() &&
	    (!scan->table_filters || !scan->table_filters->HasFilters())) {
		// Full-table delete on a partitioned table: every file selected by the scan is fully deleted.
	} else if (partition_values.empty()) {
		return false;
	}
	auto &transaction = DuckLakeTransaction::Get(context, table.catalog);
	if (!transaction.GetMetadataManager()
	         .GetInlinedDeletionTableName(table.GetTableId(), transaction.GetSnapshot())
	         .empty()) {
		return false;
	}
	auto extended_files = file_list.GetFilesExtended();
	vector<DuckLakeFileListExtendedEntry> data_files;
	optional_idx inlined_row_count;
	for (auto &file : extended_files) {
		if (file.data_type != DuckLakeDataType::DATA_FILE) {
			if (file.data_type == DuckLakeDataType::INLINED_DATA) {
				if (!inlined_row_count.IsValid()) {
					inlined_row_count = table.GetNetInlinedRowCount(transaction);
				}
				if (inlined_row_count.GetIndex() == 0) {
					continue;
				}
			}
			return false;
		}
		// Existing row-level deletes make the raw file row count different from the visible DELETE count.
		if (file.delete_file_id.IsValid() || !file.delete_file.path.empty()) {
			return false;
		}
		if (!FileMatchesMetadataDeletePartitionValues(file, partition_values)) {
			return false;
		}
		data_files.push_back(std::move(file));
	}
	if (HasActiveDeleteFilesForDataFiles(transaction, table.GetTableId(), data_files)) {
		return false;
	}
	files = std::move(data_files);
	return true;
}

PhysicalOperator &DuckLakeDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                             DuckLakeTableEntry &table, PhysicalOperator &child_plan,
                                             vector<idx_t> row_id_indexes, string encryption_key, bool allow_duplicates,
                                             unordered_map<idx_t, vector<Value>> metadata_delete_partition_values) {
	vector<DuckLakeFileListExtendedEntry> metadata_delete_files;
	if (allow_duplicates &&
	    CanUseMetadataDelete(context, table, child_plan, metadata_delete_partition_values, metadata_delete_files)) {
		return planner.Make<DuckLakeMetadataDelete>(table, std::move(metadata_delete_files));
	}

	auto delete_source = FindDeleteSource(child_plan);
	auto delete_map = make_shared_ptr<DuckLakeDeleteMap>();
	if (delete_source) {
		auto &bind_data = delete_source->bind_data->Cast<MultiFileBindData>();
		auto &reader = bind_data.multi_file_reader->Cast<DuckLakeMultiFileReader>();
		auto &file_list = bind_data.file_list->Cast<DuckLakeMultiFileList>();
		auto files = file_list.GetFilesExtended();
		for (auto &file_entry : files) {
			delete_map->AddExtendedFileInfo(std::move(file_entry));
		}
		reader.delete_map = delete_map;
	}
	return planner.Make<DuckLakeDelete>(table, child_plan, std::move(delete_map), std::move(row_id_indexes),
	                                    std::move(encryption_key), allow_duplicates);
}

static PhysicalOperator &PlanDuckLakeDelete(DuckLakeCatalog &catalog, ClientContext &context,
                                            PhysicalPlanGenerator &planner, LogicalDelete &op, PhysicalOperator &child_plan,
                                            unordered_map<idx_t, vector<Value>> metadata_delete_partition_values) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a DuckLake table");
	}
	auto encryption_key = catalog.GenerateEncryptionKey(context);
	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < 3; i++) {
		auto &bound_ref = op.expressions[i + 1]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.Index());
	}
	return DuckLakeDelete::PlanDelete(context, planner, op.table.Cast<DuckLakeTableEntry>(), child_plan,
	                                  std::move(row_id_indexes), std::move(encryption_key), true,
	                                  std::move(metadata_delete_partition_values));
}

PhysicalOperator &DuckLakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) {
	D_ASSERT(op.children.size() == 1);
	MetadataDeleteLogicalContext logical_context;
	unordered_map<idx_t, vector<Value>> metadata_delete_partition_values;
	if (!LogicalPlanGetMetadataDeletePartitionValues(op.table.Cast<DuckLakeTableEntry>(), *op.children[0],
	                                                logical_context, metadata_delete_partition_values)) {
		metadata_delete_partition_values.clear();
	}
	auto &child_plan = planner.CreatePlan(*op.children[0]);
	return PlanDuckLakeDelete(*this, context, planner, op, child_plan, std::move(metadata_delete_partition_values));
}

PhysicalOperator &DuckLakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                              PhysicalOperator &child_plan) {
	return PlanDuckLakeDelete(*this, context, planner, op, child_plan, {});
}

} // namespace duckdb
