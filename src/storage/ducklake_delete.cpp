#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_puffin.hpp"
#include "duckdb/common/map.hpp"
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
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_field_data.hpp"
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

	vector<Identifier> names_to_write {"file_path", "pos"};
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
	auto rg_entry = stats.extra_info.find("row_group_count");
	if (rg_entry != stats.extra_info.end() && !rg_entry->second.IsNull()) {
		delete_file.row_group_count = rg_entry->second.GetValue<idx_t>();
	}
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

template <typename InputType>
static DuckLakeDeleteFile WritePuffinDeleteFile(InputType &input,
                                                const vector<DuckLakePuffinWriter::BlobInput> &blobs) {
	auto delete_file_uuid = "ducklake-" + input.transaction.GenerateUUID() + "-delete.puffin";
	string delete_file_path = DuckLakeUtil::JoinPath(input.fs, input.data_path, delete_file_uuid);

	DuckLakeUtil::EnsureDirectoryExists(input.fs, input.data_path);
	auto result = DuckLakePuffinWriter::Write(input.fs, delete_file_path, input.data_file_path, blobs);

	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = input.data_file_path;
	delete_file.file_name = delete_file_path;
	delete_file.format = DeleteFileFormat::PUFFIN;
	delete_file.delete_count = result.delete_count;
	delete_file.file_size_bytes = result.file_size_bytes;
	delete_file.footer_size = result.footer_size;
	// puffin files are written in plaintext
	delete_file.encryption_key = input.encryption_key;
	delete_file.source = input.source;
	return delete_file;
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeletionVectorFile(ClientContext &context,
                                                                     WriteDeleteFileInput &input) {
	// single blob - its snapshot is the metadata begin_snapshot assigned at commit
	vector<DuckLakePuffinWriter::BlobInput> blobs;
	DuckLakePuffinWriter::BlobInput blob;
	blob.positions = &input.positions;
	blobs.push_back(blob);
	return WritePuffinDeleteFile(input, blobs);
}

DuckLakeDeleteFile
DuckLakeDeleteFileWriter::WriteDeletionVectorFileWithSnapshots(ClientContext &context,
                                                               WriteDeleteFileWithSnapshotsInput &input) {
	// group positions into per-snapshot deltas
	map<idx_t, set<idx_t>> deltas;
	for (auto &entry : input.positions) {
		deltas[NumericCast<idx_t>(entry.snapshot_id)].insert(NumericCast<idx_t>(entry.position));
	}
	// one cumulative deletion vector per snapshot
	vector<set<idx_t>> cumulative;
	cumulative.reserve(deltas.size());
	set<idx_t> running;
	for (auto &entry : deltas) {
		running.insert(entry.second.begin(), entry.second.end());
		cumulative.push_back(running);
	}
	// snapshot ids are pre-commit values, matching the parquet writer
	vector<DuckLakePuffinWriter::BlobInput> blobs;
	idx_t blob_idx = 0;
	for (auto &entry : deltas) {
		DuckLakePuffinWriter::BlobInput blob;
		blob.snapshot_id = entry.first;
		blob.positions = &cumulative[blob_idx++];
		blobs.push_back(blob);
	}
	auto delete_file = WritePuffinDeleteFile(input, blobs);
	if (!deltas.empty()) {
		delete_file.begin_snapshot = deltas.begin()->first;
	}
	return delete_file;
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::Write(ClientContext &context, WriteDeleteFileInput &input,
                                                   bool use_deletion_vectors) {
	return use_deletion_vectors ? WriteDeletionVectorFile(context, input) : WriteDeleteFile(context, input);
}

DuckLakeDeleteFile DuckLakeDeleteFileWriter::Write(ClientContext &context, WriteDeleteFileWithSnapshotsInput &input,
                                                   bool use_deletion_vectors) {
	return use_deletion_vectors ? WriteDeletionVectorFileWithSnapshots(context, input)
	                            : WriteDeleteFileWithSnapshots(context, input);
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

		chunk.SetChildCardinality(1);
		chunk.data[0].SetValue(0, Value::BIGINT(NumericCast<int64_t>(count)));
		state.finished = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	string GetName() const override {
		return "DUCKLAKE_METADATA_DELETE";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Table Name"] = table.name.GetIdentifierName();
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

void DuckLakeDelete::FlushDeleteWithSnapshots(DuckLakeTransaction &transaction, ClientContext &context,
                                              DuckLakeDeleteGlobalState &global_state, const string &filename,
                                              const DuckLakeFileListExtendedEntry &data_file_info,
                                              DuckLakeDeleteData &existing_delete_data,
                                              const set<idx_t> &sorted_deletes, DuckLakeDeleteFile &delete_file) const {
	// the commit snapshot for new deletes is current_snapshot + 1
	const auto current_snapshot = transaction.GetSnapshot();
	const idx_t new_delete_snapshot = current_snapshot.snapshot_id + 1;

	set<PositionWithSnapshot> sorted_deletes_with_snapshots;
	// add existing deletes with their snapshot IDs
	idx_t fallback_snapshot = 0;
	if (!existing_delete_data.HasEmbeddedSnapshots()) {
		fallback_snapshot = data_file_info.delete_file_begin_snapshot.GetIndex();
	}
	MergeDeletesWithSnapshots(existing_delete_data, fallback_snapshot, sorted_deletes_with_snapshots);

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
	auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	bool use_deletion_vectors = catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId());
	auto written_file = DuckLakeDeleteFileWriter::Write(context, input, use_deletion_vectors);

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

	if (!existing_delete_data &&
	    TryDropFullyDeletedFile(transaction, delete_file, data_file_info, sorted_deletes.size())) {
		return;
	}

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
	auto written_file = DuckLakeDeleteFileWriter::Write(context, input, use_deletion_vectors);

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
	result["Table Name"] = table.name.GetIdentifierName();
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

bool IsIdentityPartitionField(DuckLakeTableEntry &table, idx_t field_index) {
	optional_idx partition_key_index;
	return GetIdentityPartitionKeyIndex(table, field_index, partition_key_index);
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

bool MergeSinglePartitionKeyValues(unordered_map<idx_t, vector<Value>> &target,
                                   const unordered_map<idx_t, vector<Value>> &source) {
	if (source.size() != 1 || source.begin()->second.empty()) {
		return false;
	}
	if (target.empty()) {
		target = source;
		return true;
	}
	if (target.size() != 1 || target.begin()->first != source.begin()->first) {
		return false;
	}
	return IntersectPartitionValues(target.begin()->second, source.begin()->second);
}

bool UnionSinglePartitionKeyValues(unordered_map<idx_t, vector<Value>> &target,
                                   const unordered_map<idx_t, vector<Value>> &source) {
	if (source.size() != 1 || source.begin()->second.empty()) {
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

template <class GET_PARTITION_KEY>
bool ExtractComparisonPartitionValues(const Expression &left, const Expression &right,
                                      GET_PARTITION_KEY &get_partition_key,
                                      unordered_map<idx_t, vector<Value>> &partition_values) {
	auto partition_expr = reference<const Expression>(left);
	auto constant_expr = reference<const Expression>(right);
	if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		partition_expr = right;
		constant_expr = left;
	}
	if (constant_expr.get().GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	optional_idx partition_key_index;
	if (!get_partition_key(partition_expr.get(), partition_key_index)) {
		return false;
	}
	return AddPartitionValue(partition_values[partition_key_index.GetIndex()],
	                         constant_expr.get().Cast<BoundConstantExpression>().GetValue());
}

template <class GET_PARTITION_KEY>
bool ExtractIdentityPartitionPredicate(const Expression &expr, GET_PARTITION_KEY &get_partition_key,
                                       unordered_map<idx_t, vector<Value>> &partition_values) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::LEGACY_BOUND_COMPARISON: {
		auto &comparison = expr.Cast<LegacyBoundComparisonExpression>();
		if (comparison.GetExpressionType() != ExpressionType::COMPARE_EQUAL || !comparison.left.get() ||
		    !comparison.right.get()) {
			return false;
		}
		return ExtractComparisonPartitionValues(*comparison.left, *comparison.right, get_partition_key,
		                                        partition_values);
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (!BoundComparisonExpression::IsComparison(func) ||
		    func.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		return ExtractComparisonPartitionValues(BoundComparisonExpression::Left(func),
		                                        BoundComparisonExpression::Right(func), get_partition_key,
		                                        partition_values);
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto &children = op.GetChildren();
		if (op.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
			if (children.size() != 2 || !children[0].get() || !children[1].get()) {
				return false;
			}
			return ExtractComparisonPartitionValues(*children[0], *children[1], get_partition_key, partition_values);
		}
		if (op.GetExpressionType() != ExpressionType::COMPARE_IN || children.size() < 2 || !children[0].get()) {
			return false;
		}
		optional_idx partition_key_index;
		if (!get_partition_key(*children[0], partition_key_index)) {
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
	case ExpressionClass::BOUND_CONJUNCTION: {
		if (expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		    expr.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
			return false;
		}
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		if (conjunction.GetChildren().empty()) {
			return false;
		}
		bool is_or = expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
		for (auto &child : conjunction.GetChildren()) {
			unordered_map<idx_t, vector<Value>> child_partition_values;
			if (!child || !ExtractIdentityPartitionPredicate(*child, get_partition_key, child_partition_values)) {
				return false;
			}
			if (is_or && !UnionSinglePartitionKeyValues(partition_values, child_partition_values)) {
				return false;
			}
			if (!is_or && !MergeSinglePartitionKeyValues(partition_values, child_partition_values)) {
				return false;
			}
		}
		return true;
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
		return false;
	}
	case TableFilterType::EXPRESSION_FILTER: {
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		auto get_partition_key = [](const Expression &expr, optional_idx &partition_key_index) {
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF) {
				return false;
			}
			// ExpressionFilter table filters bind their filtered column as local BOUND_REF(0).
			if (expr.Cast<BoundReferenceExpression>().Index() != 0) {
				return false;
			}
			partition_key_index = 0;
			return true;
		};
		unordered_map<idx_t, vector<Value>> partition_values;
		if (!expression_filter.expr ||
		    !ExtractIdentityPartitionPredicate(*expression_filter.expr, get_partition_key, partition_values) ||
		    partition_values.size() != 1 || partition_values.find(0) == partition_values.end()) {
			return false;
		}
		values = std::move(partition_values.find(0)->second);
		return true;
	}
	default:
		return false;
	}
}

bool IsRootOptionalTableFilter(const TableFilter &filter) {
	if (filter.filter_type == TableFilterType::LEGACY_OPTIONAL_FILTER) {
		return true;
	}
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	return ExpressionFilter::IsRootOptionalFilter(filter);
}

struct MetadataDeleteOutputColumn {
	optional_idx field_index;
	optional_idx partition_key_index;
};

struct MetadataDeleteLogicalContext {
	optional_idx table_index;
	vector<MetadataDeleteOutputColumn> output_columns;
};

bool GetLogicalExpressionFieldIndex(DuckLakeTableEntry &, const Expression &expr,
                                    const MetadataDeleteLogicalContext &logical_context, optional_idx &field_index) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		auto &binding = col_ref.Binding();
		if (!logical_context.table_index.IsValid() ||
		    binding.table_index != TableIndex(logical_context.table_index.GetIndex())) {
			return false;
		}
		auto column_index = binding.column_index;
		if (column_index >= logical_context.output_columns.size() ||
		    !logical_context.output_columns[column_index].field_index.IsValid()) {
			return false;
		}
		field_index = logical_context.output_columns[column_index].field_index;
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto column_index = expr.Cast<BoundReferenceExpression>().Index();
		if (column_index >= logical_context.output_columns.size() ||
		    !logical_context.output_columns[column_index].field_index.IsValid()) {
			return false;
		}
		field_index = logical_context.output_columns[column_index].field_index;
		return true;
	}
	return false;
}

bool GetLogicalExpressionPartitionKeyIndex(DuckLakeTableEntry &table, const Expression &expr,
                                           const MetadataDeleteLogicalContext &logical_context,
                                           optional_idx &partition_key_index) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto column_index = expr.Cast<BoundReferenceExpression>().Index();
		if (column_index < logical_context.output_columns.size() &&
		    logical_context.output_columns[column_index].partition_key_index.IsValid()) {
			partition_key_index = logical_context.output_columns[column_index].partition_key_index;
			return true;
		}
	}
	optional_idx field_index;
	if (!GetLogicalExpressionFieldIndex(table, expr, logical_context, field_index)) {
		return false;
	}
	return GetIdentityPartitionKeyIndex(table, field_index.GetIndex(), partition_key_index);
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
		if (!entry.iterator->second) {
			continue;
		}
		if (IsRootOptionalTableFilter(entry.Filter())) {
			continue;
		}
		unordered_map<idx_t, vector<Value>> filter_partition_values;
		if (!ExtractTableFilterValues(entry.Filter(), filter_partition_values[partition_key_index.GetIndex()]) ||
		    !MergeSinglePartitionKeyValues(partition_values, filter_partition_values)) {
			return false;
		}
	}
	return true;
}

bool LogicalPlanGetMetadataDeletePartitionValues(DuckLakeTableEntry &table, LogicalOperator &op,
                                                 MetadataDeleteLogicalContext &logical_context,
                                                 unordered_map<idx_t, vector<Value>> &partition_values) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		logical_context.table_index = get.table_index.index;
		logical_context.output_columns.clear();
		for (auto &column_id : get.GetColumnIds()) {
			MetadataDeleteOutputColumn output_column;
			if (!column_id.IsVirtualColumn()) {
				auto &field_id = table.GetFieldId(PhysicalIndex(column_id.GetPrimaryIndex()));
				output_column.field_index = field_id.GetFieldIndex().index;
				GetIdentityPartitionKeyIndex(table, output_column.field_index.GetIndex(),
				                             output_column.partition_key_index);
			}
			logical_context.output_columns.push_back(output_column);
		}
		return ExtractPartitionValuesFromLogicalGet(table, get, partition_values);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op.children.size() != 1 ||
		    !LogicalPlanGetMetadataDeletePartitionValues(table, *op.children[0], logical_context, partition_values)) {
			return false;
		}
		auto &projection = op.Cast<LogicalProjection>();
		vector<MetadataDeleteOutputColumn> projection_columns;
		for (auto &expr : projection.expressions) {
			if (!expr || expr->GetExpressionClass() != ExpressionClass::BOUND_REF) {
				return false;
			}
			MetadataDeleteOutputColumn output_column;
			auto column_index = expr->Cast<BoundReferenceExpression>().Index();
			if (column_index < logical_context.output_columns.size()) {
				output_column = logical_context.output_columns[column_index];
			}
			projection_columns.push_back(output_column);
		}
		logical_context.output_columns = std::move(projection_columns);
		return true;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.children.size() != 1 ||
		    !LogicalPlanGetMetadataDeletePartitionValues(table, *filter.children[0], logical_context,
		                                                 partition_values) ||
		    !logical_context.table_index.IsValid()) {
			return false;
		}
		auto get_partition_key = [&](const Expression &expr, optional_idx &partition_key_index) {
			return GetLogicalExpressionPartitionKeyIndex(table, expr, logical_context, partition_key_index);
		};
		for (auto &expr : filter.expressions) {
			unordered_map<idx_t, vector<Value>> expression_partition_values;
			if (!expr || !ExtractIdentityPartitionPredicate(*expr, get_partition_key, expression_partition_values) ||
			    !MergeSinglePartitionKeyValues(partition_values, expression_partition_values)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

bool GetPhysicalExpressionPartitionKeyIndex(DuckLakeTableEntry &table, const Expression &expr, PhysicalTableScan &scan,
                                            optional_idx &partition_key_index) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	auto column_index = expr.Cast<BoundReferenceExpression>().Index();
	if (column_index >= scan.column_ids.size()) {
		return false;
	}
	auto &column_id = scan.column_ids[column_index];
	if (column_id.IsVirtualColumn()) {
		return false;
	}
	auto &field_id = table.GetFieldId(PhysicalIndex(column_id.GetPrimaryIndex()));
	return GetIdentityPartitionKeyIndex(table, field_id.GetFieldIndex().index, partition_key_index);
}

bool IsPassThroughProjection(PhysicalProjection &projection) {
	for (idx_t expr_idx = 0; expr_idx < projection.select_list.size(); expr_idx++) {
		auto &expr = projection.select_list[expr_idx];
		if (!expr || expr->GetExpressionClass() != ExpressionClass::BOUND_REF) {
			return false;
		}
		if (expr->Cast<BoundReferenceExpression>().Index() != expr_idx) {
			return false;
		}
	}
	return true;
}

optional_ptr<PhysicalTableScan> FindMetadataDeleteSource(DuckLakeTableEntry &table, PhysicalOperator &plan,
                                                         unordered_map<idx_t, vector<Value>> &partition_values) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		return plan.Cast<PhysicalTableScan>();
	}
	if (plan.type == PhysicalOperatorType::PROJECTION && plan.children.size() == 1) {
		auto &projection = plan.Cast<PhysicalProjection>();
		if (!IsPassThroughProjection(projection)) {
			return nullptr;
		}
		return FindMetadataDeleteSource(table, plan.children[0].get(), partition_values);
	}
	if (plan.type == PhysicalOperatorType::FILTER && plan.children.size() == 1) {
		auto scan = FindMetadataDeleteSource(table, plan.children[0].get(), partition_values);
		if (!scan) {
			return nullptr;
		}
		auto &filter = plan.Cast<PhysicalFilter>();
		if (!filter.expression) {
			return nullptr;
		}
		auto get_partition_key = [&](const Expression &child, optional_idx &partition_key_index) {
			return GetPhysicalExpressionPartitionKeyIndex(table, child, *scan, partition_key_index);
		};
		unordered_map<idx_t, vector<Value>> filter_partition_values;
		if (!ExtractIdentityPartitionPredicate(*filter.expression, get_partition_key, filter_partition_values) ||
		    !MergeSinglePartitionKeyValues(partition_values, filter_partition_values)) {
			return nullptr;
		}
		return scan;
	}
	return nullptr;
}

bool ExtractPartitionValuesFromPhysicalScan(DuckLakeTableEntry &table, PhysicalTableScan &scan,
                                            unordered_map<idx_t, vector<Value>> &partition_values) {
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return true;
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
		optional_idx partition_key_index;
		if (!GetIdentityPartitionKeyIndex(table, field_id.GetFieldIndex().index, partition_key_index)) {
			return false;
		}
		if (!entry.iterator->second) {
			continue;
		}
		if (IsRootOptionalTableFilter(entry.Filter())) {
			continue;
		}
		unordered_map<idx_t, vector<Value>> filter_partition_values;
		if (!ExtractTableFilterValues(entry.Filter(), filter_partition_values[partition_key_index.GetIndex()]) ||
		    !MergeSinglePartitionKeyValues(partition_values, filter_partition_values)) {
			return false;
		}
	}
	return true;
}

bool ScanHasOnlyIdentityPartitionFilters(DuckLakeTableEntry &table, PhysicalTableScan &scan,
                                         DuckLakeMultiFileList &file_list) {
	if (scan.dynamic_filters && scan.dynamic_filters->HasFilters()) {
		return false;
	}
	auto filter_info = file_list.GetFilterInfo();
	if (filter_info) {
		for (auto &entry : filter_info->column_filters) {
			// FilterInfo proves referenced columns, not exact values; value proof comes from scan filters.
			if (!IsIdentityPartitionField(table, entry.first)) {
				return false;
			}
		}
	}
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return true;
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
		vector<Value> values;
		if (!IsIdentityPartitionField(table, field_id.GetFieldIndex().index)) {
			return false;
		}
		if (!entry.iterator->second) {
			continue;
		}
		if (IsRootOptionalTableFilter(entry.Filter())) {
			continue;
		}
		if (!ExtractTableFilterValues(entry.Filter(), values)) {
			return false;
		}
	}
	return true;
}

enum class PartitionValueMatch { MATCH, NO_MATCH, UNKNOWN };

bool GetPartitionKeyType(DuckLakeTableEntry &table, idx_t partition_key_index, LogicalType &partition_type) {
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		return false;
	}
	for (auto &field : partition_data->fields) {
		if (field.partition_key_index != partition_key_index) {
			continue;
		}
		auto field_id = table.GetFieldData().GetByFieldIndex(field.field_id);
		if (!field_id) {
			return false;
		}
		partition_type = DuckLakePartitionUtils::GetPartitionKeyType(field.transform.type, field_id->Type());
		return true;
	}
	return false;
}

bool CastPreservesValue(const Value &value, const LogicalType &target_type, Value &target_value) {
	if (!value.DefaultTryCastAs(target_type, target_value, nullptr, true)) {
		return false;
	}
	Value roundtrip_value;
	if (!target_value.DefaultTryCastAs(value.type(), roundtrip_value, nullptr, true)) {
		return false;
	}
	return Value::NotDistinctFrom(roundtrip_value, value);
}

PartitionValueMatch PartitionValueMatches(const Value &partition_value, const vector<Value> &accepted_values,
                                          const LogicalType &partition_type) {
	if (partition_value.IsNull()) {
		return PartitionValueMatch::NO_MATCH;
	}
	Value typed_partition_value;
	if (!partition_value.DefaultTryCastAs(partition_type, typed_partition_value, nullptr, true)) {
		return PartitionValueMatch::UNKNOWN;
	}
	for (auto &accepted_value : accepted_values) {
		if (accepted_value.IsNull()) {
			continue;
		}
		Value accepted_partition_value;
		if (!CastPreservesValue(accepted_value, partition_type, accepted_partition_value)) {
			return PartitionValueMatch::UNKNOWN;
		}
		if (Value::NotDistinctFrom(typed_partition_value, accepted_partition_value)) {
			return PartitionValueMatch::MATCH;
		}
	}
	return PartitionValueMatch::NO_MATCH;
}

enum class MetadataDeleteFileMatch { MATCH, NO_MATCH, UNKNOWN };

MetadataDeleteFileMatch GetMetadataDeleteFileMatch(DuckLakeTableEntry &table, const DuckLakeFileListExtendedEntry &file,
                                                   optional_idx current_partition_id,
                                                   const unordered_map<idx_t, vector<Value>> &partition_values) {
	// partition_key_index can be reused across specs; only current partition_id values are authoritative.
	if (partition_values.size() != 1 || file.partition_id != current_partition_id) {
		return MetadataDeleteFileMatch::UNKNOWN;
	}
	auto &entry = *partition_values.begin();
	LogicalType partition_type;
	if (!GetPartitionKeyType(table, entry.first, partition_type)) {
		return MetadataDeleteFileMatch::UNKNOWN;
	}
	bool found = false;
	for (auto &file_partition : file.partition_values) {
		if (file_partition.partition_column_idx != entry.first) {
			continue;
		}
		if (found) {
			return MetadataDeleteFileMatch::UNKNOWN;
		}
		auto value_match = PartitionValueMatches(file_partition.partition_value, entry.second, partition_type);
		if (value_match == PartitionValueMatch::UNKNOWN) {
			return MetadataDeleteFileMatch::UNKNOWN;
		}
		if (value_match == PartitionValueMatch::NO_MATCH) {
			return MetadataDeleteFileMatch::NO_MATCH;
		}
		found = true;
	}
	return found ? MetadataDeleteFileMatch::MATCH : MetadataDeleteFileMatch::UNKNOWN;
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
                          const unordered_map<idx_t, vector<Value>> &planned_partition_values,
                          vector<DuckLakeFileListExtendedEntry> &files) {
	auto partition_values = planned_partition_values;
	auto scan = FindMetadataDeleteSource(table, child_plan, partition_values);
	auto partition_data = table.GetPartitionData();
	if (!scan || !partition_data) {
		return false;
	}

	auto &bind_data = scan->bind_data->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<DuckLakeMultiFileList>();
	if (!ScanHasOnlyIdentityPartitionFilters(table, *scan, file_list)) {
		return false;
	}
	if (!ExtractPartitionValuesFromPhysicalScan(table, *scan, partition_values) || partition_values.size() != 1 ||
	    partition_values.begin()->second.empty()) {
		return false;
	}
	auto &transaction = DuckLakeTransaction::Get(context, table.catalog);
	if (transaction.HasLocalInlinedFileDeletes(table.GetTableId())) {
		return false;
	}
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
			} else if (file.data_type == DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA && file.row_count == 0) {
				continue;
			}
			return false;
		}
		// Existing row-level deletes make the raw file row count different from the visible DELETE count.
		if (file.delete_file_id.IsValid() || !file.delete_file.path.empty()) {
			return false;
		}
		auto file_match = GetMetadataDeleteFileMatch(table, file, partition_data->partition_id, partition_values);
		if (file_match == MetadataDeleteFileMatch::UNKNOWN) {
			return false;
		}
		if (file_match == MetadataDeleteFileMatch::NO_MATCH) {
			continue;
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
                                             unordered_map<idx_t, vector<Value>> metadata_delete_partition_values,
                                             bool can_use_metadata_delete) {
	vector<DuckLakeFileListExtendedEntry> metadata_delete_files;
	if (allow_duplicates && can_use_metadata_delete &&
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

PhysicalOperator &DuckLakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                              PhysicalOperator &child_plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a DuckLake table");
	}
	auto encryption_key = GenerateEncryptionKey(context);
	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < 3; i++) {
		auto &bound_ref = op.expressions[i + 1]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.Index());
	}
	auto &ducklake_table = op.table.Cast<DuckLakeTableEntry>();
	unordered_map<idx_t, vector<Value>> metadata_delete_partition_values;
	auto partition_data = ducklake_table.GetPartitionData();
	auto can_use_metadata_delete = partition_data != nullptr;
	if (can_use_metadata_delete && op.children.size() == 1) {
		MetadataDeleteLogicalContext logical_context;
		unordered_map<idx_t, vector<Value>> logical_partition_values;
		if (LogicalPlanGetMetadataDeletePartitionValues(ducklake_table, *op.children[0], logical_context,
		                                                logical_partition_values) &&
		    logical_partition_values.size() == 1) {
			metadata_delete_partition_values = std::move(logical_partition_values);
		}
	}
	if (!can_use_metadata_delete || metadata_delete_partition_values.size() > 1) {
		metadata_delete_partition_values.clear();
	}
	return DuckLakeDelete::PlanDelete(context, planner, ducklake_table, child_plan, std::move(row_id_indexes),
	                                  std::move(encryption_key), true, std::move(metadata_delete_partition_values),
	                                  can_use_metadata_delete);
}

} // namespace duckdb
