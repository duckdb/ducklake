#include "storage/ducklake_catalog.hpp"
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
#include "duckdb/common/types/uuid.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "common/ducklake_data_file.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "common/ducklake_util.hpp"

namespace duckdb {

DuckLakeDeleteFile DuckLakeDeleteFileWriter::WriteDeleteFile(WriteDeleteFileInput &input) {
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

	auto function_data = copy_fun.function.copy_to_bind(input.context, bind_input, names_to_write, types_to_write);

	// generate the physical copy to file
	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	PhysicalPlan plan(Allocator::Get(input.context));
	PhysicalCopyToFile copy_to_file(plan, copy_return_types, copy_fun.function, std::move(function_data), 1);

	copy_to_file.use_tmp_file = false;
	copy_to_file.file_path = delete_file_path;
	copy_to_file.partition_output = false;
	copy_to_file.write_empty_file = false;
	copy_to_file.file_extension = "parquet";
	copy_to_file.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	copy_to_file.per_thread_output = false;
	copy_to_file.rotate = false;
	copy_to_file.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	copy_to_file.write_partition_columns = false;

	// run the copy to file
	DataChunk write_chunk;
	write_chunk.Initialize(input.context, types_to_write);
	// the first vector is constant (the file name)
	Value filename_val(input.data_file_path);
	write_chunk.data[0].Reference(filename_val);

	ThreadContext thread_context(input.context);
	ExecutionContext execution_context(input.context, thread_context, nullptr);
	InterruptState interrupt_state;

	// run the PhysicalCopyToFile Sink pipeline
	auto gstate = copy_to_file.GetGlobalSinkState(input.context);
	auto lstate = copy_to_file.GetLocalSinkState(execution_context);

	OperatorSinkInput sink_input {*gstate, *lstate, interrupt_state};
	idx_t row_count = 0;
	auto row_data = FlatVector::GetData<int64_t>(write_chunk.data[1]);
	for (auto &pos : input.positions) {
		row_data[row_count++] = NumericCast<int64_t>(pos);
		if (row_count >= STANDARD_VECTOR_SIZE) {
			write_chunk.SetCardinality(row_count);
			copy_to_file.Sink(execution_context, write_chunk, sink_input);
			row_count = 0;
		}
	}
	if (row_count > 0) {
		write_chunk.SetCardinality(row_count);
		copy_to_file.Sink(execution_context, write_chunk, sink_input);
	}

	OperatorSinkCombineInput combine_input {*gstate, *lstate, interrupt_state};
	copy_to_file.Combine(execution_context, combine_input);
	copy_to_file.FinalizeInternal(input.context, *gstate);

	// now read the stats data
	copy_to_file.sink_state = std::move(gstate);
	auto source_state = copy_to_file.GetGlobalSourceState(input.context);
	auto local_state = copy_to_file.GetLocalSourceState(execution_context, *source_state);
	DataChunk stats_chunk;
	stats_chunk.Initialize(input.context, copy_to_file.types);

	OperatorSourceInput source_input {*source_state, *local_state, interrupt_state};
	copy_to_file.GetData(execution_context, stats_chunk, source_input);

	// add to the written files
	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = input.data_file_path;
	delete_file.file_name = stats_chunk.GetValue(0, 0).GetValue<string>();
	delete_file.delete_count = input.positions.size();
	delete_file.file_size_bytes = stats_chunk.GetValue(2, 0).GetValue<idx_t>();
	delete_file.footer_size = stats_chunk.GetValue(3, 0).GetValue<idx_t>();
	delete_file.encryption_key = input.encryption_key;
	delete_file.begin_snapshot = input.begin_snapshot;
	delete_file.end_snapshot = input.end_snapshot;
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
		auto data = FlatVector::GetData<uint64_t>(file_row_id_chunk.data[0]);
		idx_t chunk_size = 0;
		for (idx_t r = 0; r < local_entry.size(); ++r) {
			data[chunk_size++] = local_entry[r];
			if (chunk_size == STANDARD_VECTOR_SIZE) {
				file_row_id_chunk.SetCardinality(chunk_size);
				deleted_row_collection->Append(append_state, file_row_id_chunk);
				chunk_size = 0;
			}
		}
		if (chunk_size > 0) {
			file_row_id_chunk.SetCardinality(chunk_size);
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
	file_row_number.ToUnifiedFormat(chunk.size(), row_data);
	auto file_row_data = UnifiedVectorFormat::GetData<int64_t>(row_data);

	UnifiedVectorFormat file_name_vdata;
	file_name_vector.ToUnifiedFormat(chunk.size(), file_name_vdata);

	UnifiedVectorFormat file_index_vdata;
	file_index_vector.ToUnifiedFormat(chunk.size(), file_index_vdata);

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
	if (existing_delete_data) {
		// deletes already exist for this file - add to set of deletes to write
		auto &existing_deletes = existing_delete_data->deleted_rows;
		sorted_deletes.insert(existing_deletes.begin(), existing_deletes.end());

		// clear the deletes
		delete_map->ClearDeletes(filename);

		// set the delete file as overwriting existing deletes
		delete_file.overwrites_existing_delete = true;
	}
	if (sorted_deletes.size() == data_file_info.row_count) {
		// ALL rows in this file are deleted - we don't need to write the deletes out to a file
		// we can just invalidate the source data file directly
		if (delete_file.data_file_id.IsValid()) {
			// persistent file - drop the file as part of the transaction
			transaction.DropFile(table.GetTableId(), delete_file.data_file_id, data_file_info.file.path);
		} else {
			// transaction-local file - we can drop the file directly
			transaction.DropTransactionLocalFile(table.GetTableId(), data_file_info.file.path);
		}
		return;
	}

	// Convert sorted_deletes to vector for the shared function
	vector<idx_t> positions(sorted_deletes.begin(), sorted_deletes.end());

	auto &fs = FileSystem::GetFileSystem(context);
	WriteDeleteFileInput input {context,          transaction,    fs,
	                            table.DataPath(), encryption_key, filename,
	                            positions,        optional_idx(), optional_idx()};
	auto written_file = DuckLakeDeleteFileWriter::WriteDeleteFile(input);

	// Copy over the additional fields
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
SourceResultType DuckLakeDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<DuckLakeDeleteGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
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

PhysicalOperator &DuckLakeDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                             DuckLakeTableEntry &table, PhysicalOperator &child_plan,
                                             vector<idx_t> row_id_indexes, string encryption_key,
                                             bool allow_duplicates) {
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
		row_id_indexes.push_back(bound_ref.index);
	}
	return DuckLakeDelete::PlanDelete(context, planner, op.table.Cast<DuckLakeTableEntry>(), child_plan,
	                                  std::move(row_id_indexes), std::move(encryption_key));
}

} // namespace duckdb
