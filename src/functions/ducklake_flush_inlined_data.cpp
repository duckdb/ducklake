#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "storage/ducklake_compaction.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "storage/ducklake_flush_data.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_delete.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"

namespace duckdb {

struct DeleteFileWriteContext {
	ClientContext &context;
	DuckLakeTableEntry &table;
	const string &encryption_key;
	DuckLakeTransaction &transaction;
	FileSystem &fs;
};

static DuckLakeDeleteFile WriteDeleteFile(DeleteFileWriteContext &ctx, const string &data_file_path,
                                          vector<idx_t> &positions, idx_t begin_snapshot_val) {
	std::sort(positions.begin(), positions.end());

	auto delete_file_uuid = "ducklake-" + ctx.transaction.GenerateUUID() + "-delete.parquet";
	string delete_file_path = DuckLakeUtil::JoinPath(ctx.fs, ctx.table.DataPath(), delete_file_uuid);

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	child_list_t<Value> values;
	values.emplace_back("file_path", Value::INTEGER(MultiFileReader::FILENAME_FIELD_ID));
	values.emplace_back("pos", Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID));
	auto field_ids = Value::STRUCT(std::move(values));
	vector<Value> field_input;
	field_input.push_back(std::move(field_ids));
	info->options["field_ids"] = std::move(field_input);

	if (!ctx.encryption_key.empty()) {
		child_list_t<Value> enc_values;
		enc_values.emplace_back("footer_key_value", Value::BLOB_RAW(ctx.encryption_key));
		vector<Value> encryption_input;
		encryption_input.push_back(Value::STRUCT(std::move(enc_values)));
		info->options["encryption_config"] = std::move(encryption_input);
	}

	auto &copy_fun = DuckLakeFunctions::GetCopyFunction(ctx.context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	vector<string> names_to_write {"file_path", "pos"};
	vector<LogicalType> types_to_write {LogicalType::VARCHAR, LogicalType::BIGINT};

	auto function_data = copy_fun.function.copy_to_bind(ctx.context, bind_input, names_to_write, types_to_write);

	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	PhysicalPlan plan(Allocator::Get(ctx.context));
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

	DataChunk write_chunk;
	write_chunk.Initialize(ctx.context, types_to_write);
	Value filename_val(data_file_path);
	write_chunk.data[0].Reference(filename_val);

	ThreadContext thread_context(ctx.context);
	ExecutionContext execution_context(ctx.context, thread_context, nullptr);
	InterruptState interrupt_state;

	auto gstate = copy_to_file.GetGlobalSinkState(ctx.context);
	auto lstate = copy_to_file.GetLocalSinkState(execution_context);

	OperatorSinkInput sink_input {*gstate, *lstate, interrupt_state};
	idx_t row_count = 0;
	auto row_data = FlatVector::GetData<int64_t>(write_chunk.data[1]);
	for (auto &pos : positions) {
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
	copy_to_file.FinalizeInternal(ctx.context, *gstate);

	copy_to_file.sink_state = std::move(gstate);
	auto source_state = copy_to_file.GetGlobalSourceState(ctx.context);
	auto local_state = copy_to_file.GetLocalSourceState(execution_context, *source_state);
	DataChunk stats_chunk;
	stats_chunk.Initialize(ctx.context, copy_to_file.types);

	OperatorSourceInput source_input {*source_state, *local_state, interrupt_state};
	copy_to_file.GetData(execution_context, stats_chunk, source_input);

	DuckLakeDeleteFile delete_file;
	delete_file.data_file_path = data_file_path;
	delete_file.file_name = stats_chunk.GetValue(0, 0).GetValue<string>();
	delete_file.delete_count = positions.size();
	delete_file.file_size_bytes = stats_chunk.GetValue(2, 0).GetValue<idx_t>();
	delete_file.footer_size = stats_chunk.GetValue(3, 0).GetValue<idx_t>();
	delete_file.encryption_key = ctx.encryption_key;
	delete_file.begin_snapshot = begin_snapshot_val;
	return delete_file;
}

static void AttachDeleteFilesToWrittenFiles(vector<DuckLakeDeleteFile> &delete_files,
                                            vector<DuckLakeDataFile> &written_files) {
	for (auto &delete_file : delete_files) {
		for (auto &written_file : written_files) {
			if (written_file.file_name == delete_file.data_file_path) {
				written_file.delete_files.push_back(std::move(delete_file));
				break;
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Flush Data Operator
//===--------------------------------------------------------------------===//
DuckLakeFlushData::DuckLakeFlushData(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                     DuckLakeTableEntry &table, DuckLakeInlinedTableInfo inlined_table_p,
                                     string encryption_key_p, optional_idx partition_id, PhysicalOperator &child)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 0), table(table),
      inlined_table(std::move(inlined_table_p)), encryption_key(std::move(encryption_key_p)),
      partition_id(partition_id) {
	children.push_back(child);
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeFlushData::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> DuckLakeFlushData::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DuckLakeInsertGlobalState>(table);
}

SinkResultType DuckLakeFlushData::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	DuckLakeInsert::AddWrittenFiles(global_state, chunk, encryption_key, partition_id, true);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType DuckLakeFlushData::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	auto &transaction = DuckLakeTransaction::Get(context, global_state.table.catalog);
	auto snapshot = transaction.GetSnapshot();

	auto end_snapshots_result = transaction.Query(snapshot, StringUtil::Format(R"(
		SELECT DISTINCT end_snapshot
		FROM {METADATA_CATALOG}.%s
		WHERE end_snapshot IS NOT NULL
		ORDER BY end_snapshot;)",
	                                                                           inlined_table.table_name));

	vector<idx_t> end_snapshots;
	for (auto &row : *end_snapshots_result) {
		end_snapshots.push_back(NumericCast<idx_t>(row.GetValue<int64_t>(0)));
	}

	if (!end_snapshots.empty() && !global_state.written_files.empty()) {
		auto &fs = FileSystem::GetFileSystem(context);

		auto min_row_id_result = transaction.Query(snapshot, StringUtil::Format(R"(
			SELECT MIN(row_id)
			FROM {METADATA_CATALOG}.%s;)",
		                                                                        inlined_table.table_name));
		idx_t row_id_offset = 0;
		for (auto &row : *min_row_id_result) {
			if (!row.IsNull(0)) {
				row_id_offset = NumericCast<idx_t>(row.GetValue<int64_t>(0));
			}
		}

		DeleteFileWriteContext write_ctx {context, table, encryption_key, transaction, fs};
		vector<DuckLakeDeleteFile> delete_files;

		for (auto current_snapshot : end_snapshots) {
			auto deleted_rows_result =
			    transaction.Query(snapshot, StringUtil::Format(R"(
				SELECT row_id
				FROM {METADATA_CATALOG}.%s
				WHERE end_snapshot <= %d
				ORDER BY row_id;)",
			                                                   inlined_table.table_name, current_snapshot));

			unordered_set<idx_t> deleted_row_ids;
			for (auto &row : *deleted_rows_result) {
				deleted_row_ids.insert(NumericCast<idx_t>(row.GetValue<int64_t>(0)));
			}

			unordered_map<string, vector<idx_t>> deletes_by_file;
			idx_t current_pos = 0;
			for (auto &written_file : global_state.written_files) {
				idx_t file_row_count = written_file.row_count;
				idx_t file_start_row_id = row_id_offset + current_pos;

				for (auto row_id : deleted_row_ids) {
					if (row_id >= file_start_row_id && row_id < file_start_row_id + file_row_count) {
						idx_t pos_in_file = row_id - file_start_row_id;
						deletes_by_file[written_file.file_name].push_back(pos_in_file);
					}
				}
				current_pos += file_row_count;
			}

			for (auto &file_entry : deletes_by_file) {
				auto delete_file = WriteDeleteFile(write_ctx, file_entry.first, file_entry.second, current_snapshot);
				delete_files.push_back(std::move(delete_file));
			}
		}

		// The last valid file, we attach it to the written files
		if (!delete_files.empty()) {
			// FIXME: What if we produce more than one data-file?
			// D_ASSERT(global_state.written_files.size() == 1);
			// auto last_file = delete_files.back();
			// delete_files.pop_back();
			// transaction.AddDeletes(global_state.table.GetTableId(), delete_files);
			// global_state.written_files[0].delete_file = make_uniq<DuckLakeDeleteFile>(std::move(last_file));
			AttachDeleteFilesToWrittenFiles(delete_files, global_state.written_files);
		}
	}

	transaction.AppendFiles(global_state.table.GetTableId(), std::move(global_state.written_files));
	transaction.DeleteInlinedData(inlined_table);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeFlushData::GetName() const {
	return "DUCKLAKE_FLUSH_DATA";
}

//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class DuckLakeLogicalFlush : public LogicalExtensionOperator {
public:
	DuckLakeLogicalFlush(idx_t table_index, DuckLakeTableEntry &table, DuckLakeInlinedTableInfo inlined_table_p,
	                     string encryption_key_p, optional_idx partition_id_p)
	    : table_index(table_index), table(table), inlined_table(std::move(inlined_table_p)),
	      encryption_key(std::move(encryption_key_p)), partition_id(partition_id_p) {
	}

	idx_t table_index;
	DuckLakeTableEntry &table;
	DuckLakeInlinedTableInfo inlined_table;
	string encryption_key;
	optional_idx partition_id;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		return planner.Make<DuckLakeFlushData>(types, table, std::move(inlined_table), std::move(encryption_key),
		                                       partition_id, child);
	}

	string GetExtensionName() const override {
		return "ducklake";
	}
	vector<ColumnBinding> GetColumnBindings() override {
		vector<ColumnBinding> result;
		result.emplace_back(table_index, 0);
		return result;
	}

	void ResolveTypes() override {
		types = {LogicalType::BOOLEAN};
	}
};

////===--------------------------------------------------------------------===//
//// Compaction Command Generator
////===--------------------------------------------------------------------===//
class DuckLakeDataFlusher {
public:
	DuckLakeDataFlusher(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
	                    Binder &binder, TableIndex table_id, const DuckLakeInlinedTableInfo &inlined_table);

	unique_ptr<LogicalOperator> GenerateFlushCommand();

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeTransaction &transaction;
	Binder &binder;
	TableIndex table_id;
	const DuckLakeInlinedTableInfo &inlined_table;
};

DuckLakeDataFlusher::DuckLakeDataFlusher(ClientContext &context, DuckLakeCatalog &catalog,
                                         DuckLakeTransaction &transaction, Binder &binder, TableIndex table_id,
                                         const DuckLakeInlinedTableInfo &inlined_table_p)
    : context(context), catalog(catalog), transaction(transaction), binder(binder), table_id(table_id),
      inlined_table(inlined_table_p) {
}

unique_ptr<LogicalOperator> DuckLakeDataFlusher::GenerateFlushCommand() {
	// get the table entry at the specified snapshot
	DuckLakeSnapshot snapshot(catalog.GetSnapshotForSchema(inlined_table.schema_version, transaction),
	                          inlined_table.schema_version, 0, 0);

	auto entry = catalog.GetEntryById(transaction, snapshot, table_id);
	if (!entry) {
		throw InternalException("DuckLakeCompactor: failed to find table entry for given snapshot id");
	}
	auto &table = entry->Cast<DuckLakeTableEntry>();

	auto table_idx = binder.GenerateTableIndex();
	unique_ptr<FunctionData> bind_data;
	EntryLookupInfo info(CatalogType::TABLE_ENTRY, table.name);
	auto scan_function = table.GetScanFunction(context, bind_data, info);

	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();
	auto &read_info = scan_function.function_info->Cast<DuckLakeFunctionInfo>();
	read_info.scan_type = DuckLakeScanType::SCAN_FOR_FLUSH;
	multi_file_bind_data.file_list = make_uniq<DuckLakeMultiFileList>(read_info, inlined_table);

	optional_idx partition_id;
	auto partition_data = table.GetPartitionData();
	if (partition_data) {
		partition_id = partition_data->partition_id;
	}

	// generate the LogicalGet
	auto &columns = table.GetColumns();

	DuckLakeCopyInput copy_input(context, table);
	copy_input.get_table_index = table_idx;
	copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;

	auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);

	auto virtual_columns = table.GetVirtualColumns();
	auto ducklake_scan =
	    make_uniq<LogicalGet>(table_idx, std::move(scan_function), std::move(bind_data), copy_options.expected_types,
	                          copy_options.names, std::move(virtual_columns));
	auto &column_ids = ducklake_scan->GetMutableColumnIds();
	for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
		column_ids.emplace_back(i);
	}
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	column_ids.emplace_back(DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID);

	auto root = unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(ducklake_scan));

	if (!copy_options.projection_list.empty()) {
		// push a projection
		auto proj = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(copy_options.projection_list));
		proj->children.push_back(std::move(root));
		root = std::move(proj);
	}

	// Add another projection with casts if necessary
	root->ResolveOperatorTypes();
	if (DuckLakeTypes::RequiresCast(root->types)) {
		root = DuckLakeInsert::InsertCasts(binder, root);
	}

	// generate the LogicalCopyToFile
	auto copy = make_uniq<LogicalCopyToFile>(std::move(copy_options.copy_function), std::move(copy_options.bind_data),
	                                         std::move(copy_options.info));

	copy->file_path = std::move(copy_options.file_path);
	copy->use_tmp_file = copy_options.use_tmp_file;
	copy->filename_pattern = std::move(copy_options.filename_pattern);
	copy->file_extension = std::move(copy_options.file_extension);
	copy->overwrite_mode = copy_options.overwrite_mode;
	copy->per_thread_output = copy_options.per_thread_output;
	copy->file_size_bytes = copy_options.file_size_bytes;
	copy->rotate = copy_options.rotate;
	copy->return_type = copy_options.return_type;

	copy->partition_output = copy_options.partition_output;
	copy->write_partition_columns = copy_options.write_partition_columns;
	copy->write_empty_file = copy_options.write_empty_file;
	copy->partition_columns = std::move(copy_options.partition_columns);
	copy->names = std::move(copy_options.names);
	copy->expected_types = std::move(copy_options.expected_types);

	copy->children.push_back(std::move(root));

	// followed by the compaction operator (that writes the results back to the
	auto compaction = make_uniq<DuckLakeLogicalFlush>(binder.GenerateTableIndex(), table, inlined_table,
	                                                  std::move(copy_input.encryption_key), partition_id);
	compaction->children.push_back(std::move(copy));
	return std::move(compaction);
}

//===--------------------------------------------------------------------===//
// Function
//===--------------------------------------------------------------------===//
static unique_ptr<LogicalOperator> FlushInlinedDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                        idx_t bind_index, vector<string> &return_names) {
	input.binder->SetAlwaysRequireRebind();
	// gather a list of files to compact
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);

	auto &named_parameters = input.named_parameters;

	unordered_map<idx_t, vector<reference<DuckLakeTableEntry>>> schema_table_map;
	string schema, table;

	auto schema_entry = named_parameters.find("schema_name");
	if (schema_entry != named_parameters.end()) {
		// specific schema
		schema = StringValue::Get(schema_entry->second);
	}
	auto table_entry = named_parameters.find("table_name");
	if (table_entry != named_parameters.end()) {
		table = StringValue::Get(table_entry->second);
	}

	// no or table schema specified - scan all schemas
	if (table.empty()) {
		// no specific table
		// scan all tables from schemas
		vector<reference<SchemaCatalogEntry>> schemas;
		if (schema.empty()) {
			// no specific schema - fetch all schemas
			schemas = ducklake_catalog.GetSchemas(context);
		} else {
			// specific schema - fetch it
			schemas.push_back(ducklake_catalog.GetSchema(context, schema));
		}

		// - scan all tables from the relevant schemas
		for (auto &schema_catalog_entry : schemas) {
			schema_catalog_entry.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type == CatalogType::TABLE_ENTRY) {
					auto &dl_schema = schema_catalog_entry.get().Cast<DuckLakeSchemaEntry>();
					schema_table_map[dl_schema.GetSchemaId().index].push_back(entry.Cast<DuckLakeTableEntry>());
				}
			});
		}
	} else {
		// specific table - fetch the table
		auto table_catalog_entry =
		    ducklake_catalog.GetEntry<TableCatalogEntry>(context, schema, table, OnEntryNotFound::THROW_EXCEPTION);
		auto &dl_schema = table_catalog_entry->schema.Cast<DuckLakeSchemaEntry>();
		schema_table_map[dl_schema.Cast<DuckLakeSchemaEntry>().GetSchemaId().index].push_back(
		    table_catalog_entry.get()->Cast<DuckLakeTableEntry>());
	}
	// try to compact all tables
	vector<unique_ptr<LogicalOperator>> flushes;
	for (auto &schema_table : schema_table_map) {
		for (auto &table_ref : schema_table.second) {
			SchemaIndex schema_index {schema_table.first};
			if (ducklake_catalog.GetConfigOption<string>("auto_compact", schema_index, table_ref.get().GetTableId(),
			                                             "true") != "true") {
				continue;
			}
			auto &table = table_ref.get();
			auto &inlined_tables = table.GetInlinedDataTables();
			for (auto &inlined_table : inlined_tables) {
				DuckLakeDataFlusher compactor(context, ducklake_catalog, transaction, *input.binder, table.GetTableId(),
				                              inlined_table);
				flushes.push_back(compactor.GenerateFlushCommand());
			}
		}
	}
	return_names.push_back("Success");
	if (flushes.empty()) {
		// nothing to write - generate empty result
		vector<ColumnBinding> bindings;
		vector<LogicalType> return_types;
		bindings.emplace_back(bind_index, 0);
		return_types.emplace_back(LogicalType::BOOLEAN);
		return make_uniq<LogicalEmptyResult>(std::move(return_types), std::move(bindings));
	}
	if (flushes.size() == 1) {
		flushes[0]->Cast<DuckLakeLogicalFlush>().table_index = bind_index;
		return std::move(flushes[0]);
	}
	auto union_op = input.binder->UnionOperators(std::move(flushes));
	union_op->Cast<LogicalSetOperation>().table_index = bind_index;
	return union_op;
}

DuckLakeFlushInlinedDataFunction::DuckLakeFlushInlinedDataFunction()
    : TableFunction("ducklake_flush_inlined_data", {LogicalType::VARCHAR}, nullptr, nullptr, nullptr) {
	named_parameters["schema_name"] = LogicalType::VARCHAR;
	named_parameters["table_name"] = LogicalType::VARCHAR;
	bind_operator = FlushInlinedDataBind;
}

} // namespace duckdb
