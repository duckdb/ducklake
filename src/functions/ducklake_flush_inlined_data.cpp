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

namespace duckdb {

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
SourceResultType DuckLakeFlushData::GetData(ExecutionContext &context, DataChunk &chunk,
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

// deletes_per_file[file_name][snapshot] = positions deleted up to that snapshot
using DeletesPerFile = unordered_map<string, map<idx_t, set<idx_t>>>;

static DeletesPerFile GroupDeletesByFile(QueryResult &deleted_rows_result, vector<DuckLakeDataFile> &written_files,
                                         vector<idx_t> &file_start_row_ids) {
	DeletesPerFile deletes_per_file;
	for (auto &row : deleted_rows_result) {
		idx_t end_snap = NumericCast<idx_t>(row.GetValue<int64_t>(0));
		idx_t row_id = NumericCast<idx_t>(row.GetValue<int64_t>(1));

		if (written_files.size() == 1) {
			// this is easy, we just handover the deleted row ids since they must be from this file
			deletes_per_file[written_files[0].file_name][end_snap].insert(row_id);
		} else {
			// lets write the deletes to the right files, in case we have multiple files
			for (idx_t file_idx = 0; file_idx < written_files.size(); file_idx++) {
				idx_t file_start = file_start_row_ids[file_idx];
				idx_t file_end = file_start + written_files[file_idx].row_count;
				if (row_id >= file_start && row_id < file_end) {
					idx_t pos_in_file = row_id - file_start;
					deletes_per_file[written_files[file_idx].file_name][end_snap].insert(pos_in_file);
					break;
				}
			}
		}
	}
	return deletes_per_file;
}

struct WriteDeleteFilesForFileInput {
	ClientContext &context;
	DuckLakeTransaction &transaction;
	FileSystem &fs;
	const string &data_path;
	const string &encryption_key;
	const string &file_name;
	map<idx_t, set<idx_t>> &snapshots_map;
};

static void WriteDeleteFilesForFile(WriteDeleteFilesForFileInput &input, vector<DuckLakeDeleteFile> &delete_files) {
	idx_t prev_count = 0;
	idx_t current_delete_file_idx = 0;

	vector<pair<idx_t, reference<set<idx_t>>>> snapshots;
	for (auto &snap_entry : input.snapshots_map) {
		snapshots.emplace_back(snap_entry.first, snap_entry.second);
	}

	for (idx_t i = 0; i < snapshots.size(); i++) {
		auto current_snapshot = snapshots[i].first;
		auto &current_positions = snapshots[i].second.get();
		// end_snapshot is the next snapshot in the list, or empty for the last one
		optional_idx next_snapshot;
		if (i + 1 < snapshots.size()) {
			next_snapshot = snapshots[i + 1].first;
		}

		if (current_positions.size() == prev_count) {
			// this means there were no deletions in between the snapshots for this file, so we just
			// extend the end snapshot
			delete_files[current_delete_file_idx].end_snapshot = next_snapshot;
			continue;
		}

		// write delete file
		WriteDeleteFileInput file_input {input.context,      input.transaction,    input.fs,
		                                 input.data_path,    input.encryption_key, input.file_name,
		                                 current_positions,  current_snapshot,     next_snapshot,
		                                 DeleteFileSource::FLUSH};
		auto delete_file = DuckLakeDeleteFileWriter::WriteDeleteFile(file_input);
		current_delete_file_idx = delete_files.size();
		delete_files.push_back(std::move(delete_file));
		prev_count = current_positions.size();
	}
}

SinkFinalizeType DuckLakeFlushData::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	auto &transaction = DuckLakeTransaction::Get(context, global_state.table.catalog);
	auto snapshot = transaction.GetSnapshot();

	if (!global_state.written_files.empty()) {
		// query cumulative deleted rows at each snapshot boundary
		auto deleted_rows_result = transaction.Query(snapshot, StringUtil::Format(R"(
			WITH snapshots AS (
				SELECT DISTINCT end_snapshot
				FROM {METADATA_CATALOG}.%s
				WHERE end_snapshot IS NOT NULL
			)
			SELECT s.end_snapshot, t.row_id
			FROM snapshots s
			JOIN {METADATA_CATALOG}.%s t ON t.end_snapshot <= s.end_snapshot
			WHERE t.end_snapshot IS NOT NULL
			ORDER BY s.end_snapshot, t.row_id;)",
		                                                                          inlined_table.table_name,
		                                                                          inlined_table.table_name));

		// lets figure out where each file ends, so we know where to place ze deletes
		vector<idx_t> file_start_row_ids;
		idx_t current_pos = 0;
		for (auto &written_file : global_state.written_files) {
			file_start_row_ids.push_back(current_pos);
			current_pos += written_file.row_count;
		}

		auto deletes_per_file = GroupDeletesByFile(*deleted_rows_result, global_state.written_files, file_start_row_ids);

		if (!deletes_per_file.empty()) {
			auto &fs = FileSystem::GetFileSystem(context);
			vector<DuckLakeDeleteFile> delete_files;

			for (auto &file_entry : deletes_per_file) {
				WriteDeleteFilesForFileInput file_input {context,         transaction,       fs,
				                                         table.DataPath(), encryption_key,   file_entry.first,
				                                         file_entry.second};
				WriteDeleteFilesForFile(file_input, delete_files);
			}
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

	// Get their default values from ducklake options
	auto schema = ducklake_catalog.GetConfigOption<string>("compaction_schema", {}, {}, "");
	auto table = ducklake_catalog.GetConfigOption<string>("compaction_table", {}, {}, "");

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
	vector<reference<DuckLakeTableEntry>> tables;
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
		for (auto &schema : schemas) {
			schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type == CatalogType::TABLE_ENTRY) {
					tables.push_back(entry.Cast<DuckLakeTableEntry>());
				}
			});
		}
	} else {
		// specific table - fetch the table
		auto table_entry =
		    ducklake_catalog.GetEntry<TableCatalogEntry>(context, schema, table, OnEntryNotFound::THROW_EXCEPTION);
		tables.push_back(table_entry->Cast<DuckLakeTableEntry>());
	}

	// try to compact all tables
	vector<unique_ptr<LogicalOperator>> flushes;
	for (auto &table_ref : tables) {
		auto &table = table_ref.get();
		auto &inlined_tables = table.GetInlinedDataTables();
		for (auto &inlined_table : inlined_tables) {
			DuckLakeDataFlusher compactor(context, ducklake_catalog, transaction, *input.binder, table.GetTableId(),
			                              inlined_table);
			flushes.push_back(compactor.GenerateFlushCommand());
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
