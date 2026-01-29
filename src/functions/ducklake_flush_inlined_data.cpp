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
#include "storage/ducklake_delete_filter.hpp"

#include "functions/ducklake_compaction_functions.hpp"

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
using DeletesPerFile = unordered_map<string, set<PositionWithSnapshot>>;

static DeletesPerFile GroupDeletesByFile(QueryResult &deleted_rows_result, vector<DuckLakeDataFile> &written_files,
                                         vector<idx_t> &file_start_row_ids) {
	DeletesPerFile deletes_per_file;
	for (auto &row : deleted_rows_result) {
		auto end_snap = row.GetValue<int64_t>(0);
		auto output_position = row.GetValue<int64_t>(1);

		if (written_files.size() == 1) {
			// Single file - use position directly
			PositionWithSnapshot pos_with_snap {output_position, end_snap};
			deletes_per_file[written_files[0].file_name].insert(pos_with_snap);
		} else {
			// Multiple files - find which file contains this position
			for (idx_t file_idx = 0; file_idx < written_files.size(); file_idx++) {
				const int64_t file_start = static_cast<int64_t>(file_start_row_ids[file_idx]);
				const int64_t file_end = static_cast<int64_t>(file_start + written_files[file_idx].row_count);
				if (output_position >= file_start && output_position < file_end) {
					int64_t pos_in_file = output_position - file_start;
					PositionWithSnapshot pos_with_snap {pos_in_file, end_snap};
					deletes_per_file[written_files[file_idx].file_name].insert(pos_with_snap);
					break;
				}
			}
		}
	}
	return deletes_per_file;
}

SinkFinalizeType DuckLakeFlushData::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	auto &transaction = DuckLakeTransaction::Get(context, global_state.table.catalog);
	auto snapshot = transaction.GetSnapshot();

	if (!global_state.written_files.empty()) {
		// Query deleted rows with their output file position
		// We use ROW_NUMBER to get the  position of each row in flush order,
		auto deleted_rows_result = transaction.Query(snapshot, StringUtil::Format(R"(
			WITH all_rows AS (
				SELECT row_id, end_snapshot, ROW_NUMBER() OVER (ORDER BY row_id) - 1 AS output_position
				FROM {METADATA_CATALOG}.%s
				WHERE {SNAPSHOT_ID} >= begin_snapshot
			)
			SELECT end_snapshot, output_position
			FROM all_rows
			WHERE end_snapshot IS NOT NULL
			ORDER BY output_position;)",
		                                                                          inlined_table.table_name));

		// lets figure out where each file ends, so we know where to place ze deletes
		vector<idx_t> file_start_row_ids;
		idx_t current_pos = 0;
		for (auto &written_file : global_state.written_files) {
			file_start_row_ids.push_back(current_pos);
			current_pos += written_file.row_count;
		}

		auto deletes_per_file =
		    GroupDeletesByFile(*deleted_rows_result, global_state.written_files, file_start_row_ids);

		if (!deletes_per_file.empty()) {
			auto &fs = FileSystem::GetFileSystem(context);
			vector<DuckLakeDeleteFile> delete_files;

			for (auto &file_entry : deletes_per_file) {
				// write single file, begin_snapshot is the minimum snapshot
				WriteDeleteFileWithSnapshotsInput file_input {context,
				                                              transaction,
				                                              fs,
				                                              table.DataPath(),
				                                              encryption_key,
				                                              file_entry.first,
				                                              file_entry.second,
				                                              DeleteFileSource::FLUSH};
				delete_files.push_back(DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(context, file_input));
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
	DuckLakeSnapshot snapshot(catalog.GetBeginSnapshotForTable(table_id, transaction), inlined_table.schema_version, 0,
	                          0);

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

	// If flush should be ordered, add Order By (and projection) to logical plan
	// Do not pull the sort setting at the time of the creation of the rows being flushed,
	// and instead pull the latest sort setting
	// First, see if there are transaction local changes to the table
	// Then fall back to latest snapshot if no local changes
	auto latest_entry = transaction.GetTransactionLocalEntry(CatalogType::TABLE_ENTRY, table.schema.name, table.name);
	if (!latest_entry) {
		auto latest_snapshot = transaction.GetSnapshot();
		latest_entry = catalog.GetEntryById(transaction, latest_snapshot, table_id);
		if (!latest_entry) {
			throw InternalException("DuckLakeDataFlusher: failed to find latest table entry for latest snapshot id");
		}
	}
	auto &latest_table = latest_entry->Cast<DuckLakeTableEntry>();

	auto sort_data = latest_table.GetSortData();
	if (sort_data) {
		root = DuckLakeCompactor::InsertSort(binder, root, latest_table, sort_data);
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
// Flush Inlined File Deletions
//===--------------------------------------------------------------------===//
struct ExistingDeleteFileInfo {
	DataFileIndex delete_file_id;
	string path;
	bool path_is_relative;
	idx_t begin_snapshot;
	string encryption_key;
	bool has_info = false;
};

static void FlushInlinedFileDeletions(ClientContext &context, DuckLakeCatalog &catalog,
                                      DuckLakeTransaction &transaction, DuckLakeTableEntry &table) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto table_id = table.GetTableId();
	auto snapshot = transaction.GetSnapshot();

	// Check if this table has an inlined deletion table
	auto inlined_table_name = metadata_manager.GetInlinedDeletionTableName(table_id, snapshot);
	if (inlined_table_name.empty()) {
		return; // No inlined deletions for this table
	}

	// Query the inlined deletions with file paths and existing delete file info
	auto deletions_result = transaction.Query(snapshot, StringUtil::Format(R"(
SELECT del.file_id, data.path, data.path_is_relative, del.row_id, del.begin_snapshot,
       existing_del.delete_file_id, existing_del.path as del_path, existing_del.path_is_relative as del_path_is_relative,
       existing_del.begin_snapshot as del_begin_snapshot, existing_del.encryption_key as del_encryption_key
FROM {METADATA_CATALOG}.%s del
JOIN {METADATA_CATALOG}.ducklake_data_file data ON del.file_id = data.data_file_id
LEFT JOIN (
    SELECT * FROM {METADATA_CATALOG}.ducklake_delete_file
    WHERE table_id = %d AND {SNAPSHOT_ID} >= begin_snapshot
          AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
) existing_del ON del.file_id = existing_del.data_file_id
ORDER BY del.file_id, del.row_id
	)",
	                                                                       inlined_table_name, table_id.index));
	if (deletions_result->HasError()) {
		return;
	}

	// Group deletions by file and track existing delete file info
	unordered_map<idx_t, string> file_paths;                              // file_id -> data file path
	unordered_map<idx_t, set<PositionWithSnapshot>> deletes_per_file;     // file_id -> inlined deletions
	unordered_map<idx_t, idx_t> max_snapshots;                            // file_id -> max_snapshot
	unordered_map<idx_t, ExistingDeleteFileInfo> existing_delete_files;   // file_id -> existing delete file info

	while (true) {
		auto chunk = deletions_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			auto file_id = chunk->GetValue(0, row_idx).GetValue<idx_t>();
			auto path = chunk->GetValue(1, row_idx).GetValue<string>();
			auto path_is_relative = chunk->GetValue(2, row_idx).GetValue<bool>();
			auto row_id = chunk->GetValue(3, row_idx).GetValue<int64_t>();
			auto begin_snapshot = chunk->GetValue(4, row_idx).GetValue<idx_t>();

			if (file_paths.find(file_id) == file_paths.end()) {
				if (path_is_relative) {
					file_paths[file_id] = table.DataPath() + path;
				} else {
					file_paths[file_id] = path;
				}
				max_snapshots[file_id] = begin_snapshot;

				// Check for existing delete file
				if (!chunk->GetValue(5, row_idx).IsNull()) {
					ExistingDeleteFileInfo del_info;
					del_info.delete_file_id = DataFileIndex(chunk->GetValue(5, row_idx).GetValue<idx_t>());
					del_info.path = chunk->GetValue(6, row_idx).GetValue<string>();
					del_info.path_is_relative = chunk->GetValue(7, row_idx).GetValue<bool>();
					del_info.begin_snapshot = chunk->GetValue(8, row_idx).GetValue<idx_t>();
					if (!chunk->GetValue(9, row_idx).IsNull()) {
						del_info.encryption_key = chunk->GetValue(9, row_idx).GetValue<string>();
					}
					del_info.has_info = true;
					existing_delete_files[file_id] = std::move(del_info);
				}
			} else {
				max_snapshots[file_id] = MaxValue(max_snapshots[file_id], begin_snapshot);
			}

			PositionWithSnapshot pos_with_snap;
			pos_with_snap.position = row_id;
			pos_with_snap.snapshot_id = static_cast<int64_t>(begin_snapshot);
			deletes_per_file[file_id].insert(pos_with_snap);
		}
	}

	if (deletes_per_file.empty()) {
		return;
	}

	// Write delete files
	auto &fs = FileSystem::GetFileSystem(context);
	vector<DuckLakeDeleteFile> delete_files;

	// Get encryption key if the catalog is encrypted
	string encryption_key;
	if (catalog.IsEncrypted()) {
		encryption_key = catalog.GenerateEncryptionKey(context);
	}

	for (auto &entry : deletes_per_file) {
		auto file_id = entry.first;
		auto &file_path = file_paths[file_id];
		auto &inlined_deletions = entry.second;

		// Check if there's an existing delete file that we need to merge with
		auto existing_entry = existing_delete_files.find(file_id);
		set<PositionWithSnapshot> merged_deletions = inlined_deletions;
		bool overwrites_existing = false;
		ExistingDeleteFileInfo existing_info;

		if (existing_entry != existing_delete_files.end() && existing_entry->second.has_info) {
			existing_info = existing_entry->second;
			overwrites_existing = true;

			// Read existing deletions from the delete file
			DuckLakeFileData existing_delete_file_data;
			if (existing_info.path_is_relative) {
				existing_delete_file_data.path = table.DataPath() + existing_info.path;
			} else {
				existing_delete_file_data.path = existing_info.path;
			}
			existing_delete_file_data.encryption_key = existing_info.encryption_key;

			auto existing_deletions = DuckLakeDeleteFilter::ScanDeleteFile(context, existing_delete_file_data);

			// Merge existing deletions with new inlined deletions
			MergeDeletesWithSnapshots(existing_deletions, existing_info.begin_snapshot, merged_deletions);

			// Update max_snapshot to include existing deletions
			for (auto &pos : merged_deletions) {
				max_snapshots[file_id] = MaxValue(max_snapshots[file_id], static_cast<idx_t>(pos.snapshot_id));
			}
		}

		WriteDeleteFileWithSnapshotsInput file_input {context,
		                                              transaction,
		                                              fs,
		                                              table.DataPath(),
		                                              encryption_key,
		                                              file_path,
		                                              merged_deletions,
		                                              DeleteFileSource::FLUSH};
		auto delete_file = DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(context, file_input);
		delete_file.data_file_id = DataFileIndex(file_id);
		delete_file.max_snapshot = max_snapshots[file_id];

		if (overwrites_existing) {
			delete_file.overwrites_existing_delete = true;
			delete_file.overwritten_delete_file.delete_file_id = existing_info.delete_file_id;
			delete_file.overwritten_delete_file.path = existing_info.path_is_relative
			                                               ? table.DataPath() + existing_info.path
			                                               : existing_info.path;
		}

		delete_files.push_back(std::move(delete_file));
	}

	// Register the delete files
	transaction.AddDeletes(table_id, std::move(delete_files));

	// Clear the inlined deletion table
	auto delete_result = transaction.Query(StringUtil::Format("DELETE FROM {METADATA_CATALOG}.%s", inlined_table_name));
	if (delete_result->HasError()) {
		delete_result->GetErrorObject().Throw("Failed to clear inlined file deletions: ");
	}
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
			// Also flush inlined file deletions for this table
			FlushInlinedFileDeletions(context, ducklake_catalog, transaction, table);
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
