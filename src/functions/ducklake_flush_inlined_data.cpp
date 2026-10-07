#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "storage/ducklake_compaction.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "storage/ducklake_flush_data.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "common/ducklake_row_helpers.hpp"
#include "functions/ducklake_compaction_functions.hpp"
#include "storage/ducklake_sort_data.hpp"

namespace duckdb {

static void AttachDeleteFilesToWrittenFiles(vector<DuckLakeDeleteFile> &delete_files,
                                            vector<DuckLakeDataFile> &written_files) {
	unordered_map<string, reference<DuckLakeDataFile>> file_map;
	file_map.reserve(written_files.size());
	for (auto &written_file : written_files) {
		file_map.emplace(written_file.file_name, written_file);
	}
	for (auto &delete_file : delete_files) {
		auto it = file_map.find(delete_file.data_file_path);
		if (it != file_map.end()) {
			it->second.get().delete_files.push_back(std::move(delete_file));
		}
	}
}

//===--------------------------------------------------------------------===//
// Flush Data Operator
//===--------------------------------------------------------------------===//
DuckLakeFlushData::DuckLakeFlushData(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                     DuckLakeTableEntry &table, DuckLakeInlinedTableInfo inlined_table_p,
                                     string encryption_key_p, optional_idx partition_id, string sort_order_sql_p,
                                     PhysicalOperator &child)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 0), table(table),
      inlined_table(std::move(inlined_table_p)), encryption_key(std::move(encryption_key_p)),
      partition_id(partition_id), sort_order_sql(std::move(sort_order_sql_p)) {
	children.push_back(child);
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeFlushData::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &gstate = this->sink_state->Cast<DuckLakeInsertGlobalState>();
	chunk.data[0].Append(Value(table.schema.GetSchemaName()));
	chunk.data[1].Append(Value(table.name.GetIdentifierName()));
	chunk.data[2].Append(Value::BIGINT(static_cast<int64_t>(gstate.rows_flushed)));
	chunk.SetChildCardinality(1);
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

SinkFinalizeType DuckLakeFlushData::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	auto &transaction = DuckLakeTransaction::Get(context, global_state.table.catalog);
	auto &metadata_manager = transaction.GetMetadataManager();
	auto snapshot = transaction.GetSnapshot();

	if (!global_state.written_files.empty()) {
		DeletesPerFile deletes_per_file;
		auto partition_sql_exprs = table.GetPartitionSQLExpressions();

		// read the rows in the same order and with the same types as the flush scan
		auto col_names = metadata_manager.InlinedColNames();
		auto order_by = metadata_manager.InlinedFlushOrder(sort_order_sql);
		auto flush_source = metadata_manager.InlinedFlushSource(inlined_table.table_name, table);

		// Track cumulative row offset per partition so each file knows its range
		unordered_map<string, idx_t> partition_row_offsets;

		for (auto &file : global_state.written_files) {
			// Build partition filter (empty string for non-partitioned tables)
			string partition_filter;
			if (!partition_sql_exprs.empty()) {
				vector<Value> values;
				for (auto &pv : file.partition_values) {
					values.push_back(pv.partition_value);
				}
				partition_filter = DuckLakePartitionUtils::BuildPartitionFilter(partition_sql_exprs, values);
			}

			idx_t file_offset = partition_row_offsets[partition_filter];
			partition_row_offsets[partition_filter] += file.row_count;

			// Query deleted rows within this file's row range, filtered to its partition
			string extra_filter = partition_filter.empty() ? "" : " AND " + partition_filter;
			auto deleted_rows_result = metadata_manager.Query(
			    snapshot, StringUtil::Format(R"(
				WITH all_rows AS (
					SELECT %s AS end_snapshot, ROW_NUMBER() OVER (ORDER BY %s) - 1 AS output_position
					FROM %s
					WHERE {SNAPSHOT_ID} >= %s%s
				)
				SELECT end_snapshot, output_position
				FROM all_rows
				WHERE end_snapshot IS NOT NULL
				AND output_position >= %d AND output_position < %d;)",
			                                 col_names.end_snapshot, order_by, flush_source, col_names.begin_snapshot,
			                                 extra_filter, file_offset, file_offset + file.row_count));
			metadata_manager.CheckInlinedDataReadError(*deleted_rows_result, inlined_table.table_name);

			for (auto &row : *deleted_rows_result) {
				auto end_snap = row.GetValue<int64_t>(0);
				auto output_position = row.GetValue<int64_t>(1);
				int64_t pos_in_file = output_position - static_cast<int64_t>(file_offset);
				PositionWithSnapshot pos_with_snap {pos_in_file, end_snap};
				deletes_per_file[file.file_name].insert(pos_with_snap);
			}
		}

		if (!deletes_per_file.empty()) {
			auto &fs = FileSystem::GetFileSystem(context);
			vector<DuckLakeDeleteFile> delete_files;

			auto &catalog = table.catalog.Cast<DuckLakeCatalog>();
			auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
			bool use_deletion_vectors =
			    catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId(), &table.GetTableOptions());
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
				auto delete_file = DuckLakeDeleteFileWriter::Write(context, file_input, use_deletion_vectors);
				delete_files.push_back(std::move(delete_file));
			}
			AttachDeleteFilesToWrittenFiles(delete_files, global_state.written_files);
		}
	}

	// Compute total rows flushed before moving files
	for (auto &file : global_state.written_files) {
		global_state.rows_flushed += file.row_count;
	}

	transaction.AppendFiles(global_state.table.GetTableId(), std::move(global_state.written_files));
	transaction.MarkInlinedDataForDeletion(inlined_table, snapshot.snapshot_id);
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
	DuckLakeLogicalFlush(TableIndex table_index, DuckLakeTableEntry &table, DuckLakeInlinedTableInfo inlined_table_p,
	                     string encryption_key_p, optional_idx partition_id_p, string sort_order_sql_p)
	    : table_index(table_index), table(table), inlined_table(std::move(inlined_table_p)),
	      encryption_key(std::move(encryption_key_p)), partition_id(partition_id_p),
	      sort_order_sql(std::move(sort_order_sql_p)) {
	}

	TableIndex table_index;
	DuckLakeTableEntry &table;
	DuckLakeInlinedTableInfo inlined_table;
	string encryption_key;
	optional_idx partition_id;
	string sort_order_sql;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		return planner.Make<DuckLakeFlushData>(types, table, std::move(inlined_table), std::move(encryption_key),
		                                       partition_id, std::move(sort_order_sql), child);
	}

	string GetName() const override {
		return "DUCKLAKE_FLUSH_DATA";
	}

	string GetExtensionName() const override {
		return "ducklake";
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, 3);
	}

	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
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
	string GetFlushSortOrderSQL(const DuckLakeTableEntry &table);

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

string DuckLakeDataFlusher::GetFlushSortOrderSQL(const DuckLakeTableEntry &table) {
	// use the latest sort setting, including one set earlier in this transaction
	auto latest_entry = transaction.GetTransactionLocalEntry(CatalogType::TABLE_ENTRY,
	                                                         table.schema.Cast<DuckLakeSchemaEntry>().GetSchemaId(),
	                                                         table.name.GetIdentifierName());
	if (!latest_entry) {
		auto latest_snapshot = transaction.GetSnapshot();
		latest_entry = catalog.GetEntryById(transaction, latest_snapshot, table_id);
		if (!latest_entry) {
			throw InternalException("DuckLakeDataFlusher: failed to find latest table entry for latest snapshot id");
		}
	}
	auto &latest_table = latest_entry->Cast<DuckLakeTableEntry>();
	auto sort_data = latest_table.GetSortData();
	if (!sort_data) {
		return string();
	}
	auto orders = DuckLakeCompactor::ParseSortOrders(*sort_data);
	// bind in the user context so an invalid sort reports the same error as an insert
	auto bound_orders = DuckLakeCompactor::BindSortOrders(binder, latest_table.GetColumns(), latest_table.name,
	                                                      binder.GenerateTableIndex(), orders);
	for (auto &order : bound_orders) {
		if (order.expression->IsVolatile()) {
			// a volatile key cannot give the file and its delete positions the same order
			return string();
		}
	}
	return DuckLakeSort::BuildSortOrderSQL(orders, latest_table, table);
}

unique_ptr<LogicalOperator> DuckLakeDataFlusher::GenerateFlushCommand() {
	// get the table entry at the specified snapshot
	DuckLakeSnapshot snapshot(
	    catalog.GetBeginSnapshotForSchemaVersion(table_id, inlined_table.schema_version, transaction),
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
	auto sort_order_sql = GetFlushSortOrderSQL(table);
	read_info.flush_sort_order_sql = sort_order_sql;
	multi_file_bind_data.file_list = make_uniq<DuckLakeMultiFileList>(read_info, inlined_table);

	optional_idx partition_id;
	auto partition_data = table.GetPartitionData();
	if (partition_data) {
		partition_id = partition_data->partition_id;
	}

	// generate the LogicalGet
	auto &columns = table.GetColumns();

	DuckLakeCopyInput copy_input(context, table);
	copy_input.get_table_index = table_idx.index;
	copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;

	auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);
	auto copy = std::move(copy_options.copy);

	auto virtual_columns = table.GetVirtualColumns();
	auto ducklake_scan =
	    make_uniq<LogicalGet>(table_idx, BoundTableFunction(std::move(scan_function)), std::move(bind_data),
	                          copy->expected_types, copy->names, std::move(virtual_columns));
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

	copy->table_index = binder.GenerateTableIndex();
	copy->batch_size = DEFAULT_ROW_GROUP_SIZE;
	copy->children.push_back(std::move(root));

	// followed by the compaction operator (that writes the results back to the
	auto compaction =
	    make_uniq<DuckLakeLogicalFlush>(binder.GenerateTableIndex(), table, inlined_table,
	                                    std::move(copy_input.encryption_key), partition_id, std::move(sort_order_sql));
	compaction->children.push_back(std::move(copy));
	return std::move(compaction);
}

//===--------------------------------------------------------------------===//
// Flush Inlined File Deletions
//===--------------------------------------------------------------------===//

struct FileDeleteInfo {
	string file_path;
	set<PositionWithSnapshot> deletions;
	idx_t max_snapshot = 0;

	// Existing delete file info (if any)
	bool has_existing_delete_file = false;
	DataFileIndex existing_delete_file_id;
	string existing_delete_path;
	bool existing_delete_path_is_relative = false;
	idx_t existing_delete_begin_snapshot = 0;
	string existing_delete_encryption_key;
	DeleteFileFormat existing_delete_format = DeleteFileFormat::PARQUET;
};

static void FlushInlinedFileDeletions(ClientContext &context, DuckLakeCatalog &catalog,
                                      DuckLakeTransaction &transaction, DuckLakeTableEntry &table) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto table_id = table.GetTableId();
	auto snapshot = transaction.GetSnapshot();

	// Check if this table has an inlined deletion table
	auto inlined_table_name = metadata_manager.GetInlinedDeletionTableName(table_id, snapshot);
	if (inlined_table_name.empty()) {
		// No inlined deletions for this table, skiddadle
		return;
	}

	// Query the inlined deletions with the delete file active when their data file was last visible
	auto deletions_result = metadata_manager.Query(snapshot, StringUtil::Format(R"(
SELECT del.file_id, data.path, data.path_is_relative, del.row_id, del.begin_snapshot,
       existing_del.delete_file_id, existing_del.path as del_path, existing_del.path_is_relative as del_path_is_relative,
       existing_del.begin_snapshot as del_begin_snapshot, existing_del.encryption_key as del_encryption_key,
       existing_del.format as del_format
FROM {METADATA_CATALOG}.%s del
JOIN {METADATA_CATALOG}.ducklake_data_file data ON del.file_id = data.data_file_id
LEFT JOIN {METADATA_CATALOG}.ducklake_delete_file existing_del
    ON del.file_id = existing_del.data_file_id AND existing_del.table_id = %d
       AND {SNAPSHOT_ID} >= existing_del.begin_snapshot
       AND (existing_del.end_snapshot IS NULL
            OR existing_del.end_snapshot >= COALESCE(data.end_snapshot, {SNAPSHOT_ID} + 1))
WHERE del.begin_snapshot <= {SNAPSHOT_ID}
	)",
	                                                                            inlined_table_name, table_id.index));
	deletions_result->ThrowIfError("Failed to query inlined file deletions for flush: ");

	unordered_map<idx_t, FileDeleteInfo> files_to_flush;

	for (auto &row : *deletions_result) {
		auto file_id = row.GetValue<idx_t>(0);
		auto row_id = row.GetValue<int64_t>(3);
		auto begin_snapshot = row.GetValue<idx_t>(4);

		auto &file_info = files_to_flush[file_id];

		// Initialize file info on first encounter
		if (file_info.file_path.empty()) {
			auto path = row.GetValue<string>(1);
			auto path_is_relative = row.GetValue<bool>(2);
			file_info.file_path = path_is_relative ? table.DataPath() + path : path;
			file_info.max_snapshot = begin_snapshot;
			if (!row.IsNull(5)) {
				file_info.has_existing_delete_file = true;
				file_info.existing_delete_file_id = DataFileIndex(row.GetValue<idx_t>(5));
				file_info.existing_delete_path = row.GetValue<string>(6);
				file_info.existing_delete_path_is_relative = row.GetValue<bool>(7);
				file_info.existing_delete_begin_snapshot = row.GetValue<idx_t>(8);
				ReadEncryptionKey(row, 9, file_info.existing_delete_encryption_key);
				if (!row.IsNull(10)) {
					file_info.existing_delete_format = DeleteFileFormatFromString(row.GetValue<string>(10));
				}
			}
		} else {
			file_info.max_snapshot = MaxValue(file_info.max_snapshot, begin_snapshot);
		}

		PositionWithSnapshot pos_with_snap;
		pos_with_snap.position = row_id;
		pos_with_snap.snapshot_id = static_cast<int64_t>(begin_snapshot);
		file_info.deletions.insert(pos_with_snap);
	}

	if (files_to_flush.empty()) {
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

	auto &schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	bool use_deletion_vectors =
	    catalog.WriteDeletionVectors(schema.GetSchemaId(), table.GetTableId(), &table.GetTableOptions());
	for (auto &entry : files_to_flush) {
		auto file_id = entry.first;
		auto &file_info = entry.second;
		set<PositionWithSnapshot> merged_deletions;
		bool overwrites_existing = false;

		if (file_info.has_existing_delete_file) {
			overwrites_existing = true;

			// Copy deletions for merging
			merged_deletions = file_info.deletions;

			// Read existing deletions from the delete file
			DuckLakeFileData existing_delete_file_data;
			existing_delete_file_data.path = file_info.existing_delete_path_is_relative
			                                     ? table.DataPath() + file_info.existing_delete_path
			                                     : file_info.existing_delete_path;
			existing_delete_file_data.encryption_key = file_info.existing_delete_encryption_key;
			existing_delete_file_data.format = file_info.existing_delete_format;

			auto existing_deletions = DuckLakeDeleteFilter::ScanDeleteFile(context, existing_delete_file_data);

			// Merge existing deletions with new inlined deletions
			MergeDeletesWithSnapshots(existing_deletions, file_info.existing_delete_begin_snapshot, merged_deletions);

			// Update max_snapshot to include existing deletions
			for (auto &pos : merged_deletions) {
				file_info.max_snapshot = MaxValue(file_info.max_snapshot, static_cast<idx_t>(pos.snapshot_id));
			}
		}

		// Use reference to either merged or original deletions
		const auto &deletions_to_write = merged_deletions.empty() ? file_info.deletions : merged_deletions;

		WriteDeleteFileWithSnapshotsInput file_input {context,
		                                              transaction,
		                                              fs,
		                                              table.DataPath(),
		                                              encryption_key,
		                                              file_info.file_path,
		                                              deletions_to_write,
		                                              DeleteFileSource::FLUSH};
		auto delete_file = DuckLakeDeleteFileWriter::Write(context, file_input, use_deletion_vectors);
		delete_file.data_file_id = DataFileIndex(file_id);
		delete_file.max_snapshot = file_info.max_snapshot;

		if (overwrites_existing) {
			delete_file.overwrites_existing_delete = true;
			delete_file.overwritten_delete_file.delete_file_id = file_info.existing_delete_file_id;
			delete_file.overwritten_delete_file.path = file_info.existing_delete_path_is_relative
			                                               ? table.DataPath() + file_info.existing_delete_path
			                                               : file_info.existing_delete_path;
		}

		delete_files.push_back(std::move(delete_file));
	}

	// Register the delete files
	transaction.AddDeletes(table_id, std::move(delete_files));
	transaction.MarkInlinedFileDeletionsFlushed(table_id, snapshot.snapshot_id);
}

//===--------------------------------------------------------------------===//
// Function
//===--------------------------------------------------------------------===//
static unique_ptr<LogicalOperator> FlushInlinedDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                        TableIndex bind_index, vector<Identifier> &return_names) {
	// gather a list of files to compact
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
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
			schemas.push_back(ducklake_catalog.ResolveSchema(context, schema));
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
		auto table_catalog_entry = ducklake_catalog.GetEntry<TableCatalogEntry>(
		    context, ducklake_catalog.ResolveEntryName(context, schema, table), OnEntryNotFound::THROW_EXCEPTION);
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
			for (auto &inlined_table : table.GetInlinedDataTables(transaction, transaction.GetSnapshot())) {
				DuckLakeDataFlusher compactor(context, ducklake_catalog, transaction, *input.binder, table.GetTableId(),
				                              inlined_table);
				flushes.push_back(compactor.GenerateFlushCommand());
			}
			// Also flush inlined file deletions for this table
			FlushInlinedFileDeletions(context, ducklake_catalog, transaction, table);
		}
	}
	return_names.push_back("schema_name");
	return_names.push_back("table_name");
	return_names.push_back("rows_flushed");
	if (flushes.empty()) {
		// nothing to write - generate empty result
		vector<LogicalType> return_types;
		return_types.emplace_back(LogicalType::VARCHAR);
		return_types.emplace_back(LogicalType::VARCHAR);
		return_types.emplace_back(LogicalType::BIGINT);
		auto bindings = LogicalOperator::GenerateColumnBindings(bind_index, return_types.size());
		return make_uniq<LogicalEmptyResult>(std::move(return_types), std::move(bindings));
	}

	auto child = input.binder->UnionOperators(std::move(flushes), 3);
	// We want to construct the query tree equivalent to the SQL query below.
	// That way we can return the number of rows that were flushed for each table
	//
	//   SELECT
	//     schema_name,
	//     table_name,
	//     SUM(rows_flushed) AS rows_flushed
	//   FROM (flush_1 UNION ALL flush_2 UNION ALL flush_3 UNION ALL ...) t
	//   GROUP BY schema_name, table_name
	//   HAVING rows_flushed > 0;

	// Resolve columns are: [0] schema_name (VARCHAR), [1] table_name (VARCHAR), [2] rows_flushed (BIGINT)
	child->ResolveOperatorTypes();
	auto child_bindings = child->GetColumnBindings();

	// Create GROUP BY expressions (schema_name, table_name)
	vector<unique_ptr<Expression>> groups;
	groups.push_back(make_uniq<BoundColumnRefExpression>(child->types[0], child_bindings[0]));
	groups.push_back(make_uniq<BoundColumnRefExpression>(child->types[1], child_bindings[1]));

	// Create SUM(rows_flushed) aggregate
	vector<unique_ptr<Expression>> sum_args;
	sum_args.push_back(make_uniq<BoundColumnRefExpression>(child->types[2], child_bindings[2]));
	auto sum_func = GetBuiltinAggregateFunction(context, "sum", {sum_args[0]->GetReturnType()});
	FunctionBinder function_binder(context);
	auto sum_aggregate = function_binder.BindAggregateFunction(std::move(sum_func), std::move(sum_args));

	// Create LogicalAggregate with GROUP BY schema_name, table_name and SUM(rows_flushed)
	auto group_index = input.binder->GenerateTableIndex();
	auto aggregate_index = input.binder->GenerateTableIndex();

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(sum_aggregate));

	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	aggregate->groups = std::move(groups);
	aggregate->children.push_back(std::move(child));
	// Resolved columns are: [0] schema_name (VARCHAR), [1] table_name (VARCHAR), [2] SUM(rows_flushed) (HUGEINT)
	aggregate->ResolveOperatorTypes();

	// Create HAVING filter (SUM(rows_flushed) > 0)
	auto agg_bindings = aggregate->GetColumnBindings();
	unique_ptr<Expression> sum_col_ref = make_uniq<BoundColumnRefExpression>(aggregate->types[2], agg_bindings[2]);
	// Note: SUM(BIGINT) returns HUGEINT. We must use the its output type for the 0 constant
	unique_ptr<Expression> zero_const = make_uniq<BoundConstantExpression>(Value::Numeric(aggregate->types[2], 0));
	unique_ptr<Expression> filter_expr = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_GREATERTHAN, std::move(sum_col_ref), std::move(zero_const));

	auto filter = make_uniq<LogicalFilter>(std::move(filter_expr));
	filter->children.push_back(std::move(aggregate));

	// Need a projection to set the correct table index for column binding resolution
	return LogicalProjection::CreateIdentity(bind_index, std::move(filter));
}

DuckLakeFlushInlinedDataFunction::DuckLakeFlushInlinedDataFunction()
    : TableFunction("ducklake_flush_inlined_data",
                    FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR), nullptr, nullptr, nullptr) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("schema_name", LogicalType::VARCHAR).Add("table_name", LogicalType::VARCHAR);
	});
	bind_operator = FlushInlinedDataBind;
}

} // namespace duckdb
