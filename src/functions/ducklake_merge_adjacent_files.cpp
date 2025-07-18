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

namespace duckdb {

//===--------------------------------------------------------------------===//
// Compaction Operator
//===--------------------------------------------------------------------===//
DuckLakeCompaction::DuckLakeCompaction(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                       DuckLakeTableEntry &table, vector<DuckLakeCompactionFileEntry> source_files_p,
                                       string encryption_key_p, optional_idx partition_id,
                                       vector<string> partition_values_p, optional_idx row_id_start,
                                       PhysicalOperator &child)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 0), table(table),
      source_files(std::move(source_files_p)), encryption_key(std::move(encryption_key_p)), partition_id(partition_id),
      partition_values(std::move(partition_values_p)), row_id_start(row_id_start) {
	children.push_back(child);
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeCompaction::GetData(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> DuckLakeCompaction::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DuckLakeInsertGlobalState>(table);
}

SinkResultType DuckLakeCompaction::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	DuckLakeInsert::AddWrittenFiles(global_state, chunk, encryption_key, partition_id);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType DuckLakeCompaction::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();

	if (global_state.written_files.size() != 1) {
		throw InternalException("DuckLakeCompaction - expected a single output file");
	}
	// set the partition values correctly
	for (auto &file : global_state.written_files) {
		for (idx_t col_idx = 0; col_idx < partition_values.size(); col_idx++) {
			DuckLakeFilePartition file_partition_info;
			file_partition_info.partition_column_idx = col_idx;
			file_partition_info.partition_value = partition_values[col_idx];
			file.partition_values.push_back(std::move(file_partition_info));
		}
	}

	DuckLakeCompactionEntry compaction_entry;
	compaction_entry.row_id_start = row_id_start;
	compaction_entry.source_files = source_files;
	compaction_entry.written_file = global_state.written_files[0];

	auto &transaction = DuckLakeTransaction::Get(context, global_state.table.catalog);
	transaction.AddCompaction(global_state.table.GetTableId(), std::move(compaction_entry));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeCompaction::GetName() const {
	return "DUCKLAKE_COMPACTION";
}

//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class DuckLakeLogicalCompaction : public LogicalExtensionOperator {
public:
	DuckLakeLogicalCompaction(idx_t table_index, DuckLakeTableEntry &table,
	                          vector<DuckLakeCompactionFileEntry> source_files_p, string encryption_key_p,
	                          optional_idx partition_id, vector<string> partition_values_p, optional_idx row_id_start)
	    : table_index(table_index), table(table), source_files(std::move(source_files_p)),
	      encryption_key(std::move(encryption_key_p)), partition_id(partition_id),
	      partition_values(std::move(partition_values_p)), row_id_start(row_id_start) {
	}

	idx_t table_index;
	DuckLakeTableEntry &table;
	vector<DuckLakeCompactionFileEntry> source_files;
	string encryption_key;
	optional_idx partition_id;
	vector<string> partition_values;
	optional_idx row_id_start;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		return planner.Make<DuckLakeCompaction>(types, table, std::move(source_files), std::move(encryption_key),
		                                        partition_id, std::move(partition_values), row_id_start, child);
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

//===--------------------------------------------------------------------===//
// Compaction Command Generator
//===--------------------------------------------------------------------===//
class DuckLakeCompactor {
public:
	DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
	                  Binder &binder, TableIndex table_id);

	void GenerateCompactions(DuckLakeTableEntry &table, vector<unique_ptr<LogicalOperator>> &compactions);
	unique_ptr<LogicalOperator> GenerateCompactionCommand(vector<DuckLakeCompactionFileEntry> source_files);

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeTransaction &transaction;
	Binder &binder;
	TableIndex table_id;
};

DuckLakeCompactor::DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
                                     Binder &binder, TableIndex table_id)
    : context(context), catalog(catalog), transaction(transaction), binder(binder), table_id(table_id) {
}

struct DuckLakeCompactionCandidates {
	vector<idx_t> candidate_files;
};

struct DuckLakeCompactionGroup {
	idx_t schema_version;
	optional_idx partition_id;
	vector<string> partition_values;
};

struct DuckLakeCompactionGroupHash {
	uint64_t operator()(const DuckLakeCompactionGroup &group) const {
		uint64_t hash = 0;
		hash ^= std::hash<idx_t>()(group.schema_version);
		if (group.partition_id.IsValid()) {
			hash ^= std::hash<idx_t>()(group.partition_id.GetIndex());
		}
		for (auto &partition_val : group.partition_values) {
			hash ^= std::hash<string>()(partition_val);
		}
		return hash;
	}
};

struct DuckLakeCompactionGroupEquality {
	bool operator()(const DuckLakeCompactionGroup &a, const DuckLakeCompactionGroup &b) const {
		return a.schema_version == b.schema_version && a.partition_id == b.partition_id &&
		       a.partition_values == b.partition_values;
	}
};

template <typename T>
using compaction_map_t =
    unordered_map<DuckLakeCompactionGroup, T, DuckLakeCompactionGroupHash, DuckLakeCompactionGroupEquality>;

void DuckLakeCompactor::GenerateCompactions(DuckLakeTableEntry &table,
                                            vector<unique_ptr<LogicalOperator>> &compactions) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto files = metadata_manager.GetFilesForCompaction(table);

	idx_t target_file_size = DuckLakeCatalog::DEFAULT_TARGET_FILE_SIZE;
	string target_file_size_str;
	if (catalog.TryGetConfigOption("target_file_size", target_file_size_str, table)) {
		target_file_size = Value(target_file_size_str).DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
	}

	// iterate over the files and split into separate compaction groups
	compaction_map_t<DuckLakeCompactionCandidates> candidates;
	for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
		auto &candidate = files[file_idx];
		if (candidate.file.data.file_size_bytes >= target_file_size) {
			// this file by itself exceeds the threshold - skip merging
			continue;
		}
		if (!candidate.delete_files.empty() || candidate.file.end_snapshot.IsValid()) {
			// FIXME: files with deletions cannot be merged currently
			continue;
		}
		// construct the compaction group for this file - i.e. the set of candidate files we can compact it with
		DuckLakeCompactionGroup group;
		group.schema_version = candidate.schema_version;
		group.partition_id = candidate.file.partition_id;
		group.partition_values = candidate.file.partition_values;

		candidates[group].candidate_files.push_back(file_idx);
	}

	// we have gathered all of the candidate files per compaction group
	// iterate over them to generate actual compaction commands
	for (auto &entry : candidates) {
		auto &candidate_list = entry.second.candidate_files;
		if (candidate_list.size() <= 1) {
			// we need at least 2 files to consider a merge
			continue;
		}
		for (idx_t start_idx = 0; start_idx < candidate_list.size(); start_idx++) {
			// check if we can merge this file with subsequent files
			idx_t current_file_size = 0;
			idx_t compaction_idx;
			for (compaction_idx = 0; compaction_idx < candidate_list.size(); compaction_idx++) {
				if (current_file_size >= target_file_size) {
					// we hit the target size already - stop
					break;
				}
				auto candidate_idx = candidate_list[compaction_idx];
				auto &candidate = files[candidate_idx];
				if (!candidate.partial_files.empty()) {
					// the file already has partial files - we can only accept this as a candidate if it is the first
					// file
					if (compaction_idx != start_idx) {
						// not the first file - we cannot compact this file together with the existing file
						break;
					}
				}
				idx_t file_size = candidate.file.data.file_size_bytes;
				if (file_size >= target_file_size) {
					// don't consider merging if the file is larger than the target size
					break;
				}
				// this file can be compacted along with the neighbors
				current_file_size += file_size;
			}
			idx_t compaction_file_count = compaction_idx - start_idx;
			if (compaction_file_count > 1) {
				vector<DuckLakeCompactionFileEntry> compaction_files;
				for (idx_t i = start_idx; i < compaction_idx; i++) {
					compaction_files.push_back(std::move(files[candidate_list[i]]));
				}
				compactions.push_back(GenerateCompactionCommand(std::move(compaction_files)));
				start_idx += compaction_file_count - 1;
			}
		}
	}
}

unique_ptr<LogicalOperator>
DuckLakeCompactor::GenerateCompactionCommand(vector<DuckLakeCompactionFileEntry> source_files) {
	// get the table entry at the specified snapshot
	auto snapshot_id = source_files[0].file.begin_snapshot;
	DuckLakeSnapshot snapshot(snapshot_id, source_files[0].schema_version, 0, 0);

	auto entry = catalog.GetEntryById(transaction, snapshot, table_id);
	if (!entry) {
		throw InternalException("DuckLakeCompactor: failed to find table entry for given snapshot id");
	}
	auto &table = entry->Cast<DuckLakeTableEntry>();

	auto table_idx = binder.GenerateTableIndex();
	unique_ptr<FunctionData> bind_data;
	EntryLookupInfo info(CatalogType::TABLE_ENTRY, table.name);
	auto scan_function = table.GetScanFunction(context, bind_data, info);

	auto partition_id = source_files[0].file.partition_id;
	auto partition_values = source_files[0].file.partition_values;

	bool files_are_adjacent = true;
	optional_idx prev_row_id;
	// set the files to scan as only the files we are trying to compact
	vector<DuckLakeFileListEntry> files_to_scan;
	for (auto &source : source_files) {
		DuckLakeFileListEntry result;
		result.file = source.file.data;
		result.row_id_start = source.file.row_id_start;
		result.snapshot_id = source.file.begin_snapshot;
		result.mapping_id = source.file.mapping_id;
		if (!source.delete_files.empty()) {
			throw InternalException("FIXME: compact deletions");
		}
		// check if this file is adjacent (row-id wise) to the previous file
		if (!source.file.row_id_start.IsValid()) {
			// the file does not have a row_id_start defined - it cannot be adjacent
			files_are_adjacent = false;
		} else {
			if (prev_row_id.IsValid() && prev_row_id.GetIndex() != source.file.row_id_start.GetIndex()) {
				// not adjacent - we need to write row-ids to the file
				files_are_adjacent = false;
			}
			prev_row_id = source.file.row_id_start.GetIndex() + source.file.row_count;
		}
		files_to_scan.push_back(std::move(result));
	}
	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();
	auto &read_info = scan_function.function_info->Cast<DuckLakeFunctionInfo>();
	multi_file_bind_data.file_list = make_uniq<DuckLakeMultiFileList>(read_info, std::move(files_to_scan));

	// generate the LogicalGet
	auto &columns = table.GetColumns();

	DuckLakeCopyInput copy_input(context, table);
	// merge_adjacent_files does not use partitioning information - instead we always merge within partitions
	copy_input.partition_data = nullptr;
	// if files are adjacent we don't need to write the row-id to the file
	if (files_are_adjacent) {
		copy_input.virtual_columns = InsertVirtualColumns::WRITE_SNAPSHOT_ID;
	} else {
		copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;
	}

	auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);

	auto virtual_columns = table.GetVirtualColumns();
	auto ducklake_scan =
	    make_uniq<LogicalGet>(table_idx, std::move(scan_function), std::move(bind_data), copy_options.expected_types,
	                          copy_options.names, std::move(virtual_columns));
	auto &column_ids = ducklake_scan->GetMutableColumnIds();
	for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
		column_ids.emplace_back(i);
	}
	if (!files_are_adjacent) {
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	column_ids.emplace_back(DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID);

	// generate the LogicalCopyToFile
	auto copy = make_uniq<LogicalCopyToFile>(std::move(copy_options.copy_function), std::move(copy_options.bind_data),
	                                         std::move(copy_options.info));

	auto &fs = FileSystem::GetFileSystem(context);
	copy->file_path = copy_options.filename_pattern.CreateFilename(fs, copy_options.file_path, "parquet", 0);
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
	copy->write_empty_file = false;
	copy->partition_columns = std::move(copy_options.partition_columns);
	copy->names = std::move(copy_options.names);
	copy->expected_types = std::move(copy_options.expected_types);
	copy->preserve_order = PreserveOrderType::PRESERVE_ORDER;
	copy->file_size_bytes = optional_idx();
	copy->rotate = false;

	copy->children.push_back(std::move(ducklake_scan));

	optional_idx target_row_id_start;
	if (files_are_adjacent) {
		target_row_id_start = source_files[0].file.row_id_start;
	}

	// followed by the compaction operator (that writes the results back to the
	auto compaction = make_uniq<DuckLakeLogicalCompaction>(binder.GenerateTableIndex(), table, std::move(source_files),
	                                                       std::move(copy_input.encryption_key), partition_id,
	                                                       std::move(partition_values), target_row_id_start);
	compaction->children.push_back(std::move(copy));
	return std::move(compaction);
}

//===--------------------------------------------------------------------===//
// Function
//===--------------------------------------------------------------------===//
unique_ptr<LogicalOperator> MergeAdjacentFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   idx_t bind_index, vector<string> &return_names) {
	// gather a list of files to compact
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);

	// try to compact all tables
	vector<unique_ptr<LogicalOperator>> compactions;
	auto schemas = ducklake_catalog.GetSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			auto &table = entry.Cast<DuckLakeTableEntry>();
			DuckLakeCompactor compactor(context, ducklake_catalog, transaction, *input.binder, table.GetTableId());
			compactor.GenerateCompactions(table, compactions);
		});
	}
	return_names.push_back("Success");
	if (compactions.empty()) {
		// nothing to compact - generate empty result
		vector<ColumnBinding> bindings;
		vector<LogicalType> return_types;
		bindings.emplace_back(bind_index, 0);
		return_types.emplace_back(LogicalType::BOOLEAN);
		return make_uniq<LogicalEmptyResult>(std::move(return_types), std::move(bindings));
	}
	if (compactions.size() == 1) {
		compactions[0]->Cast<DuckLakeLogicalCompaction>().table_index = bind_index;
		return std::move(compactions[0]);
	}
	auto union_op = input.binder->UnionOperators(std::move(compactions));
	union_op->Cast<LogicalSetOperation>().table_index = bind_index;
	return union_op;
}

DuckLakeMergeAdjacentFilesFunction::DuckLakeMergeAdjacentFilesFunction()
    : TableFunction("ducklake_merge_adjacent_files", {LogicalType::VARCHAR}, nullptr, nullptr, nullptr) {
	bind_operator = MergeAdjacentFilesBind;
}

} // namespace duckdb
