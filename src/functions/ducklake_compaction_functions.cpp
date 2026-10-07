#include "duckdb/common/bind_helpers.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "storage/ducklake_compaction.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "fmt/format.h"

#include "functions/ducklake_compaction_functions.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sort Binding Helpers
//===--------------------------------------------------------------------===//

//! Parses sort expressions from DuckLakeSort into OrderByNode vectors (DuckDB dialect only).
vector<OrderByNode> DuckLakeCompactor::ParseSortOrders(const DuckLakeSort &sort_data) {
	vector<OrderByNode> pre_bound_orders;
	for (auto &field : sort_data.fields) {
		if (field.dialect != "duckdb") {
			continue;
		}
		auto parsed_expression = Parser::GetBuiltinParser().ParseExpressionList(field.expression);
		pre_bound_orders.emplace_back(field.sort_direction, field.null_order, std::move(parsed_expression[0]));
	}
	return pre_bound_orders;
}

vector<BoundOrderByNode> DuckLakeCompactor::BindSortOrders(Binder &binder, const ColumnList &columns,
                                                           const Identifier &table_name, TableIndex table_index,
                                                           const vector<OrderByNode> &pre_bound_orders) {
	DuckLakeTableEntry::ValidateSortExpressionColumns(columns, pre_bound_orders);
	vector<OrderByNode> orders;
	for (auto &order : pre_bound_orders) {
		orders.emplace_back(order.type, order.null_order, order.expression->Copy());
	}
	return BindOrderByNodes(binder, table_index, table_name, StringsToIdentifiers(columns.GetColumnNames()),
	                        columns.GetColumnTypes(), orders);
}

//===--------------------------------------------------------------------===//
// Compaction Operator
//===--------------------------------------------------------------------===//
DuckLakeCompaction::DuckLakeCompaction(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                       DuckLakeTableEntry &table, vector<DuckLakeCompactionFileEntry> source_files_p,
                                       string encryption_key_p, optional_idx partition_id,
                                       vector<Value> partition_values_p, optional_idx row_id_start,
                                       PhysicalOperator &child, CompactionType type)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 0), table(table),
      source_files(std::move(source_files_p)), encryption_key(std::move(encryption_key_p)), partition_id(partition_id),
      partition_values(std::move(partition_values_p)), row_id_start(row_id_start), type(type) {
	children.push_back(child);
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeCompaction::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	if (!this->sink_state) {
		throw InternalException("DuckLakeCompaction - missing sink state while producing result");
	}
	auto &gstate = this->sink_state->Cast<DuckLakeInsertGlobalState>();
	auto files_created = gstate.written_files.size();

	chunk.data[0].Append(Value(table.schema.GetSchemaName()));
	chunk.data[1].Append(Value(table.name.GetIdentifierName()));
	chunk.data[2].Append(Value::BIGINT(static_cast<int64_t>(source_files.size())));
	chunk.data[3].Append(Value::BIGINT(static_cast<int64_t>(files_created)));
	chunk.SetChildCardinality(1);
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

	if (global_state.written_files.empty()) {
		idx_t rows_to_write = 0;
		for (auto &source : source_files) {
			rows_to_write += source.file.row_count;
			if (!source.delete_files.empty()) {
				rows_to_write -= source.delete_files.back().row_count;
			}
			rows_to_write -= source.inlined_file_deletions.size();
		}
		if (rows_to_write != 0) {
			throw InternalException("DuckLakeCompaction - expected output files for %llu rows", rows_to_write);
		}
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
	compaction_entry.written_files = global_state.written_files;
	compaction_entry.type = type;

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

DuckLakeCompactor::DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
                                     Binder &binder, TableIndex table_id, uint64_t max_files,
                                     DuckLakeMergeAdjacentOptions options)
    : context(context), catalog(catalog), transaction(transaction), binder(binder), table_id(table_id),
      max_files(max_files), options(options), type(CompactionType::MERGE_ADJACENT_TABLES) {
}

DuckLakeCompactor::DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
                                     Binder &binder, TableIndex table_id, uint64_t max_files, double delete_threshold_p)
    : context(context), catalog(catalog), transaction(transaction), binder(binder), table_id(table_id),
      max_files(max_files), delete_threshold(delete_threshold_p), type(CompactionType::REWRITE_DELETES) {
}

struct DuckLakeCompactionCandidates {
	vector<idx_t> candidate_files;
};

struct DuckLakeCompactionGroup {
	//! Unset when the file can be rewritten under the latest schema (compatible files then share one bucket);
	//! otherwise the file's own schema_version, keeping incompatible files isolated.
	optional_idx schema_version;
	optional_idx partition_id;
	vector<Value> partition_values;
};

struct DuckLakeCompactionGroupHash {
	uint64_t operator()(const DuckLakeCompactionGroup &group) const {
		uint64_t hash = 0;
		if (group.schema_version.IsValid()) {
			hash ^= std::hash<idx_t>()(group.schema_version.GetIndex());
		}
		if (group.partition_id.IsValid()) {
			hash ^= std::hash<idx_t>()(group.partition_id.GetIndex());
		}
		for (auto &val : group.partition_values) {
			if (val.IsNull()) {
				hash ^= 0x9E3779B97F4A7C15ULL;
			} else {
				hash ^= std::hash<string>()(val.ToString());
			}
		}
		return hash;
	}
};

struct DuckLakeCompactionGroupEquality {
	bool operator()(const DuckLakeCompactionGroup &a, const DuckLakeCompactionGroup &b) const {
		return a.schema_version == b.schema_version && a.partition_id == b.partition_id &&
		       std::equal(a.partition_values.begin(), a.partition_values.end(), b.partition_values.begin(),
		                  b.partition_values.end(), ValueEquality());
	}
};

template <typename T>
using compaction_map_t =
    unordered_map<DuckLakeCompactionGroup, T, DuckLakeCompactionGroupHash, DuckLakeCompactionGroupEquality>;

//! Returns true if every field in `older_fields` still exists in `latest` with an identical field id, name and type.
//! New fields in `latest` (ADD COLUMN) are allowed; a dropped, renamed, retyped or nested-changed field is not.
static bool FieldsPreservedInLatest(const vector<unique_ptr<DuckLakeFieldId>> &older_fields,
                                    const DuckLakeFieldData &latest) {
	for (auto &older_field : older_fields) {
		auto latest_field = latest.GetByFieldIndex(older_field->GetFieldIndex());
		if (!latest_field) {
			return false;
		}
		if (older_field->Name() != latest_field->Name()) {
			return false;
		}
		if (older_field->Type() != latest_field->Type()) {
			return false;
		}
		if (!FieldsPreservedInLatest(older_field->Children(), latest)) {
			return false;
		}
	}
	return true;
}

void DuckLakeCompactor::GenerateCompactions(DuckLakeTableEntry &table,
                                            vector<unique_ptr<LogicalOperator>> &compactions) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto snapshot = transaction.GetSnapshot();

	idx_t target_file_size = catalog.GetTargetFileSize(context, table);

	DuckLakeFileSizeOptions filter_options;
	filter_options.min_file_size = options.min_file_size;
	filter_options.max_file_size = options.max_file_size;
	filter_options.target_file_size = target_file_size;
	filter_options.newer_than = options.newer_than;
	// FIXME: pass in the sort_data so that list of files is approximately sorted in the same way
	// (sorted by the min/max metadata)
	auto files = metadata_manager.GetFilesForCompaction(table, type, delete_threshold, snapshot, filter_options);

	// Resolve, per schema_version (cached), whether a file written under it can be rewritten under the latest schema.
	auto latest_entry = catalog.GetEntryById(transaction, snapshot, table_id);
	auto &latest_table = latest_entry ? latest_entry->Cast<DuckLakeTableEntry>() : table;
	auto &latest_field_data = latest_table.GetFieldData();
	const idx_t latest_schema_version = snapshot.schema_version;
	unordered_map<idx_t, bool> merges_into_latest_schema;
	auto can_merge_into_latest = [&](idx_t schema_version) -> bool {
		if (type != CompactionType::MERGE_ADJACENT_TABLES) {
			// REWRITE_DELETES assumes the source and target schemas match - never merge across schemas
			return false;
		}
		if (schema_version == latest_schema_version) {
			return true;
		}
		if (!catalog.SupportsV1_1Metadata()) {
			// DuckLake 1.0 compaction derives the schema of a file from its begin snapshot
			return false;
		}
		auto cached = merges_into_latest_schema.find(schema_version);
		if (cached != merges_into_latest_schema.end()) {
			return cached->second;
		}
		auto version_table = catalog.GetTableAtSchemaVersion(transaction, table_id, schema_version);
		bool result =
		    version_table && FieldsPreservedInLatest(version_table->GetFieldData().GetFieldIds(), latest_field_data);
		merges_into_latest_schema[schema_version] = result;
		return result;
	};

	// iterate over the files and split into separate compaction groups
	compaction_map_t<DuckLakeCompactionCandidates> candidates;
	for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
		auto &candidate = files[file_idx];
		if (candidate.file.data.file_size_bytes >= target_file_size && type != CompactionType::REWRITE_DELETES) {
			// this file by itself exceeds the threshold - skip merging
			// (does not apply to REWRITE_DELETES - delete files must be rewritten regardless of data file size)
			continue;
		}
		if (((!candidate.delete_files.empty() || candidate.has_inlined_deletions) &&
		     type == CompactionType::MERGE_ADJACENT_TABLES) ||
		    candidate.file.end_snapshot.IsValid()) {
			// Merge Adjacent Tables doesn't perform the merge if any deletes are present
			continue;
		}
		// construct the compaction group for this file - i.e. the set of candidate files we can compact it with
		DuckLakeCompactionGroup group;
		if (!can_merge_into_latest(candidate.schema_version)) {
			// incompatible with the latest schema - keep this file isolated in its own schema_version group
			group.schema_version = candidate.schema_version;
		}
		group.partition_id = candidate.file.partition_id;
		group.partition_values = candidate.file.partition_values;

		candidates[group].candidate_files.push_back(file_idx);
	}

	// we have gathered all the candidate files per compaction group
	// iterate over them to generate actual compaction commands
	uint64_t compacted_files = 0;
	for (auto &entry : candidates) {
		auto &candidate_list = entry.second.candidate_files;
		if (type == CompactionType::MERGE_ADJACENT_TABLES && candidate_list.size() <= 1) {
			// we need at least 2 files to consider a merge
			continue;
		}
		// groups with an unset schema_version contain files that must be rewritten under the latest schema
		const bool bind_to_latest = !entry.first.schema_version.IsValid();
		for (idx_t start_idx = 0; start_idx < candidate_list.size(); start_idx++) {
			// check if we can merge this file with subsequent files
			idx_t current_file_size = 0;
			idx_t compaction_idx;
			for (compaction_idx = start_idx; compaction_idx < candidate_list.size(); compaction_idx++) {
				auto candidate_idx = candidate_list[compaction_idx];
				auto &candidate = files[candidate_idx];
				idx_t file_size = candidate.file.data.file_size_bytes;
				if (type == CompactionType::REWRITE_DELETES) {
					// estimate size of remaining rows
					file_size *= (1 - candidate.delete_ratio);
				}
				const int64_t current_size_diff = NumericCast<int64_t>(current_file_size) - target_file_size;
				const int64_t merged_size_diff = NumericCast<int64_t>(current_file_size + file_size) - target_file_size;
				if (current_file_size > 0 && std::abs(merged_size_diff) >= std::abs(current_size_diff)) {
					// adding this file would move away from target_file_size - stop
					break;
				}
				// this file can be compacted along with the neighbors
				current_file_size += file_size;
			}

			if (start_idx < compaction_idx) {
				idx_t compaction_file_count = compaction_idx - start_idx;
				if (type == CompactionType::MERGE_ADJACENT_TABLES && compaction_file_count == 1) {
					// If we only have one file to merge, we have nothing to compact
					compacted_files++;
					if (compacted_files >= max_files) {
						break;
					}
					continue;
				}
				vector<DuckLakeCompactionFileEntry> compaction_files;
				for (idx_t i = start_idx; i < compaction_idx; i++) {
					compaction_files.push_back(std::move(files[candidate_list[i]]));
				}
				auto command = GenerateCompactionCommand(std::move(compaction_files), bind_to_latest);
				if (command) {
					compactions.push_back(std::move(command));
				}
				start_idx += compaction_file_count - 1;
			}
			compacted_files++;
			if (compacted_files >= max_files) {
				break;
			}
		}
		if (compacted_files >= max_files) {
			break;
		}
	}
}

unique_ptr<LogicalOperator> DuckLakeCompactor::InsertSort(Binder &binder, unique_ptr<LogicalOperator> &plan,
                                                          DuckLakeTableEntry &table,
                                                          optional_ptr<DuckLakeSort> sort_data) {
	auto bindings = plan->GetColumnBindings();
	D_ASSERT(!bindings.empty());
	auto orders =
	    BindSortOrders(binder, table.GetColumns(), table.name, bindings[0].table_index, ParseSortOrders(*sort_data));
	if (orders.empty()) {
		// Then the sorts were not in the DuckDB dialect and we return the original plan
		return std::move(plan);
	}

	// Resolve types for the input plan (could be LogicalGet or LogicalProjection)
	plan->ResolveOperatorTypes();

	// Create the LogicalOrder operator
	auto order = make_uniq<LogicalOrder>(std::move(orders));
	order->children.push_back(std::move(plan));
	return LogicalProjection::CreateIdentity(binder.GenerateTableIndex(), std::move(order));
}

optional_ptr<DuckLakeTableEntry>
DuckLakeCompactor::ResolvePartitionSpecTable(DuckLakeTableEntry &table, const DuckLakeCompactionFileEntry &source_file,
                                             idx_t partition_id) {
	auto partition_data = table.GetPartitionData();
	if (partition_data && partition_data->partition_id == partition_id) {
		return &table;
	}
	if (!source_file.partition_schema_version.IsValid()) {
		return nullptr;
	}
	auto partition_table =
	    catalog.GetTableAtSchemaVersion(transaction, table_id, source_file.partition_schema_version.GetIndex());
	if (!partition_table) {
		throw InternalException("DuckLakeCompactor: failed to find table entry for partition schema");
	}
	partition_data = partition_table->GetPartitionData();
	if (!partition_data || partition_data->partition_id != partition_id) {
		throw InternalException("DuckLakeCompactor: failed to find partition spec");
	}
	return partition_table;
}

DuckLakeTableEntry &DuckLakeCompactor::GetLatestTableEntry(DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
                                                           const DuckLakeTableEntry &table) {
	auto schema_id = table.schema.Cast<DuckLakeSchemaEntry>().GetSchemaId();
	auto latest_entry =
	    transaction.GetTransactionLocalEntry(CatalogType::TABLE_ENTRY, schema_id, table.name.GetIdentifierName());
	if (!latest_entry) {
		latest_entry = catalog.GetEntryById(transaction, transaction.GetSnapshot(), table.GetTableId());
		if (!latest_entry) {
			throw InternalException("DuckLakeCompactor: failed to find latest table entry");
		}
	}
	return latest_entry->Cast<DuckLakeTableEntry>();
}

unique_ptr<LogicalOperator> DuckLakeCompactor::PlanRewriteScan(
    ClientContext &context, Binder &binder, DuckLakeTableEntry &table, DuckLakeCopyInput &copy_input, bool write_row_id,
    bool write_snapshot_id,
    const std::function<unique_ptr<DuckLakeMultiFileList>(DuckLakeFunctionInfo &)> &create_file_list,
    unique_ptr<LogicalCopyToFile> &copy) {
	auto table_idx = binder.GenerateTableIndex();
	unique_ptr<FunctionData> bind_data;
	EntryLookupInfo info(CatalogType::TABLE_ENTRY, table.name);
	auto scan_function = table.GetScanFunction(context, bind_data, info);
	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();
	auto &read_info = scan_function.function_info->Cast<DuckLakeFunctionInfo>();
	multi_file_bind_data.file_list = create_file_list(read_info);

	if (write_row_id && write_snapshot_id) {
		copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;
	} else if (write_row_id) {
		copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID;
	} else if (write_snapshot_id) {
		copy_input.virtual_columns = InsertVirtualColumns::WRITE_SNAPSHOT_ID;
	}
	copy_input.get_table_index = table_idx.index;
	auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);
	copy = std::move(copy_options.copy);

	auto ducklake_scan =
	    make_uniq<LogicalGet>(table_idx, BoundTableFunction(std::move(scan_function)), std::move(bind_data),
	                          copy->expected_types, copy->names, table.GetVirtualColumns());
	auto &column_ids = ducklake_scan->GetMutableColumnIds();
	for (idx_t i = 0; i < table.GetColumns().PhysicalColumnCount(); i++) {
		column_ids.emplace_back(i);
	}
	if (write_row_id) {
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	if (write_snapshot_id) {
		column_ids.emplace_back(DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID);
	}

	auto root = unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(ducklake_scan));
	if (!copy_options.projection_list.empty()) {
		auto proj = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(copy_options.projection_list));
		proj->children.push_back(std::move(root));
		root = std::move(proj);
	}
	root->ResolveOperatorTypes();
	return root;
}

unique_ptr<LogicalOperator>
DuckLakeCompactor::GenerateCompactionCommand(vector<DuckLakeCompactionFileEntry> source_files,
                                             bool bind_to_latest_schema) {
	// cross-schema groups bind to the latest snapshot and others to the start of their schema version
	optional_ptr<DuckLakeTableEntry> entry;
	if (bind_to_latest_schema) {
		auto latest_entry = catalog.GetEntryById(transaction, transaction.GetSnapshot(), table_id);
		if (latest_entry) {
			entry = &latest_entry->Cast<DuckLakeTableEntry>();
		}
	} else {
		entry = catalog.GetTableAtSchemaVersion(transaction, table_id, source_files[0].schema_version);
	}
	if (!entry) {
		throw InternalException("DuckLakeCompactor: failed to find table entry for given snapshot id");
	}
	auto &table = *entry;

	auto partition_id = source_files[0].file.partition_id;
	auto partition_values = source_files[0].file.partition_values;

	bool files_are_adjacent = true;
	optional_idx prev_row_id;
	// set the files to scan as only the files we are trying to compact
	vector<DuckLakeFileListEntry> files_to_scan;
	vector<DuckLakeCompactionFileEntry> actionable_source_files;
	for (auto &source : source_files) {
		DuckLakeFileListEntry result;
		result.file = source.file.data;
		result.file_id = source.file.id;
		result.row_id_start = source.file.row_id_start;
		result.snapshot_id = source.file.begin_snapshot;
		result.mapping_id = source.file.mapping_id;
		result.inlined_file_deletions = source.inlined_file_deletions;
		switch (type) {
		case CompactionType::REWRITE_DELETES: {
			if (!source.delete_files.empty()) {
				if (source.delete_files.back().end_snapshot.IsValid()) {
					continue;
				}
				result.delete_file = source.delete_files.back().data;
			}
			break;
		}
		case CompactionType::MERGE_ADJACENT_TABLES: {
			if (!source.delete_files.empty() && type == CompactionType::MERGE_ADJACENT_TABLES) {
				// Merge Adjacent Tables does not support compaction
				throw InternalException("merge_adjacent_files should not be used to rewrite files with deletes");
			}
			break;
		}
		default:
			throw InternalException("Invalid Compaction Type");
		}
		actionable_source_files.push_back(source);
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
	if (actionable_source_files.empty()) {
		return nullptr;
	}

	string data_path;
	if (partition_id.IsValid()) {
		auto partition_table = ResolvePartitionSpecTable(table, source_files[0], partition_id.GetIndex());
		if (partition_table) {
			data_path =
			    DuckLakePartitionUtils::BuildHivePartitionPath(*partition_table, partition_values, catalog.Separator());
		} else {
			auto &file_path = source_files[0].file.data.path;
			auto &table_path = table.DataPath();
			if (!StringUtil::StartsWith(file_path, table_path)) {
				throw InternalException("DuckLakeCompactor: failed to resolve partition path");
			}
			auto relative_path = file_path.substr(table_path.size());
			auto separator_pos = relative_path.rfind(catalog.Separator());
			if (separator_pos == string::npos) {
				throw InternalException("DuckLakeCompactor: failed to resolve partition path");
			}
			data_path = relative_path.substr(0, separator_pos + catalog.Separator().size());
		}
	}

	bool write_row_id = false;
	bool write_snapshot_id = false;
	switch (type) {
	case CompactionType::MERGE_ADJACENT_TABLES: {
		// if files are adjacent, we don't need to write the row-id to the file
		write_row_id = !files_are_adjacent;
		write_snapshot_id = true;
		break;
	}
	case CompactionType::REWRITE_DELETES: {
		// when there are delete files, we always need to write row-ids because deleted rows create gaps
		write_row_id = true;
		break;
	}
	default:
		throw InternalException("Invalid Compaction Type");
	}

	DuckLakeCopyInput copy_input(context, table, data_path);
	// merge_adjacent_files does not use partitioning information - instead we always merge within partitions
	copy_input.partition_data = nullptr;
	unique_ptr<LogicalCopyToFile> copy;
	auto root = PlanRewriteScan(
	    context, binder, table, copy_input, write_row_id, write_snapshot_id,
	    [&](DuckLakeFunctionInfo &read_info) {
		    return make_uniq<DuckLakeMultiFileList>(read_info, std::move(files_to_scan));
	    },
	    copy);

	auto &latest_table = GetLatestTableEntry(catalog, transaction, table);
	auto sort_data = latest_table.GetSortData();
	if (sort_data) {
		root = DuckLakeCompactor::InsertSort(binder, root, latest_table, sort_data);
	}

	copy->table_index = binder.GenerateTableIndex();
	if (write_row_id) {
		copy->preserve_order = PreserveOrderType::DONT_PRESERVE_ORDER;
	} else {
		auto &fs = FileSystem::GetFileSystem(context);
		copy->file_path = copy->filename_pattern.CreateFilename(fs, copy->file_path, "parquet", 0);
		copy->batch_size = DEFAULT_ROW_GROUP_SIZE;
		copy->file_size_bytes = optional_idx();
		copy->rotate = false;
		copy->preserve_order = PreserveOrderType::PRESERVE_ORDER;
	}
	copy->per_thread_output = false;
	copy->children.push_back(std::move(root));

	optional_idx target_row_id_start;
	if (!write_row_id) {
		target_row_id_start = source_files[0].file.row_id_start;
	}

	// followed by the compaction operator (that writes the results back to the
	auto compaction = make_uniq<DuckLakeLogicalCompaction>(
	    binder.GenerateTableIndex(), table, std::move(actionable_source_files), std::move(copy_input.encryption_key),
	    partition_id, std::move(partition_values), target_row_id_start, type);
	compaction->children.push_back(std::move(copy));
	return std::move(compaction);
}

//===--------------------------------------------------------------------===//
// Function
//===--------------------------------------------------------------------===//
static unique_ptr<LogicalOperator> GenerateCompactionOperator(TableFunctionBindInput &input, TableIndex bind_index,
                                                              vector<unique_ptr<LogicalOperator>> &compactions) {
	if (compactions.empty()) {
		// nothing to compact - generate an empty result
		auto return_types = DuckLakeLogicalCompaction::GetResultTypes();
		auto bindings = LogicalOperator::GenerateColumnBindings(bind_index, return_types.size());
		return make_uniq<LogicalEmptyResult>(std::move(return_types), std::move(bindings));
	}
	if (compactions.size() == 1) {
		compactions[0]->Cast<DuckLakeLogicalCompaction>().table_index = bind_index;
		return std::move(compactions[0]);
	}
	return input.binder->UnionOperators(std::move(compactions), DuckLakeLogicalCompaction::GetResultTypes().size(),
	                                    bind_index);
}

static void GenerateCompaction(ClientContext &context, DuckLakeTransaction &transaction,
                               DuckLakeCatalog &ducklake_catalog, TableFunctionBindInput &input,
                               DuckLakeTableEntry &cur_table, CompactionType type, double delete_threshold,
                               uint64_t max_files, optional_idx min_file_size, optional_idx max_file_size,
                               const Value &newer_than, vector<unique_ptr<LogicalOperator>> &compactions) {
	switch (type) {
	case CompactionType::MERGE_ADJACENT_TABLES: {
		DuckLakeMergeAdjacentOptions options;
		options.min_file_size = min_file_size;
		options.max_file_size = max_file_size;
		options.newer_than = newer_than;
		DuckLakeCompactor compactor(context, ducklake_catalog, transaction, *input.binder, cur_table.GetTableId(),
		                            max_files, options);
		compactor.GenerateCompactions(cur_table, compactions);
		break;
	}
	case CompactionType::REWRITE_DELETES: {
		DuckLakeCompactor compactor(context, ducklake_catalog, transaction, *input.binder, cur_table.GetTableId(),
		                            max_files, delete_threshold);
		compactor.GenerateCompactions(cur_table, compactions);
		break;
	}
	default:
		throw InternalException("Compaction type not recognized");
	}
}

static double GetDeleteThreshold(DuckLakeTableEntry &table_entry, const DuckLakeCatalog &ducklake_catalog,
                                 const TableFunctionBindInput &input) {
	auto schema_id = table_entry.ParentSchema().Cast<DuckLakeSchemaEntry>().GetSchemaId();
	// By default, our delete threshold is 0.95 unless it was set in the global rewrite_delete_threshold
	double delete_threshold =
	    ducklake_catalog.GetConfigOption<double>("rewrite_delete_threshold", schema_id, table_entry.GetTableId(), 0.95);
	Value delete_threshold_value;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "delete_threshold", "double", delete_threshold_value)) {
		// If the user manually sets the parameter, this has priority
		delete_threshold = DoubleValue::Get(delete_threshold_value);
	}
	if (delete_threshold > 1 || delete_threshold < 0) {
		throw BinderException("The delete_threshold option must be between 0 and 1");
	}
	return delete_threshold;
}

static unique_ptr<LogicalOperator> BindCompaction(ClientContext &context, TableFunctionBindInput &input,
                                                  TableIndex bind_index, vector<Identifier> &return_names,
                                                  CompactionType type) {
	return_names.push_back("schema_name");
	return_names.push_back("table_name");
	return_names.push_back("files_processed");
	return_names.push_back("files_created");

	auto &ducklake_catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);
	uint64_t max_files = NumericLimits<uint64_t>::Maximum() - 1;
	Value max_files_value;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "max_compacted_files", "integer", max_files_value)) {
		auto max_files_count = BigIntValue::Get(max_files_value);
		if (max_files_count <= 0) {
			throw BinderException("The max_compacted_files option must be greater than zero.");
		}
		max_files = NumericCast<uint64_t>(max_files_count);
	}

	optional_idx min_file_size;
	Value min_file_size_value;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "min_file_size", "integer", min_file_size_value)) {
		auto min_file_size_bytes = BigIntValue::Get(min_file_size_value);
		if (min_file_size_bytes < 0) {
			throw BinderException("The min_file_size option must not be negative.");
		}
		min_file_size = NumericCast<uint64_t>(min_file_size_bytes);
	}

	optional_idx max_file_size;
	Value max_file_size_value;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "max_file_size", "integer", max_file_size_value)) {
		auto max_file_size_bytes = BigIntValue::Get(max_file_size_value);
		if (max_file_size_bytes <= 0) {
			throw BinderException("The max_file_size option must be greater than zero.");
		}
		max_file_size = NumericCast<uint64_t>(max_file_size_bytes);
	}

	// Validate that min_file_size < max_file_size if both are set
	if (min_file_size.IsValid() && max_file_size.IsValid() && min_file_size.GetIndex() >= max_file_size.GetIndex()) {
		throw BinderException("The min_file_size must be less than max_file_size.");
	}

	Value newer_than;
	DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "newer_than", "timestamp", newer_than);

	auto schema = DuckLakeTableFunctionUtil::GetStringOption(input, "schema");
	vector<reference<DuckLakeTableEntry>> tables;
	if (input.inputs.size() == 2) {
		auto table = DuckLakeTableFunctionUtil::GetTableName(input.inputs[1]);
		tables.push_back(DuckLakeBaseMetadataFunction::GetTableEntry(context, ducklake_catalog, schema, table));
	} else {
		tables = DuckLakeBaseMetadataFunction::GetTablesInScope(context, ducklake_catalog, schema, string());
	}
	vector<unique_ptr<LogicalOperator>> compactions;
	for (auto &table_ref : tables) {
		auto &cur_table = table_ref.get();
		if (!ducklake_catalog.AutoCompactEnabled(cur_table)) {
			continue;
		}
		auto delete_threshold = GetDeleteThreshold(cur_table, ducklake_catalog, input);
		GenerateCompaction(context, transaction, ducklake_catalog, input, cur_table, type, delete_threshold, max_files,
		                   min_file_size, max_file_size, newer_than, compactions);
	}
	return GenerateCompactionOperator(input, bind_index, compactions);
}

static unique_ptr<LogicalOperator> MergeAdjacentFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                          TableIndex bind_index, vector<Identifier> &return_names) {
	return BindCompaction(context, input, bind_index, return_names, CompactionType::MERGE_ADJACENT_TABLES);
}

static unique_ptr<LogicalOperator> RewriteFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    TableIndex bind_index, vector<Identifier> &return_names) {
	return BindCompaction(context, input, bind_index, return_names, CompactionType::REWRITE_DELETES);
}

static TableFunctionSet GetCompactionFunctions(const char *name, table_function_bind_operator_t bind,
                                               const std::function<void(TypedKwargs &)> &add_options) {
	TableFunctionSet set(name);
	for (bool with_table : {true, false}) {
		auto signature = FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR);
		if (with_table) {
			signature.AddPositionalOnly("table_name", LogicalType::VARCHAR);
		}
		TableFunction function(name, std::move(signature), nullptr, nullptr, nullptr);
		function.bind_operator = bind;
		function.GetSignature().WithTypedKwargs("options", add_options);
		if (with_table) {
			function.GetSignature().ExtendTypedKwargs(
			    [&](TypedKwargs &options) { options.Add("schema", LogicalType::VARCHAR); });
		}
		set.AddFunction(function);
	}
	return set;
}

TableFunctionSet DuckLakeMergeAdjacentFilesFunction::GetFunctions() {
	// BIGINT rather than UBIGINT: a caller writes a plain integer literal, which has no implicit cast to
	// an unsigned parameter. The range is checked where the option is read instead.
	return GetCompactionFunctions("ducklake_merge_adjacent_files", MergeAdjacentFilesBind, [](TypedKwargs &options) {
		options.Add("min_file_size", LogicalType::BIGINT)
		    .Add("max_file_size", LogicalType::BIGINT)
		    .Add("max_compacted_files", LogicalType::BIGINT)
		    .Add("newer_than", LogicalType::TIMESTAMP_TZ);
	});
}

TableFunctionSet DuckLakeRewriteDataFilesFunction::GetFunctions() {
	return GetCompactionFunctions("ducklake_rewrite_data_files", RewriteFilesBind, [](TypedKwargs &options) {
		options.Add("delete_threshold", LogicalType::DOUBLE).Add("max_compacted_files", LogicalType::BIGINT);
	});
}

} // namespace duckdb
