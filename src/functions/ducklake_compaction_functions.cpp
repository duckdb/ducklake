#include "functions/ducklake_table_functions.hpp"
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
#include "duckdb/planner/operator/logical_set_operation.hpp"
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
		auto parsed_expression = Parser::ParseExpressionList(field.expression);
		pre_bound_orders.emplace_back(field.sort_direction, field.null_order, std::move(parsed_expression[0]));
	}
	return pre_bound_orders;
}

//! Binds ORDER BY expressions directly using ExpressionBinder.
vector<BoundOrderByNode> DuckLakeCompactor::BindSortOrders(Binder &binder, DuckLakeTableEntry &table,
                                                           TableIndex table_index,
                                                           vector<OrderByNode> &pre_bound_orders) {
	auto &columns = table.GetColumns();
	auto column_names = columns.GetColumnNames();
	auto column_types = columns.GetColumnTypes();

	// Create a child binder with the table columns in scope
	auto child_binder = Binder::CreateBinder(binder.context, &binder);
	child_binder->bind_context.AddGenericBinding(table_index, table.name, StringsToIdentifiers(column_names),
	                                             column_types);

	// Bind each ORDER BY expression directly
	vector<BoundOrderByNode> orders;
	for (auto &pre_bound_order : pre_bound_orders) {
		ExpressionBinder expr_binder(*child_binder, binder.context);
		auto bound_expr = expr_binder.Bind(pre_bound_order.expression);
		orders.emplace_back(pre_bound_order.type, pre_bound_order.null_order, std::move(bound_expr));
	}

	return orders;
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
// Source State
//===--------------------------------------------------------------------===//
class DuckLakeCompactionSourceState : public GlobalSourceState {
public:
	DuckLakeCompactionSourceState() : returned_result(false) {
	}

	bool returned_result;
};

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSourceState> DuckLakeCompaction::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DuckLakeCompactionSourceState>();
}

SourceResultType DuckLakeCompaction::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<DuckLakeCompactionSourceState>();
	if (source_state.returned_result) {
		return SourceResultType::FINISHED;
	}
	source_state.returned_result = true;

	if (!this->sink_state) {
		throw InternalException("DuckLakeCompaction - missing sink state while producing result");
	}
	auto &gstate = this->sink_state->Cast<DuckLakeInsertGlobalState>();
	auto files_created = gstate.written_files.size();

	chunk.data[0].Append(Value(table.schema.name));
	chunk.data[1].Append(Value(table.name));
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
		if (a.schema_version != b.schema_version || a.partition_id != b.partition_id) {
			return false;
		}
		if (a.partition_values.size() != b.partition_values.size()) {
			return false;
		}
		for (idx_t i = 0; i < a.partition_values.size(); i++) {
			const auto &av = a.partition_values[i];
			const auto &bv = b.partition_values[i];
			if (av.IsNull() != bv.IsNull()) {
				return false;
			}
			if (!av.IsNull() && av.ToString() != bv.ToString()) {
				return false;
			}
		}
		return true;
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
		auto cached = merges_into_latest_schema.find(schema_version);
		if (cached != merges_into_latest_schema.end()) {
			return cached->second;
		}
		auto begin_snapshot = catalog.GetBeginSnapshotForSchemaVersion(table_id, schema_version, transaction);
		DuckLakeSnapshot version_snapshot(begin_snapshot, schema_version, 0, 0);
		auto version_entry = catalog.GetEntryById(transaction, version_snapshot, table_id);
		bool result = false;
		if (version_entry) {
			auto &version_table = version_entry->Cast<DuckLakeTableEntry>();
			result = FieldsPreservedInLatest(version_table.GetFieldData().GetFieldIds(), latest_field_data);
		}
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
				compactions.push_back(GenerateCompactionCommand(std::move(compaction_files), bind_to_latest));
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
                                                          optional_ptr<DuckLakeSort> sort_data, bool add_tiebreakers) {
	auto bindings = plan->GetColumnBindings();

	// Parse the sort expressions from the sort_data
	auto pre_bound_orders = DuckLakeCompactor::ParseSortOrders(*sort_data);
	if (pre_bound_orders.empty()) {
		// Then the sorts were not in the DuckDB dialect and we return the original plan
		return std::move(plan);
	}

	// Validate all column references in sort expressions exist in the table
	DuckLakeTableEntry::ValidateSortExpressionColumns(table, pre_bound_orders);

	// Resolve types for the input plan (could be LogicalGet or LogicalProjection)
	plan->ResolveOperatorTypes();

	D_ASSERT(!bindings.empty());
	auto table_index = bindings[0].table_index;

	// Bind the ORDER BY expressions
	auto orders = DuckLakeCompactor::BindSortOrders(binder, table, table_index, pre_bound_orders);

	// Append (row_id, snapshot_id) as deterministic tiebreakers when requested so the file order
	// exactly matches the deletes-position query's ORDER BY, including ties in the user sort key.
	// Handles ALTER TABLE ADD COLUMN in the same transaction as the flush
	if (add_tiebreakers) {
		LogicalOperator *get_op = plan.get();
		while (get_op->type != LogicalOperatorType::LOGICAL_GET) {
			if (get_op->children.size() != 1) {
				throw InternalException("DuckLakeCompactor::InsertSort: expected single-child operator chain to "
				                        "LogicalGet when add_tiebreakers=true");
			}
			get_op = get_op->children[0].get();
		}
		auto &logical_get = get_op->Cast<LogicalGet>();
		auto &column_ids = logical_get.GetColumnIds();
		idx_t row_id_pos = DConstants::INVALID_INDEX;
		idx_t snapshot_id_pos = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto primary = column_ids[i].GetPrimaryIndex();
			if (primary == COLUMN_IDENTIFIER_ROW_ID) {
				row_id_pos = i;
			} else if (primary == DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID) {
				snapshot_id_pos = i;
			}
		}
		if (row_id_pos == DConstants::INVALID_INDEX || snapshot_id_pos == DConstants::INVALID_INDEX ||
		    row_id_pos >= bindings.size() || snapshot_id_pos >= bindings.size()) {
			throw InternalException("DuckLakeCompactor::InsertSort: row_id and snapshot_id virtual columns must be "
			                        "present in the LogicalGet's column_ids when add_tiebreakers=true");
		}
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
		                    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, bindings[row_id_pos]));
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
		                    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, bindings[snapshot_id_pos]));
	}

	// Create the LogicalOrder operator
	auto order = make_uniq<LogicalOrder>(std::move(orders));
	order->children.push_back(std::move(plan));
	order->ResolveOperatorTypes();

	// Create a projection to pass through all columns
	vector<unique_ptr<Expression>> cast_expressions;
	auto &types = order->types;
	auto order_bindings = order->GetColumnBindings();

	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		auto &type = types[col_idx];
		auto &binding = order_bindings[col_idx];
		auto ref_expr = make_uniq<BoundColumnRefExpression>(type, binding);
		cast_expressions.push_back(std::move(ref_expr));
	}

	auto projected = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(cast_expressions));
	projected->children.push_back(std::move(order));

	return std::move(projected);
}

optional_ptr<DuckLakeTableEntry>
DuckLakeCompactor::ResolvePartitionSpecTable(DuckLakeTableEntry &table, const DuckLakeCompactionFileEntry &source_file,
                                             idx_t partition_id) {
	auto partition_data = table.GetPartitionData();
	if (partition_data && partition_data->partition_id == partition_id) {
		return &table;
	}
	if (!source_file.partition_snapshot_id.IsValid() || !source_file.partition_schema_version.IsValid()) {
		return nullptr;
	}
	DuckLakeSnapshot partition_snapshot(source_file.partition_snapshot_id.GetIndex(),
	                                    source_file.partition_schema_version.GetIndex(), 0, 0);
	auto partition_entry = catalog.GetEntryById(transaction, partition_snapshot, table_id);
	if (!partition_entry) {
		throw InternalException("DuckLakeCompactor: failed to find table entry for partition schema");
	}
	auto &partition_table = partition_entry->Cast<DuckLakeTableEntry>();
	partition_data = partition_table.GetPartitionData();
	if (!partition_data || partition_data->partition_id != partition_id) {
		throw InternalException("DuckLakeCompactor: failed to find partition spec");
	}
	return &partition_table;
}

unique_ptr<LogicalOperator>
DuckLakeCompactor::GenerateCompactionCommand(vector<DuckLakeCompactionFileEntry> source_files,
                                             bool bind_to_latest_schema) {
	// Cross-schema groups bind to the latest snapshot so the merged file is written under the current schema (the
	// reader projects each source via its own mapping_id); same-schema groups bind to the source schema_version.
	DuckLakeSnapshot snapshot = bind_to_latest_schema ? transaction.GetSnapshot()
	                                                  : DuckLakeSnapshot(source_files[0].file.begin_snapshot,
	                                                                     source_files[0].schema_version, 0, 0);

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
	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();
	auto &read_info = scan_function.function_info->Cast<DuckLakeFunctionInfo>();
	multi_file_bind_data.file_list = make_uniq<DuckLakeMultiFileList>(read_info, std::move(files_to_scan));

	// generate the LogicalGet
	auto &columns = table.GetColumns();
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
	if (write_row_id) {
		if (write_snapshot_id) {
			copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;
		} else {
			copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID;
		}
	} else if (write_snapshot_id) {
		copy_input.virtual_columns = InsertVirtualColumns::WRITE_SNAPSHOT_ID;
	}

	auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);

	auto virtual_columns = table.GetVirtualColumns();
	auto ducklake_scan =
	    make_uniq<LogicalGet>(table_idx, std::move(scan_function), std::move(bind_data), copy_options.expected_types,
	                          StringsToIdentifiers(copy_options.names), std::move(virtual_columns));

	auto &column_ids = ducklake_scan->GetMutableColumnIds();
	for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
		column_ids.emplace_back(i);
	}
	if (write_row_id) {
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	if (write_snapshot_id) {
		column_ids.emplace_back(DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID);
	}

	// Resolve types so we can check if we need casts
	ducklake_scan->ResolveOperatorTypes();

	// Insert a cast projection if necessary
	auto root = unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(ducklake_scan));

	if (DuckLakeTypes::RequiresCast(root->types)) {
		root = DuckLakeInsert::InsertCasts(binder, root);
	}

	// If compaction should be ordered, add Order By (and projection) to logical plan
	// Do not pull the sort setting at the time of the creation of the files being compacted,
	// and instead pull the latest sort setting
	// First, see if there are transaction local changes to the table
	// Then fall back to latest snapshot if no local changes
	auto latest_entry = transaction.GetTransactionLocalEntry(
	    CatalogType::TABLE_ENTRY, table.schema.name.GetIdentifierName(), table.name.GetIdentifierName());
	if (!latest_entry) {
		auto latest_snapshot = transaction.GetSnapshot();
		latest_entry = catalog.GetEntryById(transaction, latest_snapshot, table_id);
		if (!latest_entry) {
			throw InternalException("DuckLakeCompactor: failed to find local table entry");
		}
	}
	auto &latest_table = latest_entry->Cast<DuckLakeTableEntry>();

	auto sort_data = latest_table.GetSortData();
	if (sort_data) {
		root = DuckLakeCompactor::InsertSort(binder, root, latest_table, sort_data);
	}

	// read the configured row group size before copy_options.info is moved into the LogicalCopyToFile
	idx_t configured_row_group_size = DuckLakeInsert::GetCopyBatchSize(copy_options);

	// generate the LogicalCopyToFile
	auto copy = make_uniq<LogicalCopyToFile>(std::move(copy_options.copy_function), std::move(copy_options.bind_data),
	                                         std::move(copy_options.info));

	auto &fs = FileSystem::GetFileSystem(context);
	if (write_row_id) {
		copy->file_path = copy_options.file_path;
		copy->batch_size = configured_row_group_size;
		copy->file_size_bytes = copy_options.file_size_bytes;
		copy->rotate = copy_options.rotate;
		copy->preserve_order = PreserveOrderType::DONT_PRESERVE_ORDER;
	} else {
		copy->file_path = copy_options.filename_pattern.CreateFilename(fs, copy_options.file_path, "parquet", 0);
		copy->batch_size = DEFAULT_ROW_GROUP_SIZE;
		copy->file_size_bytes = optional_idx();
		copy->rotate = false;
		copy->preserve_order = PreserveOrderType::PRESERVE_ORDER;
	}
	copy->use_tmp_file = copy_options.use_tmp_file;
	copy->filename_pattern = std::move(copy_options.filename_pattern);
	copy->file_extension = std::move(copy_options.file_extension);
	copy->overwrite_mode = copy_options.overwrite_mode;
	copy->per_thread_output = false;
	copy->return_type = copy_options.return_type;
	copy->partition_output = copy_options.partition_output;
	copy->write_partition_columns = copy_options.write_partition_columns;
	copy->write_empty_file = false;
	copy->partition_columns = std::move(copy_options.partition_columns);
	copy->names = StringsToIdentifiers(copy_options.names);
	copy->expected_types = std::move(copy_options.expected_types);
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
		vector<ColumnBinding> bindings;
		vector<LogicalType> return_types;
		bindings.emplace_back(bind_index, ProjectionIndex(0));
		bindings.emplace_back(bind_index, ProjectionIndex(1));
		bindings.emplace_back(bind_index, ProjectionIndex(2));
		bindings.emplace_back(bind_index, ProjectionIndex(3));
		return_types.emplace_back(LogicalType::VARCHAR);
		return_types.emplace_back(LogicalType::VARCHAR);
		return_types.emplace_back(LogicalType::BIGINT);
		return_types.emplace_back(LogicalType::BIGINT);
		return make_uniq<LogicalEmptyResult>(std::move(return_types), std::move(bindings));
	}
	if (compactions.size() == 1) {
		compactions[0]->Cast<DuckLakeLogicalCompaction>().table_index = bind_index;
		return std::move(compactions[0]);
	}
	auto union_op = input.binder->UnionOperators(std::move(compactions));
	auto &set_op = union_op->Cast<LogicalSetOperation>();
	set_op.table_index = bind_index;
	// Manually set column_count - this is normally derived during optimization
	// but we need it at bind time for column binding resolution
	set_op.column_count = 4;
	return union_op;
}

static void GenerateCompaction(ClientContext &context, DuckLakeTransaction &transaction,
                               DuckLakeCatalog &ducklake_catalog, TableFunctionBindInput &input,
                               DuckLakeTableEntry &cur_table, CompactionType type, double delete_threshold,
                               uint64_t max_files, optional_idx min_file_size, optional_idx max_file_size,
                               vector<unique_ptr<LogicalOperator>> &compactions) {
	switch (type) {
	case CompactionType::MERGE_ADJACENT_TABLES: {
		DuckLakeMergeAdjacentOptions options;
		options.min_file_size = min_file_size;
		options.max_file_size = max_file_size;
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

double GetDeleteThreshold(optional_ptr<DuckLakeSchemaEntry> schema_entry, const DuckLakeTableEntry &table_entry,
                          const DuckLakeCatalog &ducklake_catalog, const TableFunctionBindInput &input) {
	SchemaIndex schema_index;
	if (schema_entry) {
		schema_index = schema_entry->GetSchemaId();
	}
	// By default, our delete threshold is 0.95 unless it was set in the global rewrite_delete_threshold
	double delete_threshold = ducklake_catalog.GetConfigOption<double>("rewrite_delete_threshold", schema_index,
	                                                                   table_entry.GetTableId(), 0.95);
	auto delete_threshold_entry = input.named_parameters.find("delete_threshold");
	if (delete_threshold_entry != input.named_parameters.end()) {
		// If the user manually sets the parameter, this has priority
		delete_threshold = DoubleValue::Get(delete_threshold_entry->second);
	}
	if (delete_threshold > 1 || delete_threshold < 0) {
		throw BinderException("The delete_threshold option must be between 0 and 1");
	}
	return delete_threshold;
}

unique_ptr<LogicalOperator> BindCompaction(ClientContext &context, TableFunctionBindInput &input, TableIndex bind_index,
                                           CompactionType type) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);
	string schema, table;
	vector<unique_ptr<LogicalOperator>> compactions;
	uint64_t max_files = NumericLimits<uint64_t>::Maximum() - 1;
	auto max_files_entry = input.named_parameters.find("max_compacted_files");
	if (max_files_entry != input.named_parameters.end()) {
		if (max_files_entry->second.IsNull()) {
			throw BinderException("The max_compacted_files option must be a non-null integer.");
		}
		max_files = UBigIntValue::Get(max_files_entry->second);
		if (max_files == 0) {
			throw BinderException("The max_compacted_files option must be greater than zero.");
		}
	}

	optional_idx min_file_size;
	auto min_file_size_entry = input.named_parameters.find("min_file_size");
	if (min_file_size_entry != input.named_parameters.end()) {
		if (min_file_size_entry->second.IsNull()) {
			throw BinderException("The min_file_size option must be a non-null integer.");
		}
		min_file_size = UBigIntValue::Get(min_file_size_entry->second);
	}

	optional_idx max_file_size;
	auto max_file_size_entry = input.named_parameters.find("max_file_size");
	if (max_file_size_entry != input.named_parameters.end()) {
		if (max_file_size_entry->second.IsNull()) {
			throw BinderException("The max_file_size option must be a non-null integer.");
		}
		max_file_size = UBigIntValue::Get(max_file_size_entry->second);
		if (max_file_size.GetIndex() == 0) {
			throw BinderException("The max_file_size option must be greater than zero.");
		}
	}

	// Validate that min_file_size < max_file_size if both are set
	if (min_file_size.IsValid() && max_file_size.IsValid() && min_file_size.GetIndex() >= max_file_size.GetIndex()) {
		throw BinderException("The min_file_size must be less than max_file_size.");
	}

	if (input.inputs.size() == 1) {
		// No default schema/table, we will perform rewrites on deletes in the whole database
		auto schemas = ducklake_catalog.GetSchemas(context);
		for (auto &cur_schema : schemas) {
			cur_schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type == CatalogType::TABLE_ENTRY) {
					auto &dl_cur_schema = cur_schema.get().Cast<DuckLakeSchemaEntry>();
					auto &cur_table = entry.Cast<DuckLakeTableEntry>();
					if (ducklake_catalog.GetConfigOption<string>("auto_compact", dl_cur_schema.GetSchemaId(),
					                                             cur_table.GetTableId(), "true") == "true") {
						auto delete_threshold = GetDeleteThreshold(&dl_cur_schema, cur_table, ducklake_catalog, input);
						GenerateCompaction(context, transaction, ducklake_catalog, input, cur_table, type,
						                   delete_threshold, max_files, min_file_size, max_file_size, compactions);
					}
				}
			});
		}
		return GenerateCompactionOperator(input, bind_index, compactions);
	} else if (input.inputs.size() == 2) {
		// We have the table_name defined in our input
		table = StringValue::Get(input.inputs[1]);
	}
	// A table name is provided, so we only compact that
	auto schema_entry = input.named_parameters.find("schema");
	if (schema_entry != input.named_parameters.end()) {
		schema = StringValue::Get(schema_entry->second);
	}
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, Identifier(table), nullptr, QueryErrorContext());
	auto table_entry = catalog.GetEntry(context, Identifier(schema), table_lookup, OnEntryNotFound::THROW_EXCEPTION);
	auto &ducklake_table = table_entry->Cast<DuckLakeTableEntry>();
	optional_ptr<DuckLakeSchemaEntry> dl_schema;
	bool auto_compact;
	if (!schema.empty()) {
		auto schema_catalog =
		    catalog.GetSchema(context, catalog.GetName(), Identifier(schema), OnEntryNotFound::THROW_EXCEPTION);
		dl_schema = &schema_catalog->Cast<DuckLakeSchemaEntry>();
		auto_compact = ducklake_catalog.GetConfigOption<string>("auto_compact", dl_schema.get()->GetSchemaId(),
		                                                        ducklake_table.GetTableId(), "true") == "true";

	} else {
		auto_compact =
		    ducklake_catalog.GetConfigOption<string>("auto_compact", {}, ducklake_table.GetTableId(), "true") == "true";
	}

	if (auto_compact) {
		auto delete_threshold = GetDeleteThreshold(dl_schema, ducklake_table, ducklake_catalog, input);
		GenerateCompaction(context, transaction, ducklake_catalog, input, ducklake_table, type, delete_threshold,
		                   max_files, min_file_size, max_file_size, compactions);
	}

	return GenerateCompactionOperator(input, bind_index, compactions);
}

static unique_ptr<LogicalOperator> MergeAdjacentFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                          TableIndex bind_index, vector<string> &return_names) {
	return_names.push_back("schema_name");
	return_names.push_back("table_name");
	return_names.push_back("files_processed");
	return_names.push_back("files_created");
	return BindCompaction(context, input, bind_index, CompactionType::MERGE_ADJACENT_TABLES);
}

TableFunctionSet DuckLakeMergeAdjacentFilesFunction::GetFunctions() {
	TableFunctionSet set("ducklake_merge_adjacent_files");
	const vector<vector<LogicalType>> at_types {{LogicalType::VARCHAR, LogicalType::VARCHAR}, {LogicalType::VARCHAR}};
	for (auto &type : at_types) {
		TableFunction function("ducklake_merge_adjacent_files", type, nullptr, nullptr, nullptr);
		function.bind_operator = MergeAdjacentFilesBind;
		function.named_parameters["min_file_size"] = LogicalType::UBIGINT;
		function.named_parameters["max_file_size"] = LogicalType::UBIGINT;
		function.named_parameters["max_compacted_files"] = LogicalType::UBIGINT;
		if (type.size() == 2) {
			function.named_parameters["schema"] = LogicalType::VARCHAR;
		}
		set.AddFunction(function);
	}
	return set;
}

static unique_ptr<LogicalOperator> RewriteFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    TableIndex bind_index, vector<string> &return_names) {
	return_names.push_back("schema_name");
	return_names.push_back("table_name");
	return_names.push_back("files_processed");
	return_names.push_back("files_created");
	return BindCompaction(context, input, bind_index, CompactionType::REWRITE_DELETES);
}

TableFunctionSet DuckLakeRewriteDataFilesFunction::GetFunctions() {
	TableFunctionSet set("ducklake_rewrite_data_files");
	vector<vector<LogicalType>> at_types {{LogicalType::VARCHAR, LogicalType::VARCHAR}, {LogicalType::VARCHAR}};
	for (auto &type : at_types) {
		TableFunction function("ducklake_rewrite_data_files", type, nullptr, nullptr, nullptr);
		function.bind_operator = RewriteFilesBind;
		function.named_parameters["delete_threshold"] = LogicalType::DOUBLE;
		function.named_parameters["max_compacted_files"] = LogicalType::UBIGINT;
		if (type.size() == 2) {
			function.named_parameters["schema"] = LogicalType::VARCHAR;
		}
		set.AddFunction(function);
	}
	return set;
}

} // namespace duckdb
