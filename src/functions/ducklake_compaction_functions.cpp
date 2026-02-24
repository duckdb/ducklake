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

//! Binds ORDER BY expressions directly using ExpressionBinder.
static vector<BoundOrderByNode> BindSortOrders(Binder &binder, const string &table_name, idx_t table_index,
                                               const vector<string> &column_names,
                                               const vector<LogicalType> &column_types,
                                               vector<OrderByNode> &pre_bound_orders) {
	// Create a child binder with the table columns in scope
	auto child_binder = Binder::CreateBinder(binder.context, &binder);
	child_binder->bind_context.AddGenericBinding(table_index, table_name, column_names, column_types);

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
                                       vector<string> partition_values_p, optional_idx row_id_start,
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

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value(table.schema.name));
	chunk.SetValue(1, 0, Value(table.name));
	chunk.SetValue(2, 0, Value::BIGINT(static_cast<int64_t>(source_files.size())));
	chunk.SetValue(3, 0, Value::BIGINT(1)); // Each compaction creates 1 output file
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
                                     Binder &binder, TableIndex table_id, DuckLakeMergeAdjacentOptions options)
    : context(context), catalog(catalog), transaction(transaction), binder(binder), table_id(table_id),
      options(options), type(CompactionType::MERGE_ADJACENT_TABLES) {
}

DuckLakeCompactor::DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
                                     Binder &binder, TableIndex table_id, double delete_threshold_p)
    : context(context), catalog(catalog), transaction(transaction), binder(binder), table_id(table_id),
      delete_threshold(delete_threshold_p), type(CompactionType::REWRITE_DELETES) {
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
	auto snapshot = transaction.GetSnapshot();

	idx_t target_file_size = DuckLakeCatalog::DEFAULT_TARGET_FILE_SIZE;
	string target_file_size_str;
	if (catalog.TryGetConfigOption("target_file_size", target_file_size_str, table)) {
		target_file_size = Value(target_file_size_str).DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
	}

	DuckLakeFileSizeOptions filter_options;
	filter_options.min_file_size = options.min_file_size;
	filter_options.max_file_size = options.max_file_size;
	filter_options.target_file_size = target_file_size;
	// FIXME: pass in the sort_data so that list of files is approximately sorted in the same way
	// (sorted by the min/max metadata)
	auto files = metadata_manager.GetFilesForCompaction(table, type, delete_threshold, snapshot, filter_options);

	// iterate over the files and split into separate compaction groups
	compaction_map_t<DuckLakeCompactionCandidates> candidates;
	for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
		auto &candidate = files[file_idx];
		if (candidate.file.data.file_size_bytes >= target_file_size) {
			// this file by itself exceeds the threshold - skip merging
			continue;
		}
		if ((!candidate.delete_files.empty() && type == CompactionType::MERGE_ADJACENT_TABLES) ||
		    candidate.file.end_snapshot.IsValid() || candidate.has_inlined_deletions) {
			// Merge Adjacent Tables doesn't perform the merge if delete files are present
			continue;
		}
		// construct the compaction group for this file - i.e. the set of candidate files we can compact it with
		DuckLakeCompactionGroup group;
		group.schema_version = candidate.schema_version;
		group.partition_id = candidate.file.partition_id;
		group.partition_values = candidate.file.partition_values;

		candidates[group].candidate_files.push_back(file_idx);
	}
	if (type == CompactionType::REWRITE_DELETES) {
		// For REWRITE_DELETES, generate one compaction command per partition group
		for (auto &entry : candidates) {
			auto &candidate_list = entry.second.candidate_files;
			if (candidate_list.empty()) {
				continue;
			}
			vector<DuckLakeCompactionFileEntry> partition_files;
			for (auto &candidate_idx : candidate_list) {
				partition_files.push_back(std::move(files[candidate_idx]));
			}
			auto compaction_command = GenerateCompactionCommand(std::move(partition_files));
			if (compaction_command) {
				compactions.push_back(std::move(compaction_command));
			}
		}
		return;
	}
	// we have gathered all the candidate files per compaction group
	// iterate over them to generate actual compaction commands
	uint64_t compacted_files = 0;
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
			for (compaction_idx = start_idx; compaction_idx < candidate_list.size(); compaction_idx++) {
				if (current_file_size >= target_file_size) {
					// we hit the target size already - stop
					break;
				}
				auto candidate_idx = candidate_list[compaction_idx];
				auto &candidate = files[candidate_idx];
				idx_t file_size = candidate.file.data.file_size_bytes;
				if (file_size >= target_file_size) {
					// don't consider merging if the file is larger than the target size
					break;
				}
				// this file can be compacted along with the neighbors
				current_file_size += file_size;
			}

			if (start_idx < compaction_idx) {
				idx_t compaction_file_count = compaction_idx - start_idx;
				if (compaction_file_count == 1) {
					// If we only have one file to compact, we have nothing to compact
					continue;
				}
				vector<DuckLakeCompactionFileEntry> compaction_files;
				for (idx_t i = start_idx; i < compaction_idx; i++) {
					compaction_files.push_back(std::move(files[candidate_list[i]]));
				}
				compactions.push_back(GenerateCompactionCommand(std::move(compaction_files)));
				start_idx += compaction_file_count - 1;
			}
			compacted_files++;
			if (compacted_files >= options.max_files) {
				break;
			}
		}
	}
}

unique_ptr<LogicalOperator> DuckLakeCompactor::InsertSort(Binder &binder, unique_ptr<LogicalOperator> &plan,
                                                          DuckLakeTableEntry &table,
                                                          optional_ptr<DuckLakeSort> sort_data) {
	auto bindings = plan->GetColumnBindings();

	// Parse the sort expressions from the sort_data
	vector<OrderByNode> pre_bound_orders;
	for (auto &pre_bound_order : sort_data->fields) {
		if (pre_bound_order.dialect != "duckdb") {
			continue;
		}
		auto parsed_expression = Parser::ParseExpressionList(pre_bound_order.expression);
		OrderByNode order_node(pre_bound_order.sort_direction, pre_bound_order.null_order,
		                       std::move(parsed_expression[0]));
		pre_bound_orders.emplace_back(std::move(order_node));
	}

	if (pre_bound_orders.empty()) {
		// Then the sorts were not in the DuckDB dialect and we return the original plan
		return std::move(plan);
	}

	// Validate all column references in sort expressions exist in the table
	vector<reference<ParsedExpression>> sort_expressions;
	for (auto &pre_bound_order : pre_bound_orders) {
		sort_expressions.push_back(*pre_bound_order.expression);
	}
	DuckLakeTableEntry::ValidateSortExpressionColumns(table, sort_expressions);

	// Resolve types for the input plan (could be LogicalGet or LogicalProjection)
	plan->ResolveOperatorTypes();

	auto &columns = table.GetColumns();
	auto current_columns = columns.GetColumnNames();
	auto column_types = columns.GetColumnTypes();

	D_ASSERT(!bindings.empty());
	auto table_index = bindings[0].table_index;

	// Bind the ORDER BY expressions
	auto orders = BindSortOrders(binder, table.name, table_index, current_columns, column_types, pre_bound_orders);

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
	vector<DuckLakeCompactionFileEntry> actionable_source_files;
	for (auto &source : source_files) {
		DuckLakeFileListEntry result;
		result.file = source.file.data;
		result.row_id_start = source.file.row_id_start;
		result.snapshot_id = source.file.begin_snapshot;
		result.mapping_id = source.file.mapping_id;
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
	auto table_path = table.DataPath();
	string data_path;
	if (partition_id.IsValid()) {
		data_path = actionable_source_files[0].file.data.path;
		data_path = StringUtil::Replace(data_path, table_path, "");
		auto path_result = StringUtil::Split(data_path, catalog.Separator());
		data_path = "";
		if (path_result.size() > 1) {
			// This means we have a hive partition.
			for (idx_t i = 0; i < path_result.size() - 1; i++) {
				data_path += path_result[i];
				if (i != path_result.size() - 2) {
					data_path += catalog.Separator();
				}
			}
			// If we do have a hive partition, let's verify all files have the same one.
			for (idx_t i = 1; i < actionable_source_files.size(); i++) {
				if (!StringUtil::Contains(actionable_source_files[i].file.data.path, data_path)) {
					throw InternalException("DuckLakeCompactor: Files have different hive partition path");
				}
			}
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
	                          copy_options.names, std::move(virtual_columns));

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
	auto latest_entry = transaction.GetTransactionLocalEntry(CatalogType::TABLE_ENTRY, table.schema.name, table.name);
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
static unique_ptr<LogicalOperator> GenerateCompactionOperator(TableFunctionBindInput &input, idx_t bind_index,
                                                              vector<unique_ptr<LogicalOperator>> &compactions) {
	if (compactions.empty()) {
		// nothing to compact - generate an empty result
		vector<ColumnBinding> bindings;
		vector<LogicalType> return_types;
		bindings.emplace_back(bind_index, 0);
		bindings.emplace_back(bind_index, 1);
		bindings.emplace_back(bind_index, 2);
		bindings.emplace_back(bind_index, 3);
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
		options.max_files = max_files;
		options.min_file_size = min_file_size;
		options.max_file_size = max_file_size;
		DuckLakeCompactor compactor(context, ducklake_catalog, transaction, *input.binder, cur_table.GetTableId(),
		                            options);
		compactor.GenerateCompactions(cur_table, compactions);
		break;
	}
	case CompactionType::REWRITE_DELETES: {
		DuckLakeCompactor compactor(context, ducklake_catalog, transaction, *input.binder, cur_table.GetTableId(),
		                            delete_threshold);
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

unique_ptr<LogicalOperator> BindCompaction(ClientContext &context, TableFunctionBindInput &input, idx_t bind_index,
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
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, table, nullptr, QueryErrorContext());
	auto table_entry = catalog.GetEntry(context, schema, table_lookup, OnEntryNotFound::THROW_EXCEPTION);
	auto &ducklake_table = table_entry->Cast<DuckLakeTableEntry>();
	optional_ptr<DuckLakeSchemaEntry> dl_schema;
	bool auto_compact;
	if (!schema.empty()) {
		auto schema_catalog = catalog.GetSchema(context, catalog.GetName(), schema, OnEntryNotFound::THROW_EXCEPTION);
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
                                                          idx_t bind_index, vector<string> &return_names) {
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
                                                    idx_t bind_index, vector<string> &return_names) {
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
		if (type.size() == 2) {
			function.named_parameters["schema"] = LogicalType::VARCHAR;
		}
		set.AddFunction(function);
	}
	return set;
}

} // namespace duckdb
