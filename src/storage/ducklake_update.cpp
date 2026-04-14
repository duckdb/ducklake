#include "storage/ducklake_update.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_inline_data.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

struct FileRowId {
	uint64_t file_index;
	int64_t row_number;

	bool operator==(const FileRowId &other) const {
		return file_index == other.file_index && row_number == other.row_number;
	}
};

struct FileRowIdHash {
	hash_t operator()(const FileRowId &id) const {
		return CombineHash(Hash(id.file_index), Hash(id.row_number));
	}
};

DuckLakeUpdate::DuckLakeUpdate(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, vector<PhysicalIndex> columns_p,
                               PhysicalOperator &child, PhysicalOperator &delete_op,
                               vector<unique_ptr<Expression>> &expressions)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {}, 1), table(table),
      columns(std::move(columns_p)), delete_op(delete_op), expressions(std::move(expressions)) {
	children.push_back(child);
	row_id_index = columns.size();
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class DuckLakeUpdateGlobalState : public GlobalOperatorState {
public:
	DuckLakeUpdateGlobalState() : total_updated_count(0) {
	}

	atomic<idx_t> total_updated_count;

	//! Duplicate row detection (first-write-wins)
	mutex seen_rows_lock;
	unordered_set<FileRowId, FileRowIdHash> seen_rows;
};

class DuckLakeUpdateLocalState : public OperatorState {
public:
	unique_ptr<LocalSinkState> delete_local_state;
	unique_ptr<ExpressionExecutor> expression_executor;
	//! Chunk where the updated expressions are executed.
	DataChunk update_expression_chunk;
	DataChunk insert_chunk;
	DataChunk delete_chunk;
	idx_t updated_count = 0;
};

unique_ptr<GlobalOperatorState> DuckLakeUpdate::GetGlobalOperatorState(ClientContext &context) const {
	auto result = make_uniq<DuckLakeUpdateGlobalState>();
	// init delete_op sink state
	delete_op.sink_state = delete_op.GetGlobalSinkState(context);
	return std::move(result);
}

unique_ptr<OperatorState> DuckLakeUpdate::GetOperatorState(ExecutionContext &context) const {
	auto result = make_uniq<DuckLakeUpdateLocalState>();
	result->delete_local_state = delete_op.GetLocalSinkState(context);

	vector<LogicalType> delete_types;
	delete_types.emplace_back(LogicalType::VARCHAR);
	delete_types.emplace_back(LogicalType::UBIGINT);
	delete_types.emplace_back(LogicalType::BIGINT);

	vector<LogicalType> expression_types;
	result->expression_executor = make_uniq<ExpressionExecutor>(context.client, expressions);
	for (auto &expr : result->expression_executor->expressions) {
		expression_types.push_back(expr->return_type);
	}

	result->update_expression_chunk.Initialize(context.client, expression_types);
	result->insert_chunk.Initialize(context.client, types);

	result->delete_chunk.Initialize(context.client, delete_types);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Execute
//===--------------------------------------------------------------------===//
OperatorResultType DuckLakeUpdate::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<DuckLakeUpdateGlobalState>();
	auto &lstate = state_p.Cast<DuckLakeUpdateLocalState>();

	// filter duplicate row IDs using deletion info (last 3 columns)
	idx_t delete_idx_start = input.ColumnCount() - DELETION_INFO_SIZE;
	auto &file_index_vec = input.data[delete_idx_start + 1];
	auto &row_number_vec = input.data[delete_idx_start + 2];

	UnifiedVectorFormat file_index_data, row_number_data;
	file_index_vec.ToUnifiedFormat(input.size(), file_index_data);
	row_number_vec.ToUnifiedFormat(input.size(), row_number_data);
	auto file_indices = UnifiedVectorFormat::GetData<uint64_t>(file_index_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	SelectionVector sel(input.size());
	idx_t sel_count = 0;
	{
		lock_guard<mutex> guard(gstate.seen_rows_lock);
		for (idx_t i = 0; i < input.size(); i++) {
			auto file_idx = file_index_data.sel->get_index(i);
			auto row_idx = row_number_data.sel->get_index(i);
			FileRowId key {file_indices[file_idx], row_numbers[row_idx]};
			if (gstate.seen_rows.insert(key).second) {
				sel.set_index(sel_count++, i);
			}
		}
	}

	if (sel_count == 0) {
		// all rows were duplicates
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// slice to non-duplicate rows only
	input.Slice(sel, sel_count);

	// evaluate update expressions
	auto &update_expression_chunk = lstate.update_expression_chunk;
	auto &insert_chunk = lstate.insert_chunk;

	update_expression_chunk.SetCardinality(input.size());
	insert_chunk.SetCardinality(input.size());
	lstate.expression_executor->Execute(input, update_expression_chunk);

	const idx_t physical_column_count = columns.size();

	// build output, physical columns + row_id
	for (idx_t i = 0; i < physical_column_count; i++) {
		insert_chunk.data[i].Reference(update_expression_chunk.data[i]);
	}
	// we place row_id right after physical columns
	insert_chunk.data[physical_column_count].Reference(input.data[row_id_index]);

	chunk.Reference(insert_chunk);

	auto &delete_chunk = lstate.delete_chunk;
	delete_chunk.SetCardinality(input.size());
	for (idx_t i = 0; i < DELETION_INFO_SIZE; i++) {
		delete_chunk.data[i].Reference(input.data[delete_idx_start + i]);
	}

	InterruptState interrupt_state;
	OperatorSinkInput delete_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	delete_op.Sink(context, delete_chunk, delete_input);

	lstate.updated_count += input.size();
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType DuckLakeUpdate::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                        GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<DuckLakeUpdateGlobalState>();
	auto &lstate = state_p.Cast<DuckLakeUpdateLocalState>();

	InterruptState interrupt_state;
	OperatorSinkCombineInput del_combine_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	auto result = delete_op.Combine(context, del_combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("DuckLakeUpdate::FinalExecute does not support async child operators");
	}
	gstate.total_updated_count += lstate.updated_count;
	return OperatorFinalizeResultType::FINISHED;
}

OperatorFinalResultType DuckLakeUpdate::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         OperatorFinalizeInput &input) const {
	OperatorSinkFinalizeInput del_finalize_input {*delete_op.sink_state, input.interrupt_state};
	auto result = delete_op.Finalize(pipeline, event, context, del_finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("DuckLakeUpdate::OperatorFinalize does not support async child operators");
	}
	return OperatorFinalResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeUpdate::GetName() const {
	return "DUCKLAKE_UPDATE";
}

InsertionOrderPreservingMap<string> DuckLakeUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

DuckLakeUpdate &DuckLakeUpdate::PlanUpdateOperator(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalUpdate &op, PhysicalOperator &child_plan,
                                                   DuckLakeCopyInput &copy_input) {
	for (auto &expr : op.expressions) {
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not yet supported for updates of a DuckLake table");
		}
	}
	auto &table = op.table.Cast<DuckLakeTableEntry>();

	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < DuckLakeUpdate::DELETION_INFO_SIZE; i++) {
		row_id_indexes.push_back(i);
	}
	auto &delete_op = DuckLakeDelete::PlanDelete(context, planner, table, child_plan, std::move(row_id_indexes),
	                                             copy_input.encryption_key, false);

	// build update expressions (physical columns only, no partition cols, no casts)
	vector<unique_ptr<Expression>> expressions;
	unordered_map<idx_t, idx_t> expression_map;
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expression_map[op.columns[i].index] = i;
	}
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expressions.push_back(op.expressions[expression_map[i]]->Copy());
	}

	auto &update_op =
	    planner.Make<DuckLakeUpdate>(table, op.columns, child_plan, delete_op, expressions).Cast<DuckLakeUpdate>();

	// set output types we use physical column types + BIGINT row_id
	vector<LogicalType> update_output_types;
	for (auto &expr : update_op.expressions) {
		update_output_types.push_back(expr->return_type);
	}
	update_output_types.push_back(LogicalType::BIGINT);
	update_op.types = std::move(update_output_types);
	return update_op;
}

PhysicalOperator &DuckLakeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                              PhysicalOperator &child_plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a DuckLake table");
	}
	auto &table = op.table.Cast<DuckLakeTableEntry>();

	DuckLakeCopyInput copy_input(context, table);
	copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID;
	auto &update_op = DuckLakeUpdate::PlanUpdateOperator(context, planner, op, child_plan, copy_input);

	// follow the insert path for inlining
	optional_ptr<PhysicalOperator> plan = &update_op;
	optional_ptr<DuckLakeInlineData> inline_data;

	idx_t data_inlining_row_limit = GetInliningLimit(context, table, plan->types);
	if (data_inlining_row_limit > 0) {
		plan = planner.Make<DuckLakeInlineData>(*plan, data_inlining_row_limit);
		inline_data = plan->Cast<DuckLakeInlineData>();
	}

	auto &physical_copy = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, plan);
	auto &insert_op = DuckLakeInsert::PlanInsert(context, planner, table, std::move(copy_input.encryption_key));
	if (inline_data) {
		inline_data->insert = insert_op.Cast<DuckLakeInsert>();
	}
	insert_op.children.push_back(physical_copy);
	return insert_op;
}

void DuckLakeTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                               LogicalUpdate &update, ClientContext &context) {
	// all updates in DuckLake are deletes + inserts
	update.update_is_del_and_insert = true;

	// push projections for all columns that are not projected yet
	// FIXME: this is almost a copy of LogicalUpdate::BindExtraColumns aside from the duplicate elimination
	// add that to main DuckDB
	auto &column_ids = get.GetColumnIds();
	for (auto &column : columns.Physical()) {
		auto physical_index = column.Physical();
		bool found = false;
		for (auto &col : update.columns) {
			if (col == physical_index) {
				found = true;
				break;
			}
		}
		if (found) {
			// already updated
			continue;
		}
		// check if the column is already projected
		optional_idx column_id_index;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].GetPrimaryIndex() == physical_index.index) {
				column_id_index = i;
				break;
			}
		}
		if (!column_id_index.IsValid()) {
			// not yet projected - add to a projection list
			column_id_index = column_ids.size();
			get.AddColumnId(physical_index.index);
		}
		// column is not projected yet: project it by adding the clause "i=i" to the set of updated columns
		update.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(proj.table_index, ProjectionIndex(proj.expressions.size()))));
		proj.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(get.table_index, ProjectionIndex(column_id_index.GetIndex()))));
		get.AddColumnId(physical_index.index);
		update.columns.push_back(physical_index);
	}
}

} // namespace duckdb
