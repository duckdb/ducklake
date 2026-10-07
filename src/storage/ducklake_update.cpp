#include "storage/ducklake_update.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/row_id_deduplicator.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "storage/ducklake_delete.hpp"
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
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

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
	explicit DuckLakeUpdateGlobalState(ClientContext &context)
	    : seen_rows(context, {LogicalType::UBIGINT, LogicalType::BIGINT}) {
	}

	//! Duplicate row detection (first-write-wins)
	mutex seen_rows_lock;
	RowIdDeduplicator seen_rows;
};

class DuckLakeUpdateLocalState : public OperatorState {
public:
	unique_ptr<LocalSinkState> delete_local_state;
	unique_ptr<ExpressionExecutor> expression_executor;
	//! Chunk where the updated expressions are executed.
	DataChunk update_expression_chunk;
	DataChunk insert_chunk;
	DataChunk delete_chunk;
};

unique_ptr<GlobalOperatorState> DuckLakeUpdate::GetGlobalOperatorState(ClientContext &context) const {
	auto result = make_uniq<DuckLakeUpdateGlobalState>(context);
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
		expression_types.push_back(expr->GetReturnType());
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
	SelectionVector sel(input.size());
	idx_t sel_count;
	{
		lock_guard<mutex> guard(gstate.seen_rows_lock);
		sel_count = gstate.seen_rows.Register(input, delete_idx_start + 1, sel);
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

	lstate.expression_executor->Execute(input, update_expression_chunk);

	const idx_t physical_column_count = columns.size();

	// build output, physical columns + row_id
	for (idx_t i = 0; i < physical_column_count; i++) {
		insert_chunk.data[i].Reference(update_expression_chunk.data[i]);
	}
	// we place row_id right after physical columns
	insert_chunk.data[physical_column_count].Reference(input.data[row_id_index]);
	insert_chunk.SetChildCardinality(input.size());

	chunk.Reference(insert_chunk);

	auto &delete_chunk = lstate.delete_chunk;
	for (idx_t i = 0; i < DELETION_INFO_SIZE; i++) {
		delete_chunk.data[i].Reference(input.data[delete_idx_start + i]);
	}
	delete_chunk.SetChildCardinality(input.size());

	InterruptState interrupt_state;
	OperatorSinkInput delete_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	delete_op.Sink(context, delete_chunk, delete_input);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType DuckLakeUpdate::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                        GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &lstate = state_p.Cast<DuckLakeUpdateLocalState>();

	InterruptState interrupt_state;
	OperatorSinkCombineInput del_combine_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	auto result = delete_op.Combine(context, del_combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("DuckLakeUpdate::FinalExecute does not support async child operators");
	}
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
	result["Table Name"] = table.name.GetIdentifierName();
	return result;
}

DuckLakeUpdate &DuckLakeUpdate::PlanUpdateOperator(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalUpdate &op, PhysicalOperator &child_plan,
                                                   DuckLakeCopyInput &copy_input) {
	for (auto &expr : op.expressions) {
		if (expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
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
	vector<unique_ptr<Expression>> expressions(op.columns.size());
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expressions[op.columns[i].index] = op.expressions[i]->Copy();
	}

	auto &update_op =
	    planner.Make<DuckLakeUpdate>(table, op.columns, child_plan, delete_op, expressions).Cast<DuckLakeUpdate>();

	// set output types we use physical column types + BIGINT row_id
	vector<LogicalType> update_output_types;
	for (auto &expr : update_op.expressions) {
		update_output_types.push_back(expr->GetReturnType());
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
	auto &verified_plan = DuckLakeVerifyNotNull::Plan(planner, table, update_op);
	auto pipeline = DuckLakeInsert::PlanInsertPipeline(context, planner, verified_plan, table.GetColumns(), table.name,
	                                                   nullptr, false, GetInliningLimit(context, table));
	auto &physical_copy = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, pipeline.root.get());
	auto &insert_op = DuckLakeInsert::PlanInsert(context, planner, table, std::move(copy_input.encryption_key));
	return pipeline.AttachInsert(insert_op, physical_copy);
}

void DuckLakeTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                               LogicalUpdate &update, ClientContext &context) {
	// all updates in DuckLake are deletes + inserts
	update.update_is_del_and_insert = true;
	LogicalUpdate::BindAllColumns(*this, get, proj, update, true);
}

} // namespace duckdb
