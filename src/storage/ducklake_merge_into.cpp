#include "storage/ducklake_catalog.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "storage/ducklake_update.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_inline_data.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Merge Insert
//===--------------------------------------------------------------------===//
class DuckLakeMergeInsert : public PhysicalOperator {
public:
	DuckLakeMergeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, PhysicalOperator &insert,
	                    PhysicalOperator &copy);

	//! The copy operator that writes to the file
	PhysicalOperator &copy;
	//! The final insert operator
	PhysicalOperator &insert;
	//! Extra Projections
	vector<unique_ptr<Expression>> extra_projections;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

DuckLakeMergeInsert::DuckLakeMergeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                         PhysicalOperator &insert, PhysicalOperator &copy)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), copy(copy), insert(insert) {
}

SourceResultType DuckLakeMergeInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Shared helpers
//===--------------------------------------------------------------------===//

// Apply extra projections and casts before feeding a chunk to CopyToFile — shared by MergeInsert and MergeUpdate
static void ProjectAndCastForCopy(ClientContext &context, DataChunk &input_chunk, PhysicalOperator &copy_op,
                                  ExpressionExecutor *expression_executor, DataChunk &projected_chunk,
                                  DataChunk &cast_chunk) {
	reference<DataChunk> chunk_ref = input_chunk;
	if (expression_executor) {
		projected_chunk.Reset();
		expression_executor->Execute(input_chunk, projected_chunk);
		chunk_ref = projected_chunk;
	}
	auto &copy_types = copy_op.Cast<PhysicalCopyToFile>().expected_types;
	for (idx_t i = 0; i < chunk_ref.get().ColumnCount(); i++) {
		if (chunk_ref.get().data[i].GetType() != copy_types[i]) {
			VectorOperations::Cast(context, chunk_ref.get().data[i], cast_chunk.data[i], chunk_ref.get().size());
		} else {
			cast_chunk.data[i].Reference(chunk_ref.get().data[i]);
		}
	}
	cast_chunk.SetCardinality(chunk_ref.get().size());
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DuckLakeMergeIntoLocalState : public LocalSinkState {
public:
	unique_ptr<LocalSinkState> copy_sink_state;
	//! Used if we have extra projections
	DataChunk cast_chunk;
	DataChunk chunk;
	unique_ptr<ExpressionExecutor> expression_executor;
};

SinkResultType DuckLakeMergeInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &local_state = input.local_state.Cast<DuckLakeMergeIntoLocalState>();
	ProjectAndCastForCopy(context.client, chunk, copy, local_state.expression_executor.get(), local_state.chunk,
	                      local_state.cast_chunk);
	OperatorSinkInput sink_input {*copy.sink_state, *local_state.copy_sink_state, input.interrupt_state};
	return copy.Sink(context, local_state.cast_chunk, sink_input);
}

SinkCombineResultType DuckLakeMergeInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &local_state = input.local_state.Cast<DuckLakeMergeIntoLocalState>();
	OperatorSinkCombineInput combine_input {*copy.sink_state, *local_state.copy_sink_state, input.interrupt_state};
	return copy.Combine(context, combine_input);
}

// Scan copy operator source and sink into insert operator — shared by MergeInsert and MergeUpdate
static void FinalizeCopyToInsert(Pipeline &pipeline, Event &event, ClientContext &context, PhysicalOperator &copy_op,
                                 PhysicalOperator &insert_op, InterruptState &interrupt_state) {
	DataChunk chunk;
	chunk.Initialize(context, copy_op.types);

	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);

	auto copy_global = copy_op.GetGlobalSourceState(context);
	auto copy_local = copy_op.GetLocalSourceState(exec_context, *copy_global);
	OperatorSourceInput source_input {*copy_global, *copy_local, interrupt_state};

	auto insert_global = insert_op.GetGlobalSinkState(context);
	auto insert_local = insert_op.GetLocalSinkState(exec_context);
	OperatorSinkInput sink_input {*insert_global, *insert_local, interrupt_state};
	SourceResultType source_res = SourceResultType::HAVE_MORE_OUTPUT;
	while (source_res == SourceResultType::HAVE_MORE_OUTPUT) {
		chunk.Reset();
		source_res = copy_op.GetData(exec_context, chunk, source_input);
		if (chunk.size() == 0) {
			continue;
		}
		if (source_res == SourceResultType::BLOCKED) {
			throw InternalException("BLOCKED not supported in DuckLakeMerge");
		}

		auto sink_result = insert_op.Sink(exec_context, chunk, sink_input);
		if (sink_result != SinkResultType::NEED_MORE_INPUT) {
			throw InternalException("BLOCKED not supported in DuckLakeMerge");
		}
	}
	OperatorSinkCombineInput combine_input {*insert_global, *insert_local, interrupt_state};
	auto combine_res = insert_op.Combine(exec_context, combine_input);
	if (combine_res == SinkCombineResultType::BLOCKED) {
		throw InternalException("BLOCKED not supported in DuckLakeMerge");
	}
	OperatorSinkFinalizeInput finalize_input {*insert_global, interrupt_state};
	auto finalize_res = insert_op.Finalize(pipeline, event, context, finalize_input);
	if (finalize_res == SinkFinalizeType::BLOCKED) {
		throw InternalException("BLOCKED not supported in DuckLakeMerge");
	}
}

SinkFinalizeType DuckLakeMergeInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	OperatorSinkFinalizeInput copy_finalize {*copy.sink_state, input.interrupt_state};
	auto finalize_result = copy.Finalize(pipeline, event, context, copy_finalize);
	if (finalize_result == SinkFinalizeType::BLOCKED) {
		return SinkFinalizeType::BLOCKED;
	}

	FinalizeCopyToInsert(pipeline, event, context, copy, insert, input.interrupt_state);
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> DuckLakeMergeInsert::GetGlobalSinkState(ClientContext &context) const {
	copy.sink_state = copy.GetGlobalSinkState(context);
	return make_uniq<GlobalSinkState>();
}

unique_ptr<LocalSinkState> DuckLakeMergeInsert::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<DuckLakeMergeIntoLocalState>();
	result->copy_sink_state = copy.GetLocalSinkState(context);
	if (!extra_projections.empty()) {
		result->expression_executor = make_uniq<ExpressionExecutor>(context.client, extra_projections);
		vector<LogicalType> insert_types;
		for (auto &expr : result->expression_executor->expressions) {
			insert_types.push_back(expr->return_type);
		}
		result->chunk.Initialize(context.client, insert_types);
	}
	result->cast_chunk.Initialize(context.client, copy.Cast<PhysicalCopyToFile>().expected_types);

	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Merge Update
//===--------------------------------------------------------------------===//
class DuckLakeMergeUpdate : public PhysicalOperator {
public:
	DuckLakeMergeUpdate(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeUpdate &update_op,
	                    optional_ptr<DuckLakeInlineData> inline_data_op, PhysicalOperator &copy_op,
	                    PhysicalOperator &insert_op)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), update_op(update_op),
	      inline_data_op(inline_data_op), copy_op(copy_op), insert_op(insert_op) {
	}

	DuckLakeUpdate &update_op;
	optional_ptr<DuckLakeInlineData> inline_data_op;
	PhysicalOperator &copy_op;
	PhysicalOperator &insert_op;
	//! Extra projections for partition columns
	vector<unique_ptr<Expression>> extra_projections;

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		return SourceResultType::FINISHED;
	}
	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	string GetName() const override {
		return "DUCKLAKE_MERGE_UPDATE";
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
};

class DuckLakeMergeUpdateGlobalState : public GlobalSinkState {
public:
	unique_ptr<GlobalOperatorState> update_gstate;
	unique_ptr<GlobalOperatorState> inline_data_gstate;
};

class DuckLakeMergeUpdateLocalState : public LocalSinkState {
public:
	unique_ptr<OperatorState> update_lstate;
	unique_ptr<OperatorState> inline_data_lstate;
	unique_ptr<LocalSinkState> copy_lstate;
	DataChunk update_output;
	DataChunk inline_output;
	//! Projection and cast chunks for partition columns (same pattern as DuckLakeMergeInsert)
	unique_ptr<ExpressionExecutor> expression_executor;
	DataChunk projected_chunk;
	DataChunk cast_chunk;
};

unique_ptr<GlobalSinkState> DuckLakeMergeUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<DuckLakeMergeUpdateGlobalState>();
	result->update_gstate = update_op.GetGlobalOperatorState(context);
	if (inline_data_op) {
		result->inline_data_gstate = inline_data_op->GetGlobalOperatorState(context);
	}
	copy_op.sink_state = copy_op.GetGlobalSinkState(context);
	return std::move(result);
}

unique_ptr<LocalSinkState> DuckLakeMergeUpdate::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<DuckLakeMergeUpdateLocalState>();
	result->update_lstate = update_op.GetOperatorState(context);
	if (inline_data_op) {
		result->inline_data_lstate = inline_data_op->GetOperatorState(context);
		result->inline_output.Initialize(context.client, inline_data_op->types);
	}
	result->copy_lstate = copy_op.GetLocalSinkState(context);
	result->update_output.Initialize(context.client, update_op.types);
	if (!extra_projections.empty()) {
		result->expression_executor = make_uniq<ExpressionExecutor>(context.client, extra_projections);
		vector<LogicalType> projected_types;
		for (auto &expr : result->expression_executor->expressions) {
			projected_types.push_back(expr->return_type);
		}
		result->projected_chunk.Initialize(context.client, projected_types);
	}
	result->cast_chunk.Initialize(context.client, copy_op.Cast<PhysicalCopyToFile>().expected_types);
	return std::move(result);
}

SinkResultType DuckLakeMergeUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<DuckLakeMergeUpdateGlobalState>();
	auto &lstate = input.local_state.Cast<DuckLakeMergeUpdateLocalState>();

	lstate.update_output.Reset();
	update_op.Execute(context, chunk, lstate.update_output, *gstate.update_gstate, *lstate.update_lstate);

	if (lstate.update_output.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// if we have an inline data operator, we need to execute through it, it will either inline it or pass it
	// through to the CopyToFile sink
	if (inline_data_op) {
		auto &inline_output = lstate.inline_output;
		inline_output.Reset();
		auto result = inline_data_op->Execute(context, lstate.update_output, inline_output, *gstate.inline_data_gstate,
		                                      *lstate.inline_data_lstate);
		if (inline_output.size() > 0) {
			ProjectAndCastForCopy(context.client, inline_output, copy_op, lstate.expression_executor.get(),
			                      lstate.projected_chunk, lstate.cast_chunk);
			OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_lstate, input.interrupt_state};
			copy_op.Sink(context, lstate.cast_chunk, copy_input);
		}
		while (result == OperatorResultType::HAVE_MORE_OUTPUT) {
			inline_output.Reset();
			result = inline_data_op->Execute(context, lstate.update_output, inline_output, *gstate.inline_data_gstate,
			                                 *lstate.inline_data_lstate);
			if (inline_output.size() > 0) {
				ProjectAndCastForCopy(context.client, inline_output, copy_op, lstate.expression_executor.get(),
				                      lstate.projected_chunk, lstate.cast_chunk);
				OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_lstate, input.interrupt_state};
				copy_op.Sink(context, lstate.cast_chunk, copy_input);
			}
		}
	} else {
		ProjectAndCastForCopy(context.client, lstate.update_output, copy_op, lstate.expression_executor.get(),
		                      lstate.projected_chunk, lstate.cast_chunk);
		OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_lstate, input.interrupt_state};
		copy_op.Sink(context, lstate.cast_chunk, copy_input);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType DuckLakeMergeUpdate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<DuckLakeMergeUpdateGlobalState>();
	auto &lstate = input.local_state.Cast<DuckLakeMergeUpdateLocalState>();

	// drain inline data
	if (inline_data_op) {
		auto &inline_output = lstate.inline_output;
		while (true) {
			inline_output.Reset();
			auto fresult = inline_data_op->FinalExecute(context, inline_output, *gstate.inline_data_gstate,
			                                            *lstate.inline_data_lstate);
			if (inline_output.size() > 0) {
				ProjectAndCastForCopy(context.client, inline_output, copy_op, lstate.expression_executor.get(),
				                      lstate.projected_chunk, lstate.cast_chunk);
				OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_lstate, input.interrupt_state};
				copy_op.Sink(context, lstate.cast_chunk, copy_input);
			}
			if (fresult == OperatorFinalizeResultType::FINISHED) {
				break;
			}
		}
	}

	DataChunk dummy;
	update_op.FinalExecute(context, dummy, *gstate.update_gstate, *lstate.update_lstate);

	OperatorSinkCombineInput copy_combine {*copy_op.sink_state, *lstate.copy_lstate, input.interrupt_state};
	copy_op.Combine(context, copy_combine);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType DuckLakeMergeUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<DuckLakeMergeUpdateGlobalState>();

	OperatorFinalizeInput update_finalize {*gstate.update_gstate, input.interrupt_state};
	update_op.OperatorFinalize(pipeline, event, context, update_finalize);

	if (inline_data_op) {
		OperatorFinalizeInput inline_finalize {*gstate.inline_data_gstate, input.interrupt_state};
		inline_data_op->OperatorFinalize(pipeline, event, context, inline_finalize);
	}

	OperatorSinkFinalizeInput copy_finalize {*copy_op.sink_state, input.interrupt_state};
	copy_op.Finalize(pipeline, event, context, copy_finalize);

	FinalizeCopyToInsert(pipeline, event, context, copy_op, insert_op, input.interrupt_state);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Plan Merge Into
//===--------------------------------------------------------------------===//
static unique_ptr<MergeIntoOperator> DuckLakePlanMergeIntoAction(DuckLakeCatalog &catalog, ClientContext &context,
                                                                 LogicalMergeInto &op, PhysicalPlanGenerator &planner,
                                                                 BoundMergeIntoAction &action,
                                                                 PhysicalOperator &child_plan) {
	auto result = make_uniq<MergeIntoOperator>();

	result->action_type = action.action_type;
	result->condition = std::move(action.condition);
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	for (auto &constraint : op.bound_constraints) {
		bound_constraints.push_back(constraint->Copy());
	}
	auto return_types = op.types;

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		LogicalUpdate update(op.table);
		for (auto &def : op.bound_defaults) {
			update.bound_defaults.push_back(def->Copy());
		}
		update.bound_constraints = std::move(bound_constraints);
		update.expressions = std::move(action.expressions);
		update.columns = std::move(action.columns);
		update.update_is_del_and_insert = action.update_is_del_and_insert;

		auto &ducklake_table = op.table.Cast<DuckLakeTableEntry>();
		DuckLakeCopyInput copy_input(context, ducklake_table);
		copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID;

		auto &update_op = DuckLakeUpdate::PlanUpdateOperator(context, planner, update, child_plan, copy_input);

		// The row_id comes before the deletion information, that is always the 3 last column of the chunk.
		update_op.row_id_index = child_plan.types.size() - DuckLakeUpdate::DELETION_INFO_SIZE - 1;

		// maybe wrap with InlineData if we hit the row limit
		optional_ptr<DuckLakeInlineData> inline_data;
		idx_t data_inlining_row_limit = catalog.GetInliningLimit(context, ducklake_table, update_op.types);
		if (data_inlining_row_limit > 0) {
			auto &inline_op =
			    planner.Make<DuckLakeInlineData>(update_op, data_inlining_row_limit).Cast<DuckLakeInlineData>();
			inline_data = &inline_op;
		}

		// plan copy and insert
		auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);
		auto &copy_op = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
		auto &insert_op =
		    DuckLakeInsert::PlanInsert(context, planner, ducklake_table, std::move(copy_input.encryption_key));
		if (inline_data) {
			inline_data->insert = insert_op.Cast<DuckLakeInsert>();
		}
		insert_op.children.push_back(copy_op);

		// wrap in DuckLakeMergeUpdate
		auto &merge_update = planner.Make<DuckLakeMergeUpdate>(return_types, update_op, inline_data, copy_op, insert_op)
		                         .Cast<DuckLakeMergeUpdate>();
		merge_update.extra_projections = std::move(copy_options.projection_list);
		result->op = merge_update;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		LogicalDelete delete_op(op.table, TableIndex(0));
		delete_op.expressions.push_back(nullptr);

		vector<LogicalType> row_id_types {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::BIGINT};
		for (idx_t i = 0; i < 3; i++) {
			auto ref = make_uniq<BoundReferenceExpression>(row_id_types[i], op.row_id_start + i + 1);
			delete_op.expressions.push_back(std::move(ref));
		}
		delete_op.bound_constraints = std::move(bound_constraints);
		result->op = catalog.PlanDelete(context, planner, delete_op, child_plan);
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		LogicalInsert insert_op(op.table, TableIndex(0));
		insert_op.bound_constraints = std::move(bound_constraints);
		for (auto &def : op.bound_defaults) {
			insert_op.bound_defaults.push_back(def->Copy());
		}
		// transform expressions if required
		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &col : op.table.GetColumns().Physical()) {
				auto storage_idx = col.StorageOid();
				auto mapped_index = action.column_index_map[col.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					// push default value
					new_expressions.push_back(op.bound_defaults[storage_idx]->Copy());
				} else {
					// push reference
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);

		auto &ducklake_table = op.table.Cast<DuckLakeTableEntry>();
		DuckLakeCopyInput copy_input(context, ducklake_table);
		auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);

		auto &physical_copy = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
		auto &insert =
		    DuckLakeInsert::PlanInsert(context, planner, ducklake_table, std::move(copy_input.encryption_key));
		insert.children.push_back(physical_copy);

		auto &merge_insert =
		    planner.Make<DuckLakeMergeInsert>(insert.types, insert, physical_copy).Cast<DuckLakeMergeInsert>();
		merge_insert.extra_projections = std::move(copy_options.projection_list);
		result->op = merge_insert;
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
	return result;
}

PhysicalOperator &DuckLakeCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalMergeInto &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw NotImplementedException("RETURNING is not implemented for DuckLake yet");
	}
	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;

	// plan the merge into clauses
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
				if (update_delete_count > 1) {
					throw NotImplementedException(
					    "MERGE INTO with DuckLake only supports a single UPDATE/DELETE action currently");
				}
			}
			planned_actions.push_back(DuckLakePlanMergeIntoAction(*this, context, op, planner, *action, plan));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               true, op.return_chunk);
	result.children.push_back(plan);
	return result;
}

} // namespace duckdb
