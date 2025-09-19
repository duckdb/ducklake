#include "storage/ducklake_catalog.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "storage/ducklake_update.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_insert.hpp"
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

public:
	// // Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

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

SourceResultType DuckLakeMergeInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType DuckLakeMergeInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	OperatorSinkInput sink_input {*copy.sink_state, input.local_state, input.interrupt_state};
	return copy.Sink(context, chunk, sink_input);
}

SinkCombineResultType DuckLakeMergeInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	OperatorSinkCombineInput combine_input {*copy.sink_state, input.local_state, input.interrupt_state};
	return copy.Combine(context, combine_input);
}

SinkFinalizeType DuckLakeMergeInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	OperatorSinkFinalizeInput copy_finalize {*copy.sink_state, input.interrupt_state};
	auto finalize_result = copy.Finalize(pipeline, event, context, copy_finalize);
	if (finalize_result == SinkFinalizeType::BLOCKED) {
		return SinkFinalizeType::BLOCKED;
	}

	// now scan the copy
	DataChunk chunk;
	chunk.Initialize(context, copy.types);

	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);

	auto copy_global = copy.GetGlobalSourceState(context);
	auto copy_local = copy.GetLocalSourceState(exec_context, *copy_global);
	OperatorSourceInput source_input {*copy_global, *copy_local, input.interrupt_state};

	auto insert_global = insert.GetGlobalSinkState(context);
	auto insert_local = insert.GetLocalSinkState(exec_context);
	OperatorSinkInput sink_input {*insert_global, *insert_local, input.interrupt_state};
	SourceResultType source_res = SourceResultType::HAVE_MORE_OUTPUT;
	while (source_res == SourceResultType::HAVE_MORE_OUTPUT) {
		chunk.Reset();
		source_res = copy.GetData(exec_context, chunk, source_input);
		if (chunk.size() == 0) {
			continue;
		}
		if (source_res == SourceResultType::BLOCKED) {
			throw InternalException("BLOCKED not supported in DuckLakeMergeInsert");
		}

		auto sink_result = insert.Sink(exec_context, chunk, sink_input);
		if (sink_result != SinkResultType::NEED_MORE_INPUT) {
			throw InternalException("BLOCKED not supported in DuckLakeMergeInsert");
		}
	}
	OperatorSinkCombineInput combine_input {*insert_global, *insert_local, input.interrupt_state};
	auto combine_res = insert.Combine(exec_context, combine_input);
	if (combine_res == SinkCombineResultType::BLOCKED) {
		throw InternalException("BLOCKED not supported in DuckLakeMergeInsert");
	}
	OperatorSinkFinalizeInput finalize_input {*insert_global, input.interrupt_state};
	auto finalize_res = insert.Finalize(pipeline, event, context, finalize_input);
	if (finalize_res == SinkFinalizeType::BLOCKED) {
		throw InternalException("BLOCKED not supported in DuckLakeMergeInsert");
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> DuckLakeMergeInsert::GetGlobalSinkState(ClientContext &context) const {
	copy.sink_state = copy.GetGlobalSinkState(context);
	return make_uniq<GlobalSinkState>();
}

unique_ptr<LocalSinkState> DuckLakeMergeInsert::GetLocalSinkState(ExecutionContext &context) const {
	return copy.GetLocalSinkState(context);
}

//===--------------------------------------------------------------------===//
// Plan Merge Into
//===--------------------------------------------------------------------===//
unique_ptr<MergeIntoOperator> DuckLakePlanMergeIntoAction(DuckLakeCatalog &catalog, ClientContext &context,
                                                          LogicalMergeInto &op, PhysicalPlanGenerator &planner,
                                                          BoundMergeIntoAction &action, PhysicalOperator &child_plan) {
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
		result->op = catalog.PlanUpdate(context, planner, update, child_plan);
		auto &dl_update = result->op->Cast<DuckLakeUpdate>();
		dl_update.row_id_index = child_plan.types.size() - 1;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		LogicalDelete delete_op(op.table, 0);
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
		LogicalInsert insert_op(op.table, 0);
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
		auto &insert = catalog.PlanInsert(context, planner, insert_op, child_plan);
		auto &copy = insert.children[0].get();
		if (!RefersToSameObject(copy.children[0].get(), child_plan)) {
			auto &proj = copy.children[0].get().Cast<PhysicalProjection>();
			for (idx_t i = result->expressions.size(); i < proj.select_list.size(); i++) {
				result->expressions.push_back(std::move(proj.select_list[i]));
			}
		}
		result->op = planner.Make<DuckLakeMergeInsert>(insert.types, insert, copy);
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
