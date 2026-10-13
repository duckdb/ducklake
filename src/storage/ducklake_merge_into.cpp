#include "storage/ducklake_catalog.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_update.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//! The condition of an action restricted to rows whose row id compares to the first transaction-local one
static unique_ptr<Expression> RowIdCondition(LogicalMergeInto &op, ExpressionType comparison,
                                             const unique_ptr<Expression> &condition) {
	auto row_id_type = op.children[0]->types[op.row_id_start];
	auto result = BoundComparisonExpression::Create(
	    comparison, make_uniq<BoundReferenceExpression>(row_id_type, op.row_id_start),
	    make_uniq<BoundConstantExpression>(
	        Value::BIGINT(NumericCast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START))));
	if (!condition) {
		return result;
	}
	// the row id is compared first, so the condition runs once for every row
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(result), condition->Copy());
}

//! Split the update action into one for the rows inserted in this transaction and one for the other rows
static bool SplitUpdateByRowId(LogicalMergeInto &op) {
	for (auto &entry : op.actions) {
		auto &actions = entry.second;
		for (idx_t i = 0; i < actions.size(); i++) {
			auto &action = *actions[i];
			if (action.action_type != MergeActionType::MERGE_UPDATE) {
				continue;
			}
			auto local_rows = make_uniq<BoundMergeIntoAction>();
			local_rows->action_type = action.action_type;
			local_rows->condition = RowIdCondition(op, ExpressionType::COMPARE_GREATERTHANOREQUALTO, action.condition);
			local_rows->columns = action.columns;
			for (auto &expr : action.expressions) {
				local_rows->expressions.push_back(expr->Copy());
			}
			local_rows->column_index_map = action.column_index_map;
			local_rows->update_is_del_and_insert = action.update_is_del_and_insert;
			action.condition = RowIdCondition(op, ExpressionType::COMPARE_LESSTHAN, action.condition);
			actions.insert(actions.begin() + NumericCast<int64_t>(i), std::move(local_rows));
			return true;
		}
	}
	return false;
}

PhysicalOperator &DuckLakeCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalMergeInto &op, PhysicalOperator &plan) {
	// DuckLake writes a deletion file per data file, so it can apply at most one UPDATE/DELETE to a given row
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
			}
		}
	}
	if (update_delete_count > 1) {
		throw NotImplementedException("MERGE INTO with DuckLake only supports a single UPDATE/DELETE action currently");
	}
	// rows inserted in this transaction get their row ids at commit, so the update writes their new versions apart
	auto &table = op.table.Cast<DuckLakeTableEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, *this);
	bool split = transaction.HasTransactionLocalInserts(table.GetTableId()) && SplitUpdateByRowId(op);
	if (split) {
		context.registered_state->Insert(DuckLakeMergeUpdateSplit::KEY, make_shared_ptr<DuckLakeMergeUpdateSplit>());
	}
	optional_ptr<PhysicalMergeInto> merge_into_ptr;
	try {
		merge_into_ptr = Catalog::PlanMergeInto(context, planner, op, plan).Cast<PhysicalMergeInto>();
	} catch (...) {
		context.registered_state->Remove(DuckLakeMergeUpdateSplit::KEY);
		throw;
	}
	context.registered_state->Remove(DuckLakeMergeUpdateSplit::KEY);
	auto &merge_into = *merge_into_ptr;
	for (auto &action : merge_into.actions) {
		if (action->action_type == MergeActionType::MERGE_INSERT) {
			action->op->Cast<DuckLakeInsert>().reads_table = true;
		}
	}
	return merge_into;
}

} // namespace duckdb
