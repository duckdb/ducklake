//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_insert.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

//! The delete that the update operators of one MERGE share, set up by the first and finalized by the last
struct DuckLakeSharedDelete {
	explicit DuckLakeSharedDelete(idx_t operator_count) : operator_count(operator_count) {
	}

	mutex lock;
	idx_t operator_count;
	//! The query the delete state is set up for
	idx_t query = DConstants::INVALID_INDEX;
	idx_t finalized_count = 0;
};

//! Set while DuckLake plans a MERGE whose update action is split by row id
class DuckLakeMergeUpdateSplit : public ClientContextState {
public:
	static constexpr const char *KEY = "ducklake_merge_update_split";

	//! The delete of the first half, which the second half shares
	optional_ptr<PhysicalOperator> delete_op;
	shared_ptr<DuckLakeSharedDelete> shared_delete = make_shared_ptr<DuckLakeSharedDelete>(2);
};

class DuckLakeUpdate : public PhysicalOperator {
public:
	DuckLakeUpdate(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, vector<PhysicalIndex> columns,
	               PhysicalOperator &child, PhysicalOperator &delete_op, vector<unique_ptr<Expression>> &expressions);

	//! The table to update
	DuckLakeTableEntry &table;
	//! The order of to-be-inserted columns
	vector<PhysicalIndex> columns;
	//! The delete operator for deleting the old data
	PhysicalOperator &delete_op;
	//! The row-id-index
	idx_t row_id_index;
	vector<unique_ptr<Expression>> expressions;
	//! Set when the delete operator is shared with other update operators
	shared_ptr<DuckLakeSharedDelete> shared_delete;

	static constexpr uint8_t DELETION_INFO_SIZE = 3;

public:
	// Operator interface
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const override;
	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;

	bool ParallelOperator() const override {
		return true;
	}

	bool RequiresFinalExecute() const override {
		return true;
	}

	bool RequiresOperatorFinalize() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	//! Compares the row id at the index to the first row id of the rows inserted in the transaction
	static unique_ptr<Expression> CompareToLocalRowIds(ExpressionType comparison, const LogicalType &row_id_type,
	                                                   idx_t row_id_index);
	static DuckLakeUpdate &PlanUpdateOperator(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                                          PhysicalOperator &child_plan, DuckLakeCopyInput &copy_input,
	                                          optional_ptr<PhysicalOperator> delete_op = nullptr);
};

} // namespace duckdb
