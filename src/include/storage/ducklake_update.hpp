//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_insert.hpp"

namespace duckdb {

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

	static DuckLakeUpdate &PlanUpdateOperator(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                                          PhysicalOperator &child_plan, DuckLakeCopyInput &copy_input);
};

} // namespace duckdb
