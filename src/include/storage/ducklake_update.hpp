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
class DuckLakeInlineData;

class DuckLakeUpdate : public PhysicalOperator {
public:
	DuckLakeUpdate(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, vector<PhysicalIndex> columns,
	               PhysicalOperator &child, PhysicalOperator &copy_op, PhysicalOperator &delete_op,
	               PhysicalOperator &insert_op, vector<unique_ptr<Expression>> &expressions);

	//! The table to update
	DuckLakeTableEntry &table;
	//! The order of to-be-inserted columns
	vector<PhysicalIndex> columns;
	//! The copy operator for writing new data to files
	PhysicalOperator &copy_op;
	//! The delete operator for deleting the old data
	PhysicalOperator &delete_op;
	//! The (final) insert operator that registers inserted data
	PhysicalOperator &insert_op;
	//! The row-id-index
	idx_t row_id_index;
	vector<unique_ptr<Expression>> expressions;
	//! The (optional) inline data operator
	optional_ptr<DuckLakeInlineData> inline_data_op;

public:
	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static constexpr uint8_t DELETION_INFO_SIZE = 3;

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

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	//! Forward output from the inline data operator to the copy sink, draining HAVE_MORE_OUTPUT as needed.
	void ForwardInlineOutputToCopy(ExecutionContext &context, DataChunk &inline_output,
	                               GlobalOperatorState &inline_gstate, OperatorState &inline_lstate, DataChunk &input,
	                               LocalSinkState &copy_lstate, InterruptState &interrupt_state) const;
};

} // namespace duckdb
