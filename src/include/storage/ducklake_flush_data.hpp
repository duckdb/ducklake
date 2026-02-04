//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_flush_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeTableEntry;

class DuckLakeFlushData : public PhysicalOperator {
public:
	DuckLakeFlushData(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeTableEntry &table,
	                  DuckLakeInlinedTableInfo inlined_table, string encryption_key, optional_idx partition_id,
	                  PhysicalOperator &child);

	DuckLakeTableEntry &table;
	DuckLakeInlinedTableInfo inlined_table;
	string encryption_key;
	optional_idx partition_id;

public:
	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
};

} // namespace duckdb
