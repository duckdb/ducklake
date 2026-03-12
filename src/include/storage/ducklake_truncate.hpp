//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_truncate.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

class DuckLakeTruncate : public PhysicalOperator {
public:
	DuckLakeTruncate(PhysicalPlan &physical_plan, DuckLakeTableEntry &table);

	DuckLakeTableEntry &table;

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
