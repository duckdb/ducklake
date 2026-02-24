#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeCurrentSnapshotBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	names.emplace_back("id");
	return_types.emplace_back(LogicalType::UBIGINT);
	const auto snapshot = transaction.GetSnapshot();
	// generate the result
	auto result = make_uniq<MetadataBindData>();
	vector<Value> row_values;
	row_values.push_back(Value::UBIGINT(snapshot.snapshot_id));
	result->rows.push_back(std::move(row_values));
	return std::move(result);
}

DuckLakeCurrentSnapshotFunction::DuckLakeCurrentSnapshotFunction()
    : DuckLakeBaseMetadataFunction("ducklake_current_snapshot", DuckLakeCurrentSnapshotBind) {
}

} // namespace duckdb
