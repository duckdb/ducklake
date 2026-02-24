#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "storage/ducklake_catalog.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeLastCommittedSnapshotBind(ClientContext &context, TableFunctionBindInput &input,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	auto &duck_catalog = transaction.GetCatalog();
	names.emplace_back("id");
	return_types.emplace_back(LogicalType::UBIGINT);

	// generate the result
	auto result = make_uniq<MetadataBindData>();
	vector<Value> row_values;
	row_values.push_back(duck_catalog.GetLastCommittedSnapshotId());
	result->rows.push_back(std::move(row_values));
	return std::move(result);
}

DuckLakeLastCommittedSnapshotFunction::DuckLakeLastCommittedSnapshotFunction()
    : DuckLakeBaseMetadataFunction("ducklake_last_committed_snapshot", DuckLakeLastCommittedSnapshotBind) {
}

} // namespace duckdb
