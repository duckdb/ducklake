//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_truncate.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/ducklake_truncate.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

class DuckLakeTruncateGlobalState : public GlobalSourceState {
public:
	bool finished = false;
};

DuckLakeTruncate::DuckLakeTruncate(PhysicalPlan &physical_plan, DuckLakeTableEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::UBIGINT}, 0), table(table) {
}

unique_ptr<GlobalSourceState> DuckLakeTruncate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DuckLakeTruncateGlobalState>();
}

SourceResultType DuckLakeTruncate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<DuckLakeTruncateGlobalState>();
	if (gstate.finished) {
		return SourceResultType::FINISHED;
	}
	gstate.finished = true;

	auto &transaction = DuckLakeTransaction::Get(context.client, table.catalog);
	DuckLakeFunctionInfo read_info(table, transaction, transaction.GetSnapshot());
	auto transaction_local_files = transaction.GetTransactionLocalFiles(table.GetTableId());
	auto transaction_local_data = transaction.GetTransactionLocalInlinedData(table.GetTableId());
	DuckLakeMultiFileList file_list(read_info, std::move(transaction_local_files), transaction_local_data);

	uint64_t total_deleted_count = table.GetNetDataFileRowCount(transaction) + table.GetNetInlinedRowCount(transaction);
	auto files = file_list.GetFilesExtended();
	for (auto &file_info : files) {
		switch (file_info.data_type) {
		case DuckLakeDataType::DATA_FILE: {
			if (file_info.file_id.IsValid()) {
				transaction.DropFile(table.GetTableId(), file_info.file_id, file_info.file.path);
			} else {
				transaction.DropTransactionLocalFile(table.GetTableId(), file_info.file.path);
			}
			break;
		}
		case DuckLakeDataType::INLINED_DATA: {
			transaction.MarkInlinedDataDeleted(file_info.file.path);
			break;
		}
		case DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA: {
			transaction.TruncateLocalInlinedData(table.GetTableId());
			break;
		}
		default:
			throw InternalException("Unsupported DuckLakeDataType in truncate");
		}
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::UBIGINT(total_deleted_count));
	return SourceResultType::FINISHED;
}

string DuckLakeTruncate::GetName() const {
	return "DUCKLAKE_TRUNCATE";
}

InsertionOrderPreservingMap<string> DuckLakeTruncate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

PhysicalOperator &DuckLakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) {
	bool delete_all = false;
	if (op.children.size() == 1 && op.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.children[0]->Cast<LogicalGet>();
		delete_all = get.table_filters.filters.empty();
	}
	if (!delete_all) {
		return Catalog::PlanDelete(context, planner, op);
	}
	auto &table = op.table.Cast<DuckLakeTableEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, *this);
	if (transaction.HasAnyLocalChanges(table.GetTableId())) {
		return Catalog::PlanDelete(context, planner, op);
	}
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a DuckLake table");
	}
	return planner.Make<DuckLakeTruncate>(table);
}

} // namespace duckdb
