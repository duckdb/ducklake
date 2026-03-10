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

namespace duckdb {

class DuckLakeTruncateGlobalState : public GlobalSourceState {
public:
	bool finished = false;
};

DuckLakeTruncate::DuckLakeTruncate(PhysicalPlan &physical_plan, DuckLakeTableEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 0), table(table) {
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

	idx_t total_deleted_count = 0;
	auto &metadata_manager = transaction.GetMetadataManager();
	auto snapshot = transaction.GetSnapshot();
	for (auto &inlined_table : table.GetInlinedDataTables()) {
		auto inlined_count = metadata_manager.GetNetInlinedRowCount(inlined_table.table_name, snapshot);
		total_deleted_count += inlined_count;
		metadata_manager.DeleteInlinedData(inlined_table);
	}

	auto files = file_list.GetFilesExtended();
	for (auto &file_info : files) {
		if (file_info.data_type == DuckLakeDataType::INLINED_DATA) {
			// handled via metadata manager
			continue;
		}
		idx_t visible_rows = file_info.row_count;
		if (file_info.delete_count <= visible_rows) {
			visible_rows -= file_info.delete_count;
		}
		total_deleted_count += visible_rows;

		if (file_info.data_type == DuckLakeDataType::DATA_FILE) {
			if (file_info.file_id.IsValid()) {
				transaction.DropFile(table.GetTableId(), file_info.file_id, file_info.file.path);
			} else {
				transaction.DropTransactionLocalFile(table.GetTableId(), file_info.file.path);
			}
			continue;
		}

		set<idx_t> deletes;
		for (idx_t i = 0; i < file_info.row_count; i++) {
			deletes.insert(i);
		}
		if (file_info.data_type == DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA) {
			transaction.DeleteFromLocalInlinedData(table.GetTableId(), std::move(deletes));
		} else {
			transaction.AddNewInlinedDeletes(table.GetTableId(), file_info.file.path, std::move(deletes));
		}
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(total_deleted_count)));
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
	bool delete_all = !op.children.empty() && op.children[0]->type == LogicalOperatorType::LOGICAL_GET;
	if (!delete_all) {
		return Catalog::PlanDelete(context, planner, op);
	}
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a DuckLake table");
	}
	return planner.Make<DuckLakeTruncate>(op.table.Cast<DuckLakeTableEntry>());
}

} // namespace duckdb
