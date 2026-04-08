//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/ducklake_compaction_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "storage/ducklake_compaction.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {
//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class DuckLakeLogicalCompaction : public LogicalExtensionOperator {
public:
	DuckLakeLogicalCompaction(idx_t table_index, DuckLakeTableEntry &table,
	                          vector<DuckLakeCompactionFileEntry> source_files_p, string encryption_key_p,
	                          optional_idx partition_id, vector<string> partition_values_p, optional_idx row_id_start,
	                          CompactionType type)
	    : table_index(table_index), table(table), source_files(std::move(source_files_p)),
	      encryption_key(std::move(encryption_key_p)), partition_id(partition_id),
	      partition_values(std::move(partition_values_p)), row_id_start(row_id_start), type(type) {
	}

	idx_t table_index;
	DuckLakeTableEntry &table;
	vector<DuckLakeCompactionFileEntry> source_files;
	string encryption_key;
	optional_idx partition_id;
	vector<string> partition_values;
	optional_idx row_id_start;
	CompactionType type;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		return planner.Make<DuckLakeCompaction>(types, table, std::move(source_files), std::move(encryption_key),
		                                        partition_id, std::move(partition_values), row_id_start, child, type);
	}

	string GetName() const override {
		return "DUCKLAKE_COMPACTION";
	}

	string GetExtensionName() const override {
		return "ducklake";
	}
	vector<ColumnBinding> GetColumnBindings() override {
		vector<ColumnBinding> result;
		result.emplace_back(table_index, 0);
		result.emplace_back(table_index, 1);
		result.emplace_back(table_index, 2);
		result.emplace_back(table_index, 3);
		return result;
	}

	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
	}
};

//===--------------------------------------------------------------------===//
// Compaction Command Generator
//===--------------------------------------------------------------------===//
class DuckLakeCompactor {
public:
	DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
	                  Binder &binder, TableIndex table_id, DuckLakeMergeAdjacentOptions options);
	DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
	                  Binder &binder, TableIndex table_id, double delete_threshold);
	void GenerateCompactions(DuckLakeTableEntry &table, vector<unique_ptr<LogicalOperator>> &compactions);
	unique_ptr<LogicalOperator> GenerateCompactionCommand(vector<DuckLakeCompactionFileEntry> source_files);
	static unique_ptr<LogicalOperator> InsertSort(Binder &binder, unique_ptr<LogicalOperator> &plan,
	                                              DuckLakeTableEntry &table, optional_ptr<DuckLakeSort> sort_data);
	static vector<OrderByNode> ParseSortOrders(const DuckLakeSort &sort_data);
	static vector<BoundOrderByNode> BindSortOrders(Binder &binder, DuckLakeTableEntry &table, idx_t table_index,
	                                               vector<OrderByNode> &pre_bound_orders);

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeTransaction &transaction;
	Binder &binder;
	TableIndex table_id;
	double delete_threshold = 0.95;
	DuckLakeMergeAdjacentOptions options;

	CompactionType type;
};

} // namespace duckdb
