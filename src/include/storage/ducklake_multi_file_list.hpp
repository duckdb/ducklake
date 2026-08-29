//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_multi_file_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

//! The DuckLakeMultiFileList implements the MultiFileList API to allow injecting it into the regular DuckDB parquet
//! scan
class DuckLakeMultiFileList : public MultiFileList {
	static constexpr const char *DUCKLAKE_TRANSACTION_LOCAL_INLINED_FILENAME =
	    "__ducklake_inlined_transaction_local_data";

public:
	DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info, vector<DuckLakeDataFile> transaction_local_files,
	                      shared_ptr<DuckLakeInlinedData> transaction_local_data,
	                      unique_ptr<FilterPushdownInfo> filter_info = nullptr);
	DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info, vector<DuckLakeFileListEntry> files_to_scan);
	DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info, const DuckLakeInlinedTableInfo &inlined_table);

	unique_ptr<MultiFileList> DynamicFilterPushdown(MultiFileDynamicPushdownInfo &pushdown_info) const override;

	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;

	vector<OpenFileInfo> GetAllFiles() const override;
	FileExpandResult GetExpandResult() const override;
	idx_t GetTotalFileCount() const override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;
	DuckLakeTableEntry &GetTable();
	unique_ptr<MultiFileList> Copy() const override;
	vector<DuckLakeFileListExtendedEntry> GetFilesExtended() const;
	const vector<DuckLakeFileListEntry> &GetFiles() const;
	const DuckLakeFileListEntry &GetFileEntry(idx_t file_idx) const;
	optional_ptr<const FilterPushdownInfo> GetFilterInfo() const {
		return filter_info.get();
	}

	bool CanUseGlobalStats() const;
	bool IsDeleteScan() const;
	const DuckLakeDeleteScanEntry &GetDeleteScanEntry(idx_t file_idx);

protected:
	//! Get the i-th expanded file
	OpenFileInfo GetFile(idx_t i) const override;

private:
	void GetFilesForTable() const;
	void GetTableInsertions() const;
	void GetTableDeletions() const;
	void AddFilterToPushdownInfo(FilterPushdownInfo &pushdown_info, column_t column_id,
	                             unique_ptr<TableFilter> filter) const;
	//! Build the node for a table filter on one column - a leaf, or a conjunction of them when it
	//! constrains several nested fields. nullptr for virtual columns and subjects that have no stats.
	unique_ptr<DuckLakeFilterNode> GetFilterNode(column_t column_id, unique_ptr<TableFilter> filter) const;
	//! Build the node for a filter expression reading one table column, possibly several of its fields
	unique_ptr<DuckLakeFilterNode> GetColumnFilterNode(column_t column_id, const Expression &expr,
	                                                   const LogicalType &column_type) const;
	//! Record the leaves of a node as single-column filters
	void AddFilterNodeToPushdownInfo(FilterPushdownInfo &pushdown_info, DuckLakeFilterNode &node) const;
	//! Build a leaf node directly from an expression that constrains a single column
	unique_ptr<DuckLakeFilterNode> GetExpressionFilterNode(MultiFilePushdownInfo &info, const Expression &expr) const;
	//! Resolve the field a filter subject reads, descending into nested fields
	optional_ptr<const DuckLakeFieldId> ResolveFilterField(const Expression &subject, column_t column_id) const;
	//! Reduce an expression to per-column filters using the FilterCombiner
	unique_ptr<DuckLakeFilterNode> CombineFilterNode(ClientContext &context, MultiFilePushdownInfo &info,
	                                                 const Expression &expr) const;
	//! Budgets for building one filter tree, and what the walk found that per-column filters cannot express
	struct FilterTreeState {
		//! Bounds how much of the expression we walk - every step runs a FilterCombiner over it
		static constexpr idx_t MAX_VISITED_NODES = 64;
		//! Bounds what the generated query costs: one stats condition per leaf, all nested in one OR/AND
		//! chain. Separate from the walk, since one visit can yield a leaf per column. The leaves share
		//! the column's stats join, but planning the chain still grows superlinearly in its size.
		static constexpr idx_t MAX_LEAVES = 64;

		idx_t visited_nodes = 0;
		idx_t leaves = 0;
		//! Set when a budget runs out - the tree is then dropped rather than kept half-built
		bool exhausted = false;
		//! Set when a branch matching nothing was dropped from a disjunction, which only a tree expresses
		bool removed_branch = false;

		bool VisitNode();
		bool AddLeaves(const DuckLakeFilterNode &node);
	};
	//! Build a filter tree, or nullptr if nothing in the expression can be evaluated against column stats.
	//! A tree is not a promise that it prunes - that is decided when the SQL is generated.
	unique_ptr<DuckLakeFilterNode> BuildFilterTree(ClientContext &context, MultiFilePushdownInfo &info,
	                                               const Expression &expr, FilterTreeState &state) const;
	//! Get the row_id_start for transaction-local inlined data.
	idx_t GetTransactionLocalRowIdStart(idx_t transaction_row_start) const;

private:
	mutable mutex file_lock;
	DuckLakeFunctionInfo &read_info;
	//! The set of files to read
	mutable vector<DuckLakeFileListEntry> files;
	mutable bool read_file_list;
	//! The set of transaction-local files
	vector<DuckLakeDataFile> transaction_local_files;
	//! Inlined transaction-local data
	shared_ptr<DuckLakeInlinedData> transaction_local_data;
	//! Inlined data tables
	mutable vector<DuckLakeInlinedTableInfo> inlined_data_tables;
	//! The set of delete scans, only used when scanning deleted tuples using ducklake_table_deletions
	mutable vector<DuckLakeDeleteScanEntry> delete_scans;
	//! Filter pushdown information
	unique_ptr<FilterPushdownInfo> filter_info;
};

} // namespace duckdb
