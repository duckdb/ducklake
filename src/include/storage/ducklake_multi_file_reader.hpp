//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_inlined_data.hpp"

namespace duckdb {
class DuckLakeMultiFileList;
struct DuckLakeDeleteMap;
class DuckLakeFieldData;

struct DuckLakeMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
	DuckLakeMultiFileReaderGlobalState(const MultiFileList &file_list_p, bool internally_projected_rowid_p,
	                                   optional_idx deletion_scan_rowid_col_p,
	                                   optional_idx deletion_scan_snapshot_col_p,
	                                   optional_idx deletion_scan_internal_rowid_col_p)
	    : MultiFileReaderGlobalState(internally_projected_rowid_p ? vector<LogicalType> {LogicalType::BIGINT}
	                                                              : vector<LogicalType> {},
	                                 file_list_p),
	      internally_projected_rowid(internally_projected_rowid_p), deletion_scan_rowid_col(deletion_scan_rowid_col_p),
	      deletion_scan_snapshot_col(deletion_scan_snapshot_col_p),
	      deletion_scan_internal_rowid_col(deletion_scan_internal_rowid_col_p) {
	}

	//! Whether row_id was internally projected (not in user's query)
	//! This is necessary for DCF queries over inlined deletions
	const bool internally_projected_rowid;
	//! Output positions in global_column_ids order
	const optional_idx deletion_scan_rowid_col;
	const optional_idx deletion_scan_snapshot_col;
	//! Position of the appended rowid expression
	const optional_idx deletion_scan_internal_rowid_col;
};

struct DuckLakeMultiFileReader : public MultiFileReader {
public:
	static constexpr column_t COLUMN_IDENTIFIER_SNAPSHOT_ID = UINT64_C(10000000000000000000);

public:
	explicit DuckLakeMultiFileReader(DuckLakeFunctionInfo &read_info);
	~DuckLakeMultiFileReader() override;

	DuckLakeFunctionInfo &read_info;
	shared_ptr<DuckLakeDeleteMap> delete_map;

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function);
	//! Return a DuckLakeMultiFileList
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &options) override;

	//! Override the regular parquet bind using the MultiFileReader Bind. The bind from these are what DuckDB's file
	//! readers will try read
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	//! Override the Options bind
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids) override;

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &gstate) override;

	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        const OpenFileInfo &file, idx_t file_idx,
	                                        const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                        BaseFileReaderOptions &options, const MultiFileOptions &file_options,
	                                        MultiFileReaderInterface &interface) override;

	ReaderInitializeType CreateMappingWithGlobalState(
	    ClientContext &context, MultiFileReaderData &reader_data,
	    const vector<MultiFileColumnDefinition> &global_columns, const vector<ColumnIndex> &global_column_ids,
	    optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list, const MultiFileReaderBindData &bind_data,
	    const virtual_column_map_t &virtual_columns, const DuckLakeMultiFileReaderGlobalState &global_state);

	ReaderInitializeType CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	                                   const vector<MultiFileColumnDefinition> &global_columns,
	                                   const vector<ColumnIndex> &global_column_ids,
	                                   optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
	                                   const MultiFileReaderBindData &bind_data,
	                                   const virtual_column_map_t &virtual_columns) override;

	MultiFileReaderVirtualColumnBinding
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, const idx_t column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_index) override;

	unique_ptr<MultiFileReader> Copy() const override;

	void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
	                   const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
	                   ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) override;

	static vector<MultiFileColumnDefinition> ColumnsFromFieldData(const DuckLakeFieldData &field_data,
	                                                              bool emit_key_value = false);

private:
	shared_ptr<BaseFileReader> TryCreateInlinedDataReader(const OpenFileInfo &file);
	//! Gather per-row snapshot_id values using the rowid values produced by the scan
	void GatherDeletionScanSnapshots(BaseFileReader &reader, const MultiFileReaderData &reader_data,
	                                 const Vector &rowid_vector, Vector &snapshot_vector, idx_t count) const;

private:
	unique_ptr<MultiFileColumnDefinition> row_id_column;
	unique_ptr<MultiFileColumnDefinition> snapshot_id_column;
	//! Inlined transaction-local data
	shared_ptr<DuckLakeInlinedData> transaction_local_data;
};

} // namespace duckdb
