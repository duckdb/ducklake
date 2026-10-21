#include "storage/ducklake_inlined_data_reader.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

DuckLakeInlinedDataReader::DuckLakeInlinedDataReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info,
                                                     string table_name_p, vector<MultiFileColumnDefinition> columns_p)
    : BaseFileReader(info), read_info(read_info), table_name(std::move(table_name_p)) {
	columns = std::move(columns_p);
}

DuckLakeInlinedDataReader::DuckLakeInlinedDataReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info,
                                                     shared_ptr<DuckLakeInlinedData> data_p,
                                                     vector<MultiFileColumnDefinition> columns_p)
    : BaseFileReader(info), read_info(read_info), data(std::move(data_p)) {
	columns = std::move(columns_p);
}

void DuckLakeInlinedDataReader::SetCommittedRows(shared_ptr<DuckLakeCommittedInlinedRows> rows) {
	committed_rows = std::move(rows);
}

void DuckLakeInlinedDataReader::GetColumnToRead(const DuckLakeFunctionInfo &read_info,
                                                DuckLakeMetadataManager &metadata_manager,
                                                const DuckLakeInlinedColNames &col_names,
                                                const MultiFileColumnDefinition &col, string &column,
                                                LogicalType &type) {
	if (!col.identifier.IsNull() && col.identifier.type().id() == LogicalTypeId::INTEGER) {
		string virtual_column;
		switch (IntegerValue::Get(col.identifier)) {
		case MultiFileReader::ORDINAL_FIELD_ID:
		case MultiFileReader::ROW_ID_FIELD_ID:
			virtual_column = col_names.row_id;
			break;
		case MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID:
			if (read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS) {
				// when scanning deletions end_snapshot is the snapshot marker
				virtual_column = col_names.end_snapshot;
			} else {
				virtual_column = col_names.begin_snapshot;
			}
			break;
		default:
			break;
		}
		if (!virtual_column.empty()) {
			column = SQLIdentifier::ToString(virtual_column);
			type = LogicalType::BIGINT;
			return;
		}
	}
	column = SQLIdentifier::ToString(col.name.GetIdentifierName());
	if (read_info.scan_type != DuckLakeScanType::SCAN_FOR_FLUSH) {
		// the flush source already casts its columns
		column = metadata_manager.CastColumnToTarget(column, col.type);
	}
	type = col.type;
}

shared_ptr<DuckLakeCommittedInlinedRows>
DuckLakeInlinedDataReader::ReadCommittedRows(DuckLakeFunctionInfo &read_info, const string &table_name,
                                             const vector<MultiFileColumnDefinition> &columns,
                                             optional_idx &next_file_id) {
	auto transaction = read_info.GetTransaction();
	auto &metadata_manager = transaction->GetMetadataManager();
	auto col_names = metadata_manager.InlinedColNames();
	auto result = make_shared_ptr<DuckLakeCommittedInlinedRows>();
	vector<LogicalType> types;
	for (auto &col : columns) {
		string column;
		LogicalType type;
		GetColumnToRead(read_info, metadata_manager, col_names, col, column, type);
		result->columns.push_back(std::move(column));
		types.push_back(std::move(type));
	}
	result->columns.push_back("(SELECT MAX(next_file_id) FROM {METADATA_CATALOG}.ducklake_snapshot)");
	types.push_back(LogicalType::BIGINT);
	auto query_result = metadata_manager.ReadInlinedData(read_info.snapshot, table_name, result->columns);
	result->rows = metadata_manager.TransformInlinedData(*query_result, types, table_name);
	auto &rows = *result->rows->data;
	if (rows.Count() > 0) {
		auto file_id = rows.GetValue(types.size() - 1, 0).GetValue<idx_t>();
		if (!next_file_id.IsValid() || file_id < next_file_id.GetIndex()) {
			next_file_id = file_id;
		}
	}
	return result;
}

//! Select the columns from rows read before, or return nullptr when one of them was not read
static shared_ptr<DuckLakeInlinedData> ProjectCommittedRows(ClientContext &context,
                                                            const DuckLakeCommittedInlinedRows &committed_rows,
                                                            const vector<string> &columns_to_read) {
	auto &source = *committed_rows.rows->data;
	vector<column_t> column_ids;
	vector<LogicalType> types;
	for (auto &column : columns_to_read) {
		auto entry = std::find(committed_rows.columns.begin(), committed_rows.columns.end(), column);
		if (entry == committed_rows.columns.end()) {
			return nullptr;
		}
		column_ids.push_back(NumericCast<column_t>(entry - committed_rows.columns.begin()));
		types.push_back(source.Types()[column_ids.back()]);
	}
	auto result = make_shared_ptr<DuckLakeInlinedData>();
	result->data = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataScanState scan_state;
	source.InitializeScan(scan_state, column_ids);
	DataChunk chunk;
	chunk.Initialize(context, types);
	while (source.Scan(scan_state, chunk)) {
		result->data->Append(chunk);
	}
	return result;
}

bool DuckLakeInlinedDataReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
                                                  LocalTableFunctionState &lstate) {
	{
		// check if we are the reader responsible for scanning
		lock_guard<mutex> guard(lock);
		if (initialized_scan) {
			return false;
		}
		initialized_scan = true;
	}
	if (!data) {
		// scanning data from a table - read it from the metadata catalog
		auto transaction = read_info.GetTransaction();
		auto &metadata_manager = transaction->GetMetadataManager();
		auto col_names = metadata_manager.InlinedColNames();
		// push the projections directly into the read
		vector<string> columns_to_read;
		vector<LogicalType> expected_types;
		for (auto &column_id : column_indexes) {
			string column;
			LogicalType type;
			GetColumnToRead(read_info, metadata_manager, col_names, columns[column_id.GetPrimaryIndex()], column, type);
			columns_to_read.push_back(std::move(column));
			expected_types.push_back(std::move(type));
		}
		if (deletion_filter) {
			// we have a deletion filter - the deletions are on row-ids, not on ordinals
			// we need to transform from row-ids to ordinals by scanning the ACTUAL row-ids and doing the mapping
			// set-up the scan to emit the row-id column, but to ignore it in the final result
			for (idx_t i = 0; i < columns_to_read.size(); i++) {
				scan_column_ids.push_back(i);
				virtual_columns.push_back(InlinedVirtualColumn::NONE);
			}
			columns_to_read.push_back(SQLIdentifier::ToString(col_names.row_id));
			expected_types.push_back(LogicalType::BIGINT);
			virtual_columns.emplace_back(InlinedVirtualColumn::COLUMN_EMPTY);
		}
		if (columns_to_read.empty()) {
			// COUNT(*) - read row_id but don't emit
			columns_to_read.push_back(SQLIdentifier::ToString(col_names.row_id));
			expected_types.push_back(LogicalType::BIGINT);
			virtual_columns.emplace_back(InlinedVirtualColumn::COLUMN_EMPTY);
		}
		if (!expression_map.empty() && virtual_columns.empty()) {
			for (idx_t i = 0; i < columns_to_read.size(); i++) {
				scan_column_ids.push_back(i);
				virtual_columns.push_back(InlinedVirtualColumn::NONE);
			}
		}
		if (committed_rows) {
			data = ProjectCommittedRows(context, *committed_rows, columns_to_read);
		}
		if (!data) {
			unique_ptr<QueryResult> query_result;
			switch (read_info.scan_type) {
			case DuckLakeScanType::SCAN_TABLE:
				query_result = metadata_manager.ReadInlinedData(read_info.snapshot, table_name, columns_to_read);
				break;
			case DuckLakeScanType::SCAN_INSERTIONS:
				query_result = metadata_manager.ReadInlinedDataInsertions(*read_info.start_snapshot, read_info.snapshot,
				                                                          table_name, columns_to_read);
				break;
			case DuckLakeScanType::SCAN_DELETIONS:
				query_result = metadata_manager.ReadInlinedDataDeletions(*read_info.start_snapshot, read_info.snapshot,
				                                                         table_name, columns_to_read);
				break;
			case DuckLakeScanType::SCAN_FOR_FLUSH:
				query_result = metadata_manager.ReadAllInlinedDataForFlush(
				    read_info.snapshot, table_name, read_info.table, read_info.flush_sort_order_sql, columns_to_read);
				break;
			default:
				throw InternalException("Unknown DuckLake scan type");
			}
			data = metadata_manager.TransformInlinedData(*query_result, expected_types, table_name);
		}
		if (!virtual_columns.empty()) {
			auto scan_types = data->data->Types();
			scan_chunk.Initialize(context, scan_types);
		}
		if (deletion_filter) {
			// map the deleted row-ids to the deleted ordinals to obtain the correct deleted rows
			auto &filter = static_cast<DuckLakeDeleteFilter &>(*deletion_filter);
			vector<idx_t> deleted_ordinals;
			auto &deleted_row_ids = filter.delete_data->deleted_rows;
			idx_t current_idx = 0;
			idx_t ordinal_position = 0;
			for (auto &chunk : data->data->Chunks()) {
				auto &row_id_vector = chunk.data.back();
				auto row_id_data = FlatVector::GetData<int64_t>(row_id_vector);
				for (idx_t r = 0; r < chunk.size(); r++) {
					auto row_id = NumericCast<idx_t>(row_id_data[r]);
					if (current_idx < deleted_row_ids.size() && deleted_row_ids[current_idx] == row_id) {
						deleted_ordinals.push_back(ordinal_position);
						current_idx++;
					}
					ordinal_position++;
				}
			}
			filter.delete_data->deleted_rows = std::move(deleted_ordinals);
		}
		data->data->InitializeScan(state);
	} else {
		// scanning from transaction-local data - we already have the data
		// push the projections into the scan
		vector<LogicalType> scan_types;
		auto &types = data->data->Types();
		for (idx_t i = 0; i < column_indexes.size(); ++i) {
			auto &column_id = column_indexes[i];
			auto col_id = column_id.GetPrimaryIndex();
			if (col_id >= types.size()) {
				auto &identifier = columns[col_id].identifier;
				auto is_row_id =
				    !identifier.IsNull() && IntegerValue::Get(identifier) == MultiFileReader::ROW_ID_FIELD_ID;
				virtual_columns.emplace_back(is_row_id ? InlinedVirtualColumn::COLUMN_ROW_ID
				                                       : InlinedVirtualColumn::COLUMN_FILE_ROW_NUMBER);
				continue;
			}
			scan_types.push_back(types[col_id]);
			scan_column_ids.push_back(col_id);
			virtual_columns.emplace_back(InlinedVirtualColumn::NONE);
		}
		if (!scan_types.empty() || !virtual_columns.empty()) {
			// add an extra column to scan to determine the cardinality
			// this is needed even when all columns are virtual (e.g., only rowid)
			scan_types.push_back(types[0]);
			scan_column_ids.push_back(0);
		}
		scan_chunk.Initialize(context, scan_types);
		data->data->InitializeScan(state, scan_column_ids);
	}
	for (auto &entry : expression_map) {
		auto &column_index = column_indexes[entry.first];
		auto column_id = column_index.GetPrimaryIndex();
		auto &expression_data = entry.second;
		auto &expression = *expression_data.expression;
		expression_executors[column_id] = make_uniq<ExpressionExecutor>(context, expression);
	}
	return true;
}

bool DuckLakeInlinedDataReader::TryEvaluateExpression(ClientContext &context, idx_t virtual_col_idx,
                                                      Vector &input_vector, const LogicalType &input_type,
                                                      Vector &output_vector) {
	if (expression_map.empty() || virtual_col_idx >= column_ids.size()) {
		return false;
	}
	auto local_id = column_ids[MultiFileLocalIndex(virtual_col_idx)];
	auto expr_it = expression_executors.find(local_id);
	if (expr_it == expression_executors.end()) {
		return false;
	}
	DataChunk expr_input;
	expr_input.Initialize(Allocator::Get(context), {input_type});
	expr_input.Reset();
	expr_input.data[0].Reference(input_vector);
	expr_input.SetChildCardinality(scan_chunk.size());
	expr_it->second->ExecuteExpression(expr_input, output_vector);
	return true;
}

AsyncResult DuckLakeInlinedDataReader::Scan(ClientContext &context, GlobalTableFunctionState &global_state,
                                            LocalTableFunctionState &local_state, DataChunk &chunk) {
	if (!virtual_columns.empty()) {
		scan_chunk.Reset();
		data->data->Scan(state, scan_chunk);
		idx_t source_idx = 0;
		for (idx_t c = 0; c < virtual_columns.size(); c++) {
			switch (virtual_columns[c]) {
			case InlinedVirtualColumn::NONE: {
				auto column_id = source_idx++;
				if (TryEvaluateExpression(context, c, scan_chunk.data[column_id], scan_chunk.data[column_id].GetType(),
				                          chunk.data[c])) {
					break;
				}
				if (chunk.data[c].GetType() != scan_chunk.data[column_id].GetType()) {
					// type was changed, we gotta cast the data
					VectorOperations::Cast(context, scan_chunk.data[column_id], chunk.data[c], scan_chunk.size());
				} else {
					chunk.data[c].Reference(scan_chunk.data[column_id]);
				}
				break;
			}
			case InlinedVirtualColumn::COLUMN_ROW_ID:
			case InlinedVirtualColumn::COLUMN_FILE_ROW_NUMBER: {
				Vector ordinal_vector(LogicalType::BIGINT);
				if (virtual_columns[c] == InlinedVirtualColumn::COLUMN_ROW_ID) {
					// use preserved row_ids from update inlining
					auto ordinal_data = FlatVector::GetDataMutable<int64_t>(ordinal_vector);
					for (idx_t r = 0; r < scan_chunk.size(); r++) {
						ordinal_data[r] = data->row_ids[file_row_number + r];
					}
					FlatVector::SetSize(ordinal_vector, scan_chunk.size());
				} else {
					VectorOperations::GenerateSequence(ordinal_vector, scan_chunk.size(), file_row_number);
				}
				if (TryEvaluateExpression(context, c, ordinal_vector, LogicalType::BIGINT, chunk.data[c])) {
					continue;
				}
				VectorOperations::Copy(ordinal_vector, chunk.data[c], scan_chunk.size(), 0, 0);
				continue;
			}
			case InlinedVirtualColumn::COLUMN_EMPTY:
				break;
			}
		}
		chunk.SetChildCardinality(scan_chunk.size());
	} else {
		data->data->Scan(state, chunk);
	}
	idx_t scan_count = chunk.size();
	if (scan_count == 0) {
		return AsyncResult(SourceResultType::FINISHED);
	}
	if (filters || deletion_filter) {
		SelectionVector sel;
		idx_t approved_tuple_count = scan_count;
		if (deletion_filter) {
			approved_tuple_count = deletion_filter->Filter(file_row_number, approved_tuple_count, sel);
		}
		if (filters) {
			for (auto &entry : *filters) {
				auto &filter = entry.Filter();
				if (ExpressionFilter::IsRootOptionalFilter(filter)) {
					continue;
				}
				auto column_id = entry.GetIndex().GetIndex();
				auto filter_state = TableFilterState::Initialize(context, filter);
				approved_tuple_count = ColumnSegment::FilterSelection(sel, chunk.data[column_id], *filter_state,
				                                                      chunk.size(), approved_tuple_count);
			}
		}
		if (approved_tuple_count != chunk.size()) {
			chunk.Slice(sel, approved_tuple_count);
		}
	}
	file_row_number += NumericCast<int64_t>(scan_count);
	return AsyncResult(SourceResultType::HAVE_MORE_OUTPUT);
}

void DuckLakeInlinedDataReader::AddVirtualColumn(column_t virtual_column_id) {
	if (virtual_column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		columns.back().identifier = Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID);
	} else {
		throw InternalException("Unsupported virtual column id %d for inlined data reader", virtual_column_id);
	}
}

// only reached by pdet/ducklake_internal#112
// LCOV_EXCL_START
string DuckLakeInlinedDataReader::GetReaderType() const {
	return "DuckLake Inlined Data";
}
// LCOV_EXCL_STOP

} // namespace duckdb
