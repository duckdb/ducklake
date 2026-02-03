#include "storage/ducklake_inlined_data_reader.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_delete_filter.hpp"

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
	if (!expression_map.empty()) {
		throw InternalException("FIXME: support expression_map");
	}

	if (!data) {
		// scanning data from a table - read it from the metadata catalog
		auto transaction = read_info.GetTransaction();
		auto &metadata_manager = transaction->GetMetadataManager();
		auto &ducklake_catalog = transaction->GetCatalog();
		// push the projections directly into the read
		vector<string> columns_to_read;
		vector<LogicalType> expected_types;
		for (auto &column_id : column_indexes) {
			auto index = column_id.GetPrimaryIndex();
			auto &col = columns[index];
			if (!col.identifier.IsNull() && col.identifier.type().id() == LogicalTypeId::INTEGER) {
				auto identifier = IntegerValue::Get(col.identifier);
				string virtual_column;
				switch (identifier) {
				case MultiFileReader::ORDINAL_FIELD_ID:
				case MultiFileReader::ROW_ID_FIELD_ID:
					virtual_column = "row_id";
					break;
				case MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID:
					if (read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS) {
						// when scanning deletions end_snapshot is the snapshot marker
						virtual_column = "end_snapshot";
					} else {
						virtual_column = "begin_snapshot";
					}
					break;
				default:
					break;
				}
				if (!virtual_column.empty()) {
					columns_to_read.push_back(KeywordHelper::WriteOptionallyQuoted(virtual_column));
					expected_types.push_back(LogicalType::BIGINT);
					continue;
				}
			}
			string projected_column = KeywordHelper::WriteOptionallyQuoted(columns[index].name);
			if (!ducklake_catalog.IsDuckCatalog()) {
				// If it's not a duckdb catalog, we add a cast.
				projected_column = metadata_manager.CastColumnToTarget(projected_column, columns[index].type);
			}
			columns_to_read.push_back(projected_column);
			expected_types.push_back(col.type);
		}
		if (deletion_filter) {
			// we have a deletion filter - the deletions are on row-ids, not on ordinals
			// we need to transform from row-ids to ordinals by scanning the ACTUAL row-ids and doing the mapping
			// set-up the scan to emit the row-id column, but to ignore it in the final result
			for (idx_t i = 0; i < columns_to_read.size(); i++) {
				scan_column_ids.push_back(i);
				virtual_columns.push_back(InlinedVirtualColumn::NONE);
			}
			columns_to_read.push_back(KeywordHelper::WriteOptionallyQuoted("row_id"));
			expected_types.push_back(LogicalType::BIGINT);
			virtual_columns.emplace_back(InlinedVirtualColumn::COLUMN_EMPTY);
		}
		if (columns_to_read.empty()) {
			// COUNT(*) - read row_id but don't emit
			columns_to_read.push_back(KeywordHelper::WriteOptionallyQuoted("row_id"));
			expected_types.push_back(LogicalType::BIGINT);
			virtual_columns.emplace_back(InlinedVirtualColumn::COLUMN_EMPTY);
		}
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
			query_result = metadata_manager.ReadAllInlinedDataForFlush(read_info.snapshot, table_name, columns_to_read);
			break;
		default:
			throw InternalException("Unknown DuckLake scan type");
		}
		data = metadata_manager.TransformInlinedData(*query_result, expected_types);
		if (!virtual_columns.empty()) {
			auto scan_types = data->data->Types();
			scan_chunk.Initialize(context, scan_types);
		}
		if (deletion_filter) {
			// map the deleted row-ids to the deleted ordinals to obtain the correct deleted rows
			auto &filter = reinterpret_cast<DuckLakeDeleteFilter &>(*deletion_filter);
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
				virtual_columns.emplace_back(InlinedVirtualColumn::COLUMN_ROW_ID);
				continue;
			}
			scan_types.push_back(types[col_id]);
			scan_column_ids.push_back(col_id);
			virtual_columns.emplace_back(InlinedVirtualColumn::NONE);
		}
		if (!scan_types.empty()) {
			scan_types.push_back(types[0]);
			scan_column_ids.push_back(0);
		}
		scan_chunk.Initialize(context, scan_types);

		data->data->InitializeScan(state, scan_column_ids);
	}
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
				chunk.data[c].Reference(scan_chunk.data[column_id]);
				break;
			}
			case InlinedVirtualColumn::COLUMN_ROW_ID: {
				auto row_id_data = FlatVector::GetData<int64_t>(chunk.data[c]);
				for (idx_t r = 0; r < scan_chunk.size(); r++) {
					row_id_data[r] = NumericCast<int64_t>(file_row_number + r);
				}
				continue;
			}
			case InlinedVirtualColumn::COLUMN_EMPTY:
				break;
			}
		}
		chunk.SetCardinality(scan_chunk.size());
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
			for (auto &entry : filters->filters) {
				if (entry.second->filter_type == TableFilterType::OPTIONAL_FILTER) {
					continue;
				}
				auto column_id = entry.first;
				auto &vec = chunk.data[column_id];

				UnifiedVectorFormat vdata;
				vec.ToUnifiedFormat(chunk.size(), vdata);

				auto &filter = *entry.second;
				auto filter_state = TableFilterState::Initialize(context, filter);

				approved_tuple_count = ColumnSegment::FilterSelection(sel, vec, vdata, filter, *filter_state,
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

string DuckLakeInlinedDataReader::GetReaderType() const {
	return "DuckLake Inlined Data";
}

} // namespace duckdb
