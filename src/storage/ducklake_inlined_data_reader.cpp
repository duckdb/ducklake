#include "storage/ducklake_inlined_data_reader.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"

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
	if (!data) {
		// scanning data from a table - read it from the metadata catalog
		auto transaction = read_info.GetTransaction();
		auto &metadata_manager = transaction->GetMetadataManager();
		auto col_names = metadata_manager.InlinedColNames();
		// push the projections directly into the read
		for (auto &column_id : column_indexes) {
			auto index = column_id.GetPrimaryIndex();
			auto &col = columns[index];
			if (!col.identifier.IsNull() && col.identifier.type().id() == LogicalTypeId::INTEGER) {
				auto identifier = IntegerValue::Get(col.identifier);
				string virtual_column;
				switch (identifier) {
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
					columns_to_read.push_back(SQLIdentifier::ToString(virtual_column));
					expected_types.push_back(LogicalType::BIGINT);
					continue;
				}
			}
			auto column_name = SQLIdentifier::ToString(columns[index].name.GetIdentifierName());
			if (read_info.scan_type != DuckLakeScanType::SCAN_FOR_FLUSH) {
				// the flush source already casts its columns
				column_name = metadata_manager.CastColumnToTarget(column_name, col.type);
			}
			columns_to_read.push_back(column_name);
			expected_types.push_back(col.type);
		}
		// every projected column is copied from the page, the row id and begin snapshot that order the pages follow
		for (idx_t i = 0; i < columns_to_read.size(); i++) {
			scan_column_ids.push_back(i);
			virtual_columns.push_back(InlinedVirtualColumn::NONE);
		}
		for (auto &key_column : {col_names.row_id, col_names.begin_snapshot}) {
			columns_to_read.push_back(SQLIdentifier::ToString(key_column));
			expected_types.push_back(LogicalType::BIGINT);
			virtual_columns.emplace_back(InlinedVirtualColumn::COLUMN_EMPTY);
		}
		if (deletion_filter) {
			// the deletions are on row ids, the pages map them to ordinals as they are read
			auto &filter = static_cast<DuckLakeDeleteFilter &>(*deletion_filter);
			deleted_row_ids = std::move(filter.delete_data->deleted_rows);
			filter.delete_data->deleted_rows.clear();
		}
		scan_chunk.Initialize(context, expected_types);
		LoadNextPage(context);
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

//! The size of the values of a vector, with the strings they hold
static idx_t ValueBytes(Vector &vector, idx_t count) {
	auto &type = vector.GetType();
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR: {
		UnifiedVectorFormat format;
		vector.ToUnifiedFormat(format);
		auto strings = UnifiedVectorFormat::GetData<string_t>(format);
		idx_t bytes = 0;
		for (idx_t i = 0; i < count; i++) {
			auto idx = format.sel->get_index(i);
			bytes += sizeof(string_t) + (format.validity.RowIsValid(idx) ? strings[idx].GetSize() : 0);
		}
		return bytes;
	}
	case PhysicalType::LIST: {
		vector.Flatten();
		return count * sizeof(list_entry_t) +
		       ValueBytes(ListVector::GetChildMutable(vector), ListVector::GetListSize(vector));
	}
	case PhysicalType::STRUCT: {
		vector.Flatten();
		idx_t bytes = 0;
		for (auto &child : StructVector::GetEntries(vector)) {
			bytes += ValueBytes(child, count);
		}
		return bytes;
	}
	case PhysicalType::ARRAY: {
		vector.Flatten();
		return ValueBytes(ArrayVector::GetChildMutable(vector), count * ArrayType::GetSize(type));
	}
	default:
		return count * GetTypeIdSize(type.InternalType());
	}
}

unique_ptr<QueryResult> DuckLakeInlinedDataReader::ReadPage(const DuckLakeInlinedDataPage &query_page) {
	auto transaction = read_info.GetTransaction();
	auto &metadata_manager = transaction->GetMetadataManager();
	switch (read_info.scan_type) {
	case DuckLakeScanType::SCAN_TABLE:
		return metadata_manager.ReadInlinedData(read_info.snapshot, table_name, columns_to_read, query_page);
	case DuckLakeScanType::SCAN_INSERTIONS:
		return metadata_manager.ReadInlinedDataInsertions(*read_info.start_snapshot, read_info.snapshot, table_name,
		                                                  columns_to_read, query_page);
	case DuckLakeScanType::SCAN_DELETIONS:
		return metadata_manager.ReadInlinedDataDeletions(*read_info.start_snapshot, read_info.snapshot, table_name,
		                                                 columns_to_read, query_page);
	case DuckLakeScanType::SCAN_FOR_FLUSH:
		return metadata_manager.ReadAllInlinedDataForFlush(read_info.snapshot, table_name, read_info.table,
		                                                   read_info.flush_sort_order_sql, columns_to_read, query_page);
	default:
		throw InternalException("Unknown DuckLake scan type");
	}
}

bool DuckLakeInlinedDataReader::ReadNextKeys() {
	static constexpr idx_t KEYS_PER_QUERY = 1ULL << 20ULL;
	if (read_all_keys) {
		return false;
	}
	key_page.type = DuckLakeInlinedPageType::NEXT_KEYS;
	key_page.limit = KEYS_PER_QUERY;
	auto result = ReadPage(key_page);
	auto key_data = read_info.GetTransaction()->GetMetadataManager().TransformInlinedData(
	    *result, {LogicalType::BIGINT, LogicalType::BIGINT}, table_name);
	keys.clear();
	key_position = 0;
	for (auto &chunk : key_data->data->Chunks()) {
		auto row_ids = FlatVector::GetData<int64_t>(chunk.data[0]);
		auto begin_snapshots = FlatVector::GetData<int64_t>(chunk.data[1]);
		for (idx_t r = 0; r < chunk.size(); r++) {
			keys.emplace_back(NumericCast<idx_t>(row_ids[r]), NumericCast<idx_t>(begin_snapshots[r]));
		}
	}
	read_all_keys = keys.size() < KEYS_PER_QUERY;
	key_page.offset += keys.size();
	if (!keys.empty()) {
		key_page.after_row_id = keys.back().first;
		key_page.after_begin_snapshot = keys.back().second;
	}
	return !keys.empty();
}

bool DuckLakeInlinedDataReader::LoadNextPage(ClientContext &context) {
	if (read_all_pages) {
		return false;
	}
	// a page holds a sixteenth of the memory, rows up to the size where a vector of them fits a page are read
	// in ranges, larger ones one row at a time unless the read keys fit a page
	static constexpr idx_t MIN_PAGE_BYTES = 1ULL << 20ULL;
	static constexpr idx_t PAGE_GROWTH = 16;
	auto page_bytes = MaxValue<idx_t>(MIN_PAGE_BYTES, BufferManager::GetBufferManager(context).GetMaxMemory() / 16);
	auto small_row_bytes = page_bytes / STANDARD_VECTOR_SIZE;
	auto &metadata_manager = read_info.GetTransaction()->GetMetadataManager();
	bool constant_size = true;
	idx_t constant_row_bytes = 0;
	for (auto &type : expected_types) {
		constant_size = constant_size && TypeIsConstantSize(type.InternalType());
		constant_row_bytes += constant_size ? GetTypeIdSize(type.InternalType()) : 0;
	}
	// the catalog query sorts the rows of a sorted flush, so each page would sort every row again
	bool single_page =
	    !metadata_manager.ReadsInlinedDataInPages() ||
	    (read_info.scan_type == DuckLakeScanType::SCAN_FOR_FLUSH && !read_info.flush_sort_order_sql.empty());
	if (!page_type_set) {
		page_type_set = true;
		if (constant_size) {
			row_bytes = constant_row_bytes;
		} else if (!single_page) {
			// the size of the rows is unknown, a first page of one row measures it
			read_by_keys = true;
		}
	}
	if (read_by_keys && key_position == keys.size() && !ReadNextKeys()) {
		read_all_pages = true;
		data = make_shared_ptr<DuckLakeInlinedData>();
		data->data = make_uniq<ColumnDataCollection>(context, expected_types);
		data->data->InitializeScan(state);
		return false;
	}
	idx_t limit;
	if (single_page) {
		limit = 0;
	} else if (!read_by_keys) {
		limit = MaxValue<idx_t>(1, page_bytes / MaxValue<idx_t>(1, row_bytes));
	} else if (row_bytes == 0) {
		limit = 1;
	} else if (keys.size() - key_position <= page_bytes / largest_row_bytes) {
		// the read keys fit a page even at the size of the largest rows, which reads each vector of values once
		limit = keys.size() - key_position;
	} else if (row_bytes > small_row_bytes) {
		limit = 1;
	} else {
		limit = MinValue<idx_t>(page.limit * PAGE_GROWTH, page_bytes / row_bytes);
	}
	page.limit = limit;
	bool measures_row = read_by_keys && row_bytes == 0;

	unique_ptr<QueryResult> query_result;
	DuckLakeInlinedDataPage rows_page;
	if (!read_by_keys) {
		page.type = DuckLakeInlinedPageType::NEXT_ROWS;
		query_result = ReadPage(page);
	} else {
		rows_page.type = DuckLakeInlinedPageType::LISTED_ROWS;
		// a single large row is read with filters the scan can apply before reading its values
		rows_page.order_listed_rows = limit > 1;
		auto page_end = MinValue<idx_t>(keys.size(), key_position + limit);
		rows_page.keys.assign(keys.begin() + NumericCast<int64_t>(key_position),
		                      keys.begin() + NumericCast<int64_t>(page_end));
		key_position = page_end;
		query_result = ReadPage(rows_page);
	}
	data = metadata_manager.TransformInlinedData(*query_result, expected_types, table_name);
	query_result.reset();
	if (read_by_keys && !rows_page.order_listed_rows) {
		OrderListedRows(context, rows_page.keys);
	}
	auto row_count = data->data->Count();
	if (!read_by_keys) {
		read_all_pages = limit == 0 || row_count < limit;
	}
	data->data->InitializeScan(state);
	if (row_count == 0) {
		return read_by_keys ? LoadNextPage(context) : false;
	}
	DataChunk last_chunk;
	data->data->InitializeScanChunk(last_chunk);
	data->data->FetchChunk(data->data->ChunkCount() - 1, last_chunk);
	auto last_row = last_chunk.size() - 1;
	auto key_column = expected_types.size() - 2;
	page.after_row_id = NumericCast<idx_t>(last_chunk.data[key_column].GetValue(last_row).GetValue<int64_t>());
	page.after_begin_snapshot =
	    NumericCast<idx_t>(last_chunk.data[key_column + 1].GetValue(last_row).GetValue<int64_t>());
	if (!constant_size) {
		idx_t measured_bytes = 0;
		for (auto &chunk : data->data->Chunks()) {
			for (auto &vector : chunk.data) {
				measured_bytes += ValueBytes(vector, chunk.size());
			}
		}
		auto page_row_bytes = MaxValue<idx_t>(1, measured_bytes / row_count);
		largest_row_bytes = MaxValue(largest_row_bytes, page_row_bytes);
		// a large row weighs on the next pages until rows of the measured size follow
		row_bytes = MaxValue<idx_t>(page_row_bytes, row_bytes / PAGE_GROWTH);
	}
	if (measures_row && row_bytes <= small_row_bytes && key_position < keys.size()) {
		// a small measured row is read again with the next rows instead of in a vector of its own
		key_position--;
		return LoadNextPage(context);
	}
	MapDeletedRows();
	page.offset += row_count;
	return true;
}

void DuckLakeInlinedDataReader::OrderListedRows(ClientContext &context, const vector<pair<idx_t, idx_t>> &page_keys) {
	// the rows of a page of large values are few, the listed ones are copied one by one in the order of their keys
	auto key_column = expected_types.size() - 2;
	vector<unique_ptr<DataChunk>> chunks;
	map<pair<idx_t, idx_t>, pair<idx_t, idx_t>> row_of_key;
	bool ordered = true;
	idx_t row_number = 0;
	for (auto &chunk : data->data->Chunks()) {
		chunks.push_back(make_uniq<DataChunk>());
		auto &stored = *chunks.back();
		stored.Initialize(context, expected_types, chunk.size());
		chunk.Copy(stored);
		for (idx_t r = 0; r < chunk.size(); r++, row_number++) {
			auto key = make_pair(NumericCast<idx_t>(stored.data[key_column].GetValue(r).GetValue<int64_t>()),
			                     NumericCast<idx_t>(stored.data[key_column + 1].GetValue(r).GetValue<int64_t>()));
			ordered = ordered && row_number < page_keys.size() && page_keys[row_number] == key;
			row_of_key.emplace(key, make_pair(chunks.size() - 1, r));
		}
	}
	if (ordered && row_number == page_keys.size()) {
		return;
	}
	auto ordered_data = make_uniq<ColumnDataCollection>(context, expected_types);
	DataChunk output;
	output.Initialize(context, expected_types);
	for (auto &key : page_keys) {
		auto entry = row_of_key.find(key);
		if (entry == row_of_key.end()) {
			continue;
		}
		auto &source = *chunks[entry->second.first];
		for (idx_t c = 0; c < expected_types.size(); c++) {
			output.data[c].SetValue(output.size(), source.data[c].GetValue(entry->second.second));
		}
		output.SetChildCardinality(output.size() + 1);
		if (output.size() == STANDARD_VECTOR_SIZE) {
			ordered_data->Append(output);
			output.Reset();
		}
	}
	if (output.size() > 0) {
		ordered_data->Append(output);
	}
	data->data = std::move(ordered_data);
}

void DuckLakeInlinedDataReader::MapDeletedRows() {
	if (!deletion_filter) {
		return;
	}
	auto &filter = static_cast<DuckLakeDeleteFilter &>(*deletion_filter);
	auto &deleted_ordinals = filter.delete_data->deleted_rows;
	auto row_id_column = expected_types.size() - 2;
	auto ordinal = page.offset;
	for (auto &chunk : data->data->Chunks()) {
		auto row_id_data = FlatVector::GetData<int64_t>(chunk.data[row_id_column]);
		for (idx_t r = 0; r < chunk.size(); r++, ordinal++) {
			auto row_id = NumericCast<idx_t>(row_id_data[r]);
			while (deleted_row_idx < deleted_row_ids.size() && deleted_row_ids[deleted_row_idx] < row_id) {
				deleted_row_idx++;
			}
			if (deleted_row_idx < deleted_row_ids.size() && deleted_row_ids[deleted_row_idx] == row_id) {
				deleted_ordinals.push_back(ordinal);
				deleted_row_idx++;
			}
		}
	}
}

AsyncResult DuckLakeInlinedDataReader::Scan(ClientContext &context, GlobalTableFunctionState &global_state,
                                            LocalTableFunctionState &local_state, DataChunk &chunk) {
	if (!virtual_columns.empty()) {
		scan_chunk.Reset();
		data->data->Scan(state, scan_chunk);
		while (scan_chunk.size() == 0 && !table_name.empty() && LoadNextPage(context)) {
			scan_chunk.Reset();
			data->data->Scan(state, scan_chunk);
		}
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

string DuckLakeInlinedDataReader::GetReaderType() const {
	return "DuckLake Inlined Data";
}

} // namespace duckdb
