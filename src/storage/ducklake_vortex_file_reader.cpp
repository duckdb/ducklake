#include "storage/ducklake_vortex_file_reader.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {

DuckLakeVortexFileReader::DuckLakeVortexFileReader(ClientContext &context, const OpenFileInfo &info,
                                                   DuckLakeFileData file_data_p)
    : BaseFileReader(info), file_data(std::move(file_data_p)) {
	if (!file_data.encryption_key.empty()) {
		throw NotImplementedException("Reading encrypted Vortex data files is not supported yet");
	}
	scanner = make_uniq<VortexFileScanner>(context, file_data);
	auto &names = scanner->GetNames();
	auto &types = scanner->GetTypes();
	for (idx_t i = 0; i < names.size(); i++) {
		columns.emplace_back(names[i], types[i]);
		if (names[i] == "_ducklake_internal_row_id") {
			columns.back().identifier = Value::INTEGER(MultiFileReader::ROW_ID_FIELD_ID);
		} else if (names[i] == "_ducklake_internal_snapshot_id") {
			columns.back().identifier = Value::INTEGER(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID);
		}
	}
}

bool DuckLakeVortexFileReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
                                                 LocalTableFunctionState &lstate) {
	{
		lock_guard<mutex> guard(lock);
		if (initialized_scan) {
			return false;
		}
		initialized_scan = true;
	}

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		auto col_id = column_indexes[i].GetPrimaryIndex();
		if (col_id < columns.size() && columns[col_id].identifier.type().id() == LogicalTypeId::INTEGER &&
		    IntegerValue::Get(columns[col_id].identifier) == MultiFileReader::ORDINAL_FIELD_ID) {
			projected_columns.push_back(VortexReaderColumn::FILE_ROW_NUMBER);
			continue;
		}
		if (col_id >= columns.size()) {
			throw InternalException("Vortex reader column index out of range");
		}
		scan_column_ids.push_back(col_id);
		scan_types.push_back(columns[col_id].type);
		projected_columns.push_back(VortexReaderColumn::NONE);
	}
	if (projected_columns.empty()) {
		if (columns.empty()) {
			throw InvalidInputException("Cannot scan an empty-column Vortex file");
		}
		scan_column_ids.push_back(0);
		scan_types.push_back(columns[0].type);
		projected_columns.push_back(VortexReaderColumn::EMPTY);
	}
	if (scan_column_ids.empty() && !columns.empty()) {
		scan_column_ids.push_back(0);
		scan_types.push_back(columns[0].type);
	}
	scan_chunk.Initialize(context, scan_types);
	scanner->SetColumnIds(scan_column_ids);
	scanner->InitializeScan();

	for (auto &entry : expression_map) {
		expression_executors[entry.first] = make_uniq<ExpressionExecutor>(context, *entry.second);
	}
	return true;
}

bool DuckLakeVortexFileReader::TryEvaluateExpression(ClientContext &context, idx_t virtual_col_idx,
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

AsyncResult DuckLakeVortexFileReader::Scan(ClientContext &context, GlobalTableFunctionState &global_state,
                                           LocalTableFunctionState &local_state, DataChunk &chunk) {
	scan_chunk.Reset();
	if (!scanner->Scan(scan_chunk)) {
		return AsyncResult(SourceResultType::FINISHED);
	}

	idx_t source_idx = 0;
	for (idx_t c = 0; c < projected_columns.size(); c++) {
		switch (projected_columns[c]) {
		case VortexReaderColumn::NONE: {
			auto column_id = source_idx++;
			if (TryEvaluateExpression(context, c, scan_chunk.data[column_id], scan_chunk.data[column_id].GetType(),
			                          chunk.data[c])) {
				break;
			}
			if (chunk.data[c].GetType() != scan_chunk.data[column_id].GetType()) {
				VectorOperations::Cast(context, scan_chunk.data[column_id], chunk.data[c], scan_chunk.size());
			} else {
				chunk.data[c].Reference(scan_chunk.data[column_id]);
			}
			break;
		}
		case VortexReaderColumn::FILE_ROW_NUMBER: {
			Vector ordinal_vector(LogicalType::BIGINT);
			auto ordinal_data = FlatVector::GetDataMutable<int64_t>(ordinal_vector);
			for (idx_t r = 0; r < scan_chunk.size(); r++) {
				ordinal_data[r] = file_row_number + NumericCast<int64_t>(r);
			}
			FlatVector::SetSize(ordinal_vector, scan_chunk.size());
			if (TryEvaluateExpression(context, c, ordinal_vector, LogicalType::BIGINT, chunk.data[c])) {
				break;
			}
			auto row_id_data = FlatVector::GetDataMutable<int64_t>(chunk.data[c]);
			for (idx_t r = 0; r < scan_chunk.size(); r++) {
				row_id_data[r] = ordinal_data[r];
			}
			break;
		}
		case VortexReaderColumn::EMPTY:
			break;
		}
	}

	chunk.SetChildCardinality(scan_chunk.size());
	auto scan_count = chunk.size();
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
				auto &vec = chunk.data[column_id];

				UnifiedVectorFormat vdata;
				vec.ToUnifiedFormat(chunk.size(), vdata);

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

void DuckLakeVortexFileReader::AddVirtualColumn(column_t virtual_column_id) {
	if (virtual_column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		columns.back().identifier = Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID);
		return;
	}
	throw InternalException("Unsupported virtual column id %d for Vortex file reader", virtual_column_id);
}

string DuckLakeVortexFileReader::GetReaderType() const {
	return "DuckLake Vortex Data";
}

} // namespace duckdb
