#include "rest/ducklake_rest_arrow.hpp"

#include "rest/ducklake_rest_arrow_ipc.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

string RESTArrow::Write(ClientContext &context, const vector<string> &names, const ColumnDataCollection &rows) {
	auto &arrow_types = rows.Types();
	// types that Arrow does not have are sent as extension types
	ClientProperties options("UTC", ArrowOffsetSize::REGULAR, false, false, true, ArrowFormatVersion::V1_0, &context);
	ArrowSchemaWrapper schema;
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, arrow_types, names, options);
	vector<ArrowArrayWrapper> wrappers;
	vector<ArrowArray> batches;
	auto extensions = ArrowTypeExtensionData::GetExtensionTypes(context, arrow_types);
	for (auto &chunk : rows.Chunks()) {
		wrappers.emplace_back();
		ArrowConverter::ToArrowArray(chunk, &wrappers.back().arrow_array, options, extensions);
		batches.push_back(wrappers.back().arrow_array);
	}
	return RESTArrowIPC::Write(schema.arrow_schema, batches);
}

unique_ptr<ColumnDataCollection> RESTArrow::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset,
                                                 idx_t size, const vector<LogicalType> &types, vector<string> &names) {
	auto stream = RESTArrowIPC::Read(std::move(data), offset, size);
	ArrowTableSchema arrow_table;
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, stream->schema);
	vector<LogicalType> arrow_types;
	names.clear();
	for (idx_t col_idx = 0; col_idx < NumericCast<idx_t>(stream->schema.n_children); col_idx++) {
		arrow_types.push_back(arrow_table.GetColumns().at(col_idx)->GetDuckType());
		names.push_back(stream->schema.children[col_idx]->name);
	}
	if (!types.empty() && types.size() != arrow_types.size()) {
		throw InvalidInputException("The Arrow stream has %llu columns instead of %llu", arrow_types.size(),
		                            types.size());
	}
	auto &result_types = types.empty() ? arrow_types : types;
	auto result = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), result_types);
	DataChunk scanned;
	scanned.Initialize(Allocator::DefaultAllocator(), arrow_types);
	DataChunk converted;
	if (!types.empty()) {
		converted.Initialize(Allocator::DefaultAllocator(), types);
	}
	for (auto &batch : stream->batches) {
		auto wrapper = make_uniq<ArrowArrayWrapper>();
		wrapper->arrow_array = batch;
		batch.release = nullptr;
		ArrowScanLocalState state(std::move(wrapper), context);
		auto length = NumericCast<idx_t>(state.chunk->arrow_array.length);
		while (state.chunk_offset < length) {
			auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, length - state.chunk_offset);
			scanned.Reset();
			scanned.SetChildCardinality(count);
			ArrowTableFunction::ArrowToDuckDB(state, arrow_table.GetColumns(), scanned);
			state.chunk_offset += count;
			if (types.empty()) {
				result->Append(scanned);
				continue;
			}
			converted.Reset();
			for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
				if (scanned.data[col_idx].GetType() == types[col_idx]) {
					converted.data[col_idx].Reference(scanned.data[col_idx]);
				} else {
					VectorOperations::Cast(context, scanned.data[col_idx], converted.data[col_idx], count);
				}
			}
			converted.SetChildCardinality(count);
			result->Append(converted);
		}
	}
	return result;
}

} // namespace duckdb
