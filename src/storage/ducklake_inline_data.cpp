#include "storage/ducklake_inline_data.hpp"
#include "storage/ducklake_stats.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

DuckLakeInlineData::DuckLakeInlineData(PhysicalPlan &physical_plan, PhysicalOperator &child, idx_t inline_row_limit)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.types, child.estimated_cardinality),
      inline_row_limit(inline_row_limit) {
	children.push_back(child);
}

enum class InlinePhase { INLINING_ROWS, EMITTING_PREVIOUSLY_INLINED_ROWS, PASS_THROUGH_ROWS };

class InlineDataState : public OperatorState {
public:
	explicit InlineDataState() {
	}

	InlinePhase phase = InlinePhase::INLINING_ROWS;
	unique_ptr<ColumnDataCollection> inlined_data;
	ColumnDataScanState emit_scan;
};

class InlineDataGlobalState : public GlobalOperatorState {
public:
	explicit InlineDataGlobalState(const DuckLakeInlineData &op) : op(op), total_inlined_rows(0) {
	}

	InlinePhase AddRows(InlineDataState &state, idx_t new_rows) {
		lock_guard<mutex> guard(lock);
		total_inlined_rows += new_rows;
		if (total_inlined_rows > op.inline_row_limit) {
			// we have exceeded the total amount of rows - bail
			if (global_inlined_data) {
				// if we have any global inlined data - add it to the local inlined data so we can emit it
				AddToCollection(std::move(global_inlined_data), state.inlined_data);
			}
			return InlinePhase::PASS_THROUGH_ROWS;
		}
		// we are still inlining rows
		return InlinePhase::INLINING_ROWS;
	}

	static void AddToCollection(unique_ptr<ColumnDataCollection> source, unique_ptr<ColumnDataCollection> &target) {
		if (!target) {
			target = std::move(source);
			return;
		}
		ColumnDataAppendState append_state;
		target->InitializeAppend(append_state);
		for (auto &chunk : source->Chunks()) {
			target->Append(append_state, chunk);
		}
	}

	const DuckLakeInlineData &op;
	mutex lock;
	idx_t total_inlined_rows = 0;
	InlinePhase global_phase = InlinePhase::INLINING_ROWS;
	unique_ptr<ColumnDataCollection> global_inlined_data;
};

unique_ptr<OperatorState> DuckLakeInlineData::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<InlineDataState>();
}

unique_ptr<GlobalOperatorState> DuckLakeInlineData::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<InlineDataGlobalState>(*this);
}

OperatorResultType DuckLakeInlineData::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = state_p.Cast<InlineDataState>();
	auto &gstate = gstate_p.Cast<InlineDataGlobalState>();
	if (state.phase == InlinePhase::PASS_THROUGH_ROWS) {
		// not inlining rows - forward the input directly
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	if (state.phase == InlinePhase::EMITTING_PREVIOUSLY_INLINED_ROWS) {
		// we are emitting previously inlined rows - this happens if we decided not to inline data after all
		state.inlined_data->Scan(state.emit_scan, chunk);
		if (chunk.size() == 0) {
			// finished emitting previously inlined rows - destroy them and pass through any subsequent rows
			state.inlined_data.reset();
			state.phase = InlinePhase::PASS_THROUGH_ROWS;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	// we have a new batch of rows
	// add the count to the global count and check if we are still inlining rows
	auto new_phase = gstate.AddRows(state, input.size());
	if (new_phase != InlinePhase::INLINING_ROWS) {
		// we have exceeded the limit - start emitting rows from the `inlined_data` if we have it
		if (state.inlined_data) {
			// start a scan
			state.inlined_data->InitializeScan(state.emit_scan);
			state.phase = InlinePhase::EMITTING_PREVIOUSLY_INLINED_ROWS;
		} else {
			// no inline data yet - immediately start emitting rows
			state.phase = InlinePhase::PASS_THROUGH_ROWS;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	// we are inlining these rows - append to the local collection
	if (!state.inlined_data) {
		state.inlined_data = make_uniq<ColumnDataCollection>(context.client, types);
	}
	state.inlined_data->Append(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType DuckLakeInlineData::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                            GlobalOperatorState &gstate_p,
                                                            OperatorState &state_p) const {
	auto &state = state_p.Cast<InlineDataState>();
	auto &gstate = gstate_p.Cast<InlineDataGlobalState>();
	if (state.phase == InlinePhase::EMITTING_PREVIOUSLY_INLINED_ROWS) {
		// emitting previously inlined rows
		state.inlined_data->Scan(state.emit_scan, chunk);
		if (chunk.size() != 0) {
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}
		// finished emitting previously inlined rows - we're done
		state.inlined_data.reset();
		return OperatorFinalizeResultType::FINISHED;
	}
	if (!state.inlined_data) {
		// no inlined data, we are done
		return OperatorFinalizeResultType::FINISHED;
	}
	// push the inlined data into the global inlined data
	lock_guard<mutex> guard(gstate.lock);
	if (gstate.total_inlined_rows > inline_row_limit) {
		// we have changed our mind on inlining - we need to emit the rows again
		state.inlined_data->InitializeScan(state.emit_scan);
		state.phase = InlinePhase::EMITTING_PREVIOUSLY_INLINED_ROWS;
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}
	InlineDataGlobalState::AddToCollection(std::move(state.inlined_data), gstate.global_inlined_data);
	return OperatorFinalizeResultType::FINISHED;
}

struct DuckLakeBaseColumnStats {
	explicit DuckLakeBaseColumnStats(FieldIndex field_id, const LogicalType &type) : field_id(field_id), stats(type) {
	}

	FieldIndex field_id;
	DuckLakeColumnStats stats;
	bool has_stats = false;
	vector<DuckLakeBaseColumnStats> children;
};

struct StatsNumericFallbackOperator {
	static bool SmallerThan(const LogicalType &type, string_t left, string_t right) {
		return Value(left).DefaultCastAs(type) < Value(right).DefaultCastAs(type);
	}
	static bool GreaterThan(const LogicalType &type, string_t left, string_t right) {
		return Value(left).DefaultCastAs(type) > Value(right).DefaultCastAs(type);
	}
	static string GetFinalStats(string_t input) {
		return input.GetString();
	}
};

struct StatsFallbackOperator {
	static bool SmallerThan(const LogicalType &type, string_t left, string_t right) {
		return left < right;
	}
	static bool GreaterThan(const LogicalType &type, string_t left, string_t right) {
		return left > right;
	}
	static string GetFinalStats(string_t input) {
		return input.GetString();
	}
};

template <class T, class OP>
DuckLakeColumnStats TemplatedUpdateStats(Vector &input_vec, const LogicalType &type, idx_t row_count) {
	UnifiedVectorFormat format;
	input_vec.ToUnifiedFormat(row_count, format);

	auto data = UnifiedVectorFormat::GetData<T>(format);
	auto &validity = format.validity;
	DuckLakeColumnStats result(type);
	result.any_valid = false;
	result.has_null_count = true;
	optional_idx min_idx;
	optional_idx max_idx;

	for (idx_t i = 0; i < row_count; i++) {
		auto idx = format.sel->get_index(i);
		if (!validity.RowIsValid(idx)) {
			result.null_count++;
			continue;
		}
		auto &val = data[idx];
		if (!min_idx.IsValid()) {
			min_idx = idx;
		} else if (OP::SmallerThan(type, val, data[min_idx.GetIndex()])) {
			min_idx = idx;
		}
		if (!max_idx.IsValid()) {
			max_idx = idx;
		} else if (OP::GreaterThan(type, val, data[max_idx.GetIndex()])) {
			max_idx = idx;
		}
		result.any_valid = true;
	}
	if (min_idx.IsValid()) {
		result.has_min = true;
		result.has_max = true;
		result.min = OP::GetFinalStats(data[min_idx.GetIndex()]);
		result.max = OP::GetFinalStats(data[max_idx.GetIndex()]);
	}
	return result;
}

DuckLakeColumnStats GetVectorStats(Vector &input_vec, idx_t row_count) {
	auto &type = input_vec.GetType();
	Vector str_vector(LogicalType::VARCHAR, row_count);
	VectorOperations::DefaultCast(input_vec, str_vector, row_count);
	// FIXME: we can be more efficient here by templating on other types (numerics...)
	// FIXME: we can gather nan statistics for FLOAT/DOUBLE
	if (RequiresValueComparison(type)) {
		return TemplatedUpdateStats<string_t, StatsNumericFallbackOperator>(str_vector, type, row_count);
	}
	return TemplatedUpdateStats<string_t, StatsFallbackOperator>(str_vector, type, row_count);
}

void UpdateStats(vector<DuckLakeBaseColumnStats> &stats, idx_t c, Vector &data, idx_t row_count,
                 const DuckLakeFieldId &field_id) {
	if (c >= stats.size()) {
		if (c != stats.size()) {
			throw InternalException("Column stats not accessed in order?");
		}
		stats.emplace_back(field_id.GetFieldIndex(), data.GetType());
	}
	auto &column_stats = stats[c];
	auto &type = data.GetType();
	if (type.IsNested() && type.id() != LogicalTypeId::VARIANT) {
		// nested - recurse into children
		switch (data.GetType().id()) {
		case LogicalTypeId::STRUCT: {
			auto &children = StructVector::GetEntries(data);
			for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
				UpdateStats(column_stats.children, child_idx, *children[child_idx], row_count,
				            field_id.GetChildByIndex(child_idx));
			}
			break;
		}
		case LogicalTypeId::LIST: {
			auto &child = ListVector::GetEntry(data);
			UpdateStats(column_stats.children, 0, child, ListVector::GetListSize(data), field_id.GetChildByIndex(0));
			break;
		}
		case LogicalTypeId::MAP: {
			auto &keys = MapVector::GetKeys(data);
			auto &values = MapVector::GetValues(data);
			auto map_size = ListVector::GetListSize(data);
			UpdateStats(column_stats.children, 0, keys, map_size, field_id.GetChildByIndex(0));
			UpdateStats(column_stats.children, 1, values, map_size, field_id.GetChildByIndex(1));
			break;
		}
		default:
			throw InternalException("FIXME: unsupported nested type");
		}
		return;
	}
	auto new_stats = GetVectorStats(data, row_count);
	if (column_stats.has_stats) {
		column_stats.stats.MergeStats(new_stats);
	} else {
		column_stats.stats = std::move(new_stats);
		column_stats.has_stats = true;
	}
}

void SetFinalStats(DuckLakeBaseColumnStats &stats, DuckLakeInlinedData &result) {
	if (stats.has_stats) {
		result.column_stats.insert(make_pair(stats.field_id, std::move(stats.stats)));
	}
	for (auto &child_stats : stats.children) {
		SetFinalStats(child_stats, result);
	}
}

OperatorFinalResultType DuckLakeInlineData::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                             OperatorFinalizeInput &input) const {
	// push inlined data to transaction
	auto &gstate = input.global_state.Cast<InlineDataGlobalState>();
	if (!gstate.global_inlined_data) {
		// no inlined data, we are done
		return OperatorFinalResultType::FINISHED;
	}
	{
		auto cinsert = const_cast<DuckLakeInsert *>(insert.get());
		lock_guard<mutex> lock(cinsert->lock);
		if (!cinsert->sink_state) {
			cinsert->sink_state = insert->GetGlobalSinkState(context);
		}
	}
	auto &insert_gstate = insert->sink_state->Cast<DuckLakeInsertGlobalState>();
	if (insert_gstate.total_insert_count != 0) {
		throw InternalException("Inlining rows but also inserting rows through a file");
	}
	auto &table = insert_gstate.table;
	auto result = make_uniq<DuckLakeInlinedData>();
	auto &inlined_data = *gstate.global_inlined_data;
	result->data = std::move(gstate.global_inlined_data);
	// set the insert count to the total number of inlined rows
	insert_gstate.total_insert_count = inlined_data.Count();

	// compute the column stats for the data
	vector<DuckLakeBaseColumnStats> new_stats;
	auto &field_data = table.GetFieldData();
	for (auto &chunk : inlined_data.Chunks()) {
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			UpdateStats(new_stats, c, chunk.data[c], chunk.size(), field_data.GetByRootIndex(PhysicalIndex(c)));
		}
	}
	// set the final stats and verify NOT NULL constraints
	auto not_null_fields = table.GetNotNullFields();
	for (idx_t c = 0; c < new_stats.size(); c++) {
		auto &column_stats = new_stats[c];
		SetFinalStats(column_stats, *result);

		if (column_stats.stats.null_count > 0) {
			auto column_name = table.GetColumn(LogicalIndex(c)).GetName();
			if (not_null_fields.count(column_name)) {
				throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, column_name);
			}
		}
	}

	// push the inlined data into the transaction
	auto &transaction = DuckLakeTransaction::Get(context, table.ParentCatalog());
	transaction.AppendInlinedData(table.GetTableId(), std::move(result));
	return OperatorFinalResultType::FINISHED;
}

string DuckLakeInlineData::GetName() const {
	return "DUCKLAKE_INLINE_DATA";
}

} // namespace duckdb
