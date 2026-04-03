#include "storage/ducklake_inlined_data.hpp"

#include <algorithm>

namespace duckdb {

bool DuckLakeInlinedData::HasPreservedRowIds() const {
	return !row_ids.empty();
}

idx_t DuckLakeInlinedData::GetRowId(idx_t position) const {
	if (HasPreservedRowIds()) {
		return NumericCast<idx_t>(row_ids[position]);
	}
	return position;
}

int64_t DuckLakeInlinedData::GetOutputRowId(idx_t position) const {
	if (HasPreservedRowIds()) {
		return row_ids[position];
	}
	return NumericCast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START + position);
}

void DuckLakeInlinedData::MergeRowIds(const DuckLakeInlinedData &new_data, idx_t new_data_count) {
	if (!new_data.HasPreservedRowIds() && !HasPreservedRowIds()) {
		return;
	}
	if (!HasPreservedRowIds()) {
		// if the existing data doesnt have preserved row ids, we assign them sequentially from
		// transaction local id
		idx_t existing_count = data->Count() - new_data_count;
		row_ids.reserve(existing_count + new_data.row_ids.size());
		auto next_id = NumericCast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START);
		for (idx_t i = 0; i < existing_count; i++) {
			row_ids.push_back(next_id + NumericCast<int64_t>(i));
		}
	}
	if (new_data.HasPreservedRowIds()) {
		// if this is already preserved we can just insert it
		row_ids.insert(row_ids.end(), new_data.row_ids.begin(), new_data.row_ids.end());
	} else {
		// new data doesnt preserve row_ids, we need to use the TRANSACTION_LOCAL_ROW_ID_START
		int64_t next_id = NumericCast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START);
		if (!row_ids.empty()) {
			// we only use max_id if it's guaranteed to be transactional (over next_id)
			auto max_id = *std::max_element(row_ids.begin(), row_ids.end());
			if (max_id >= next_id) {
				next_id = max_id + 1;
			}
		}
		for (idx_t i = 0; i < new_data_count; i++) {
			row_ids.push_back(next_id + NumericCast<int64_t>(i));
		}
	}
}

} // namespace duckdb
