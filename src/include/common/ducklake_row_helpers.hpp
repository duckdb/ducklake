//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_row_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

template <class ROW>
idx_t AsIdx(ROW &row, idx_t col) {
	return static_cast<idx_t>(row.template GetValue<int64_t>(col));
}

template <class ROW>
optional_idx OptIdx(ROW &row, idx_t col) {
	if (row.IsNull(col)) {
		return optional_idx();
	}
	return optional_idx(AsIdx(row, col));
}

template <class ROW>
bool OptBoolFalse(ROW &row, idx_t col) {
	return !row.IsNull(col) && row.template GetValue<bool>(col);
}

template <class ROW>
void ReadEncryptionKey(ROW &row, idx_t col, string &out) {
	if (!row.IsNull(col)) {
		out = Blob::FromBase64(string_t(row.template GetValue<string>(col)));
	}
}

} // namespace duckdb
