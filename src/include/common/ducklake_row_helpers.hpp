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
#include "duckdb/main/query_result.hpp"

namespace duckdb {

using QueryResultRow = decltype(QueryResult::iterator::current_row);

inline idx_t AsIdx(const QueryResultRow &row, idx_t col) {
	return static_cast<idx_t>(row.GetValue<int64_t>(col));
}

inline optional_idx OptIdx(const QueryResultRow &row, idx_t col) {
	if (row.IsNull(col)) {
		return optional_idx();
	}
	return optional_idx(AsIdx(row, col));
}

template <class T>
bool TryReadValue(const QueryResultRow &row, idx_t col, T &out) {
	if (row.IsNull(col)) {
		return false;
	}
	out = row.template GetValue<T>(col);
	return true;
}

inline bool OptBoolFalse(const QueryResultRow &row, idx_t col) {
	return !row.IsNull(col) && row.GetValue<bool>(col);
}

inline void ReadEncryptionKey(const QueryResultRow &row, idx_t col, string &out) {
	if (!row.IsNull(col)) {
		out = Blob::FromBase64(string_t(row.GetValue<string>(col)));
	}
}

} // namespace duckdb
