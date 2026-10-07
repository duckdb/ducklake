//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/typed_index.hpp"

namespace duckdb {

struct DuckLakeConstants {
	static constexpr const idx_t TRANSACTION_LOCAL_ID_START = 9223372036854775808ULL;
	static constexpr const idx_t TRANSACTION_LOCAL_ROW_ID_START = 1000000000000000000ULL;

	static bool IsTransactionLocalRowId(int64_t rid) {
		return rid >= 0 && static_cast<idx_t>(rid) >= TRANSACTION_LOCAL_ROW_ID_START;
	}
};

struct SchemaIndex : public TypedIndex<SchemaIndex> {
	using TypedIndex::TypedIndex;

	bool IsTransactionLocal() const {
		D_ASSERT(IsValid());
		return index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
	}
};

inline bool IsTransactionLocal(const TableIndex &idx) {
	D_ASSERT(idx.IsValid());
	return idx.index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
}

struct MacroIndex : public TypedIndex<MacroIndex> {
	using TypedIndex::TypedIndex;
};

struct FieldIndex : public TypedIndex<FieldIndex> {
	using TypedIndex::TypedIndex;
};

struct DataFileIndex : public TypedIndex<DataFileIndex> {
	using TypedIndex::TypedIndex;
};

struct MappingIndex : public TypedIndex<MappingIndex> {
	using TypedIndex::TypedIndex;
};

} // namespace duckdb
