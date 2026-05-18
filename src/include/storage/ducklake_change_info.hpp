//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_change_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "common/index.hpp"

namespace duckdb {

enum class ChangeKind : uint8_t {
	NONE,
	CREATED_SCHEMA,
	DROPPED_SCHEMA,
	CREATED_TABLE,
	CREATED_SCALAR_MACRO,
	CREATED_TABLE_MACRO,
	ALTERED_TABLE,
	ALTERED_TABLE_WITH_SCHEMA_VERSION_CHANGES,
	ALTERED_VIEW,
	DROPPED_TABLE,
	DROPPED_VIEW,
	DROPPED_SCALAR_MACRO,
	DROPPED_TABLE_MACRO,
	INSERTED_INTO,
	DELETED_FROM,
	INSERTED_INLINED,
	DELETED_INLINED,
	FLUSHED_INLINED,
	COMPACTED,
	MERGE_ADJACENT,
	REWRITE_DELETE,
	COMMIT_HEADER,
	DATA_FILES,
};

inline string OptionalIdxOrNull(const optional_idx &v) {
	return v.IsValid() ? std::to_string(v.GetIndex()) : "NULL";
}

inline string MappingIdOrNull(const MappingIndex &m) {
	return m.IsValid() ? std::to_string(m.index) : "NULL";
}

inline string EncryptionKeyLiteral(const string &key) {
	if (key.empty()) {
		return "NULL";
	}
	return "'" + Blob::ToBase64(string_t(key)) + "'";
}

template <class T, ChangeKind Kind = ChangeKind::NONE>
class ChangeInfo {
public:
	ChangeInfo() = default;
	explicit ChangeInfo(T data_p) : data(std::move(data_p)) {
	}

	string Serialize(const string &uuid_suffix) const {
		return "";
	}
	string Drop(const string &uuid_suffix) const {
		return "";
	}

	T &Items() {
		return data;
	}
	const T &Items() const {
		return data;
	}

private:
	T data;
};

} // namespace duckdb
