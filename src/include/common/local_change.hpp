//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/local_change.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "common/index.hpp"

namespace duckdb {

enum class LocalChangeType {
	NONE,
	CREATED,
	RENAMED,
	SET_PARTITION_KEY,
	SET_COMMENT,
	SET_COLUMN_COMMENT,
	SET_NULL,
	DROP_NULL,
	RENAME_COLUMN,
	ADD_COLUMN,
	REMOVE_COLUMN,
	CHANGE_COLUMN_TYPE,
	SET_DEFAULT,
	SET_SORT_KEY
};

struct LocalChange {
	LocalChange(LocalChangeType type) // NOLINT: allow implicit conversion from LocalChangeType
	    : type(type) {
	}

	LocalChangeType type;
	//! For operations that alter individual columns
	FieldIndex field_index;

	static LocalChange SetColumnComment(FieldIndex field_idx) {
		LocalChange result(LocalChangeType::SET_COLUMN_COMMENT);
		result.field_index = field_idx;
		return result;
	}
	static LocalChange SetNull(FieldIndex field_idx) {
		LocalChange result(LocalChangeType::SET_NULL);
		result.field_index = field_idx;
		return result;
	}
	static LocalChange DropNull(FieldIndex field_idx) {
		LocalChange result(LocalChangeType::DROP_NULL);
		result.field_index = field_idx;
		return result;
	}
	static LocalChange SetDefault(FieldIndex field_idx) {
		LocalChange result(LocalChangeType::SET_DEFAULT);
		result.field_index = field_idx;
		return result;
	}
	static LocalChange RenameColumn(FieldIndex field_idx) {
		LocalChange result(LocalChangeType::RENAME_COLUMN);
		result.field_index = field_idx;
		return result;
	}
	static LocalChange RemoveColumn(FieldIndex field_idx) {
		LocalChange result(LocalChangeType::REMOVE_COLUMN);
		result.field_index = field_idx;
		return result;
	}
};

} // namespace duckdb
