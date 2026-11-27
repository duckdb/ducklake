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
	LocalChange(LocalChangeType type, FieldIndex field_index = FieldIndex()) // NOLINT: allow implicit conversion
	    : type(type), field_index(field_index) {
	}

	LocalChangeType type;
	//! For operations that alter individual columns
	FieldIndex field_index;
	//! For SET_COLUMN_COMMENT on views
	Identifier view_column_comment_name;

	static LocalChange SetViewColumnComment(const Identifier &column_name) {
		LocalChange result(LocalChangeType::SET_COLUMN_COMMENT);
		result.view_column_comment_name = column_name;
		return result;
	}
};

} // namespace duckdb
