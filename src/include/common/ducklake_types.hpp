//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
struct DuckLakeColumnInfo;

class DuckLakeTypes {
public:
	static LogicalType FromString(const string &str);
	static LogicalType FromColumnInfo(const DuckLakeColumnInfo &col);
	static string ToString(const LogicalType &str);
	static void CheckSupportedType(const LogicalType &type, DuckLakeVersion version);
	static void CheckSupportedTypes(const ColumnList &columns, DuckLakeVersion version);

	//! VARCHAR and BLOB, whose values can be the text NULL
	static bool IsStringType(const LogicalType &type);
	//! Nested types other than VARIANT, which DuckLake stores without child types
	static bool IsNested(const LogicalType &type);
};

} // namespace duckdb
