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

class DuckLakeTypes {
public:
	static LogicalType FromString(const string &str);
	static string ToString(const LogicalType &str);
	static void CheckSupportedType(const LogicalType &type, DuckLakeVersion version);
	static void CheckSupportedTypes(const ColumnList &columns, DuckLakeVersion version);

	static bool RequiresCast(const LogicalType &type);
	static bool RequiresCast(const vector<LogicalType> &types);
	//! If this type requires a cast, return the type to cast to
	static LogicalType GetCastedType(const LogicalType &type);
};

} // namespace duckdb
