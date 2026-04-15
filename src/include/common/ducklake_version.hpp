//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_version.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class DuckLakeVersion { UNSET, V1_0 };

DuckLakeVersion DuckLakeVersionFromString(const string &version_str);
string DuckLakeVersionToString(DuckLakeVersion version);

} // namespace duckdb
