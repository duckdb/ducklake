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

enum class DuckLakeVersion : uint8_t {
	UNSET = 0,
	V0_1 = 1,
	V0_2 = 2,
	V0_3_DEV1 = 3,
	V0_3 = 4,
	V0_4_DEV1 = 5,
	V0_4 = 6,
	V1_0 = 7,
	V1_1_DEV_1 = 8,
	V1_1 = 9,
	V2_0_DEV1 = 10
};

static constexpr DuckLakeVersion DUCKLAKE_LATEST_VERSION = DuckLakeVersion::V2_0_DEV1;

DuckLakeVersion DuckLakeVersionFromString(const string &version_str);
//! The versions this build attaches without a migration, the releases and the version it writes
bool DuckLakeVersionIsSupported(DuckLakeVersion version);
DuckLakeVersion ParseWritableDuckLakeVersion(const string &version_str, const string &option_name);
string DuckLakeVersionToString(DuckLakeVersion version);
[[noreturn]] void ThrowUnsupportedByVersion(DuckLakeVersion version, const string &feature);

} // namespace duckdb
