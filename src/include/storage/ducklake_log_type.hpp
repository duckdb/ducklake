//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_log_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/logging/log_type.hpp"

namespace duckdb {

class DuckLakeMetadataLogType : public LogType {
public:
	static constexpr const char *NAME = "DuckLakeMetadata";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	DuckLakeMetadataLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const string &catalog_name, const string &query, int64_t elapsed_ms);
};

} // namespace duckdb
