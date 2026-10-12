//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_timetz.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct DuckLakeColumnStats;

//! A TIMETZ column is written to data files of 1.1 catalogs as a struct of its UTC time and its offset in seconds
struct DuckLakeTimeTZ {
	static constexpr const char *TIME_FIELD = "time";
	static constexpr const char *OFFSET_FIELD = "offset";

	static LogicalType StorageType();
	static unique_ptr<Expression> ToStorage(ClientContext &context, unique_ptr<Expression> expr);
	static unique_ptr<Expression> FromStorage(ClientContext &context, unique_ptr<Expression> expr);
	//! Replaces the casts of stored structs to TIMETZ by the conversion from storage
	static void ReplaceStorageCasts(ClientContext &context, unique_ptr<Expression> &expr);
	//! Sets TIMETZ bounds that hold the values of the time and offset fields of a file
	static void SetBounds(DuckLakeColumnStats &time_stats, const DuckLakeColumnStats &offset_stats);
};

} // namespace duckdb
