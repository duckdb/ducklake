//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_commit_outcome.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Whether a commit published its changes. The values are ordered: a commit only moves to a later one
enum class DuckLakeCommitOutcome : uint8_t {
	//! Nothing was published, so the commit can run again
	NOT_PUBLISHED,
	//! The commit was sent and not rejected, so it may have been published
	PUBLICATION_UNKNOWN,
	//! The changes are in the metadata, whatever fails after that
	PUBLISHED
};

} // namespace duckdb
