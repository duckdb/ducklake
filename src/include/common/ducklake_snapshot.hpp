//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_snapshot.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct DuckLakeSnapshot {
	DuckLakeSnapshot(idx_t snapshot_id, idx_t schema_version, idx_t next_catalog_id, idx_t next_file_id)
	    : snapshot_id(snapshot_id), schema_version(schema_version), next_catalog_id(next_catalog_id),
	      next_file_id(next_file_id) {
	}

	DuckLakeSnapshot()
	    : snapshot_id(DConstants::INVALID_INDEX), schema_version(DConstants::INVALID_INDEX),
	      next_catalog_id(DConstants::INVALID_INDEX), next_file_id(DConstants::INVALID_INDEX) {
	}

	idx_t snapshot_id;
	idx_t schema_version;
	idx_t next_catalog_id;
	idx_t next_file_id;
};

} // namespace duckdb
