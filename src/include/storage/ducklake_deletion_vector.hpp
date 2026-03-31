//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_deletion_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/unordered_map.hpp"
#include <roaring/roaring.hh>

namespace duckdb {

//! DuckLakeDeletionVectorData holds the roaring bitmap representation of deleted row positions.
//! Follows the Iceberg deletion-vector-v1 blob format (puffin spec).
struct DuckLakeDeletionVectorData {
public:
	//! Deserialize a deletion vector from a puffin blob
	static unique_ptr<DuckLakeDeletionVectorData> FromBlob(data_ptr_t blob_start, idx_t blob_length);
	//! Serialize deleted row positions into a deletion-vector-v1 puffin blob
	static vector<data_t> ToBlob(const set<idx_t> &positions);

	//! Convert the bitmaps to a sorted set of deleted row positions
	void ToSet(set<idx_t> &out) const;

public:
	//! Map from high 32-bit key to roaring bitmap of low 32-bit values
	unordered_map<int32_t, roaring::Roaring> bitmaps;
};

} // namespace duckdb
