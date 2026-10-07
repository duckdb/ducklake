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
#include <roaring/roaring64map.hh>

namespace duckdb {

//! DuckLakeDeletionVectorData holds the roaring bitmap representation of deleted row positions.
//! Follows the Iceberg deletion-vector-v1 blob format (puffin spec).
struct DuckLakeDeletionVectorData {
public:
	//! Iceberg deletion-vector-v1 blob magic bytes
	static constexpr data_t DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};

public:
	//! Deserialize a deletion vector from a puffin blob
	static unique_ptr<DuckLakeDeletionVectorData> FromBlob(data_ptr_t blob_start, idx_t blob_length);
	//! Serialize deleted row positions into a deletion-vector-v1 puffin blob
	static vector<data_t> ToBlob(const set<idx_t> &positions);

public:
	roaring::Roaring64Map bitmap;
};

} // namespace duckdb
