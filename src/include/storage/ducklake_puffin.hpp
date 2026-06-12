//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_puffin.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/set.hpp"

namespace duckdb {
class FileSystem;
struct DuckLakeDeletionVectorData;

//! A deletion-vector blob within a puffin container
struct DuckLakePuffinBlob {
	//! DuckLake snapshot of the blob (invalid if assigned at commit)
	optional_idx snapshot_id;
	idx_t offset = 0;
	idx_t length = 0;
};

struct DuckLakePuffinWriteResult {
	idx_t file_size_bytes = 0;
	idx_t footer_size = 0;
	idx_t delete_count = 0;
};

//! Writes deletion vectors into an Iceberg puffin container
struct DuckLakePuffinWriter {
	struct BlobInput {
		optional_idx snapshot_id;
		//! Complete deletion vector as of snapshot_id (cumulative, not a delta)
		const set<idx_t> *positions = nullptr;
	};

	static DuckLakePuffinWriteResult Write(FileSystem &fs, const string &path, const string &data_file_path,
	                                       const vector<BlobInput> &blobs);
};

//! Reads deletion vectors from an Iceberg puffin container
class DuckLakePuffinReader {
public:
	DuckLakePuffinReader(data_ptr_t data, idx_t size, const string &path);

	const vector<DuckLakePuffinBlob> &Blobs() const {
		return blobs;
	}
	unique_ptr<DuckLakeDeletionVectorData> DecodeBlob(const DuckLakePuffinBlob &blob) const;

private:
	void ParseFooter();

private:
	data_ptr_t data;
	idx_t size;
	string path;
	vector<DuckLakePuffinBlob> blobs;
};

} // namespace duckdb
