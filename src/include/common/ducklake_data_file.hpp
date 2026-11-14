//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_data_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_stats.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "common/index.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct DuckLakeFilePartition {
	idx_t partition_column_idx;
	Value partition_value;
};

enum class DeleteFileFormat : uint8_t {
	PARQUET, //! Positional delete file in Parquet format
	PUFFIN   //! Deletion vector in Puffin format (Iceberg V3)
};

string DeleteFileFormatToString(DeleteFileFormat format);
DeleteFileFormat DeleteFileFormatFromString(const string &str);

enum class DeleteFileSource : uint8_t {
	REGULAR, //! Regular delete file created during a DELETE operation
	FLUSH    //! Delete file created during a flush operation (flushing inlined data)
};

struct DuckLakeOverwrittenDeleteFile {
	DataFileIndex delete_file_id;
	string path;
	//! The writer of the replaced file, the branch of the add event the commit read it through
	idx_t branch_id = 0;
};

struct DuckLakeDeleteFile {
	DataFileIndex data_file_id;
	string data_file_path;
	string file_name;
	DeleteFileFormat format = DeleteFileFormat::PARQUET;
	idx_t delete_count;
	idx_t file_size_bytes;
	idx_t footer_size;
	optional_idx row_group_count;
	string encryption_key;
	bool overwrites_existing_delete = false;
	//! The old delete file being overwritten (for deletion from metadata and disk)
	DuckLakeOverwrittenDeleteFile overwritten_delete_file;
	optional_idx begin_snapshot;
	//! Optional max_snapshot information for partial deletion files.
	optional_idx max_snapshot;
	DeleteFileSource source = DeleteFileSource::REGULAR;

	//! Whether the new deletes are dated to max_snapshot, which has to be the commit snapshot
	bool DatesNewDeletes() const {
		return source == DeleteFileSource::REGULAR && max_snapshot.IsValid();
	}
};

//! An end another branch wrote on a row a flush moved, the deletion row of the written file at that branch
struct DuckLakeRehomedDeletion {
	idx_t position;
	idx_t snapshot_id;
	idx_t branch_id;
};

//! The key of such an end in the shared companion of the inlined rows
struct DuckLakeRehomedEnd {
	idx_t branch_id;
	idx_t row_id;
	idx_t row_snapshot;
};

struct DuckLakeDataFile {
	string file_name;
	idx_t row_count;
	idx_t file_size_bytes;
	optional_idx footer_size;
	optional_idx row_group_count;
	optional_idx partition_id;
	vector<DuckLakeDeleteFile> delete_files;
	map<FieldIndex, DuckLakeColumnStats> column_stats;
	vector<DuckLakeFilePartition> partition_values;
	string encryption_key;
	MappingIndex mapping_id;
	optional_idx begin_snapshot;
	optional_idx max_partial_file_snapshot;
	//! The row_id_start extracted from the file (for flushed files that have embedded row_ids)
	optional_idx flush_row_id_start;
	//! The ends of other branches on the rows of a flushed file
	vector<DuckLakeRehomedDeletion> rehomed_deletions;
	//! If the file was created by ducklake
	bool created_by_ducklake = true;
};

} // namespace duckdb
