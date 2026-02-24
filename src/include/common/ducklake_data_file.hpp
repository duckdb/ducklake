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

namespace duckdb {

struct DuckLakeFilePartition {
	idx_t partition_column_idx;
	string partition_value;
};

enum class DeleteFileSource : uint8_t {
	REGULAR, //! Regular delete file created during a DELETE operation
	FLUSH    //! Delete file created during a flush operation (flushing inlined data)
};

struct DuckLakeOverwrittenDeleteFile {
	DataFileIndex delete_file_id;
	string path;
};

struct DuckLakeDeleteFile {
	DataFileIndex data_file_id;
	string data_file_path;
	string file_name;
	idx_t delete_count;
	idx_t file_size_bytes;
	idx_t footer_size;
	string encryption_key;
	bool overwrites_existing_delete = false;
	//! The old delete file being overwritten (for deletion from metadata and disk)
	DuckLakeOverwrittenDeleteFile overwritten_delete_file;
	optional_idx begin_snapshot;
	//! Optional max_snapshot information for partial deletion files.
	optional_idx max_snapshot;
	DeleteFileSource source = DeleteFileSource::REGULAR;
};

struct DuckLakeDataFile {
	DuckLakeDataFile() = default;
	DuckLakeDataFile(const DuckLakeDataFile &other);
	DuckLakeDataFile &operator=(const DuckLakeDataFile &);

	string file_name;
	idx_t row_count;
	idx_t file_size_bytes;
	optional_idx footer_size;
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
	//! If the file was created by ducklake
	bool created_by_ducklake = true;
};

} // namespace duckdb
