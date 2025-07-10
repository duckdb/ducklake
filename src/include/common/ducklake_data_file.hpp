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

struct DuckLakeDeleteFile {
	DataFileIndex data_file_id;
	string data_file_path;
	string file_name;
	idx_t delete_count;
	idx_t file_size_bytes;
	idx_t footer_size;
	string encryption_key;
	bool overwrites_existing_delete = false;
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
	unique_ptr<DuckLakeDeleteFile> delete_file;
	map<FieldIndex, DuckLakeColumnStats> column_stats;
	vector<DuckLakeFilePartition> partition_values;
	string encryption_key;
	MappingIndex mapping_id;
	optional_idx begin_snapshot;
	optional_idx max_partial_file_snapshot;
};

} // namespace duckdb
