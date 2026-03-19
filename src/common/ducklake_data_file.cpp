#include "common/ducklake_data_file.hpp"

namespace duckdb {

DuckLakeDataFile::DuckLakeDataFile(const DuckLakeDataFile &other) {
	file_name = other.file_name;
	row_count = other.row_count;
	file_size_bytes = other.file_size_bytes;
	footer_size = other.footer_size;
	partition_id = other.partition_id;
	delete_files = other.delete_files;
	column_stats = other.column_stats;
	partition_values = other.partition_values;
	encryption_key = other.encryption_key;
	mapping_id = other.mapping_id;
	begin_snapshot = other.begin_snapshot;
	max_partial_file_snapshot = other.max_partial_file_snapshot;
	flush_row_id_start = other.flush_row_id_start;
	created_by_ducklake = other.created_by_ducklake;
}

DuckLakeDataFile &DuckLakeDataFile::operator=(const DuckLakeDataFile &other) {
	file_name = other.file_name;
	row_count = other.row_count;
	file_size_bytes = other.file_size_bytes;
	footer_size = other.footer_size;
	partition_id = other.partition_id;
	delete_files = other.delete_files;
	column_stats = other.column_stats;
	partition_values = other.partition_values;
	encryption_key = other.encryption_key;
	mapping_id = other.mapping_id;
	begin_snapshot = other.begin_snapshot;
	max_partial_file_snapshot = other.max_partial_file_snapshot;
	flush_row_id_start = other.flush_row_id_start;
	created_by_ducklake = other.created_by_ducklake;
	return *this;
}

} // namespace duckdb
