#include "common/ducklake_data_file.hpp"

namespace duckdb {

DuckLakeDataFile::DuckLakeDataFile(const DuckLakeDataFile &other) {
	file_name = other.file_name;
	row_count = other.row_count;
	file_size_bytes = other.file_size_bytes;
	footer_size = other.footer_size;
	partition_id = other.partition_id;
	if (other.delete_file) {
		delete_file = make_uniq<DuckLakeDeleteFile>(*other.delete_file);
	}
	column_stats = other.column_stats;
	partition_values = other.partition_values;
	encryption_key = other.encryption_key;
	mapping_id = other.mapping_id;
	begin_snapshot = other.begin_snapshot;
	max_partial_file_snapshot = other.max_partial_file_snapshot;
}

DuckLakeDataFile &DuckLakeDataFile::operator=(const DuckLakeDataFile &other) {
	file_name = other.file_name;
	row_count = other.row_count;
	file_size_bytes = other.file_size_bytes;
	footer_size = other.footer_size;
	partition_id = other.partition_id;
	if (other.delete_file) {
		delete_file = make_uniq<DuckLakeDeleteFile>(*other.delete_file);
	}
	column_stats = other.column_stats;
	partition_values = other.partition_values;
	encryption_key = other.encryption_key;
	mapping_id = other.mapping_id;
	begin_snapshot = other.begin_snapshot;
	max_partial_file_snapshot = other.max_partial_file_snapshot;
	return *this;
}

} // namespace duckdb
