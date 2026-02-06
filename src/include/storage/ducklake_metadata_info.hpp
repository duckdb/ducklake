//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_metadata_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "common/index.hpp"
#include "common/ducklake_data_file.hpp"
#include "common/ducklake_name_map.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/enums/order_type.hpp"

namespace duckdb {
struct DuckLakeVariantStatsInfo;

//===--------------------------------------------------------------------===//
// Compaction Type
//===--------------------------------------------------------------------===//

enum class CompactionType {
	MERGE_ADJACENT_TABLES, // Merge adjacent tables
	REWRITE_DELETES        // Rewrite deletes that delete more than a % of the table, might also do merge of files.
};

enum class CleanupType {
	OLD_FILES,     // If the files are old, e.g., from an expired snapshot and can now be removed.
	ORPHANED_FILES // If the file is an orphan e.g., the file was generated but never committed to the catalog
};

struct DuckLakeTag {
	string key;
	string value;
};

struct DuckLakeSchemaSetting {
	SchemaIndex schema_id;
	DuckLakeTag tag;
};

struct DuckLakeTableSetting {
	TableIndex table_id;
	DuckLakeTag tag;
};

struct DuckLakeMetadata {
	vector<DuckLakeTag> tags;
	vector<DuckLakeSchemaSetting> schema_settings;
	vector<DuckLakeTableSetting> table_settings;
};

struct DuckLakeSchemaInfo {
	SchemaIndex id;
	string uuid;
	string name;
	string path;
	vector<DuckLakeTag> tags;
};

struct DuckLakeColumnInfo {
	FieldIndex id;
	string name;
	string type;
	Value initial_default;
	Value default_value;
	string default_value_type;
	bool nulls_allowed {};
	vector<DuckLakeColumnInfo> children;
	vector<DuckLakeTag> tags;
};

struct DuckLakeInlinedTableInfo {
	string table_name;
	idx_t schema_version;
};

struct DuckLakeTableInfo {
	TableIndex id;
	SchemaIndex schema_id;
	string uuid;
	string name;
	string path;
	vector<DuckLakeColumnInfo> columns;
	vector<DuckLakeTag> tags;
	vector<DuckLakeInlinedTableInfo> inlined_data_tables;
};

//! Stores the information on macro parameters
struct DuckLakeMacroParameters {
	string parameter_name;
	string parameter_type;
	Value default_value;
	string default_value_type;
};

//! Stores information on macro implementations, since one macro can have multiple implementations
struct DuckLakeMacroImplementation {
	string dialect;
	string sql;
	string type;
	vector<DuckLakeMacroParameters> parameters;
};

//! Stores the actual macro info
struct DuckLakeMacroInfo {
	SchemaIndex schema_id;
	MacroIndex macro_id;
	string macro_name;
	vector<DuckLakeMacroImplementation> implementations;
};

struct DuckLakeColumnStatsInfo {
	FieldIndex column_id;
	string value_count;
	string null_count;
	string column_size_bytes;
	string min_val;
	string max_val;
	string contains_nan;
	string extra_stats;
	vector<DuckLakeVariantStatsInfo> variant_stats;

	static DuckLakeColumnStatsInfo FromColumnStats(FieldIndex field_id, const DuckLakeColumnStats &stats);
};

struct DuckLakeVariantStatsInfo {
	string field_name;
	string shredded_type;
	DuckLakeColumnStatsInfo field_stats;
};

struct DuckLakeFilePartitionInfo {
	idx_t partition_column_idx;
	string partition_value;
};

struct DuckLakeFileInfo {
	DataFileIndex id;
	TableIndex table_id;
	string file_name;
	idx_t row_count;
	idx_t file_size_bytes;
	optional_idx footer_size;
	optional_idx row_id_start;
	optional_idx partition_id;
	optional_idx begin_snapshot;
	optional_idx max_partial_file_snapshot;
	string encryption_key;
	MappingIndex mapping_id;
	vector<DuckLakeColumnStatsInfo> column_stats;
	vector<DuckLakeFilePartitionInfo> partition_values;
};

struct DuckLakeInlinedDataInfo {
	TableIndex table_id;
	idx_t row_id_start;
	optional_ptr<DuckLakeInlinedData> data;
};

struct DuckLakeDeletedInlinedDataInfo {
	TableIndex table_id;
	string table_name;
	vector<idx_t> deleted_row_ids;
};

//! Info for all inlined file deletions for a single table
struct DuckLakeInlinedFileDeletionInfo {
	TableIndex table_id;
	//! Maps file_id -> set of deleted row_ids
	DuckLakeInlinedFileDeletes file_deletions;
};

struct DuckLakeDeleteFileInfo {
	DataFileIndex id;
	TableIndex table_id;
	DataFileIndex data_file_id;
	string path;
	idx_t delete_count;
	idx_t file_size_bytes;
	idx_t footer_size;
	string encryption_key;
	optional_idx begin_snapshot;
	//! Optional max_snapshot information for partial deletion files.
	optional_idx max_snapshot;
};

struct DuckLakePartitionFieldInfo {
	idx_t partition_key_index = 0;
	FieldIndex field_id;
	string transform;
	bool operator!=(const DuckLakePartitionFieldInfo &new_field) const {
		return field_id != new_field.field_id || transform != new_field.transform;
	}
};

struct DuckLakePartitionInfo {
	optional_idx id;
	TableIndex table_id;
	vector<DuckLakePartitionFieldInfo> fields;
	bool operator==(const DuckLakePartitionInfo &new_partition) const {
		if (table_id != new_partition.table_id || fields.size() != new_partition.fields.size()) {
			return false;
		}
		for (idx_t i = 0; i < fields.size(); i++) {
			if (fields[i] != new_partition.fields[i]) {
				return false;
			}
		}
		return true;
	}
	bool operator!=(vector<DuckLakePartitionInfo>::const_reference value) const {
		return !(*this == value);
	}
};

struct DuckLakeSortFieldInfo {
	idx_t sort_key_index = 0;
	string expression;
	string dialect;
	OrderType sort_direction;
	OrderByNullType null_order;
	bool operator!=(const DuckLakeSortFieldInfo &new_field) const {
		return expression != new_field.expression || dialect != new_field.dialect ||
		       sort_direction != new_field.sort_direction || null_order != new_field.null_order;
	}
};

struct DuckLakeSortInfo {
	optional_idx id;
	TableIndex table_id;
	vector<DuckLakeSortFieldInfo> fields;
	bool operator==(const DuckLakeSortInfo &new_sort) const {
		if (table_id != new_sort.table_id || fields.size() != new_sort.fields.size()) {
			return false;
		}
		for (idx_t i = 0; i < fields.size(); i++) {
			if (fields[i] != new_sort.fields[i]) {
				return false;
			}
		}
		return true;
	}
	bool operator!=(vector<DuckLakeSortInfo>::const_reference value) const {
		return !(*this == value);
	}
};

struct DuckLakeGlobalColumnStatsInfo {
	FieldIndex column_id;

	bool contains_null = false;
	bool has_contains_null = false;

	bool contains_nan = false;
	bool has_contains_nan = false;

	string min_val;
	bool has_min = false;

	string max_val;
	bool has_max = false;

	string extra_stats;
	bool has_extra_stats = false;
};

struct DuckLakeGlobalStatsInfo {
	TableIndex table_id;
	bool initialized;
	idx_t record_count;
	idx_t next_row_id;
	idx_t table_size_bytes;
	vector<DuckLakeGlobalColumnStatsInfo> column_stats;
};

struct SnapshotChangeInfo {
	string changes_made;
};

struct SnapshotDeletedFromFiles {
	set<DataFileIndex> deleted_from_files;
};

struct DuckLakeSnapshotInfo {
	idx_t id;
	timestamp_tz_t time;
	idx_t schema_version;
	SnapshotChangeInfo change_info;
	Value author;
	Value commit_message;
	Value commit_extra_info;
};

struct DuckLakeViewInfo {
	TableIndex id;
	SchemaIndex schema_id;
	string uuid;
	string name;
	string dialect;
	vector<string> column_aliases;
	string sql;
	vector<DuckLakeTag> tags;
};

struct DuckLakeTagInfo {
	idx_t id;
	string key;
	Value value;
};

struct DuckLakeColumnTagInfo {
	TableIndex table_id;
	FieldIndex field_index;
	string key;
	Value value;
};

struct DuckLakeDroppedColumn {
	TableIndex table_id;
	FieldIndex field_id;
};

struct DuckLakeNewColumn {
	TableIndex table_id;
	DuckLakeColumnInfo column_info;
	optional_idx parent_idx;
};

struct DuckLakeCatalogInfo {
	vector<DuckLakeSchemaInfo> schemas;
	vector<DuckLakeTableInfo> tables;
	vector<DuckLakeViewInfo> views;
	vector<DuckLakeMacroInfo> macros;
	vector<DuckLakePartitionInfo> partitions;
	vector<DuckLakeSortInfo> sorts;
};

struct DuckLakeFileData {
	string path;
	string encryption_key;
	idx_t file_size_bytes = 0;
	optional_idx footer_size;
};

enum class DuckLakeDataType {
	DATA_FILE,
	INLINED_DATA,
	TRANSACTION_LOCAL_INLINED_DATA,
};

struct DuckLakeFileListEntry {
	optional_idx data_file_id;
	DuckLakeFileData file;
	DuckLakeFileData delete_file;
	optional_idx row_id_start;
	optional_idx snapshot_id;
	optional_idx max_row_count;
	//! Upper bound filter, we only include rows where _ducklake_internal_snapshot_id <= snapshot_filter
	optional_idx snapshot_filter_max;
	//! Lower bound filter, we only include rows where _ducklake_internal_snapshot_id >= snapshot_filter_min
	optional_idx snapshot_filter_min;
	MappingIndex mapping_id;
	DuckLakeDataType data_type = DuckLakeDataType::DATA_FILE;
	//! The data file id
	DataFileIndex file_id;
	//! Inlined file deletions (row positions that have been deleted and stored in the metadata database)
	set<idx_t> inlined_file_deletions;
	//! Column min/max values for dynamic filter pushdown
	unordered_map<idx_t, pair<string, string>> column_min_max;
};

struct DuckLakeDeleteScanEntry {
	DuckLakeFileData file;
	DuckLakeFileData delete_file;
	DuckLakeFileData previous_delete_file;
	idx_t row_count;
	optional_idx row_id_start;
	MappingIndex mapping_id;
	optional_idx snapshot_id;
	//! The start of the snapshot range for filtering
	optional_idx start_snapshot;
	//! The end of the snapshot range for filtering
	optional_idx end_snapshot;
	//! Data file ID for matching inlined deletions
	DataFileIndex file_id;
	//! Inlined file deletions {row_id -> snapshot_id}
	unordered_map<idx_t, idx_t> inlined_file_deletions;
};

struct DuckLakeFileListExtendedEntry {
	DataFileIndex file_id;
	DataFileIndex delete_file_id;
	DuckLakeFileData file;
	DuckLakeFileData delete_file;
	optional_idx row_id_start;
	optional_idx snapshot_id;
	optional_idx delete_file_begin_snapshot;
	idx_t row_count;
	idx_t delete_count = 0;
	DuckLakeDataType data_type = DuckLakeDataType::DATA_FILE;
};

struct DuckLakeCompactionBaseFileData {
	DataFileIndex id;
	DuckLakeFileData data;
	idx_t row_count = 0;
	idx_t begin_snapshot = 0;
	optional_idx end_snapshot;
	optional_idx max_row_count;
};

struct DuckLakeFileForCleanup {
	DataFileIndex id;
	string path;
	timestamp_tz_t time;
};

struct DuckLakeCompactionFileData : public DuckLakeCompactionBaseFileData {
	optional_idx row_id_start;
	MappingIndex mapping_id;
	optional_idx partition_id;
	vector<string> partition_values;
};

struct DuckLakeCompactionDeleteFileData : public DuckLakeCompactionBaseFileData {
	DataFileIndex delete_file_id;
	optional_idx max_snapshot;
};

struct DuckLakeCompactionFileEntry {
	DuckLakeCompactionFileData file;
	// optional_idx
	vector<DuckLakeCompactionDeleteFileData> delete_files;
	optional_idx max_partial_file_snapshot;
	idx_t schema_version;
	//! Whether this file has inlined deletions (stored in metadata database rather than delete files)
	bool has_inlined_deletions = false;
};

struct DuckLakeRewriteFileEntry {
	DuckLakeCompactionFileData file;
	vector<DuckLakeCompactionDeleteFileData> delete_files;
	optional_idx max_partial_file_snapshot;
	idx_t schema_version;
};

struct DuckLakeCompactionEntry {
	vector<DuckLakeCompactionFileEntry> source_files;
	DuckLakeDataFile written_file;
	optional_idx row_id_start;
	CompactionType type;
};

struct DuckLakeCompactedFileInfo {
	string path;
	DataFileIndex source_id;
	DataFileIndex new_id;
	//! Info on delete files, in case the compaction is a delete-rewrite
	string delete_file_path;
	DataFileIndex delete_file_id;
	optional_idx start_snapshot;
	TableIndex table_index;
	optional_idx delete_file_start_snapshot;
	optional_idx delete_file_end_snapshot;
};

struct DuckLakeMergeAdjacentOptions {
	uint64_t max_files;
	optional_idx min_file_size;
	optional_idx max_file_size;
};

struct DuckLakeFileSizeOptions {
	optional_idx min_file_size;
	optional_idx max_file_size;
	idx_t target_file_size;
};

struct DuckLakeTableSizeInfo {
	SchemaIndex schema_id;
	TableIndex table_id;
	string table_name;
	string table_uuid;
	idx_t file_size_bytes = 0;
	idx_t delete_file_size_bytes = 0;
	idx_t file_count = 0;
	idx_t delete_file_count = 0;
};

struct DuckLakePath {
	string path;
	bool path_is_relative;
};

struct DuckLakeSnapshotCommit {
	//! Author of the commit
	Value author;
	//! The commit message for the snapshot
	Value commit_message;
	//! Additional extra info about the commit
	Value commit_extra_info;
	//! If the user set the commit info for the snapshot
	bool is_commit_info_set = false;
};

struct DuckLakeConfigOption {
	DuckLakeTag option;
	//! schema_id, if scoped to a schema
	SchemaIndex schema_id;
	//! table_id, if scoped to a table
	TableIndex table_id;
};

struct DuckLakeNameMapColumnInfo {
	idx_t column_id;
	string source_name;
	FieldIndex target_field_id;
	bool hive_partition = false;
	optional_idx parent_column;
};

struct DuckLakeColumnMappingInfo {
	TableIndex table_id;
	MappingIndex mapping_id;
	string map_type;
	vector<DuckLakeNameMapColumnInfo> map_columns;
};

} // namespace duckdb
