//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_commit_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_snapshot.hpp"
#include "common/index.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_view_entry.hpp"

namespace duckdb {

struct NewTableInfo {
	vector<DuckLakeTableInfo> new_tables;
	vector<DuckLakeViewInfo> new_views;
	vector<DuckLakePartitionInfo> new_partition_keys;
	vector<DuckLakeTagInfo> new_tags;
	vector<DuckLakeColumnTagInfo> new_column_tags;
	vector<DuckLakeDroppedColumn> dropped_columns;
	vector<DuckLakeNewColumn> new_columns;
	vector<DuckLakeTableInfo> new_inlined_data_tables;
	vector<DuckLakeSortInfo> new_sort_keys;
};

struct NewMacroInfo {
	vector<DuckLakeMacroInfo> new_macros;
};

struct NewNameMapInfo {
	vector<DuckLakeColumnMappingInfo> new_column_mappings;
};

struct NewDataInfo {
	vector<DuckLakeFileInfo> new_files;
	vector<DuckLakeInlinedDataInfo> new_inlined_data;
};

struct CompactionStatsChange {
	idx_t removed_record_count = 0;
	idx_t removed_file_size_bytes = 0;
	idx_t added_record_count = 0;
	idx_t added_file_size_bytes = 0;
};

struct CompactionInformation {
	vector<DuckLakeCompactedFileInfo> compacted_files;
	vector<DuckLakeFileInfo> new_files;
	map<TableIndex, CompactionStatsChange> stats_changes;
};

struct DuckLakeCommitState {
	explicit DuckLakeCommitState(DuckLakeSnapshot &snapshot) : commit_snapshot(snapshot) {
	}

	DuckLakeSnapshot &commit_snapshot;
	map<SchemaIndex, SchemaIndex> committed_schemas;
	map<TableIndex, TableIndex> committed_tables;
	map<idx_t, idx_t> committed_partition_ids;
	map<MappingIndex, MappingIndex> committed_mapping_indexes;
	map<TableIndex, vector<DuckLakeDeleteFile>> local_delete_files;

	void RemapIdentifier(SchemaIndex &schema_id) const {
		auto entry = committed_schemas.find(schema_id);
		if (entry != committed_schemas.end()) {
			schema_id = entry->second;
		}
	}
	void RemapIdentifier(TableIndex &table_id) const {
		auto entry = committed_tables.find(table_id);
		if (entry != committed_tables.end()) {
			table_id = entry->second;
		}
	}
	void RemapPartitionId(optional_idx &partition_id) const {
		if (!partition_id.IsValid()) {
			return;
		}
		auto entry = committed_partition_ids.find(partition_id.GetIndex());
		if (entry != committed_partition_ids.end()) {
			partition_id = entry->second;
		}
	}
	void RemapMappingIndex(MappingIndex &table_id) const {
		auto entry = committed_mapping_indexes.find(table_id);
		if (entry != committed_mapping_indexes.end()) {
			table_id = entry->second;
		}
	}

	SchemaIndex GetSchemaId(DuckLakeSchemaEntry &schema) const {
		auto schema_id = schema.GetSchemaId();
		RemapIdentifier(schema_id);
		return schema_id;
	}
	TableIndex GetTableId(DuckLakeTableEntry &table) const {
		auto table_id = table.GetTableId();
		RemapIdentifier(table_id);
		return table_id;
	}
	TableIndex GetTableId(TableIndex table_id) const {
		RemapIdentifier(table_id);
		return table_id;
	}
	TableIndex GetViewId(DuckLakeViewEntry &view) const {
		auto view_id = view.GetViewId();
		RemapIdentifier(view_id);
		return view_id;
	}
};

} // namespace duckdb
