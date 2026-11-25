#include "storage/ducklake_staged_commit.hpp"

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

// The column-stats payload of the data-file and inlined column-stats tables, written by ColumnStatsValues and read
// by DuckLakeServerSideCommit::ReadColumnStatsRow
#define STAGED_STAT_COLUMNS                                                                                            \
	"column_size_bytes BIGINT, has_num_values BOOLEAN, num_values BIGINT, "                                            \
	"has_null_count BOOLEAN, null_count BIGINT, has_min BOOLEAN, min_value VARCHAR, "                                  \
	"has_max BOOLEAN, max_value VARCHAR, has_contains_nan BOOLEAN, contains_nan BOOLEAN, "                             \
	"any_valid BOOLEAN, extra_stats VARCHAR, min_is_exact BOOLEAN, max_is_exact BOOLEAN"

struct StagedTableDef {
	DuckLakeStagedTableType type;
	const char *name;
	const char *columns;
};

static const StagedTableDef STAGED_TABLES[] = {
    {DuckLakeStagedTableType::COMMIT_HEADER, "ducklake_staged_commit",
     "commit_author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR, "
     "transaction_snapshot_id BIGINT, data_path VARCHAR, path_separator VARCHAR, "
     "transaction_next_catalog_id UBIGINT, transaction_next_file_id UBIGINT"},
    {DuckLakeStagedTableType::DATA_FILE, "ducklake_staged_data_file",
     "data_file_id BIGINT, table_id BIGINT, file_order BIGINT, "
     "path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, "
     "record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, "
     "row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, "
     "mapping_id UBIGINT, partial_max BIGINT, begin_snapshot BIGINT, "
     "compaction_id BIGINT, row_group_count BIGINT, created_by_ducklake BOOLEAN"},
    {DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS, "ducklake_staged_data_file_column_stats",
     "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, " STAGED_STAT_COLUMNS},
    {DuckLakeStagedTableType::DATA_FILE_PARTITION, "ducklake_staged_data_file_partition",
     "local_file_id BIGINT, partition_column_idx BIGINT, partition_value VARCHAR"},
    {DuckLakeStagedTableType::DELETE_FILE, "ducklake_staged_delete_file",
     "table_id BIGINT, data_file_path VARCHAR, data_file_id BIGINT, "
     "file_name VARCHAR, format VARCHAR, delete_count BIGINT, "
     "file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
     "begin_snapshot BIGINT, max_snapshot BIGINT, source VARCHAR, "
     "overwrites_existing_delete BOOLEAN, "
     "overwrite_delete_file_id BIGINT, overwrite_delete_file_path VARCHAR, "
     "attached_local_file_id BIGINT, row_group_count BIGINT"},
    {DuckLakeStagedTableType::INLINED_DATA, "ducklake_staged_inlined_data",
     "table_id BIGINT, has_preserved_row_ids BOOLEAN"},
    {DuckLakeStagedTableType::INLINED_ROW, "ducklake_staged_inlined_row",
     "table_id BIGINT, row_order BIGINT, preserved_row_id BIGINT, value_literals VARCHAR"},
    {DuckLakeStagedTableType::INLINED_COLUMN_STATS, "ducklake_staged_inlined_column_stats",
     "table_id BIGINT, column_id BIGINT, " STAGED_STAT_COLUMNS},
    {DuckLakeStagedTableType::INLINED_DELETE, "ducklake_staged_inlined_delete",
     "table_id BIGINT, inlined_table_name VARCHAR, deleted_row_id BIGINT"},
    {DuckLakeStagedTableType::INLINED_FILE_DELETE, "ducklake_staged_inlined_file_delete",
     "table_id BIGINT, file_id BIGINT, deleted_row_id BIGINT"},
    {DuckLakeStagedTableType::DROPPED_FILE, "ducklake_staged_dropped_file", "path VARCHAR, data_file_id BIGINT"},
    {DuckLakeStagedTableType::TABLES_DELETED_FROM, "ducklake_staged_tables_deleted_from", "table_id BIGINT"},
    {DuckLakeStagedTableType::TABLES_DELETE_ATTEMPTED, "ducklake_staged_tables_delete_attempted", "table_id BIGINT"},
    {DuckLakeStagedTableType::FLUSHED_INLINED, "ducklake_staged_flushed_inlined",
     "inlined_table_name VARCHAR, schema_version BIGINT, flush_snapshot_id BIGINT"},
    {DuckLakeStagedTableType::COMPACTION, "ducklake_staged_compaction",
     "compaction_id BIGINT, table_id BIGINT, compaction_type VARCHAR, row_id_start BIGINT"},
    {DuckLakeStagedTableType::COMPACTION_SOURCE, "ducklake_staged_compaction_source",
     "compaction_id BIGINT, source_order BIGINT, "
     "source_data_file_id BIGINT, source_path VARCHAR, "
     "source_row_count BIGINT, source_begin_snapshot BIGINT, "
     "source_max_partial_file_snapshot BIGINT, source_inlined_delete_count BIGINT, "
     "last_delete_file_id BIGINT, last_delete_path VARCHAR, "
     "last_delete_row_count BIGINT, last_delete_end_snapshot BIGINT"},
    {DuckLakeStagedTableType::NAME_MAP, "ducklake_staged_name_map", "map_id UBIGINT, table_id BIGINT"},
    {DuckLakeStagedTableType::NAME_MAP_ENTRY, "ducklake_staged_name_map_entry",
     "map_id UBIGINT, entry_id BIGINT, parent_entry_id BIGINT, "
     "source_name VARCHAR, target_field_id BIGINT, hive_partition BOOLEAN"}};
#undef STAGED_STAT_COLUMNS

const char *DuckLakeStagedTable::BaseName(DuckLakeStagedTableType type) {
	for (auto &table : STAGED_TABLES) {
		if (table.type == type) {
			return table.name;
		}
	}
	throw InternalException("Unknown DuckLakeStagedTableType");
}

string DuckLakeStagedTable::CreateAllSql() {
	string sql;
	for (auto &table : STAGED_TABLES) {
		sql += StringUtil::Format("CREATE TEMPORARY TABLE IF NOT EXISTS %s(%s);", table.name, table.columns);
	}
	return sql;
}

string DuckLakeStagedTable::TruncateAllSql() {
	string sql;
	for (auto &table : STAGED_TABLES) {
		sql += StringUtil::Format("TRUNCATE %s;", table.name);
	}
	return sql;
}

const char *DuckLakeStagedTable::DeleteFileSourceToString(DeleteFileSource source) {
	return source == DeleteFileSource::FLUSH ? "FLUSH" : "REGULAR";
}

DeleteFileSource DuckLakeStagedTable::DeleteFileSourceFromString(const string &source) {
	return source == "FLUSH" ? DeleteFileSource::FLUSH : DeleteFileSource::REGULAR;
}

//! The rows staged for each table, in the order they are inserted
using StagedRows = map<DuckLakeStagedTableType, vector<string>>;

static string ColumnStatsValues(const DuckLakeColumnStats &s) {
	string num_values = s.has_num_values ? std::to_string(s.num_values) : "NULL";
	string null_count = s.has_null_count ? std::to_string(s.null_count) : "NULL";
	string min_val = s.has_min ? DuckLakeUtil::StatsToString(s.min) : "NULL";
	string max_val = s.has_max ? DuckLakeUtil::StatsToString(s.max) : "NULL";
	bool has_min_emit = s.has_min && min_val != "NULL";
	bool has_max_emit = s.has_max && max_val != "NULL";
	string contains_nan = s.has_contains_nan ? DuckLakeUtil::BoolLiteral(s.contains_nan) : "NULL";
	string extra_stats = "NULL";
	if (s.extra_stats) {
		string serialized;
		if (s.extra_stats->TrySerialize(serialized) && !serialized.empty()) {
			// TrySerialize already returns a ready-to-emit SQL literal; emit it as-is.
			extra_stats = serialized;
		}
	}
	return StringUtil::Format(
	    "%llu, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s", s.column_size_bytes,
	    DuckLakeUtil::BoolLiteral(s.has_num_values), num_values, DuckLakeUtil::BoolLiteral(s.has_null_count),
	    null_count, DuckLakeUtil::BoolLiteral(has_min_emit), min_val, DuckLakeUtil::BoolLiteral(has_max_emit), max_val,
	    DuckLakeUtil::BoolLiteral(s.has_contains_nan), contains_nan, DuckLakeUtil::BoolLiteral(s.any_valid),
	    extra_stats, DuckLakeUtil::BoolLiteral(s.min_is_exact), DuckLakeUtil::BoolLiteral(s.max_is_exact));
}

//! Stages a data file with its column stats and partition values
static void StageDataFile(StagedRows &staged, const DuckLakeDataFile &file, idx_t local_file_id, TableIndex table_id,
                          idx_t file_order, const string &compaction_id_literal) {
	staged[DuckLakeStagedTableType::DATA_FILE].push_back(StringUtil::Format(
	    "(%llu, %llu, %llu, %s, false, 'parquet', %llu, %llu, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", local_file_id,
	    table_id.index, file_order, SQLString(file.file_name), file.row_count, file.file_size_bytes,
	    DuckLakeUtil::OptionalIdxOrNull(file.footer_size), DuckLakeUtil::OptionalIdxOrNull(file.flush_row_id_start),
	    DuckLakeUtil::OptionalIdxOrNull(file.partition_id), DuckLakeUtil::EncryptionKeyLiteral(file.encryption_key),
	    DuckLakeUtil::MappingIdOrNull(file.mapping_id), DuckLakeUtil::OptionalIdxOrNull(file.max_partial_file_snapshot),
	    DuckLakeUtil::OptionalIdxOrNull(file.begin_snapshot), compaction_id_literal,
	    DuckLakeUtil::OptionalIdxOrNull(file.row_group_count), DuckLakeUtil::BoolLiteral(file.created_by_ducklake)));
	for (auto &stat : file.column_stats) {
		staged[DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS].push_back(StringUtil::Format(
		    "(%llu, %llu, %llu, %s)", local_file_id, table_id.index, stat.first.index, ColumnStatsValues(stat.second)));
	}
	for (auto &part : file.partition_values) {
		staged[DuckLakeStagedTableType::DATA_FILE_PARTITION].push_back(
		    StringUtil::Format("(%llu, %llu, %s)", local_file_id, part.partition_column_idx,
		                       DuckLakeUtil::TextLiteralOrNull(part.partition_value)));
	}
}

//! Stages a delete file, of a committed data file by its path or of a staged one by its local id
static void StageDeleteFile(StagedRows &staged, const DuckLakeDeleteFile &file, TableIndex table_id,
                            optional_ptr<const string> data_file_path, optional_idx attached_local_file_id) {
	string data_file_path_literal = "NULL";
	string data_file_id = "NULL";
	bool overwrites_existing_delete = false;
	string overwrite_id = "NULL";
	string overwrite_path = "NULL";
	if (data_file_path) {
		data_file_path_literal = SQLString::ToString(*data_file_path);
		data_file_id = std::to_string(file.data_file_id.index);
		overwrites_existing_delete = file.overwrites_existing_delete;
		auto &overwritten = file.overwritten_delete_file;
		if (overwritten.delete_file_id.IsValid()) {
			// LCOV_EXCL_START
			overwrite_id = std::to_string(overwritten.delete_file_id.index);
			// LCOV_EXCL_STOP
		}
		if (!overwritten.path.empty()) {
			// LCOV_EXCL_START
			overwrite_path = SQLString::ToString(overwritten.path);
			// LCOV_EXCL_STOP
		}
	}
	staged[DuckLakeStagedTableType::DELETE_FILE].push_back(StringUtil::Format(
	    "(%llu, %s, %s, %s, %s, %llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s, %s, %s)", table_id.index,
	    data_file_path_literal, data_file_id, SQLString(file.file_name),
	    SQLString(DeleteFileFormatToString(file.format)), file.delete_count, file.file_size_bytes, file.footer_size,
	    DuckLakeUtil::EncryptionKeyLiteral(file.encryption_key), DuckLakeUtil::OptionalIdxOrNull(file.begin_snapshot),
	    DuckLakeUtil::OptionalIdxOrNull(file.max_snapshot),
	    SQLString(DuckLakeStagedTable::DeleteFileSourceToString(file.source)),
	    DuckLakeUtil::BoolLiteral(overwrites_existing_delete), overwrite_id, overwrite_path,
	    DuckLakeUtil::OptionalIdxOrNull(attached_local_file_id),
	    DuckLakeUtil::OptionalIdxOrNull(file.row_group_count)));
}

static void StageCommitHeader(StagedRows &staged, const DuckLakeSnapshotCommit &h, const DuckLakeSnapshot &snap,
                              const string &data_path, const string &separator) {
	bool has_snapshot = snap.snapshot_id != DConstants::INVALID_INDEX;
	string snap_id = has_snapshot ? std::to_string(snap.snapshot_id) : "NULL";
	string next_cat = has_snapshot ? std::to_string(snap.next_catalog_id) : "NULL";
	string next_file = has_snapshot ? std::to_string(snap.next_file_id) : "NULL";
	staged[DuckLakeStagedTableType::COMMIT_HEADER].push_back(StringUtil::Format(
	    "(%s, %s, %s, %s, %s, %s, %s, %s)", DuckLakeUtil::TextLiteralOrNull(h.author),
	    DuckLakeUtil::TextLiteralOrNull(h.commit_message), DuckLakeUtil::TextLiteralOrNull(h.commit_extra_info),
	    snap_id, SQLString::ToString(data_path), SQLString::ToString(separator), next_cat, next_file));
}

//! Stages the new data files and the delete files attached to them
static void StageDataFiles(StagedRows &staged, const LocalTableChanges &local_changes, idx_t &local_file_id) {
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.first;
		idx_t file_order = 0;
		for (auto &file : entry.second.new_data_files) {
			StageDataFile(staged, file, local_file_id, table_id, file_order, "NULL");
			for (auto &del : file.delete_files) {
				StageDeleteFile(staged, del, table_id, nullptr, local_file_id);
			}
			local_file_id++;
			file_order++;
		}
	}
}

//! Stages the inlined rows as tuple literals, with their column stats
static void StageInlinedData(StagedRows &staged, const LocalTableChanges &local_changes,
                             DuckLakeTransaction &transaction) {
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.first;
		auto &table_changes = entry.second;
		if (!table_changes.new_inlined_data) {
			continue;
		}
		auto &inlined = *table_changes.new_inlined_data;
		const bool has_preserved = inlined.HasPreservedRowIds();
		staged[DuckLakeStagedTableType::INLINED_DATA].push_back(
		    StringUtil::Format("(%llu, %s)", table_id.index, DuckLakeUtil::BoolLiteral(has_preserved)));
		auto rows = DuckLakeUtil::InlinedDataToSQL(transaction, *inlined.data);
		for (idx_t row_order = 0; row_order < rows.size(); row_order++) {
			string tuple = "(" + rows[row_order] + ")";
			string preserved_row_id = "NULL";
			if (has_preserved) {
				auto rid = inlined.row_ids[row_order];
				if (!DuckLakeConstants::IsTransactionLocalRowId(rid)) {
					preserved_row_id = std::to_string(rid);
				}
			}
			staged[DuckLakeStagedTableType::INLINED_ROW].push_back(StringUtil::Format(
			    "(%llu, %llu, %s, %s)", table_id.index, row_order, preserved_row_id, SQLString(tuple)));
		}
		for (auto &stat : inlined.column_stats) {
			staged[DuckLakeStagedTableType::INLINED_COLUMN_STATS].push_back(StringUtil::Format(
			    "(%llu, %llu, %s)", table_id.index, stat.first.index, ColumnStatsValues(stat.second)));
		}
	}
}

//! Stages the deletes of inlined rows and the inlined deletes of file rows
static void StageInlinedDeletes(StagedRows &staged, const LocalTableChanges &local_changes) {
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.first;
		auto &table_changes = entry.second;
		for (auto &deletes_entry : table_changes.new_inlined_data_deletes) {
			auto inlined_table_name = SQLString(deletes_entry.first.GetIdentifierName());
			for (auto &row_id : deletes_entry.second->rows) {
				staged[DuckLakeStagedTableType::INLINED_DELETE].push_back(
				    StringUtil::Format("(%llu, %s, %llu)", table_id.index, inlined_table_name, row_id));
			}
		}
		if (!table_changes.new_inlined_file_deletes) {
			continue;
		}
		for (auto &file_entry : table_changes.new_inlined_file_deletes->file_deletes) {
			for (auto &row_id : file_entry.second) {
				staged[DuckLakeStagedTableType::INLINED_FILE_DELETE].push_back(
				    StringUtil::Format("(%llu, %llu, %llu)", table_id.index, file_entry.first, row_id));
			}
		}
	}
}

//! Stages the delete files of committed data files
static void StageDeleteFiles(StagedRows &staged, const LocalTableChanges &local_changes) {
	for (auto &entry : local_changes.Changes()) {
		for (auto &delete_entry : entry.second.new_delete_files) {
			for (auto &file : delete_entry.second) {
				StageDeleteFile(staged, file, entry.first, delete_entry.first, optional_idx());
			}
		}
	}
}

//! Stages the compactions with their output and source files
static void StageCompactions(StagedRows &staged, const LocalTableChanges &local_changes, idx_t &local_file_id) {
	idx_t compaction_id = 0;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.first;
		for (auto &compaction : entry.second.compactions) {
			idx_t output_order = 0;
			for (auto &written_file : compaction.written_files) {
				StageDataFile(staged, written_file, local_file_id, table_id, output_order,
				              std::to_string(compaction_id));
				local_file_id++;
				output_order++;
			}

			staged[DuckLakeStagedTableType::COMPACTION].push_back(
			    StringUtil::Format("(%llu, %llu, %s, %s)", compaction_id, table_id.index,
			                       SQLString(CompactionTypeToString(compaction.type)),
			                       DuckLakeUtil::OptionalIdxOrNull(compaction.row_id_start)));

			idx_t source_order = 0;
			for (auto &source : compaction.source_files) {
				string last_id = "NULL";
				string last_path = "NULL";
				string last_row_count = "NULL";
				string last_end_snap = "NULL";
				if (!source.delete_files.empty()) {
					auto &last = source.delete_files.back();
					last_id = std::to_string(last.delete_file_id.index);
					last_path = SQLString::ToString(last.data.path);
					last_row_count = std::to_string(last.row_count);
					last_end_snap = DuckLakeUtil::OptionalIdxOrNull(last.end_snapshot);
				}
				staged[DuckLakeStagedTableType::COMPACTION_SOURCE].push_back(StringUtil::Format(
				    "(%llu, %llu, %llu, %s, %llu, %llu, %s, %llu, %s, %s, %s, %s)", compaction_id, source_order,
				    source.file.id.index, SQLString::ToString(source.file.data.path), source.file.row_count,
				    source.file.begin_snapshot, DuckLakeUtil::OptionalIdxOrNull(source.max_partial_file_snapshot),
				    static_cast<idx_t>(source.inlined_file_deletions.size()), last_id, last_path, last_row_count,
				    last_end_snap));
				source_order++;
			}
			compaction_id++;
		}
	}
}

static void StageNameMaps(StagedRows &staged, const DuckLakeNameMapSet &name_maps) {
	for (auto &entry : name_maps.name_maps) {
		auto &map = *entry.second;
		staged[DuckLakeStagedTableType::NAME_MAP].push_back(
		    StringUtil::Format("(%llu, %llu)", map.id.index, map.table_id.index));
		for (auto &col : map.FlattenColumns()) {
			staged[DuckLakeStagedTableType::NAME_MAP_ENTRY].push_back(
			    StringUtil::Format("(%llu, %llu, %s, %s, %llu, %s)", map.id.index, col.column_id,
			                       DuckLakeUtil::OptionalIdxOrNull(col.parent_column), SQLString(col.source_name),
			                       col.target_field_id.index, DuckLakeUtil::BoolLiteral(col.hive_partition)));
		}
	}
}

//! Stages the dropped files, the flushed inlined tables and the tables that deletes ran against
static void StageDroppedData(StagedRows &staged, DuckLakeTransaction &transaction) {
	for (auto &entry : transaction.GetDroppedFiles()) {
		staged[DuckLakeStagedTableType::DROPPED_FILE].push_back(
		    StringUtil::Format("(%s, %llu)", SQLString(entry.first), entry.second.index));
	}
	for (auto &table_id : transaction.GetTablesDeletedFrom()) {
		staged[DuckLakeStagedTableType::TABLES_DELETED_FROM].push_back(StringUtil::Format("(%llu)", table_id.index));
	}
	for (auto &table_id : transaction.GetTablesDeleteAttempted()) {
		staged[DuckLakeStagedTableType::TABLES_DELETE_ATTEMPTED].push_back(
		    StringUtil::Format("(%llu)", table_id.index));
	}
	for (auto &entry : transaction.GetFlushedInlinedTables()) {
		// LCOV_EXCL_START
		staged[DuckLakeStagedTableType::FLUSHED_INLINED].push_back(
		    StringUtil::Format("(%s, %llu, %llu)", SQLString(entry.inlined_table.table_name.GetIdentifierName()),
		                       entry.inlined_table.schema_version, entry.flush_snapshot_id));
		// LCOV_EXCL_STOP
	}
}

string DuckLakeStagedCommit::Build(DuckLakeTransaction &transaction, const DuckLakeSnapshot &transaction_snapshot,
                                   const DuckLakeRetryConfig &retry_config) const {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &local_changes = transaction.GetLocalChanges();

	StagedRows staged;
	StageCommitHeader(staged, transaction.GetCommitInfo(), transaction_snapshot, ducklake_catalog.DataPath(),
	                  ducklake_catalog.Separator());
	idx_t local_file_id = 0;
	StageDataFiles(staged, local_changes, local_file_id);
	StageCompactions(staged, local_changes, local_file_id);
	StageInlinedData(staged, local_changes, transaction);
	StageInlinedDeletes(staged, local_changes);
	StageDeleteFiles(staged, local_changes);
	StageDroppedData(staged, transaction);
	StageNameMaps(staged, transaction.GetNewNameMaps());

	string batch = DuckLakeStagedTable::CreateAllSql() + DuckLakeStagedTable::TruncateAllSql();
	for (auto &entry : staged) {
		batch += DuckLakeMetadataManager::InsertValuesIntoSql(DuckLakeStagedTable::BaseName(entry.first), entry.second);
	}
	int64_t schema_version_param = transaction_snapshot.snapshot_id != DConstants::INVALID_INDEX
	                                   ? static_cast<int64_t>(transaction_snapshot.schema_version)
	                                   : -1;
	// a server that passes on the commit of a client tells the next server whose files these are
	string files_owned_by_client = transaction.FilesOwnedByClient() ? ", files_owned_by_client => true" : "";
	batch += StringUtil::Format("SELECT * FROM ducklake_commit(%s, %lld, "
	                            "max_retry_count => %llu, retry_wait_ms => %llu, retry_backoff => %f%s);",
	                            SQLString::ToString(ducklake_catalog.MetadataSchemaName().GetIdentifierName()),
	                            schema_version_param, retry_config.max_retry_count, retry_config.retry_wait_ms,
	                            retry_config.retry_backoff, files_owned_by_client);
	return batch;
}

} // namespace duckdb
