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

const char *DuckLakeStagedTable::BaseName(DuckLakeStagedTableType type) {
	switch (type) {
	case DuckLakeStagedTableType::COMMIT_HEADER:
		return "ducklake_staged_commit";
	case DuckLakeStagedTableType::DATA_FILE:
		return "ducklake_staged_data_file";
	case DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS:
		return "ducklake_staged_data_file_column_stats";
	case DuckLakeStagedTableType::DATA_FILE_PARTITION:
		return "ducklake_staged_data_file_partition";
	case DuckLakeStagedTableType::DELETE_FILE:
		return "ducklake_staged_delete_file";
	case DuckLakeStagedTableType::INLINED_DATA:
		return "ducklake_staged_inlined_data";
	case DuckLakeStagedTableType::INLINED_ROW:
		return "ducklake_staged_inlined_row";
	case DuckLakeStagedTableType::INLINED_COLUMN_STATS:
		return "ducklake_staged_inlined_column_stats";
	case DuckLakeStagedTableType::INLINED_DELETE:
		return "ducklake_staged_inlined_delete";
	case DuckLakeStagedTableType::INLINED_FILE_DELETE:
		return "ducklake_staged_inlined_file_delete";
	case DuckLakeStagedTableType::DROPPED_FILE:
		return "ducklake_staged_dropped_file";
	case DuckLakeStagedTableType::TABLES_DELETED_FROM:
		return "ducklake_staged_tables_deleted_from";
	case DuckLakeStagedTableType::FLUSHED_INLINED:
		return "ducklake_staged_flushed_inlined";
	case DuckLakeStagedTableType::COMPACTION:
		return "ducklake_staged_compaction";
	case DuckLakeStagedTableType::COMPACTION_SOURCE:
		return "ducklake_staged_compaction_source";
	case DuckLakeStagedTableType::NAME_MAP:
		return "ducklake_staged_name_map";
	case DuckLakeStagedTableType::NAME_MAP_ENTRY:
		return "ducklake_staged_name_map_entry";
	default:
		throw InternalException("Unknown DuckLakeStagedTableType");
	}
}

const char *DuckLakeStagedTable::Columns(DuckLakeStagedTableType type) {
	switch (type) {
	case DuckLakeStagedTableType::COMMIT_HEADER:
		return "commit_author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR, "
		       "transaction_snapshot_id BIGINT, data_path VARCHAR, path_separator VARCHAR, "
		       "transaction_next_catalog_id UBIGINT, transaction_next_file_id UBIGINT";
	case DuckLakeStagedTableType::DATA_FILE:
		return "data_file_id BIGINT, table_id BIGINT, file_order BIGINT, "
		       "path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, "
		       "record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, "
		       "row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, "
		       "mapping_id UBIGINT, partial_max BIGINT, begin_snapshot BIGINT, "
		       "compaction_id BIGINT";
	case DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS:
		return "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, "
		       "column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, "
		       "min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR";
	case DuckLakeStagedTableType::DATA_FILE_PARTITION:
		return "local_file_id BIGINT, partition_column_idx BIGINT, partition_value VARCHAR";
	case DuckLakeStagedTableType::DELETE_FILE:
		return "table_id BIGINT, data_file_path VARCHAR, data_file_id BIGINT, "
		       "file_name VARCHAR, format VARCHAR, delete_count BIGINT, "
		       "file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
		       "begin_snapshot BIGINT, max_snapshot BIGINT, source VARCHAR, "
		       "overwrites_existing_delete BOOLEAN, "
		       "overwrite_delete_file_id BIGINT, overwrite_delete_file_path VARCHAR, "
		       "attached_local_file_id BIGINT";
	case DuckLakeStagedTableType::INLINED_DATA:
		return "table_id BIGINT, has_preserved_row_ids BOOLEAN";
	case DuckLakeStagedTableType::INLINED_ROW:
		return "table_id BIGINT, row_order BIGINT, preserved_row_id BIGINT, value_literals VARCHAR";
	case DuckLakeStagedTableType::INLINED_COLUMN_STATS:
		return "table_id BIGINT, column_id BIGINT, "
		       "column_size_bytes BIGINT, has_num_values BOOLEAN, num_values BIGINT, "
		       "has_null_count BOOLEAN, null_count BIGINT, "
		       "has_min BOOLEAN, min_value VARCHAR, has_max BOOLEAN, max_value VARCHAR, "
		       "has_contains_nan BOOLEAN, contains_nan BOOLEAN, "
		       "any_valid BOOLEAN, extra_stats VARCHAR";
	case DuckLakeStagedTableType::INLINED_DELETE:
		return "table_id BIGINT, inlined_table_name VARCHAR, deleted_row_id BIGINT";
	case DuckLakeStagedTableType::INLINED_FILE_DELETE:
		return "table_id BIGINT, file_id BIGINT, deleted_row_id BIGINT";
	case DuckLakeStagedTableType::DROPPED_FILE:
		return "path VARCHAR, data_file_id BIGINT";
	case DuckLakeStagedTableType::TABLES_DELETED_FROM:
		return "table_id BIGINT";
	case DuckLakeStagedTableType::FLUSHED_INLINED:
		return "inlined_table_name VARCHAR, schema_version BIGINT, flush_snapshot_id BIGINT";
	case DuckLakeStagedTableType::COMPACTION:
		return "compaction_id BIGINT, table_id BIGINT, compaction_type VARCHAR, "
		       "row_id_start BIGINT, output_local_file_id BIGINT";
	case DuckLakeStagedTableType::COMPACTION_SOURCE:
		return "compaction_id BIGINT, source_order BIGINT, "
		       "source_data_file_id BIGINT, source_path VARCHAR, "
		       "source_row_count BIGINT, source_begin_snapshot BIGINT, "
		       "source_max_partial_file_snapshot BIGINT, source_inlined_delete_count BIGINT, "
		       "last_delete_file_id BIGINT, last_delete_path VARCHAR, "
		       "last_delete_row_count BIGINT, last_delete_end_snapshot BIGINT";
	case DuckLakeStagedTableType::NAME_MAP:
		return "map_id UBIGINT, table_id BIGINT";
	case DuckLakeStagedTableType::NAME_MAP_ENTRY:
		return "map_id UBIGINT, entry_id BIGINT, parent_entry_id BIGINT, "
		       "source_name VARCHAR, target_field_id BIGINT, hive_partition BOOLEAN";
	default:
		throw InternalException("Unknown DuckLakeStagedTableType");
	}
}

string DuckLakeStagedTable::FullQualifiedName(DuckLakeStagedTableType type, const string &suffix) {
	return StringUtil::Format("%s_%s", BaseName(type), suffix);
}

const vector<DuckLakeStagedTableType> &DuckLakeStagedTable::AllTypes() {
	static const vector<DuckLakeStagedTableType> kinds = {DuckLakeStagedTableType::COMMIT_HEADER,
	                                                      DuckLakeStagedTableType::DATA_FILE,
	                                                      DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS,
	                                                      DuckLakeStagedTableType::DATA_FILE_PARTITION,
	                                                      DuckLakeStagedTableType::DELETE_FILE,
	                                                      DuckLakeStagedTableType::INLINED_DATA,
	                                                      DuckLakeStagedTableType::INLINED_ROW,
	                                                      DuckLakeStagedTableType::INLINED_COLUMN_STATS,
	                                                      DuckLakeStagedTableType::INLINED_DELETE,
	                                                      DuckLakeStagedTableType::INLINED_FILE_DELETE,
	                                                      DuckLakeStagedTableType::DROPPED_FILE,
	                                                      DuckLakeStagedTableType::TABLES_DELETED_FROM,
	                                                      DuckLakeStagedTableType::FLUSHED_INLINED,
	                                                      DuckLakeStagedTableType::COMPACTION,
	                                                      DuckLakeStagedTableType::COMPACTION_SOURCE,
	                                                      DuckLakeStagedTableType::NAME_MAP,
	                                                      DuckLakeStagedTableType::NAME_MAP_ENTRY};
	return kinds;
}

string DuckLakeStagedTable::CreateAllSql(const string &suffix) {
	string sql;
	for (auto kind : AllTypes()) {
		sql += StringUtil::Format("CREATE TEMPORARY TABLE IF NOT EXISTS %s_%s(%s);", BaseName(kind), suffix,
		                          Columns(kind));
	}
	return sql;
}

string DuckLakeStagedTable::DropAllSql(const string &suffix) {
	string sql;
	for (auto kind : AllTypes()) {
		sql += StringUtil::Format("DROP TABLE IF EXISTS %s_%s;", BaseName(kind), suffix);
	}
	return sql;
}

DuckLakeStagedCommit::DuckLakeStagedCommit(string commit_uuid)
    : commit_uuid(std::move(commit_uuid)), identifier_suffix(StringUtil::Replace(this->commit_uuid, "-", "")) {
}

string DuckLakeStagedCommit::StagedFullQualifyName(DuckLakeStagedTableType type) const {
	return DuckLakeStagedTable::FullQualifiedName(type, identifier_suffix);
}

void DuckLakeStagedCommit::EmitDataFileRow(string &sql, const DuckLakeDataFile &file, idx_t local_file_id,
                                           TableIndex table_id, idx_t file_order,
                                           const string &compaction_id_literal) const {
	sql += StringUtil::Format(
	    "INSERT INTO %s VALUES "
	    "(%llu, %llu, %llu, %s, false, 'parquet', %llu, %llu, %s, %s, %s, %s, %s, %s, %s, %s);",
	    StagedFullQualifyName(DuckLakeStagedTableType::DATA_FILE), local_file_id, table_id.index, file_order,
	    SQLString(file.file_name), file.row_count, file.file_size_bytes,
	    DuckLakeUtil::OptionalIdxOrNull(file.footer_size), DuckLakeUtil::OptionalIdxOrNull(file.flush_row_id_start),
	    DuckLakeUtil::OptionalIdxOrNull(file.partition_id), DuckLakeUtil::EncryptionKeyLiteral(file.encryption_key),
	    DuckLakeUtil::MappingIdOrNull(file.mapping_id), DuckLakeUtil::OptionalIdxOrNull(file.max_partial_file_snapshot),
	    DuckLakeUtil::OptionalIdxOrNull(file.begin_snapshot), compaction_id_literal);
	for (auto &stat : file.column_stats) {
		auto info = DuckLakeColumnStatsInfo::FromColumnStats(stat.first, stat.second);
		sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s);",
		                          StagedFullQualifyName(DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS), local_file_id,
		                          table_id.index, info.column_id.index, info.column_size_bytes, info.value_count,
		                          info.null_count, info.min_val, info.max_val, info.contains_nan, info.extra_stats);
	}
	for (auto &part : file.partition_values) {
		sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %llu, %s);",
		                          StagedFullQualifyName(DuckLakeStagedTableType::DATA_FILE_PARTITION), local_file_id,
		                          part.partition_column_idx, DuckLakeUtil::PartitionValueLiteral(part.partition_value));
	}
}

void DuckLakeStagedCommit::EmitDeleteFileRow(string &sql, const DuckLakeDeleteFile &file, TableIndex table_id,
                                             const string &data_file_path) const {
	string overwrite_id = file.overwritten_delete_file.delete_file_id.IsValid()
	                          ? std::to_string(file.overwritten_delete_file.delete_file_id.index)
	                          : string("NULL");
	string overwrite_path = file.overwritten_delete_file.path.empty()
	                            ? string("NULL")
	                            : DuckLakeUtil::SQLLiteralToString(file.overwritten_delete_file.path);
	sql += StringUtil::Format("INSERT INTO %s VALUES "
	                          "(%llu, %s, %llu, %s, %s, %llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s, NULL);",
	                          StagedFullQualifyName(DuckLakeStagedTableType::DELETE_FILE), table_id.index,
	                          SQLString(data_file_path), file.data_file_id.index, SQLString(file.file_name),
	                          SQLString(DeleteFileFormatToString(file.format)), file.delete_count, file.file_size_bytes,
	                          file.footer_size, DuckLakeUtil::EncryptionKeyLiteral(file.encryption_key),
	                          DuckLakeUtil::OptionalIdxOrNull(file.begin_snapshot),
	                          DuckLakeUtil::OptionalIdxOrNull(file.max_snapshot),
	                          SQLString(file.source == DeleteFileSource::FLUSH ? "FLUSH" : "REGULAR"),
	                          file.overwrites_existing_delete ? "true" : "false", overwrite_id, overwrite_path);
}

void DuckLakeStagedCommit::EmitAttachedDeleteRow(string &sql, const DuckLakeDeleteFile &del, TableIndex table_id,
                                                 idx_t local_file_id) const {
	sql += StringUtil::Format(
	    "INSERT INTO %s VALUES "
	    "(%llu, NULL, NULL, %s, %s, %llu, %llu, %llu, %s, %s, %s, %s, false, NULL, NULL, %llu);",
	    StagedFullQualifyName(DuckLakeStagedTableType::DELETE_FILE), table_id.index, SQLString(del.file_name),
	    SQLString(DeleteFileFormatToString(del.format)), del.delete_count, del.file_size_bytes, del.footer_size,
	    DuckLakeUtil::EncryptionKeyLiteral(del.encryption_key), DuckLakeUtil::OptionalIdxOrNull(del.begin_snapshot),
	    DuckLakeUtil::OptionalIdxOrNull(del.max_snapshot),
	    SQLString(del.source == DeleteFileSource::FLUSH ? "FLUSH" : "REGULAR"), local_file_id);
}

void DuckLakeStagedCommit::EmitInlinedColumnStatsRow(string &sql, TableIndex table_id, FieldIndex column_id,
                                                     const DuckLakeColumnStats &s) const {
	string num_values = s.has_num_values ? std::to_string(s.num_values) : "NULL";
	string null_count = s.has_null_count ? std::to_string(s.null_count) : "NULL";
	string min_val = s.has_min ? DuckLakeUtil::StatsToString(s.min) : "NULL";
	string max_val = s.has_max ? DuckLakeUtil::StatsToString(s.max) : "NULL";
	bool has_min_emit = s.has_min && min_val != "NULL";
	bool has_max_emit = s.has_max && max_val != "NULL";
	string contains_nan = s.has_contains_nan ? (s.contains_nan ? "true" : "false") : "NULL";
	string extra_stats = "NULL";
	if (s.extra_stats) {
		string serialized;
		if (s.extra_stats->TrySerialize(serialized) && !serialized.empty()) {
			extra_stats = DuckLakeUtil::SQLLiteralToString(serialized);
		}
	}
	sql += StringUtil::Format(
	    "INSERT INTO %s VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
	    StagedFullQualifyName(DuckLakeStagedTableType::INLINED_COLUMN_STATS), table_id.index, column_id.index,
	    s.column_size_bytes, DuckLakeUtil::BoolLiteral(s.has_num_values), num_values,
	    DuckLakeUtil::BoolLiteral(s.has_null_count), null_count, DuckLakeUtil::BoolLiteral(has_min_emit), min_val,
	    DuckLakeUtil::BoolLiteral(has_max_emit), max_val, DuckLakeUtil::BoolLiteral(s.has_contains_nan), contains_nan,
	    DuckLakeUtil::BoolLiteral(s.any_valid), extra_stats);
}

void DuckLakeStagedCommit::FlattenNameMapEntry(const DuckLakeNameMapEntry &entry, idx_t map_id, idx_t parent_id,
                                               idx_t &next_entry_id, string &sql) const {
	idx_t entry_id = next_entry_id++;
	string parent_literal = parent_id == NumericLimits<idx_t>::Maximum() ? string("NULL") : std::to_string(parent_id);
	sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %llu, %s, %s, %llu, %s);",
	                          StagedFullQualifyName(DuckLakeStagedTableType::NAME_MAP_ENTRY), map_id, entry_id,
	                          parent_literal, SQLString(entry.source_name), entry.target_field_id.index,
	                          entry.hive_partition ? "true" : "false");
	for (auto &child : entry.child_entries) {
		FlattenNameMapEntry(*child, map_id, entry_id, next_entry_id, sql);
	}
}

string DuckLakeStagedCommit::EmitCommitHeader(const DuckLakeSnapshotCommit &h, const DuckLakeSnapshot &snap,
                                              const string &data_path, const string &separator) const {
	bool has_snapshot = snap.snapshot_id != DConstants::INVALID_INDEX;
	string snap_id = has_snapshot ? std::to_string(snap.snapshot_id) : "NULL";
	string next_cat = has_snapshot ? std::to_string(snap.next_catalog_id) : "NULL";
	string next_file = has_snapshot ? std::to_string(snap.next_file_id) : "NULL";
	return StringUtil::Format("INSERT INTO %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
	                          StagedFullQualifyName(DuckLakeStagedTableType::COMMIT_HEADER), h.author.ToSQLString(),
	                          h.commit_message.ToSQLString(), h.commit_extra_info.ToSQLString(), snap_id,
	                          DuckLakeUtil::SQLLiteralToString(data_path), DuckLakeUtil::SQLLiteralToString(separator),
	                          next_cat, next_file);
}

string DuckLakeStagedCommit::EmitDataFiles(const LocalTableChanges &local_changes, idx_t &local_file_id) const {
	string sql;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		idx_t file_order = 0;
		for (auto &file : table_changes.new_data_files) {
			EmitDataFileRow(sql, file, local_file_id, table_id, file_order, "NULL");
			for (auto &del : file.delete_files) {
				EmitAttachedDeleteRow(sql, del, table_id, local_file_id);
			}
			local_file_id++;
			file_order++;
		}
	}
	return sql;
}

string DuckLakeStagedCommit::EmitInlinedData(const LocalTableChanges &local_changes,
                                             DuckLakeTransaction &transaction) const {
	string sql;
	auto context_ref = transaction.context.lock();
	auto &context = *context_ref;
	auto &metadata_manager = transaction.GetMetadataManager();

	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		if (!table_changes.new_inlined_data) {
			continue;
		}
		auto &inlined = *table_changes.new_inlined_data;
		const bool has_preserved = inlined.HasPreservedRowIds();
		sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %s);",
		                          StagedFullQualifyName(DuckLakeStagedTableType::INLINED_DATA), table_id.index,
		                          DuckLakeUtil::BoolLiteral(has_preserved));
		idx_t row_order = 0;
		idx_t global_row_idx = 0;
		for (auto &chunk : inlined.data->Chunks()) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				string tuple = "(" + DuckLakeUtil::ChunkRowToSQL(metadata_manager, context, chunk, r) + ")";
				string preserved_row_id = "NULL";
				if (has_preserved) {
					auto rid = inlined.row_ids[global_row_idx];
					if (!DuckLakeConstants::IsTransactionLocalRowId(rid)) {
						preserved_row_id = std::to_string(rid);
					}
				}
				sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %llu, %s, %s);",
				                          StagedFullQualifyName(DuckLakeStagedTableType::INLINED_ROW), table_id.index,
				                          row_order, preserved_row_id, SQLString(tuple));
				row_order++;
				global_row_idx++;
			}
		}
		for (auto &stat : inlined.column_stats) {
			EmitInlinedColumnStatsRow(sql, table_id, stat.first, stat.second);
		}
	}
	return sql;
}

string DuckLakeStagedCommit::EmitInlinedDeletes(const LocalTableChanges &local_changes) const {
	string sql;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		for (auto &deletes_entry : table_changes.new_inlined_data_deletes) {
			auto &inlined_table_name = deletes_entry.first;
			auto &deletes = *deletes_entry.second;
			for (auto &row_id : deletes.rows) {
				sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %s, %llu);",
				                          StagedFullQualifyName(DuckLakeStagedTableType::INLINED_DELETE),
				                          table_id.index, SQLString(inlined_table_name), row_id);
			}
		}
	}
	return sql;
}

string DuckLakeStagedCommit::EmitInlinedFileDeletes(const LocalTableChanges &local_changes) const {
	string sql;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		if (!table_changes.new_inlined_file_deletes) {
			continue;
		}
		for (auto &file_entry : table_changes.new_inlined_file_deletes->file_deletes) {
			auto file_id = file_entry.first;
			for (auto &row_id : file_entry.second) {
				sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %llu, %llu);",
				                          StagedFullQualifyName(DuckLakeStagedTableType::INLINED_FILE_DELETE),
				                          table_id.index, file_id, row_id);
			}
		}
	}
	return sql;
}

string DuckLakeStagedCommit::EmitDeleteFiles(const LocalTableChanges &local_changes) const {
	string sql;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		for (auto &delete_entry : table_changes.new_delete_files) {
			auto &data_file_path = delete_entry.first;
			for (auto &file : delete_entry.second) {
				EmitDeleteFileRow(sql, file, table_id, data_file_path);
			}
		}
	}
	return sql;
}

string DuckLakeStagedCommit::EmitCompactions(const LocalTableChanges &local_changes, idx_t &local_file_id) const {
	string sql;
	idx_t compaction_id = 0;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		for (auto &compaction : table_changes.compactions) {
			string output_local_id = "NULL";
			if (!compaction.written_file.file_name.empty()) {
				EmitDataFileRow(sql, compaction.written_file, local_file_id, table_id, 0,
				                std::to_string(compaction_id));
				output_local_id = std::to_string(local_file_id);
				local_file_id++;
			}

			sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %llu, %s, %s, %s);",
			                          StagedFullQualifyName(DuckLakeStagedTableType::COMPACTION), compaction_id,
			                          table_id.index, SQLString(CompactionTypeToString(compaction.type)),
			                          DuckLakeUtil::OptionalIdxOrNull(compaction.row_id_start), output_local_id);

			idx_t source_order = 0;
			for (auto &source : compaction.source_files) {
				string last_id = "NULL";
				string last_path = "NULL";
				string last_row_count = "NULL";
				string last_end_snap = "NULL";
				if (!source.delete_files.empty()) {
					auto &last = source.delete_files.back();
					last_id = std::to_string(last.delete_file_id.index);
					last_path = DuckLakeUtil::SQLLiteralToString(last.data.path);
					last_row_count = std::to_string(last.row_count);
					last_end_snap = DuckLakeUtil::OptionalIdxOrNull(last.end_snapshot);
				}
				sql += StringUtil::Format(
				    "INSERT INTO %s VALUES (%llu, %llu, %llu, %s, %llu, %llu, %s, %llu, %s, %s, %s, %s);",
				    StagedFullQualifyName(DuckLakeStagedTableType::COMPACTION_SOURCE), compaction_id, source_order,
				    source.file.id.index, DuckLakeUtil::SQLLiteralToString(source.file.data.path),
				    source.file.row_count, source.file.begin_snapshot,
				    DuckLakeUtil::OptionalIdxOrNull(source.max_partial_file_snapshot),
				    static_cast<idx_t>(source.inlined_file_deletions.size()), last_id, last_path, last_row_count,
				    last_end_snap);
				source_order++;
			}
			compaction_id++;
		}
	}
	return sql;
}

string DuckLakeStagedCommit::EmitNameMaps(const DuckLakeNameMapSet &name_maps) const {
	string sql;
	for (auto &entry : name_maps.name_maps) {
		auto &map = *entry.second;
		sql += StringUtil::Format("INSERT INTO %s VALUES (%llu, %llu);",
		                          StagedFullQualifyName(DuckLakeStagedTableType::NAME_MAP), map.id.index,
		                          map.table_id.index);
		idx_t next_entry_id = 0;
		for (auto &col : map.column_maps) {
			FlattenNameMapEntry(*col, map.id.index, NumericLimits<idx_t>::Maximum(), next_entry_id, sql);
		}
	}
	return sql;
}

string DuckLakeStagedCommit::EmitFlushedInlinedTables(const vector<FlushedInlinedTableInfo> &flushed) const {
	string sql;
	for (auto &entry : flushed) {
		sql += StringUtil::Format(
		    "INSERT INTO %s VALUES (%s, %llu, %llu);", StagedFullQualifyName(DuckLakeStagedTableType::FLUSHED_INLINED),
		    SQLString(entry.inlined_table.table_name), entry.inlined_table.schema_version, entry.flush_snapshot_id);
	}
	return sql;
}

string DuckLakeStagedCommit::EmitDroppedFiles(DuckLakeTransaction &transaction) const {
	string sql;
	for (auto &entry : transaction.GetDroppedFiles()) {
		sql += StringUtil::Format("INSERT INTO %s VALUES (%s, %llu);",
		                          StagedFullQualifyName(DuckLakeStagedTableType::DROPPED_FILE), SQLString(entry.first),
		                          entry.second.index);
	}
	for (auto &table_id : transaction.GetTablesDeletedFrom()) {
		sql += StringUtil::Format("INSERT INTO %s VALUES (%llu);",
		                          StagedFullQualifyName(DuckLakeStagedTableType::TABLES_DELETED_FROM), table_id.index);
	}
	return sql;
}

string DuckLakeStagedCommit::Build(DuckLakeTransaction &transaction,
                                   const TransactionChangeInformation &transaction_changes,
                                   const DuckLakeSnapshot &transaction_snapshot,
                                   const DuckLakeRetryConfig &retry_config) const {
	(void)transaction_changes;
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &local_changes = transaction.GetLocalChanges();

	string batch;
	batch += DuckLakeStagedTable::CreateAllSql(identifier_suffix);
	batch += EmitCommitHeader(transaction.GetCommitInfo(), transaction_snapshot, ducklake_catalog.DataPath(),
	                          ducklake_catalog.Separator());
	idx_t local_file_id = 0;
	batch += EmitDataFiles(local_changes, local_file_id);
	batch += EmitCompactions(local_changes, local_file_id);
	batch += EmitInlinedData(local_changes, transaction);
	batch += EmitInlinedDeletes(local_changes);
	batch += EmitInlinedFileDeletes(local_changes);
	batch += EmitDeleteFiles(local_changes);
	batch += EmitDroppedFiles(transaction);
	batch += EmitFlushedInlinedTables(transaction.GetFlushedInlinedTables());
	batch += EmitNameMaps(transaction.GetNewNameMaps());

	int64_t schema_version_param = transaction_snapshot.snapshot_id != DConstants::INVALID_INDEX
	                                   ? static_cast<int64_t>(transaction_snapshot.schema_version)
	                                   : -1;
	batch += StringUtil::Format("SELECT * FROM ducklake_commit(%s, %s, %lld, "
	                            "max_retry_count => %llu, retry_wait_ms => %llu, retry_backoff => %f);",
	                            DuckLakeUtil::SQLLiteralToString(identifier_suffix),
	                            DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName()),
	                            schema_version_param, retry_config.max_retry_count, retry_config.retry_wait_ms,
	                            retry_config.retry_backoff);
	return batch;
}

} // namespace duckdb
