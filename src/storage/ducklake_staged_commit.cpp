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

DuckLakeStagedCommit::DuckLakeStagedCommit(string commit_uuid)
    : commit_uuid(std::move(commit_uuid)), identifier_suffix(StringUtil::Replace(this->commit_uuid, "-", "")) {
}

static string EmitStagedCommitHeader(const DuckLakeSnapshotCommit &h, const DuckLakeSnapshot &transaction_snapshot,
                                     const string &data_path, const string &separator, const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_commit_%s("
	                          "commit_author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR, "
	                          "transaction_snapshot_id BIGINT, data_path VARCHAR, path_separator VARCHAR);",
	                          sfx);
	sql += StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_commit_%s VALUES (%s, %s, %s, %llu, %s, %s);", sfx,
	    h.author.ToSQLString(), h.commit_message.ToSQLString(), h.commit_extra_info.ToSQLString(),
	    transaction_snapshot.snapshot_id, DuckLakeUtil::SQLLiteralToString(data_path),
	    DuckLakeUtil::SQLLiteralToString(separator));
	return sql;
}

static string EmitStagedDataFiles(const LocalTableChanges &local_changes, const string &sfx, idx_t &local_file_id) {
	string sql;
	// `compaction_id BIGINT` is NULL for regular new data files and set to a per-commit sequential
	// id for compaction-output files (the `written_file` of a DuckLakeCompactionEntry). The server
	// uses this to route the file into local_changes.compactions[i].written_file instead of
	// local_changes.new_data_files; both paths share this staging table so column stats etc. are
	// captured the same way.
	// `mapping_id UBIGINT`: name-map ids can be transaction-local sentinels (≥ 2^63), beyond BIGINT.
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_data_file_%s("
	                          "data_file_id BIGINT, table_id BIGINT, file_order BIGINT, "
	                          "path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, "
	                          "record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, "
	                          "row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, "
	                          "mapping_id UBIGINT, partial_max BIGINT, begin_snapshot BIGINT, "
	                          "compaction_id BIGINT);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_data_file_column_stats_%s("
	                          "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, "
	                          "column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, "
	                          "min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);",
	                          sfx);
	// Partition values per data file: one row per (file, partition_column_idx). Server attaches
	// these to DuckLakeDataFile.partition_values; WriteNewDataFilesSqlBatch already handles them.
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_data_file_partition_%s("
	                          "local_file_id BIGINT, partition_column_idx BIGINT, partition_value VARCHAR);",
	                          sfx);
	// `attached_local_file_id BIGINT` lets us share this table between loose delete files
	// (NULL → goes into LocalTableChanges::new_delete_files) and delete files attached to a
	// transaction-local data file (non-NULL → attached to the data file with that local_file_id
	// in new_data_files[i].delete_files). The real `data_file_id` is unknown at staging time for
	// attached deletes and gets assigned during state.CommitChanges.
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_delete_file_%s("
	                          "table_id BIGINT, data_file_path VARCHAR, data_file_id BIGINT, "
	                          "file_name VARCHAR, format VARCHAR, delete_count BIGINT, "
	                          "file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
	                          "begin_snapshot BIGINT, max_snapshot BIGINT, source VARCHAR, "
	                          "overwrites_existing_delete BOOLEAN, "
	                          "overwrite_delete_file_id BIGINT, overwrite_delete_file_path VARCHAR, "
	                          "attached_local_file_id BIGINT);",
	                          sfx);

	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		idx_t file_order = 0;
		for (auto &file : table_changes.new_data_files) {
			sql += StringUtil::Format(
			    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_%s VALUES "
			    "(%llu, %llu, %llu, %s, false, 'parquet', %llu, %llu, %s, %s, %s, %s, %s, %s, %s, NULL);",
			    sfx, local_file_id, table_id.index, file_order, SQLString(file.file_name), file.row_count,
			    file.file_size_bytes, DuckLakeUtil::OptionalIdxOrNull(file.footer_size),
			    DuckLakeUtil::OptionalIdxOrNull(file.flush_row_id_start),
			    DuckLakeUtil::OptionalIdxOrNull(file.partition_id),
			    DuckLakeUtil::EncryptionKeyLiteral(file.encryption_key), DuckLakeUtil::MappingIdOrNull(file.mapping_id),
			    DuckLakeUtil::OptionalIdxOrNull(file.max_partial_file_snapshot),
			    DuckLakeUtil::OptionalIdxOrNull(file.begin_snapshot));
			for (auto &stat : file.column_stats) {
				auto info = DuckLakeColumnStatsInfo::FromColumnStats(stat.first, stat.second);
				sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_column_stats_%s "
				                          "VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s);",
				                          sfx, local_file_id, table_id.index, info.column_id.index,
				                          info.column_size_bytes, info.value_count, info.null_count, info.min_val,
				                          info.max_val, info.contains_nan, info.extra_stats);
			}
			// Partition values (for partitioned tables). partition_value is stored as VARCHAR;
			// WriteNewDataFilesSqlBatch reads `.ToString()` from it, so round-tripping via VARCHAR
			// is loss-free (all DuckLake metadata stores partition values as text anyway).
			for (auto &part : file.partition_values) {
				string val_literal = part.partition_value.IsNull()
				                         ? string("NULL")
				                         : DuckLakeUtil::SQLLiteralToString(part.partition_value.ToString());
				sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_partition_%s "
				                          "VALUES (%llu, %llu, %s);",
				                          sfx, local_file_id, part.partition_column_idx, val_literal);
			}
			// Delete files attached to this transaction-local data file (DELETE applied to rows
			// of a file inserted in the same transaction). data_file_id is unknown at staging time.
			for (auto &del : file.delete_files) {
				sql += StringUtil::Format(
				    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_delete_file_%s VALUES "
				    "(%llu, NULL, NULL, %s, %s, %llu, %llu, %llu, %s, %s, %s, %s, false, NULL, NULL, %llu);",
				    sfx, table_id.index, SQLString(del.file_name), SQLString(DeleteFileFormatToString(del.format)),
				    del.delete_count, del.file_size_bytes, del.footer_size,
				    DuckLakeUtil::EncryptionKeyLiteral(del.encryption_key),
				    DuckLakeUtil::OptionalIdxOrNull(del.begin_snapshot),
				    DuckLakeUtil::OptionalIdxOrNull(del.max_snapshot),
				    SQLString(del.source == DeleteFileSource::FLUSH ? "FLUSH" : "REGULAR"), local_file_id);
			}
			local_file_id++;
			file_order++;
		}
	}
	return sql;
}

static string EmitStagedInlinedData(const LocalTableChanges &local_changes, DuckLakeTransaction &transaction,
                                    const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_data_%s("
	                          "table_id BIGINT, has_preserved_row_ids BOOLEAN);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_row_%s("
	                          "table_id BIGINT, row_order BIGINT, preserved_row_id BIGINT, value_literals VARCHAR);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_column_stats_%s("
	                          "table_id BIGINT, column_id BIGINT, "
	                          "column_size_bytes BIGINT, has_num_values BOOLEAN, num_values BIGINT, "
	                          "has_null_count BOOLEAN, null_count BIGINT, "
	                          "has_min BOOLEAN, min_value VARCHAR, has_max BOOLEAN, max_value VARCHAR, "
	                          "has_contains_nan BOOLEAN, contains_nan BOOLEAN, "
	                          "any_valid BOOLEAN, extra_stats VARCHAR);",
	                          sfx);

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
		bool has_preserved = inlined.HasPreservedRowIds();
		sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_data_%s VALUES (%llu, %s);",
		                          sfx, table_id.index, has_preserved ? "true" : "false");
		idx_t row_order = 0;
		idx_t global_row_idx = 0;
		for (auto &chunk : inlined.data->Chunks()) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				string tuple = "(";
				for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
					if (c > 0) {
						tuple += ", ";
					}
					tuple += DuckLakeUtil::ValueToSQL(metadata_manager, context, chunk.GetValue(c, r));
				}
				tuple += ")";
				string preserved_row_id = "NULL";
				if (has_preserved) {
					auto rid = inlined.row_ids[global_row_idx];
					if (!DuckLakeConstants::IsTransactionLocalRowId(rid)) {
						preserved_row_id = std::to_string(rid);
					}
				}
				sql += StringUtil::Format(
				    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_row_%s VALUES (%llu, %llu, %s, %s);", sfx,
				    table_id.index, row_order, preserved_row_id, SQLString(tuple));
				row_order++;
				global_row_idx++;
			}
		}
		for (auto &stat : inlined.column_stats) {
			auto &s = stat.second;
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
			    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_column_stats_%s "
			    "VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
			    sfx, table_id.index, stat.first.index, s.column_size_bytes, s.has_num_values ? "true" : "false",
			    num_values, s.has_null_count ? "true" : "false", null_count, has_min_emit ? "true" : "false", min_val,
			    has_max_emit ? "true" : "false", max_val, s.has_contains_nan ? "true" : "false", contains_nan,
			    s.any_valid ? "true" : "false", extra_stats);
		}
	}
	return sql;
}

static string EmitStagedInlinedDeletes(const LocalTableChanges &local_changes, const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_delete_%s("
	                          "table_id BIGINT, inlined_table_name VARCHAR, deleted_row_id BIGINT);",
	                          sfx);
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		for (auto &deletes_entry : table_changes.new_inlined_data_deletes) {
			auto &inlined_table_name = deletes_entry.first;
			auto &deletes = *deletes_entry.second;
			for (auto &row_id : deletes.rows) {
				sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_delete_%s "
				                          "VALUES (%llu, %s, %llu);",
				                          sfx, table_id.index, SQLString(inlined_table_name), row_id);
			}
		}
	}
	return sql;
}

static string EmitStagedDeleteFiles(const LocalTableChanges &local_changes, const string &sfx) {
	// Loose delete-vector files (DELETE against pre-existing parquet files). Source:
	// new_delete_files. The staged table is also written to by EmitStagedDataFiles for deletes
	// attached to transaction-local data files; CREATE IF NOT EXISTS keeps it idempotent.
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_delete_file_%s("
	                          "table_id BIGINT, data_file_path VARCHAR, data_file_id BIGINT, "
	                          "file_name VARCHAR, format VARCHAR, delete_count BIGINT, "
	                          "file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
	                          "begin_snapshot BIGINT, max_snapshot BIGINT, source VARCHAR, "
	                          "overwrites_existing_delete BOOLEAN, "
	                          "overwrite_delete_file_id BIGINT, overwrite_delete_file_path VARCHAR, "
	                          "attached_local_file_id BIGINT);",
	                          sfx);
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		for (auto &delete_entry : table_changes.new_delete_files) {
			auto &data_file_path = delete_entry.first;
			for (auto &file : delete_entry.second) {
				string overwrite_id = file.overwritten_delete_file.delete_file_id.IsValid()
				                          ? std::to_string(file.overwritten_delete_file.delete_file_id.index)
				                          : string("NULL");
				string overwrite_path = file.overwritten_delete_file.path.empty()
				                            ? string("NULL")
				                            : DuckLakeUtil::SQLLiteralToString(file.overwritten_delete_file.path);
				sql += StringUtil::Format(
				    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_delete_file_%s VALUES "
				    "(%llu, %s, %llu, %s, %s, %llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s, NULL);",
				    sfx, table_id.index, SQLString(data_file_path), file.data_file_id.index, SQLString(file.file_name),
				    SQLString(DeleteFileFormatToString(file.format)), file.delete_count, file.file_size_bytes,
				    file.footer_size, DuckLakeUtil::EncryptionKeyLiteral(file.encryption_key),
				    DuckLakeUtil::OptionalIdxOrNull(file.begin_snapshot),
				    DuckLakeUtil::OptionalIdxOrNull(file.max_snapshot),
				    SQLString(file.source == DeleteFileSource::FLUSH ? "FLUSH" : "REGULAR"),
				    file.overwrites_existing_delete ? "true" : "false", overwrite_id, overwrite_path);
			}
		}
	}
	return sql;
}

static string EmitStagedCompactions(const LocalTableChanges &local_changes, const string &sfx, idx_t &local_file_id) {
	// Stages MERGE_ADJACENT and REWRITE_DELETES compactions. Each entry has:
	//   - A compaction header (type, row_id_start, optional output file).
	//   - One row per source file (with the LAST delete file inlined, which is all WriteCompactions reads).
	// The compaction-output `written_file` reuses ducklake_staged_data_file with compaction_id set,
	// so its column stats flow through the existing column-stats staging. The server-side reader
	// routes those rows into the compaction entry instead of new_data_files.
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_compaction_%s("
	                          "compaction_id BIGINT, table_id BIGINT, compaction_type VARCHAR, "
	                          "row_id_start BIGINT, output_local_file_id BIGINT);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_compaction_source_%s("
	                          "compaction_id BIGINT, source_order BIGINT, "
	                          "source_data_file_id BIGINT, source_path VARCHAR, "
	                          "source_row_count BIGINT, source_begin_snapshot BIGINT, "
	                          "source_max_partial_file_snapshot BIGINT, source_inlined_delete_count BIGINT, "
	                          "last_delete_file_id BIGINT, last_delete_path VARCHAR, "
	                          "last_delete_row_count BIGINT, last_delete_end_snapshot BIGINT);",
	                          sfx);

	idx_t compaction_id = 0;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		for (auto &compaction : table_changes.compactions) {
			bool has_written_file = !compaction.written_file.file_name.empty();
			string output_local_id = "NULL";
			if (has_written_file) {
				// Emit the written_file into the shared staged_data_file table with compaction_id
				// set. column_stats follow via the same loop below (mirrors regular new_data_files).
				auto &out = compaction.written_file;
				sql += StringUtil::Format(
				    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_%s VALUES "
				    "(%llu, %llu, 0, %s, false, 'parquet', %llu, %llu, %s, %s, %s, %s, %s, %s, %s, %llu);",
				    sfx, local_file_id, table_id.index, SQLString(out.file_name), out.row_count, out.file_size_bytes,
				    DuckLakeUtil::OptionalIdxOrNull(out.footer_size),
				    DuckLakeUtil::OptionalIdxOrNull(out.flush_row_id_start),
				    DuckLakeUtil::OptionalIdxOrNull(out.partition_id),
				    DuckLakeUtil::EncryptionKeyLiteral(out.encryption_key),
				    DuckLakeUtil::MappingIdOrNull(out.mapping_id),
				    DuckLakeUtil::OptionalIdxOrNull(out.max_partial_file_snapshot),
				    DuckLakeUtil::OptionalIdxOrNull(out.begin_snapshot), compaction_id);
				for (auto &stat : out.column_stats) {
					auto info = DuckLakeColumnStatsInfo::FromColumnStats(stat.first, stat.second);
					sql += StringUtil::Format(
					    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_column_stats_%s "
					    "VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s);",
					    sfx, local_file_id, table_id.index, info.column_id.index, info.column_size_bytes,
					    info.value_count, info.null_count, info.min_val, info.max_val, info.contains_nan,
					    info.extra_stats);
				}
				for (auto &part : out.partition_values) {
					string val_literal = part.partition_value.IsNull()
					                         ? string("NULL")
					                         : DuckLakeUtil::SQLLiteralToString(part.partition_value.ToString());
					sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_partition_%s "
					                          "VALUES (%llu, %llu, %s);",
					                          sfx, local_file_id, part.partition_column_idx, val_literal);
				}
				output_local_id = std::to_string(local_file_id);
				local_file_id++;
			}

			const char *type_str =
			    compaction.type == CompactionType::REWRITE_DELETES ? "rewrite_delete" : "merge_adjacent";
			sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_compaction_%s "
			                          "VALUES (%llu, %llu, %s, %s, %s);",
			                          sfx, compaction_id, table_id.index, SQLString(type_str),
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
				    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_compaction_source_%s "
				    "VALUES (%llu, %llu, %llu, %s, %llu, %llu, %s, %llu, %s, %s, %s, %s);",
				    sfx, compaction_id, source_order, source.file.id.index,
				    DuckLakeUtil::SQLLiteralToString(source.file.data.path), source.file.row_count,
				    source.file.begin_snapshot, DuckLakeUtil::OptionalIdxOrNull(source.max_partial_file_snapshot),
				    static_cast<idx_t>(source.inlined_file_deletions.size()), last_id, last_path, last_row_count,
				    last_end_snap);
				source_order++;
			}
			compaction_id++;
		}
	}
	return sql;
}

static void FlattenNameMapEntry(const DuckLakeNameMapEntry &entry, idx_t map_id, idx_t parent_id,
                                idx_t &next_entry_id, const string &sfx, string &sql) {
	idx_t entry_id = next_entry_id++;
	string parent_literal = parent_id == NumericLimits<idx_t>::Maximum() ? string("NULL") : std::to_string(parent_id);
	sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_name_map_entry_%s "
	                          "VALUES (%llu, %llu, %s, %s, %llu, %s);",
	                          sfx, map_id, entry_id, parent_literal, SQLString(entry.source_name),
	                          entry.target_field_id.index, entry.hive_partition ? "true" : "false");
	for (auto &child : entry.child_entries) {
		FlattenNameMapEntry(*child, map_id, entry_id, next_entry_id, sfx, sql);
	}
}

static string EmitStagedNameMaps(const DuckLakeNameMapSet &name_maps, const string &sfx) {
	// Transaction-local column mappings (`mapping_id.index >= TRANSACTION_LOCAL_ID_START` on data
	// files). State.GetNewNameMaps allocates real IDs and remaps file references at commit time;
	// we just need to ship the map tree.
	string sql;
	// `map_id UBIGINT`: transaction-local sentinel values exceed BIGINT range.
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_name_map_%s("
	                          "map_id UBIGINT, table_id BIGINT);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_name_map_entry_%s("
	                          "map_id UBIGINT, entry_id BIGINT, parent_entry_id BIGINT, "
	                          "source_name VARCHAR, target_field_id BIGINT, hive_partition BOOLEAN);",
	                          sfx);
	for (auto &entry : name_maps.name_maps) {
		auto &map = *entry.second;
		sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_name_map_%s VALUES (%llu, %llu);", sfx,
		                          map.id.index, map.table_id.index);
		idx_t next_entry_id = 0;
		for (auto &col : map.column_maps) {
			FlattenNameMapEntry(*col, map.id.index, NumericLimits<idx_t>::Maximum(), next_entry_id, sfx, sql);
		}
	}
	return sql;
}

static string EmitStagedFlushedInlinedTables(const vector<FlushedInlinedTableInfo> &flushed, const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_flushed_inlined_%s("
	                          "inlined_table_name VARCHAR, schema_version BIGINT, flush_snapshot_id BIGINT);",
	                          sfx);
	for (auto &entry : flushed) {
		sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_flushed_inlined_%s "
		                          "VALUES (%s, %llu, %llu);",
		                          sfx, SQLString(entry.inlined_table.table_name), entry.inlined_table.schema_version,
		                          entry.flush_snapshot_id);
	}
	return sql;
}

static string EmitStagedDroppedFiles(const unordered_map<string, DataFileIndex> &dropped_files, const string &sfx) {
	// Whole-file drops (DELETE covering every live row of a file). Source: state->dropped_files.
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_dropped_file_%s("
	                          "path VARCHAR, data_file_id BIGINT);",
	                          sfx);
	for (auto &entry : dropped_files) {
		sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_dropped_file_%s "
		                          "VALUES (%s, %llu);",
		                          sfx, SQLString(entry.first), entry.second.index);
	}
	return sql;
}

static string EmitStagedInlinedFileDeletes(const LocalTableChanges &local_changes, const string &sfx) {
	// Row-level deletes against parquet files that are recorded in metadata rather than written as
	// delete-vector parquet (see ducklake_inlined_delete_<table_id>). Source: new_inlined_file_deletes.
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_file_delete_%s("
	                          "table_id BIGINT, file_id BIGINT, deleted_row_id BIGINT);",
	                          sfx);
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		if (!table_changes.new_inlined_file_deletes) {
			continue;
		}
		for (auto &file_entry : table_changes.new_inlined_file_deletes->file_deletes) {
			auto file_id = file_entry.first;
			for (auto &row_id : file_entry.second) {
				sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_file_delete_%s "
				                          "VALUES (%llu, %llu, %llu);",
				                          sfx, table_id.index, file_id, row_id);
			}
		}
	}
	return sql;
}

string DuckLakeStagedCommit::Build(DuckLakeTransaction &transaction,
                                   const TransactionChangeInformation &transaction_changes,
                                   const DuckLakeSnapshot &transaction_snapshot,
                                   const DuckLakeRetryConfig &retry_config) const {
	(void)transaction_changes; // server rebuilds the change set from LocalTableChanges
	auto &ducklake_catalog = transaction.GetCatalog();
	auto schema_name = ducklake_catalog.MetadataSchemaName();
	string batch;
	batch += EmitStagedCommitHeader(transaction.GetCommitInfo(), transaction_snapshot, ducklake_catalog.DataPath(),
	                                ducklake_catalog.Separator(), identifier_suffix);
	idx_t local_file_id = 0;
	batch += EmitStagedDataFiles(transaction.GetLocalChanges(), identifier_suffix, local_file_id);
	batch += EmitStagedCompactions(transaction.GetLocalChanges(), identifier_suffix, local_file_id);
	batch += EmitStagedInlinedData(transaction.GetLocalChanges(), transaction, identifier_suffix);
	batch += EmitStagedInlinedDeletes(transaction.GetLocalChanges(), identifier_suffix);
	batch += EmitStagedInlinedFileDeletes(transaction.GetLocalChanges(), identifier_suffix);
	batch += EmitStagedDeleteFiles(transaction.GetLocalChanges(), identifier_suffix);
	batch += EmitStagedDroppedFiles(transaction.GetDroppedFiles(), identifier_suffix);
	batch += EmitStagedFlushedInlinedTables(transaction.GetFlushedInlinedTables(), identifier_suffix);
	batch += EmitStagedNameMaps(transaction.GetNewNameMaps(), identifier_suffix);
	batch += StringUtil::Format("SELECT * FROM ducklake_commit(%s, %s, %lld, "
	                            "max_retry_count => %llu, retry_wait_ms => %llu, retry_backoff => %f);",
	                            DuckLakeUtil::SQLLiteralToString(identifier_suffix),
	                            DuckLakeUtil::SQLLiteralToString(schema_name), transaction_snapshot.schema_version,
	                            retry_config.max_retry_count, retry_config.retry_wait_ms, retry_config.retry_backoff);
	return batch;
}

} // namespace duckdb
