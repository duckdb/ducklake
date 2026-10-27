#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_catalog.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

//! The rows of the alias that are visible at the snapshot
static string VisibleAt(const string &alias, idx_t snapshot_id) {
	return StringUtil::Format("%s.begin_snapshot <= %llu AND (%s.end_snapshot IS NULL OR %s.end_snapshot > %llu)",
	                          alias, snapshot_id, alias, alias, snapshot_id);
}

//! The data files of the source visible at the snapshot, with their ids in the copy
static string DataFileIdMap(const DuckLakeForkInfo &fork, idx_t first_id) {
	return StringUtil::Format(
	    "(SELECT f.data_file_id AS old_id, %llu + row_number() OVER (ORDER BY f.data_file_id) - 1 AS new_id FROM "
	    "{METADATA_CATALOG}.ducklake_data_file f WHERE f.table_id = %llu AND %s)",
	    first_id, fork.source_table_id.index, VisibleAt("f", fork.snapshot_id));
}

//! The delete files of the copied data files, with their ids in the copy
static string DeleteFileIdMap(const DuckLakeForkInfo &fork, idx_t first_id) {
	return StringUtil::Format(
	    "(SELECT d.delete_file_id AS old_id, %llu + row_number() OVER (ORDER BY d.delete_file_id) - 1 AS new_id FROM "
	    "{METADATA_CATALOG}.ducklake_delete_file d JOIN {METADATA_CATALOG}.ducklake_data_file f USING (data_file_id) "
	    "WHERE d.table_id = %llu AND %s AND %s)",
	    first_id, fork.source_table_id.index, VisibleAt("d", fork.snapshot_id), VisibleAt("f", fork.snapshot_id));
}

//! The column mappings of the copied data files, with their ids in the copy
static string MappingIdMap(const DuckLakeForkInfo &fork, idx_t first_id) {
	return StringUtil::Format(
	    "(SELECT m.mapping_id AS old_id, %llu + row_number() OVER (ORDER BY m.mapping_id) - 1 AS new_id FROM (SELECT "
	    "DISTINCT f.mapping_id FROM {METADATA_CATALOG}.ducklake_data_file f WHERE f.table_id = %llu AND %s AND "
	    "f.mapping_id IS NOT NULL) m)",
	    first_id, fork.source_table_id.index, VisibleAt("f", fork.snapshot_id));
}

void DuckLakeMetadataManager::CheckTableFork(TableIndex source_table_id, idx_t snapshot_id) {
	// the migration of an open attachment records the boundary after the catalog read its options
	auto result = Query("SELECT CAST(value AS BIGINT) FROM {METADATA_CATALOG}.ducklake_metadata WHERE key = "
	                    "'origin_boundary_snapshot' AND scope IS NULL");
	result->ThrowIfError("Failed to read the origin boundary of DuckLake: ");
	for (auto &row : *result) {
		auto boundary = row.GetValue<idx_t>(0);
		if (snapshot_id < boundary) {
			throw InvalidInputException("Cannot clone a table at snapshot %llu, which is older than the snapshot %llu "
			                            "at which the catalog started recording the origins of its rows",
			                            snapshot_id, boundary);
		}
	}
	// a file whose rows or deletions postdate the snapshot holds more than its state at the snapshot
	result = Query(StringUtil::Format(R"(
SELECT path FROM {METADATA_CATALOG}.ducklake_data_file f
WHERE f.table_id = %llu AND %s AND f.partial_max > %llu
UNION ALL
SELECT path FROM {METADATA_CATALOG}.ducklake_delete_file f
WHERE f.table_id = %llu AND %s AND f.partial_max > %llu
LIMIT 1)",
	                                  source_table_id.index, VisibleAt("f", snapshot_id), snapshot_id,
	                                  source_table_id.index, VisibleAt("f", snapshot_id), snapshot_id));
	result->ThrowIfError("Failed to read the files of the table to clone in DuckLake: ");
	for (auto &row : *result) {
		throw NotImplementedException("Cannot clone a table at snapshot %llu, the file \"%s\" holds rows or deletions "
		                              "of later snapshots, clone the table at a later snapshot",
		                              snapshot_id, row.GetValue<string>(0));
	}
	result = Query(StringUtil::Format(R"(
SELECT f.path FROM {METADATA_CATALOG}.ducklake_data_file f
JOIN {METADATA_CATALOG}.ducklake_partition_info p USING (partition_id)
WHERE f.table_id = %llu AND %s AND p.begin_snapshot > %llu
LIMIT 1)",
	                                  source_table_id.index, VisibleAt("f", snapshot_id), snapshot_id));
	result->ThrowIfError("Failed to read the partitions of the table to clone in DuckLake: ");
	for (auto &row : *result) {
		throw NotImplementedException("Cannot clone a table at snapshot %llu, the file \"%s\" is partitioned by a "
		                              "partition layout of a later snapshot, clone the table at a later snapshot",
		                              snapshot_id, row.GetValue<string>(0));
	}
}

optional_idx DuckLakeMetadataManager::GetForkSnapshot(TableIndex table_id) {
	auto result = Query(StringUtil::Format(R"(
SELECT t.begin_snapshot FROM {METADATA_CATALOG}.ducklake_table t
WHERE t.table_id = %llu AND t.begin_snapshot = (
	SELECT MIN(begin_snapshot) FROM {METADATA_CATALOG}.ducklake_table WHERE table_id = %llu)
AND EXISTS (
	SELECT 1 FROM {METADATA_CATALOG}.ducklake_schema_versions v WHERE v.table_id = %llu AND
	v.begin_snapshot < t.begin_snapshot))",
	                                       table_id.index, table_id.index, table_id.index));
	result->ThrowIfError("Failed to read the creation of a table in DuckLake: ");
	for (auto &row : *result) {
		return row.GetValue<idx_t>(0);
	}
	return optional_idx();
}

//! Whether no snapshot after the fork snapshot changed the source, so that its current statistics hold for the copy
static bool ForkAtHead(DuckLakeMetadataManager &metadata_manager, const DuckLakeForkInfo &fork) {
	auto result = metadata_manager.Query(StringUtil::Format(
	    "SELECT changes_made FROM {METADATA_CATALOG}.ducklake_snapshot_changes WHERE snapshot_id > %llu",
	    fork.snapshot_id));
	result->ThrowIfError("Failed to read the changes of the table to clone in DuckLake: ");
	auto table_id = fork.source_table_id;
	for (auto &row : *result) {
		if (row.IsNull(0)) {
			continue;
		}
		auto changes = SnapshotChangeInformation::ParseChangesMade(row.GetValue<string>(0));
		for (auto tables :
		     {&changes.altered_tables, &changes.dropped_tables, &changes.inserted_tables, &changes.tables_deleted_from,
		      &changes.tables_compacted, &changes.tables_merge_adjacent, &changes.tables_rewrite_delete,
		      &changes.tables_inserted_inlined, &changes.tables_deleted_inlined, &changes.tables_flushed_inlined}) {
			if (tables->count(table_id)) {
				return false;
			}
		}
	}
	return true;
}

string DuckLakeMetadataManager::ForkTable(const DuckLakeForkInfo &fork, DuckLakeSnapshot &commit_snapshot,
                                          const std::function<string(const string &, const string &)> &guard_sql) {
	CheckTableFork(fork.source_table_id, fork.snapshot_id);
	auto source = fork.source_table_id.index;
	auto target = fork.table_id.index;
	auto at = fork.snapshot_id;
	// the snapshot whose state the copy takes still exists when the commit writes it
	string batch = guard_sql(StringUtil::Format("'The snapshot %llu cannot be cloned, it was expired'", at),
	                         StringUtil::Format("FROM (SELECT 1) AS fork WHERE NOT EXISTS (SELECT 1 FROM "
	                                            "{METADATA_CATALOG}.ducklake_snapshot WHERE snapshot_id = %llu)",
	                                            at));

	// the ids of the copied files and mappings follow each other from the next file id
	auto counts = Query(StringUtil::Format("SELECT (SELECT count(*) FROM %s AS m), (SELECT count(*) FROM %s AS m), "
	                                       "(SELECT count(*) FROM %s AS m)",
	                                       DataFileIdMap(fork, 0), DeleteFileIdMap(fork, 0), MappingIdMap(fork, 0)));
	counts->ThrowIfError("Failed to count the files of the table to clone in DuckLake: ");
	idx_t data_file_count = 0;
	idx_t delete_file_count = 0;
	idx_t mapping_count = 0;
	for (auto &row : *counts) {
		data_file_count = row.GetValue<idx_t>(0);
		delete_file_count = row.GetValue<idx_t>(1);
		mapping_count = row.GetValue<idx_t>(2);
	}
	auto first_data_file_id = commit_snapshot.next_file_id;
	auto first_delete_file_id = first_data_file_id + data_file_count;
	auto first_mapping_id = first_delete_file_id + delete_file_count;
	commit_snapshot.next_file_id = first_mapping_id + mapping_count;
	auto data_files = DataFileIdMap(fork, first_data_file_id);

	// the partition specs that the copied files reference and the one of the snapshot, with fresh ids
	auto specs = Query(StringUtil::Format(R"(
SELECT p.partition_id, p.end_snapshot FROM {METADATA_CATALOG}.ducklake_partition_info p
WHERE p.table_id = %llu AND (%s OR p.partition_id IN (
	SELECT f.partition_id FROM {METADATA_CATALOG}.ducklake_data_file f WHERE f.table_id = %llu AND %s))
ORDER BY p.partition_id)",
	                                      source, VisibleAt("p", at), source, VisibleAt("f", at)));
	specs->ThrowIfError("Failed to read the partitions of the table to clone in DuckLake: ");
	vector<string> spec_ids;
	string spec_remap;
	for (auto &row : *specs) {
		auto spec_id = row.GetValue<idx_t>(0);
		auto new_spec_id = commit_snapshot.next_catalog_id++;
		// a spec that ended after the snapshot is open on the copy, earlier specs keep their intervals
		string end_snapshot = "NULL";
		if (!row.IsNull(1) && row.GetValue<idx_t>(1) <= at) {
			end_snapshot = to_string(row.GetValue<idx_t>(1));
		}
		batch += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_partition_info (partition_id, table_id, begin_snapshot, "
		    "end_snapshot) SELECT %llu, %llu, begin_snapshot, %s FROM {METADATA_CATALOG}.ducklake_partition_info "
		    "WHERE partition_id = %llu;\n",
		    new_spec_id, target, end_snapshot, spec_id);
		spec_ids.push_back(to_string(spec_id));
		spec_remap += StringUtil::Format(" WHEN %llu THEN %llu", spec_id, new_spec_id);
	}
	string partition_id = "NULL";
	if (!spec_ids.empty()) {
		auto spec_list = StringUtil::Join(spec_ids, ", ");
		partition_id = "CASE f.partition_id" + spec_remap + " END";
		batch += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_partition_column (partition_id, table_id, partition_key_index, "
		    "column_id, transform) SELECT CASE partition_id%s END, %llu, partition_key_index, column_id, transform "
		    "FROM {METADATA_CATALOG}.ducklake_partition_column WHERE partition_id IN (%s);\n",
		    spec_remap, target, spec_list);
	}

	// the sort spec of the snapshot, which the copy takes from its own creation
	auto sorts = Query(StringUtil::Format(
	    "SELECT s.sort_id FROM {METADATA_CATALOG}.ducklake_sort_info s WHERE s.table_id = %llu AND %s", source,
	    VisibleAt("s", at)));
	sorts->ThrowIfError("Failed to read the sort keys of the table to clone in DuckLake: ");
	for (auto &row : *sorts) {
		auto sort_id = row.GetValue<idx_t>(0);
		auto new_sort_id = commit_snapshot.next_catalog_id++;
		batch += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_sort_info (sort_id, table_id, begin_snapshot, end_snapshot) "
		    "VALUES (%llu, %llu, {SNAPSHOT_ID}, NULL);\n"
		    "INSERT INTO {METADATA_CATALOG}.ducklake_sort_expression (sort_id, table_id, sort_key_index, expression, "
		    "dialect, sort_direction, null_order) SELECT %llu, %llu, sort_key_index, expression, dialect, "
		    "sort_direction, null_order FROM {METADATA_CATALOG}.ducklake_sort_expression WHERE sort_id = %llu;\n",
		    new_sort_id, target, new_sort_id, target, sort_id);
	}

	// the column history up to the snapshot, the columns of later snapshots reserve their ids and are never visible
	batch += StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, column_order,
	column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, default_value_type,
	default_value_dialect)
SELECT column_id, CASE WHEN begin_snapshot > %llu THEN {SNAPSHOT_ID} ELSE begin_snapshot END,
	CASE WHEN begin_snapshot > %llu THEN {SNAPSHOT_ID} WHEN end_snapshot > %llu THEN NULL ELSE end_snapshot END, %llu,
	column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column,
	default_value_type, default_value_dialect
FROM {METADATA_CATALOG}.ducklake_column WHERE table_id = %llu;
INSERT INTO {METADATA_CATALOG}.ducklake_column_tag (table_id, column_id, begin_snapshot, end_snapshot, key, value)
SELECT %llu, column_id, begin_snapshot, CASE WHEN end_snapshot > %llu THEN NULL ELSE end_snapshot END, key, value
FROM {METADATA_CATALOG}.ducklake_column_tag WHERE table_id = %llu AND begin_snapshot <= %llu;
INSERT INTO {METADATA_CATALOG}.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value)
SELECT %llu, {SNAPSHOT_ID}, NULL, key, value FROM {METADATA_CATALOG}.ducklake_tag t WHERE t.object_id = %llu AND %s;
INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions (begin_snapshot, schema_version, table_id)
SELECT begin_snapshot, schema_version, %llu FROM {METADATA_CATALOG}.ducklake_schema_versions
WHERE table_id = %llu AND begin_snapshot <= %llu;
)",
	                            at, at, at, target, source, target, at, source, at, target, source, VisibleAt("t", at),
	                            target, source, at);

	// the files keep their physical identity, their paths are resolved when the copy has another directory
	bool same_directory = fork.source_path == fork.table_path;
	auto file_path = [&](const string &alias) {
		if (same_directory) {
			return StringUtil::Format("%s.path, %s.path_is_relative", alias, alias);
		}
		return StringUtil::Format("CASE WHEN %s.path_is_relative THEN %s || %s.path ELSE %s.path END, %s", alias,
		                          SQLString::ToString(fork.source_path), alias, alias,
		                          DuckLakeUtil::BoolLiteral(false));
	};
	auto mappings = MappingIdMap(fork, first_mapping_id);
	batch += StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_column_mapping (mapping_id, table_id, type)
SELECT m.new_id, %llu, c.type FROM {METADATA_CATALOG}.ducklake_column_mapping c JOIN %s AS m ON c.mapping_id = m.old_id;
INSERT INTO {METADATA_CATALOG}.ducklake_name_mapping (mapping_id, column_id, source_name, target_field_id,
	parent_column, is_partition)
SELECT m.new_id, n.column_id, n.source_name, n.target_field_id, n.parent_column, n.is_partition
FROM {METADATA_CATALOG}.ducklake_name_mapping n JOIN %s AS m ON n.mapping_id = m.old_id;
INSERT INTO {METADATA_CATALOG}.ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order,
	path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id,
	encryption_key, mapping_id, partial_max, row_group_count, origin_file_id, origin_snapshot)
SELECT m.new_id, %llu, f.begin_snapshot, NULL, f.file_order, %s, f.file_format, f.record_count, f.file_size_bytes,
	f.footer_size, f.row_id_start, %s, f.encryption_key, cm.new_id, f.partial_max, f.row_group_count,
	COALESCE(f.origin_file_id, f.data_file_id), f.origin_snapshot
FROM {METADATA_CATALOG}.ducklake_data_file f JOIN %s AS m ON f.data_file_id = m.old_id
LEFT JOIN %s AS cm ON f.mapping_id = cm.old_id;
INSERT INTO {METADATA_CATALOG}.ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot,
	data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key,
	partial_max, row_group_count, origin_file_id)
SELECT dm.new_id, %llu, d.begin_snapshot, NULL, m.new_id, %s, d.format, d.delete_count, d.file_size_bytes,
	d.footer_size, d.encryption_key, d.partial_max, d.row_group_count, COALESCE(d.origin_file_id, d.delete_file_id)
FROM {METADATA_CATALOG}.ducklake_delete_file d JOIN %s AS dm ON d.delete_file_id = dm.old_id
JOIN %s AS m ON d.data_file_id = m.old_id;
INSERT INTO {METADATA_CATALOG}.ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes,
	value_count, null_count, min_value, max_value, contains_nan, extra_stats, min_is_exact, max_is_exact)
SELECT m.new_id, %llu, s.column_id, s.column_size_bytes, s.value_count, s.null_count, s.min_value, s.max_value,
	s.contains_nan, s.extra_stats, s.min_is_exact, s.max_is_exact
FROM {METADATA_CATALOG}.ducklake_file_column_stats s JOIN %s AS m ON s.data_file_id = m.old_id;
INSERT INTO {METADATA_CATALOG}.ducklake_file_variant_stats (data_file_id, table_id, column_id, variant_path,
	shredded_type, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats)
SELECT m.new_id, %llu, s.column_id, s.variant_path, s.shredded_type, s.column_size_bytes, s.value_count,
	s.null_count, s.min_value, s.max_value, s.contains_nan, s.extra_stats
FROM {METADATA_CATALOG}.ducklake_file_variant_stats s JOIN %s AS m ON s.data_file_id = m.old_id;
INSERT INTO {METADATA_CATALOG}.ducklake_file_partition_value (data_file_id, table_id, partition_key_index,
	partition_value)
SELECT m.new_id, %llu, v.partition_key_index, v.partition_value
FROM {METADATA_CATALOG}.ducklake_file_partition_value v JOIN %s AS m ON v.data_file_id = m.old_id;
)",
	                            target, mappings, mappings, target, file_path("f"), partition_id, data_files, mappings,
	                            target, file_path("d"), DeleteFileIdMap(fork, first_delete_file_id), data_files, target,
	                            data_files, target, data_files, target, data_files);

	// the inlined rows visible at the snapshot keep their row ids and insert snapshots
	auto col_names = InlinedColNames();
	auto visible_rows = StringUtil::Format("%s <= %llu AND (%s IS NULL OR %s > %llu)", col_names.begin_snapshot, at,
	                                       col_names.end_snapshot, col_names.end_snapshot, at);
	auto containers = Query(StringUtil::Format("SELECT table_name, schema_version FROM "
	                                           "{METADATA_CATALOG}.ducklake_inlined_data_tables WHERE table_id = %llu",
	                                           source));
	containers->ThrowIfError("Failed to read the inlined data tables of the table to clone in DuckLake: ");
	bool has_inlined_rows = false;
	vector<string> registrations;
	for (auto &row : *containers) {
		auto source_name = row.GetValue<string>(0);
		auto schema_version = row.GetValue<idx_t>(1);
		auto copied_rows = Query(StringUtil::Format("SELECT count(*) FROM {METADATA_CATALOG}.%s WHERE %s",
		                                            SQLIdentifier(source_name), visible_rows));
		copied_rows->ThrowIfError("Failed to read the inlined data of the table to clone in DuckLake: ");
		idx_t count = 0;
		for (auto &count_row : *copied_rows) {
			count = count_row.GetValue<idx_t>(0);
		}
		if (count == 0) {
			continue;
		}
		has_inlined_rows = true;
		auto name = InlinedTableNameFor(target, schema_version);
		batch += StringUtil::Format("CREATE TABLE {METADATA_CATALOG}.%s AS SELECT * FROM {METADATA_CATALOG}.%s WHERE "
		                            "%s;\nUPDATE {METADATA_CATALOG}.%s SET %s = NULL;\n",
		                            SQLIdentifier(name), SQLIdentifier(source_name), visible_rows, SQLIdentifier(name),
		                            col_names.end_snapshot);
		registrations.push_back(InlinedTableRegistrationTuple(target, name, schema_version));
	}
	batch += InsertValuesSql("ducklake_inlined_data_tables", registrations);
	if (!registrations.empty()) {
		MarkPendingCacheClear();
	}
	auto deletion_table = InlinedFileDeletionTableName(fork.source_table_id);
	bool has_inlined_deletions = false;
	if (InlinedDeletionTableExists(deletion_table)) {
		auto deletions = Query(StringUtil::Format(
		    "SELECT count(*) FROM {METADATA_CATALOG}.%s d JOIN %s AS m ON d.file_id = m.old_id WHERE "
		    "d.begin_snapshot <= %llu",
		    SQLIdentifier(deletion_table), data_files, at));
		deletions->ThrowIfError("Failed to read the inlined deletions of the table to clone in DuckLake: ");
		for (auto &row : *deletions) {
			has_inlined_deletions = row.GetValue<idx_t>(0) > 0;
		}
	}
	if (has_inlined_deletions) {
		auto copied_table = InlinedFileDeletionTableName(fork.table_id);
		batch += InlinedFileDeletionTableDdl(copied_table);
		batch += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.%s (file_id, row_id, begin_snapshot) SELECT m.new_id, d.row_id, "
		    "d.begin_snapshot FROM {METADATA_CATALOG}.%s d JOIN %s AS m ON d.file_id = m.old_id WHERE d.begin_snapshot "
		    "<= %llu;\n",
		    SQLIdentifier(copied_table), SQLIdentifier(deletion_table), data_files, at);
		MarkPendingCacheClear();
	}

	// the statistics of the source hold for the copy when nothing changed the source after the snapshot
	if (ForkAtHead(*this, fork)) {
		batch += StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
SELECT %llu, record_count, next_row_id, file_size_bytes FROM {METADATA_CATALOG}.ducklake_table_stats
WHERE table_id = %llu;
INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan,
	min_value, max_value, extra_stats, min_is_exact, max_is_exact)
SELECT %llu, column_id, contains_null, contains_nan, min_value, max_value, extra_stats, min_is_exact, max_is_exact
FROM {METADATA_CATALOG}.ducklake_table_column_stats WHERE table_id = %llu;
)",
		                            target, source, target, source);
		return batch;
	}
	// otherwise the row count holds without inlined rows and the bounds of the columns are unknown
	string record_count = "NULL";
	if (!has_inlined_rows && !has_inlined_deletions) {
		record_count = StringUtil::Format(
		    "(SELECT COALESCE(SUM(f.record_count), 0) FROM {METADATA_CATALOG}.ducklake_data_file f JOIN %s AS m ON "
		    "f.data_file_id = m.old_id) - (SELECT COALESCE(SUM(d.delete_count), 0) FROM "
		    "{METADATA_CATALOG}.ducklake_delete_file d JOIN %s AS dm ON d.delete_file_id = dm.old_id)",
		    data_files, DeleteFileIdMap(fork, first_delete_file_id));
	}
	batch += StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
SELECT %llu, %s, next_row_id, NULL FROM {METADATA_CATALOG}.ducklake_table_stats WHERE table_id = %llu;
INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan,
	min_value, max_value, extra_stats, min_is_exact, max_is_exact)
SELECT %llu, c.column_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM {METADATA_CATALOG}.ducklake_column c
WHERE c.table_id = %llu AND %s AND EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_table_stats WHERE table_id = %llu);
)",
	                            target, record_count, source, target, source, VisibleAt("c", at), source);
	return batch;
}

} // namespace duckdb
