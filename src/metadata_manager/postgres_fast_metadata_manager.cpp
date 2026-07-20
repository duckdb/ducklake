#include "metadata_manager/postgres_fast_metadata_manager.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

#include <atomic>

namespace duckdb {

PostgresFastMetadataManager::PostgresFastMetadataManager(DuckLakeTransaction &transaction)
    : PostgresMetadataManager(transaction) {
}

vector<DuckLakeSnapshotInfo> PostgresFastMetadataManager::GetAllSnapshots(const string &filter) {
	// The scanner is marginally faster when every snapshot must be returned. PostgreSQL-native
	// execution pays off when expiration supplies a selective predicate and avoids scanning rejected rows.
	if (filter.empty()) {
		return PostgresMetadataManager::GetAllSnapshots(filter);
	}
	static atomic<idx_t> next_selection_id(0);
	auto selection_name = StringUtil::Format("ducklake_fast_expired_snapshots_%llu", next_selection_id++);
	auto create_selection = StringUtil::Format(R"SQL(
CREATE TEMP TABLE %s ON COMMIT DROP AS
SELECT snapshot_id
FROM {METADATA_CATALOG}.ducklake_snapshot
WHERE %s;
CREATE UNIQUE INDEX ON %s(snapshot_id);
)SQL",
	                                           selection_name, filter, selection_name);
	auto selection_result = PostgresMetadataManager::Execute({}, create_selection);
	if (selection_result->HasError()) {
		selection_result->GetErrorObject().Throw("Failed to select snapshots using the PostgreSQL fast path: ");
	}
	auto snapshot_query = StringUtil::Format(R"SQL(
SELECT snapshot.snapshot_id, snapshot.snapshot_time, snapshot.schema_version, snapshot.next_file_id,
       changes.changes_made, changes.author, changes.commit_message, changes.commit_extra_info
FROM {METADATA_CATALOG}.ducklake_snapshot snapshot
JOIN pg_temp.%s expired USING (snapshot_id)
LEFT JOIN {METADATA_CATALOG}.ducklake_snapshot_changes changes USING (snapshot_id)
ORDER BY snapshot.snapshot_id
)SQL",
	                                         selection_name);
	auto result = QueryPostgres({}, snapshot_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get snapshots using the PostgreSQL fast path: ");
	}

	auto context = transaction.context.lock();
	vector<DuckLakeSnapshotInfo> snapshots;
	for (auto &row : *result) {
		DuckLakeSnapshotInfo snapshot_info;
		snapshot_info.id = row.GetValue<idx_t>(0);
		auto time = row.GetChunk().GetValue(1, row.GetRowInChunk());
		snapshot_info.time = time.CastAs(*context, LogicalType::TIMESTAMP_TZ).GetValue<timestamp_tz_t>();
		snapshot_info.schema_version = row.GetValue<idx_t>(2);
		snapshot_info.next_file_id = row.GetValue<idx_t>(3);
		snapshot_info.change_info.changes_made = row.IsNull(4) ? string() : row.GetValue<string>(4);
		snapshot_info.author = row.GetChunk().GetValue(5, row.GetRowInChunk());
		snapshot_info.commit_message = row.GetChunk().GetValue(6, row.GetRowInChunk());
		snapshot_info.commit_extra_info = row.GetChunk().GetValue(7, row.GetRowInChunk());
		snapshot_info.expiration_selection = selection_name;
		snapshots.push_back(std::move(snapshot_info));
	}
	for (auto &snapshot : snapshots) {
		snapshot.expiration_selection_size = snapshots.size();
	}
	return snapshots;
}

vector<DuckLakeTableSizeInfo> PostgresFastMetadataManager::GetTableSizes(DuckLakeSnapshot snapshot) {
	auto result = QueryPostgres(snapshot, R"SQL(
SELECT tbl.schema_id, tbl.table_id, tbl.table_name, tbl.table_uuid,
       COALESCE(data_files.file_count, 0) AS data_file_count,
       COALESCE(data_files.total_file_size, 0) AS data_total_size,
       COALESCE(delete_files.file_count, 0) AS delete_file_count,
       COALESCE(delete_files.total_file_size, 0) AS delete_total_size
FROM {METADATA_CATALOG}.ducklake_table tbl
LEFT JOIN (
  SELECT table_id, COUNT(*) AS file_count, COALESCE(SUM(file_size_bytes), 0) AS total_file_size
  FROM {METADATA_CATALOG}.ducklake_data_file
  WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
  GROUP BY table_id
) data_files USING (table_id)
LEFT JOIN (
  SELECT table_id, COUNT(*) AS file_count, COALESCE(SUM(file_size_bytes), 0) AS total_file_size
  FROM {METADATA_CATALOG}.ducklake_delete_file
  WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
  GROUP BY table_id
) delete_files USING (table_id)
WHERE {SNAPSHOT_ID} >= tbl.begin_snapshot
  AND ({SNAPSHOT_ID} < tbl.end_snapshot OR tbl.end_snapshot IS NULL)
)SQL");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table sizes using the PostgreSQL fast path: ");
	}

	vector<DuckLakeTableSizeInfo> table_sizes;
	for (auto &row : *result) {
		DuckLakeTableSizeInfo table_size;
		table_size.schema_id = SchemaIndex(row.GetValue<idx_t>(0));
		table_size.table_id = TableIndex(row.GetValue<idx_t>(1));
		table_size.table_name = row.GetValue<string>(2);
		table_size.table_uuid = row.GetValue<string>(3);
		table_size.file_count = row.GetValue<idx_t>(4);
		table_size.file_size_bytes = row.GetValue<idx_t>(5);
		table_size.delete_file_count = row.GetValue<idx_t>(6);
		table_size.delete_file_size_bytes = row.GetValue<idx_t>(7);
		table_sizes.push_back(std::move(table_size));
	}
	return table_sizes;
}

void PostgresFastMetadataManager::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	if (snapshots.empty()) {
		return;
	}
	auto &selection_name = snapshots[0].expiration_selection;
	if (selection_name.empty()) {
		PostgresMetadataManager::DeleteSnapshots(snapshots);
		return;
	}
	for (auto &snapshot : snapshots) {
		if (snapshot.expiration_selection != selection_name || snapshot.expiration_selection_size != snapshots.size()) {
			throw InternalException("PostgreSQL fast snapshot selection does not match the snapshots being deleted");
		}
	}

	vector<TableIndex> stats_table_ids;
	string stats_query = "SELECT DISTINCT table_id FROM {METADATA_CATALOG}.ducklake_table_stats;";
	auto stats_result = QueryPostgres({}, stats_query);
	if (stats_result->HasError()) {
		stats_result->GetErrorObject().Throw("Failed to list table stats for cache invalidation in DuckLake: ");
	}
	for (auto &row : *stats_result) {
		stats_table_ids.push_back(TableIndex(row.GetValue<idx_t>(0)));
	}

	string batch = R"SQL(
DELETE FROM {METADATA_CATALOG}.ducklake_snapshot_changes c
USING {EXPIRATION_SELECTION} expired
WHERE c.snapshot_id = expired.snapshot_id;
DELETE FROM {METADATA_CATALOG}.ducklake_snapshot s
USING {EXPIRATION_SELECTION} expired
WHERE s.snapshot_id = expired.snapshot_id;

DROP TABLE IF EXISTS pg_temp.ducklake_fast_dead_tables;
CREATE TEMP TABLE ducklake_fast_dead_tables ON COMMIT DROP AS
SELECT DISTINCT t.table_id
FROM {METADATA_CATALOG}.ducklake_table t
WHERE t.end_snapshot IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
      WHERE s.snapshot_id >= t.begin_snapshot AND s.snapshot_id < t.end_snapshot)
  AND NOT EXISTS (
      SELECT 1 FROM {METADATA_CATALOG}.ducklake_table t2
      WHERE t2.table_id = t.table_id
        AND (t2.end_snapshot IS NULL OR EXISTS (
            SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s2
            WHERE s2.snapshot_id >= t2.begin_snapshot AND s2.snapshot_id < t2.end_snapshot)));
CREATE UNIQUE INDEX ON ducklake_fast_dead_tables(table_id);

DROP TABLE IF EXISTS pg_temp.ducklake_fast_dead_data_files;
CREATE TEMP TABLE ducklake_fast_dead_data_files ON COMMIT DROP AS
SELECT f.data_file_id,
       CASE
         WHEN NOT f.path_is_relative THEN f.path
         WHEN t.path IS NOT NULL AND NOT t.path_is_relative THEN t.path || f.path
         WHEN s.path IS NOT NULL AND NOT s.path_is_relative THEN s.path || COALESCE(t.path, '') || f.path
         ELSE {DATA_PATH} || COALESCE(s.path, '') || COALESCE(t.path, '') || f.path
       END AS cleanup_path,
       FALSE AS cleanup_path_is_relative
FROM {METADATA_CATALOG}.ducklake_data_file f
JOIN LATERAL (
  SELECT table_id, schema_id, path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_table table_path
  WHERE table_path.table_id = f.table_id
  ORDER BY begin_snapshot DESC LIMIT 1
) t ON true
JOIN LATERAL (
  SELECT path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_schema schema_path
  WHERE schema_path.schema_id = t.schema_id
  ORDER BY begin_snapshot DESC LIMIT 1
) s ON true
WHERE EXISTS (SELECT 1 FROM ducklake_fast_dead_tables dead WHERE dead.table_id = f.table_id)
   OR (f.end_snapshot IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot snap
       WHERE snap.snapshot_id >= f.begin_snapshot AND snap.snapshot_id < f.end_snapshot));
CREATE UNIQUE INDEX ON ducklake_fast_dead_data_files(data_file_id);

INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
SELECT data_file_id, cleanup_path, cleanup_path_is_relative, NOW()
FROM ducklake_fast_dead_data_files;
DELETE FROM {METADATA_CATALOG}.ducklake_file_column_stats x USING ducklake_fast_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;
DELETE FROM {METADATA_CATALOG}.ducklake_file_variant_stats x USING ducklake_fast_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;
DELETE FROM {METADATA_CATALOG}.ducklake_file_partition_value x USING ducklake_fast_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;

DROP TABLE IF EXISTS pg_temp.ducklake_fast_dead_delete_files;
CREATE TEMP TABLE ducklake_fast_dead_delete_files ON COMMIT DROP AS
SELECT f.delete_file_id,
       CASE
         WHEN NOT f.path_is_relative THEN f.path
         WHEN t.path IS NOT NULL AND NOT t.path_is_relative THEN t.path || f.path
         WHEN s.path IS NOT NULL AND NOT s.path_is_relative THEN s.path || COALESCE(t.path, '') || f.path
         ELSE {DATA_PATH} || COALESCE(s.path, '') || COALESCE(t.path, '') || f.path
       END AS cleanup_path,
       FALSE AS cleanup_path_is_relative
FROM {METADATA_CATALOG}.ducklake_delete_file f
JOIN LATERAL (
  SELECT table_id, schema_id, path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_table table_path
  WHERE table_path.table_id = f.table_id
  ORDER BY begin_snapshot DESC LIMIT 1
) t ON true
JOIN LATERAL (
  SELECT path, path_is_relative
  FROM {METADATA_CATALOG}.ducklake_schema schema_path
  WHERE schema_path.schema_id = t.schema_id
  ORDER BY begin_snapshot DESC LIMIT 1
) s ON true
WHERE EXISTS (SELECT 1 FROM ducklake_fast_dead_tables dead WHERE dead.table_id = f.table_id)
   OR EXISTS (SELECT 1 FROM ducklake_fast_dead_data_files dead WHERE dead.data_file_id = f.data_file_id)
   OR (f.end_snapshot IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot snap
       WHERE snap.snapshot_id >= f.begin_snapshot AND snap.snapshot_id < f.end_snapshot));
CREATE UNIQUE INDEX ON ducklake_fast_dead_delete_files(delete_file_id);

INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
SELECT delete_file_id, cleanup_path, cleanup_path_is_relative, NOW()
FROM ducklake_fast_dead_delete_files;
DELETE FROM {METADATA_CATALOG}.ducklake_delete_file x USING ducklake_fast_dead_delete_files dead
WHERE x.delete_file_id = dead.delete_file_id;
DELETE FROM {METADATA_CATALOG}.ducklake_data_file x USING ducklake_fast_dead_data_files dead
WHERE x.data_file_id = dead.data_file_id;

DO $ducklake_fast$
DECLARE inlined RECORD;
BEGIN
  FOR inlined IN
    SELECT table_name FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt
    JOIN ducklake_fast_dead_tables dead USING (table_id)
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS %%I.%%I', {METADATA_SCHEMA_NAME_LITERAL}, inlined.table_name);
  END LOOP;
END
$ducklake_fast$;

DELETE FROM {METADATA_CATALOG}.ducklake_table x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_table_stats x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_table_column_stats x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_partition_info x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_partition_column x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_column x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_column_tag x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_sort_info x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_sort_expression x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_schema_versions x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_inlined_data_tables x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;
DELETE FROM {METADATA_CATALOG}.ducklake_column_mapping x USING ducklake_fast_dead_tables dead
WHERE x.table_id = dead.table_id;

DELETE FROM {METADATA_CATALOG}.ducklake_schema x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_view x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_tag x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_macro x
WHERE x.end_snapshot IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_snapshot s
  WHERE s.snapshot_id >= x.begin_snapshot AND s.snapshot_id < x.end_snapshot);
DELETE FROM {METADATA_CATALOG}.ducklake_macro_impl x
WHERE NOT EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_macro m WHERE m.macro_id = x.macro_id);
DELETE FROM {METADATA_CATALOG}.ducklake_macro_parameters x
WHERE NOT EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_macro m WHERE m.macro_id = x.macro_id);

DROP TABLE IF EXISTS pg_temp.ducklake_fast_orphan_mappings;
CREATE TEMP TABLE ducklake_fast_orphan_mappings ON COMMIT DROP AS
SELECT DISTINCT n.mapping_id
FROM {METADATA_CATALOG}.ducklake_name_mapping n
WHERE NOT EXISTS (
  SELECT 1 FROM {METADATA_CATALOG}.ducklake_column_mapping m WHERE m.mapping_id = n.mapping_id);
DELETE FROM {METADATA_CATALOG}.ducklake_name_mapping n
USING ducklake_fast_orphan_mappings orphan
WHERE n.mapping_id = orphan.mapping_id
  AND NOT EXISTS (
    SELECT 1 FROM {METADATA_CATALOG}.ducklake_column_mapping m WHERE m.mapping_id = n.mapping_id);
)SQL";

	DuckLakeSnapshot execution_snapshot;
	batch = StringUtil::Replace(batch, "{EXPIRATION_SELECTION}", "pg_temp." + selection_name);
	auto result = PostgresMetadataManager::Execute(execution_snapshot, batch);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to expire snapshots using the PostgreSQL fast path: ");
	}

	string mappings_query = "SELECT mapping_id FROM pg_temp.ducklake_fast_orphan_mappings";
	auto mappings = QueryPostgres({}, mappings_query);
	if (mappings->HasError()) {
		mappings->GetErrorObject().Throw("Failed to list deleted name mappings in DuckLake: ");
	}
	for (auto &row : *mappings) {
		transaction.DeferNameMapCacheInvalidation(MappingIndex(row.GetValue<idx_t>(0)));
	}
	set<idx_t> next_file_ids;
	for (auto &snapshot : snapshots) {
		next_file_ids.insert(snapshot.next_file_id);
	}
	auto &catalog = transaction.GetCatalog();
	for (auto next_file_id : next_file_ids) {
		for (auto &table_id : stats_table_ids) {
			catalog.InvalidateTableStatsCache(next_file_id, table_id);
		}
	}
}

} // namespace duckdb
