//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_transaction_changes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/reference_map.hpp"
#include "common/ducklake_snapshot.hpp"
#include "common/index.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {
class CatalogEntry;
class DuckLakeSchemaEntry;

struct TransactionChangeInformation {
	case_insensitive_map_t<reference<DuckLakeSchemaEntry>> created_schemas;
	map<SchemaIndex, reference<DuckLakeSchemaEntry>> dropped_schemas;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_tables;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_scalar_macros;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_table_macros;

	set<TableIndex> altered_tables;
	set<TableIndex> altered_tables_with_schema_version_changes;
	//! Tables that got a NOT NULL constraint, which a concurrent insert can violate
	set<TableIndex> tables_set_not_null;
	set<TableIndex> altered_views;
	set<TableIndex> dropped_tables;
	set<TableIndex> dropped_views;
	set<MacroIndex> dropped_scalar_macros;
	set<MacroIndex> dropped_table_macros;
	set<TableIndex> tables_inserted_into;
	set<TableIndex> tables_deleted_from;
	//! Tables a delete predicate was evaluated against, regardless of whether any rows matched
	set<TableIndex> tables_delete_attempted;
	set<TableIndex> tables_inserted_inlined;
	set<TableIndex> tables_deleted_inlined;
	set<TableIndex> tables_flushed_inlined;
	set<TableIndex> tables_compacted;
	set<TableIndex> tables_merge_adjacent;
	set<TableIndex> tables_rewrite_delete;
	//! The snapshot tags created and dropped, by name
	case_insensitive_set_t created_snapshot_tags;
	case_insensitive_set_t dropped_snapshot_tags;
	//! The snapshots the commit expires, read in each commit attempt
	set<idx_t> expired_snapshots;

	//! Whether the commit changes catalog-wide names, which every commit attempt validates
	bool HasRefChanges() const {
		return !created_snapshot_tags.empty() || !dropped_snapshot_tags.empty();
	}

	//! The tables whose data or schema the transaction changes
	vector<TableIndex> GetChangedTables() const {
		set<TableIndex> result;
		for (auto tables :
		     {&altered_tables, &altered_tables_with_schema_version_changes, &tables_set_not_null, &tables_inserted_into,
		      &tables_deleted_from, &tables_delete_attempted, &tables_inserted_inlined, &tables_deleted_inlined,
		      &tables_flushed_inlined, &tables_compacted, &tables_merge_adjacent, &tables_rewrite_delete}) {
			result.insert(tables->begin(), tables->end());
		}
		return vector<TableIndex>(result.begin(), result.end());
	}
};

struct SnapshotChangeInformation {
	case_insensitive_set_t created_schemas;
	set<SchemaIndex> dropped_schemas;
	case_insensitive_map_t<case_insensitive_map_t<string>> created_tables;
	case_insensitive_map_t<case_insensitive_map_t<string>> created_scalar_macros;
	case_insensitive_map_t<case_insensitive_map_t<string>> created_table_macros;
	set<TableIndex> altered_tables;
	set<TableIndex> altered_views;
	set<TableIndex> dropped_tables;
	set<TableIndex> dropped_views;
	set<MacroIndex> dropped_scalar_macros;
	set<MacroIndex> dropped_table_macros;
	set<TableIndex> inserted_tables;
	set<TableIndex> tables_deleted_from;
	set<TableIndex> tables_compacted;
	set<TableIndex> tables_merge_adjacent;
	set<TableIndex> tables_rewrite_delete;
	set<TableIndex> tables_inserted_inlined;
	set<TableIndex> tables_deleted_inlined;
	set<TableIndex> tables_flushed_inlined;
	case_insensitive_set_t created_snapshot_tags;
	case_insensitive_set_t dropped_snapshot_tags;
	set<idx_t> expired_snapshots;
	static SnapshotChangeInformation ParseChangesMade(const string &changes_made);
};

} // namespace duckdb
