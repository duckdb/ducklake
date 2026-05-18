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
#include "storage/ducklake_change_info.hpp"

namespace duckdb {
class CatalogEntry;
class DuckLakeSchemaEntry;

struct TransactionChangeInformation {
	ChangeInfo<case_insensitive_set_t, ChangeKind::CREATED_SCHEMA> created_schemas;
	ChangeInfo<map<SchemaIndex, reference<DuckLakeSchemaEntry>>, ChangeKind::DROPPED_SCHEMA> dropped_schemas;
	ChangeInfo<case_insensitive_map_t<reference_set_t<CatalogEntry>>, ChangeKind::CREATED_TABLE> created_tables;
	ChangeInfo<case_insensitive_map_t<reference_set_t<CatalogEntry>>, ChangeKind::CREATED_SCALAR_MACRO>
	    created_scalar_macros;
	ChangeInfo<case_insensitive_map_t<reference_set_t<CatalogEntry>>, ChangeKind::CREATED_TABLE_MACRO>
	    created_table_macros;

	ChangeInfo<set<TableIndex>, ChangeKind::ALTERED_TABLE> altered_tables;
	ChangeInfo<set<TableIndex>, ChangeKind::ALTERED_TABLE_WITH_SCHEMA_VERSION_CHANGES>
	    altered_tables_with_schema_version_changes;
	ChangeInfo<set<TableIndex>, ChangeKind::ALTERED_VIEW> altered_views;
	ChangeInfo<set<TableIndex>, ChangeKind::DROPPED_TABLE> dropped_tables;
	ChangeInfo<set<TableIndex>, ChangeKind::DROPPED_VIEW> dropped_views;
	ChangeInfo<set<MacroIndex>, ChangeKind::DROPPED_SCALAR_MACRO> dropped_scalar_macros;
	ChangeInfo<set<MacroIndex>, ChangeKind::DROPPED_TABLE_MACRO> dropped_table_macros;
	ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INTO> tables_inserted_into;
	ChangeInfo<set<TableIndex>, ChangeKind::DELETED_FROM> tables_deleted_from;
	ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INLINED> tables_inserted_inlined;
	ChangeInfo<set<TableIndex>, ChangeKind::DELETED_INLINED> tables_deleted_inlined;
	ChangeInfo<set<TableIndex>, ChangeKind::FLUSHED_INLINED> tables_flushed_inlined;
	ChangeInfo<set<TableIndex>, ChangeKind::COMPACTED> tables_compacted;
	ChangeInfo<set<TableIndex>, ChangeKind::MERGE_ADJACENT> tables_merge_adjacent;
	ChangeInfo<set<TableIndex>, ChangeKind::REWRITE_DELETE> tables_rewrite_delete;
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
	static SnapshotChangeInformation ParseChangesMade(const string &changes_made);
};

} // namespace duckdb
