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

//! A config option a commit set, recorded in its snapshot changes as `set_option:<scope>.<scope_id>.<key>`
struct DuckLakeSetOption {
	//! "global", "schema" or "table"
	string scope;
	idx_t scope_id;
	string key;
	//! Only known for this transaction's own options - the change record does not carry it
	string value;

	string ToChangeValue() const;
	static DuckLakeSetOption FromChangeValue(const string &value);
	bool operator<(const DuckLakeSetOption &other) const {
		return std::tie(scope, scope_id, key) < std::tie(other.scope, other.scope_id, other.key);
	}
};

struct TransactionChangeInformation {
	case_insensitive_map_t<reference<DuckLakeSchemaEntry>> created_schemas;
	map<SchemaIndex, reference<DuckLakeSchemaEntry>> dropped_schemas;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_tables;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_scalar_macros;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_table_macros;

	set<TableIndex> altered_tables;
	set<TableIndex> altered_tables_with_schema_version_changes;
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
	set<DuckLakeSetOption> set_options;
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
	set<DuckLakeSetOption> set_options;
	static SnapshotChangeInformation ParseChangesMade(const string &changes_made);
};

} // namespace duckdb
