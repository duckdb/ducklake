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
#include "common/ducklake_snapshot.hpp"
#include "common/index.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

struct SnapshotChangeInformation {
	case_insensitive_set_t created_schemas;
	set<SchemaIndex> dropped_schemas;
	case_insensitive_map_t<case_insensitive_map_t<string>> created_tables;
	set<TableIndex> altered_tables;
	set<TableIndex> altered_views;
	set<TableIndex> dropped_tables;
	set<TableIndex> dropped_views;
	set<TableIndex> inserted_tables;
	set<TableIndex> tables_deleted_from;
	set<TableIndex> tables_compacted;
	set<TableIndex> tables_inserted_inlined;
	set<TableIndex> tables_deleted_inlined;
	set<TableIndex> tables_flushed_inlined;

	static SnapshotChangeInformation ParseChangesMade(const string &changes_made);
};

} // namespace duckdb
