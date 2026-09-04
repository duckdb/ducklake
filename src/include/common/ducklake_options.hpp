//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "common/ducklake_encryption.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "common/index.hpp"

namespace duckdb {

using option_map_t = unordered_map<string, string>;

//! Config options by scope. Callers name exactly one: a valid table_id, a valid schema_id, or
//! neither for the global scope.
struct DuckLakeConfigOptions {
	option_map_t &GetScope(SchemaIndex schema_id, TableIndex table_id) {
		if (table_id.IsValid()) {
			return table[table_id];
		}
		if (schema_id.IsValid()) {
			return schema[schema_id];
		}
		return global;
	}
	bool TryGet(const string &option, string &result, SchemaIndex schema_id, TableIndex table_id) const {
		if (table_id.IsValid()) {
			return Find(table, table_id, option, result);
		}
		if (schema_id.IsValid()) {
			return Find(schema, schema_id, option, result);
		}
		return Find(global, option, result);
	}
	option_map_t global;
	map<SchemaIndex, option_map_t> schema;
	map<TableIndex, option_map_t> table;

private:
	static bool Find(const option_map_t &scope, const string &option, string &result) {
		auto entry = scope.find(option);
		if (entry == scope.end()) {
			return false;
		}
		result = entry->second;
		return true;
	}
	template <class SCOPE_MAP, class SCOPE_ID>
	static bool Find(const SCOPE_MAP &scope_map, SCOPE_ID scope_id, const string &option, string &result) {
		auto entry = scope_map.find(scope_id);
		return entry != scope_map.end() && Find(entry->second, option, result);
	}
};

struct DuckLakeOptions {
	string metadata_database;
	string metadata_path;
	Identifier metadata_schema;
	string data_path;
	bool override_data_path = false;
	AccessMode access_mode = AccessMode::AUTOMATIC;
	DuckLakeEncryption encryption = DuckLakeEncryption::AUTOMATIC;
	bool create_if_not_exists = true;
	bool automatic_migration = false;
	bool hide_metadata_catalog = true;
	unique_ptr<BoundAtClause> at_clause;
	case_insensitive_map_t<Value> metadata_parameters;
	DuckLakeConfigOptions config;
	idx_t busy_timeout = 5000;
	DuckLakeVersion ducklake_version = DuckLakeVersion::UNSET;
};

} // namespace duckdb
