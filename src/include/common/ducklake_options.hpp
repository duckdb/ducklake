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
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "common/index.hpp"

namespace duckdb {

using option_map_t = unordered_map<string, string>;

struct DuckLakeOptions {
	string metadata_database;
	string metadata_path;
	string metadata_schema;
	string data_path;
	string static_backup;
	bool override_data_path = false;
	AccessMode access_mode = AccessMode::AUTOMATIC;
	DuckLakeEncryption encryption = DuckLakeEncryption::AUTOMATIC;
	bool create_if_not_exists = true;
	bool automatic_migration = false;
	unique_ptr<BoundAtClause> at_clause;
	case_insensitive_map_t<Value> metadata_parameters;
	option_map_t config_options;
	map<SchemaIndex, option_map_t> schema_options;
	map<TableIndex, option_map_t> table_options;
	idx_t busy_timeout = 5000;
};

} // namespace duckdb
