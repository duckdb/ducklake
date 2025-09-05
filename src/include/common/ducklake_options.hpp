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
	bool override_data_path = false;
	AccessMode access_mode = AccessMode::AUTOMATIC;
	DuckLakeEncryption encryption = DuckLakeEncryption::AUTOMATIC;
	bool create_if_not_exists = true;
	bool migrate_if_required = true;
	unique_ptr<BoundAtClause> at_clause;
	unordered_map<string, Value> metadata_parameters;
	option_map_t config_options;
	map<SchemaIndex, option_map_t> schema_options;
	map<TableIndex, option_map_t> table_options;
};

} // namespace duckdb
