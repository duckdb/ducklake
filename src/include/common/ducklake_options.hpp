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
	option_map_t config_options;
	map<SchemaIndex, option_map_t> schema_options;
	map<TableIndex, option_map_t> table_options;
	idx_t busy_timeout = 5000;
	DuckLakeVersion ducklake_version = DuckLakeVersion::UNSET;

	//! Encryption envelope: address of the key service. Unset means no envelope.
	string encryption_socket;
	//! Scopes every key in this lake; required whenever encryption_socket is set.
	string encryption_lake_id;
	//! Distinguishes an option supplied as empty from an option not supplied.
	bool encryption_socket_supplied = false;
	bool encryption_lake_id_supplied = false;
	//! How long an unwrapped key may stay cached; the default lives in
	//! DuckLakeEncryptionProvider, hence the flag rather than a pre-set value.
	int64_t encryption_cache_ttl_seconds = 0;
	bool encryption_cache_ttl_seconds_supplied = false;
};

} // namespace duckdb
