//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_staged_commit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

class DuckLakeTransaction;
enum class DeleteFileSource : uint8_t;
struct DuckLakeRetryConfig;

enum class DuckLakeStagedTableType : uint8_t {
	COMMIT_HEADER,
	DATA_FILE,
	DATA_FILE_COLUMN_STATS,
	DATA_FILE_PARTITION,
	DELETE_FILE,
	INLINED_DATA,
	INLINED_ROW,
	INLINED_COLUMN_STATS,
	INLINED_DELETE,
	INLINED_FILE_DELETE,
	DROPPED_FILE,
	TABLES_DELETED_FROM,
	TABLES_DELETE_ATTEMPTED,
	FLUSHED_INLINED,
	COMPACTION,
	COMPACTION_SOURCE,
	NAME_MAP,
	NAME_MAP_ENTRY,
};

//! A staging table bound to a specific commit (schema + identifier suffix).
class DuckLakeStagedTable {
public:
	static const char *BaseName(DuckLakeStagedTableType type);
	static string CreateAllSql();
	static string TruncateAllSql();
	static const char *DeleteFileSourceToString(DeleteFileSource source);
	static DeleteFileSource DeleteFileSourceFromString(const string &source);
};

//! Builds the SQL batch that stages a commit into temporary tables and calls ducklake_commit.
class DuckLakeStagedCommit {
public:
	//! Build the full SQL batch from transaction state.
	string Build(DuckLakeTransaction &transaction, const DuckLakeSnapshot &transaction_snapshot,
	             const DuckLakeRetryConfig &retry_config) const;
};

} // namespace duckdb
