//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/ducklake_table_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
class DuckLakeCatalog;
struct DuckLakeSnapshotInfo;

class DuckLakeTableFunctionUtil {
public:
	// Conform timestamp to ISO-8601 extended format with optional fractional seconds and timezone offset, e.g.:
	// "2025-12-26T06:13:30.673176+00:00" (UTC) or "2025-12-26T01:13:30.673176-05:00" (EST)
	static string FormatTimestampISO8601(const timestamp_t timestamp) {
		auto ts_string = Timestamp::ToString(timestamp);
		std::replace(ts_string.begin(), ts_string.end(), ' ', 'T');
		return ts_string + "+00";
	}
};

struct MetadataBindData : public TableFunctionData {
	MetadataBindData() {
	}

	vector<vector<Value>> rows;
};

class DuckLakeBaseMetadataFunction : public TableFunction {
public:
	DuckLakeBaseMetadataFunction(string name, table_function_bind_t bind);

	static Catalog &GetCatalog(ClientContext &context, const Value &input);
};

class DuckLakeSnapshotsFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeSnapshotsFunction();

	static void GetSnapshotTypes(vector<LogicalType> &return_types, vector<string> &names);
	static vector<Value> GetSnapshotValues(const DuckLakeSnapshotInfo &snapshot);
};

class DuckLakeTableInfoFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeTableInfoFunction();
};

class DuckLakeTableInsertionsFunction {
public:
	static TableFunctionSet GetFunctions();
	static unique_ptr<CreateMacroInfo> GetDuckLakeTableChanges();
};

class DuckLakeTableDeletionsFunction {
public:
	static TableFunctionSet GetFunctions();
};

class DuckLakeMergeAdjacentFilesFunction : public TableFunction {
public:
	static TableFunctionSet GetFunctions();
};

class DuckLakeRewriteDataFilesFunction : public TableFunction {
public:
	static TableFunctionSet GetFunctions();
};

class DuckLakeCleanupOldFilesFunction : public TableFunction {
public:
	DuckLakeCleanupOldFilesFunction();
};

class DuckLakeCleanupOrphanedFilesFunction : public TableFunction {
public:
	DuckLakeCleanupOrphanedFilesFunction();
};

class DuckLakeExpireSnapshotsFunction : public TableFunction {
public:
	DuckLakeExpireSnapshotsFunction();
};

class DuckLakeFlushInlinedDataFunction : public TableFunction {
public:
	DuckLakeFlushInlinedDataFunction();
};

class DuckLakeSetOptionFunction : public TableFunction {
public:
	DuckLakeSetOptionFunction();
};

class DuckLakeSetCommitMessage : public TableFunction {
public:
	DuckLakeSetCommitMessage();
};

class DuckLakeOptionsFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeOptionsFunction();
};

class DuckLakeLastCommittedSnapshotFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeLastCommittedSnapshotFunction();
};

class DuckLakeListFilesFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeListFilesFunction();
};

class DuckLakeCurrentSnapshotFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeCurrentSnapshotFunction();
};

class DuckLakeAddDataFilesFunction : public TableFunction {
public:
	static TableFunctionSet GetFunctions();
};

class DuckLakeSettingsFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeSettingsFunction();
};

} // namespace duckdb
