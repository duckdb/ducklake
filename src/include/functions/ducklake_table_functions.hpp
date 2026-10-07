//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/ducklake_table_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
class BoundAtClause;
class DuckLakeCatalog;
class DuckLakeTableEntry;
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
	static string FormatTimestampOlderThan(const string &interval);
	static bool TryGetNonNullOption(const TableFunctionBindInput &input, const string &name, const string &type_name,
	                                Value &result);
	static string GetStringOption(const TableFunctionBindInput &input, const string &name);
	static string GetTableName(const Value &input);
	static BoundAtClause AtClauseFromValue(const Value &input);
};

struct MetadataBindData : public TableFunctionData {
	MetadataBindData() {
	}

	vector<vector<Value>> rows;
};

struct DuckLakeRunOnceState : public GlobalTableFunctionState {
	bool executed = false;
	idx_t offset = 0;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
};

class DuckLakeBaseMetadataFunction : public TableFunction {
public:
	DuckLakeBaseMetadataFunction(Identifier name, table_function_bind_t bind);

	static DuckLakeCatalog &GetCatalog(ClientContext &context, TableFunctionBindInput &input);
	static DuckLakeTableEntry &GetTableEntry(ClientContext &context, DuckLakeCatalog &catalog, const string &schema,
	                                         const EntryLookupInfo &lookup, const string &not_a_table_hint = "");
	static DuckLakeTableEntry &GetTableEntry(ClientContext &context, DuckLakeCatalog &catalog, const string &schema,
	                                         const string &table);
	static vector<reference<DuckLakeTableEntry>> GetTablesInSchema(ClientContext &context, SchemaCatalogEntry &schema);
	static vector<reference<DuckLakeTableEntry>> GetTablesInScope(ClientContext &context, DuckLakeCatalog &catalog,
	                                                              const string &schema, const string &table);
	static void ScanRows(const vector<vector<Value>> &rows, idx_t &offset, DataChunk &output);
};

class DuckLakeSnapshotsFunction : public DuckLakeBaseMetadataFunction {
public:
	DuckLakeSnapshotsFunction();

	static void GetSnapshotTypes(vector<LogicalType> &return_types, vector<Identifier> &names);
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

class DuckLakeCommitFunction : public TableFunction {
public:
	DuckLakeCommitFunction();
};

} // namespace duckdb
