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

struct MetadataBindData : public TableFunctionData {
	MetadataBindData() {
	}

	vector<vector<Value>> rows;
};

class BaseMetadataFunction : public TableFunction {
public:
	BaseMetadataFunction(string name, table_function_bind_t bind);

	static Catalog &GetCatalog(ClientContext &context, const Value &input);
};

class DuckLakeSnapshotsFunction : public BaseMetadataFunction {
public:
	DuckLakeSnapshotsFunction();

	static void GetSnapshotTypes(vector<LogicalType> &return_types, vector<string> &names);
	static vector<Value> GetSnapshotValues(const DuckLakeSnapshotInfo &snapshot);
};

class DuckLakeTableInfoFunction : public BaseMetadataFunction {
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
	DuckLakeTableDeletionsFunction();
};

class DuckLakeMergeAdjacentFilesFunction : public TableFunction {
public:
	DuckLakeMergeAdjacentFilesFunction();
};

class DuckLakeCleanupOldFilesFunction : public TableFunction {
public:
	DuckLakeCleanupOldFilesFunction();
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

class DuckLakeOptionsFunction : public BaseMetadataFunction {
public:
	DuckLakeOptionsFunction();
};

class DuckLakeLastCommittedSnapshotFunction : public BaseMetadataFunction {
public:
	DuckLakeLastCommittedSnapshotFunction();
};

class DuckLakeListFilesFunction : public BaseMetadataFunction {
public:
	DuckLakeListFilesFunction();
};

class DuckLakeCurrentSnapshotFunction : public BaseMetadataFunction {
public:
	DuckLakeCurrentSnapshotFunction();
};

class DuckLakeAddDataFilesFunction : public TableFunction {
public:
	DuckLakeAddDataFilesFunction();
};

} // namespace duckdb
