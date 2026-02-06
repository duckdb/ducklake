//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/function/table_function.hpp"
#include "common/ducklake_snapshot.hpp"
#include "common/index.hpp"

namespace duckdb {
class DuckLakeMultiFileList;
class DuckLakeTableEntry;
class DuckLakeTransaction;

class DuckLakeFunctions {
public:
	//! Table Functions
	static TableFunction GetDuckLakeScanFunction(DatabaseInstance &instance);

	static unique_ptr<FunctionData> BindDuckLakeScan(ClientContext &context, TableFunction &function);

	static CopyFunctionCatalogEntry &GetCopyFunction(ClientContext &context, const string &name);
};

enum class DuckLakeScanType { SCAN_TABLE, SCAN_INSERTIONS, SCAN_DELETIONS, SCAN_FOR_FLUSH };

struct DuckLakeFunctionInfo : public TableFunctionInfo {
	DuckLakeFunctionInfo(DuckLakeTableEntry &table, DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot);

	DuckLakeTableEntry &table;
	weak_ptr<DuckLakeTransaction> transaction;
	string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	DuckLakeSnapshot snapshot;
	TableIndex table_id;
	DuckLakeScanType scan_type = DuckLakeScanType::SCAN_TABLE;
	//! Start snapshot - only set for DuckLakeScanType::SCAN_INSERTIONS and DuckLakeScanType::SCAN_DELETIONS
	unique_ptr<DuckLakeSnapshot> start_snapshot;

	shared_ptr<DuckLakeTransaction> GetTransaction();
};

} // namespace duckdb
