//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ColumnList;
class DuckLakeMetadataManager;
class FileSystem;
class TableFilter;
class DynamicFilter;
struct DuckLakeColumnInfo;

struct ParsedCatalogEntry {
	string schema;
	string name;
};

class DuckLakeUtil {
public:
	static string ParseQuotedValue(const string &input, idx_t &pos);
	static string ToQuotedList(const vector<string> &input, char list_separator = ',');
	static vector<string> ParseQuotedList(const string &input, char list_separator = ',');
	static string SQLIdentifierToString(const string &text);
	static string SQLLiteralToString(const string &text);
	static string StatsToString(const string &text);
	static string ValueToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, const Value &val);

	static ParsedCatalogEntry ParseCatalogEntry(const string &input);
	static string JoinPath(FileSystem &fs, const string &a, const string &b);

	static DynamicFilter *GetOptionalDynamicFilter(const TableFilter &filter);

	//! Returns true if the given column name conflicts with inlined data system columns
	static bool IsInlinedSystemColumn(const string &name);
	//! Returns true if any column name conflicts with inlined data system columns
	static bool HasInlinedSystemColumnConflict(const ColumnList &columns);
	static bool HasInlinedSystemColumnConflict(const vector<DuckLakeColumnInfo> &columns);
};

} // namespace duckdb
