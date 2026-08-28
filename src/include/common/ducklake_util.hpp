//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/index.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;
class DataChunk;
class ColumnList;
class DuckLakeCatalog;
class DuckLakeMetadataManager;
class FileSystem;
class Expression;
class LogicalType;
class TableFilter;
struct DynamicFilterData;

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
	static string SQLIdentifierToString(const Identifier &identifier);
	static string SQLLiteralToString(const string &text);
	static string StatsToString(const string &text);
	static string ValueToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, const Value &val);

	static ParsedCatalogEntry ParseCatalogEntry(const string &input);
	static string JoinPath(FileSystem &fs, const string &a, const string &b);

	static shared_ptr<DynamicFilterData> GetOptionalDynamicFilterData(const TableFilter &filter);

	//! Combine two filter expressions - both must hold, so AND their conjuncts and drop duplicates
	static unique_ptr<Expression> MergeFilterExpressions(unique_ptr<Expression> left, unique_ptr<Expression> right);
	//! Whether an expression reads a struct field by a constant name
	static bool IsStructExtract(const Expression &expr);
	//! A leaf filter is evaluated against a single column's stats, so it may only read one column. Returns that
	//! sub-expression, or nullptr when the filter reads several - those are split into a tree of leaves instead.
	static optional_ptr<const Expression> GetFilterSubject(const Expression &expr);
	//! Peel the struct fields a subject reads through, outermost first, and return the reference underneath
	static const Expression &GetFilterSubjectPath(const Expression &subject, vector<string> &path);
	//! Rewrite the subject to the column placeholder an ExpressionFilter is evaluated against
	static unique_ptr<Expression> ReplaceFilterSubject(const Expression &expr, const Expression &subject,
	                                                   const LogicalType &type);

	//! Create the data path directory if it does not yet exist
	static void EnsureDirectoryExists(FileSystem &fs, const string &data_path);

	//! Replace occurrences of `from` with `to`, skipping content inside
	//! single-quoted string literals and double-quoted identifiers.
	static string ReplaceSkippingQuotes(const string &sql, const string &from, const string &to);

	//! Returns true if the given column name conflicts with inlined data system columns
	static bool IsInlinedSystemColumn(const string &name, bool prefixed_inlined_columns);

	static string OptionalIdxOrNull(const optional_idx &v);

	static string MappingIdOrNull(const MappingIndex &m);

	static string EncryptionKeyLiteral(const string &key);

	static const char *BoolLiteral(bool v);

	static string PartitionValueLiteral(const Value &v);

	static string ChunkRowToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, DataChunk &chunk,
	                            idx_t row);
	//! Throws if a column name is reserved for inlined data metadata on this catalog
	static void ValidateInlinedSystemColumn(DuckLakeCatalog &catalog, ClientContext &context, SchemaIndex schema_id,
	                                        TableIndex table_id, const string &name);
	static void ValidateNoInlinedSystemColumns(DuckLakeCatalog &catalog, ClientContext &context, SchemaIndex schema_id,
	                                           const ColumnList &columns);
	//! Throws if a column conflicts with inlined data metadata columns when enabling inlining
	static void ValidateCanEnableInlining(const ColumnList &columns, bool prefixed_inlined_columns,
	                                      const string &table_name);

	//! Copy extension-registered settings from one context onto another. Core engine settings
	//! are not copied.
	static void CopyExtensionSettings(ClientContext &from, ClientContext &to);
};

} // namespace duckdb
