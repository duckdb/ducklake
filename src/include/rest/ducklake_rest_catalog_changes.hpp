//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_catalog_changes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_api.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {
class ClientContext;
struct AlterInfo;
struct AlterEntryData;

//! Conversion of catalog changes (ALTER statements, column definitions) to and from their REST API representation
struct DuckLakeRESTCatalogChanges {
	static RESTColumnDefinition WriteColumn(const ColumnDefinition &column, bool not_null);
	static ColumnDefinition ReadColumn(ClientContext &context, const RESTColumnDefinition &column);

	static unique_ptr<RESTAlteration> WriteAlter(const AlterInfo &info);
	static unique_ptr<AlterInfo> ReadAlter(ClientContext &context, const RESTAlteration &alteration,
	                                       const AlterEntryData &entry);
};

} // namespace duckdb
