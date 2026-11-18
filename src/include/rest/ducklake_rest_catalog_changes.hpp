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
	static RESTColumnDefinition WriteColumn(const ColumnDefinition &column, bool not_null,
	                                        const Value &initial_default = Value());
	//! The value that rows written before the column existed get, from the default in the session of the client
	static Value GetInitialDefault(ClientContext &context, const ColumnDefinition &column);
	//! The initial default a client sent, NULL if it sent none. Rejects a default that the time zone of the server
	//! would decide without one
	static Value ReadInitialDefault(ClientContext &context, const RESTColumnDefinition &column);
	static ColumnDefinition ReadColumn(ClientContext &context, const RESTColumnDefinition &column);

	static unique_ptr<RESTAlteration> WriteAlter(ClientContext &context, const AlterInfo &info);
	static unique_ptr<AlterInfo> ReadAlter(ClientContext &context, const RESTAlteration &alteration,
	                                       const AlterEntryData &entry);
};

} // namespace duckdb
