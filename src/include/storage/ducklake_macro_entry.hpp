//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_macro_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "common/index.hpp"
#include "common/local_change.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"

namespace duckdb {
struct SetCommentInfo;
class DuckLakeTransaction;

class DuckLakeScalarMacroEntry : public ScalarMacroCatalogEntry {
public:
	DuckLakeScalarMacroEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info,
	                         MacroIndex &macro_index)
	    : ScalarMacroCatalogEntry(catalog, schema, info), index(macro_index) {};

	MacroIndex GetIndex() const {
		return index;
	}

private:
	MacroIndex index;
};

class DuckLakeTableMacroEntry : public TableMacroCatalogEntry {
public:
	DuckLakeTableMacroEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info,
	                        MacroIndex &macro_index)
	    : TableMacroCatalogEntry(catalog, schema, info), index(macro_index) {};

	MacroIndex GetIndex() const {
		return index;
	}

private:
	MacroIndex index;
};

} // namespace duckdb
