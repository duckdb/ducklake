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

namespace duckdb {
struct SetCommentInfo;
class DuckLakeTransaction;

class DuckLakeMacroEntry : public MacroCatalogEntry {};
} // namespace duckdb
