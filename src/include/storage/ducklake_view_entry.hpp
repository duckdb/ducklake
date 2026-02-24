//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_view_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "common/index.hpp"
#include "common/local_change.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {
struct SetCommentInfo;
class DuckLakeTransaction;

class DuckLakeViewEntry : public ViewCatalogEntry {
public:
	DuckLakeViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info, TableIndex view_id,
	                  string view_uuid, string query_sql, LocalChange local_change);

public:
	TableIndex GetViewId() const {
		return view_id;
	}
	const string &GetViewUUID() const {
		return view_uuid;
	}
	bool IsTransactionLocal() const {
		return local_change.type != LocalChangeType::NONE;
	}
	LocalChange GetLocalChange() const {
		return local_change;
	}

public:
	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	const SelectStatement &GetQuery() override;
	unique_ptr<CreateInfo> GetInfo() const override;
	string ToSQL() const override;

	string GetQuerySQL();

public:
	// ALTER VIEW
	DuckLakeViewEntry(DuckLakeViewEntry &parent, CreateViewInfo &info, LocalChange local_change);

private:
	unique_ptr<SelectStatement> ParseSelectStatement() const;

private:
	mutable mutex lock;
	TableIndex view_id;
	string view_uuid;
	string query_sql;
	LocalChange local_change;
};

} // namespace duckdb
