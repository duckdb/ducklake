#include "storage/ducklake_view_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

DuckLakeViewEntry::DuckLakeViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info,
                                     TableIndex view_id, string view_uuid_p, string query_sql_p,
                                     LocalChange local_change)
    : ViewCatalogEntry(catalog, schema, info), view_id(view_id), view_uuid(std::move(view_uuid_p)),
      query_sql(std::move(query_sql_p)), local_change(local_change) {
}

DuckLakeViewEntry::DuckLakeViewEntry(DuckLakeViewEntry &parent, CreateViewInfo &info, LocalChange local_change)
    : DuckLakeViewEntry(parent.catalog, parent.schema, info, parent.GetViewId(), parent.GetViewUUID(), parent.query_sql,
                        local_change) {
}

unique_ptr<CatalogEntry> DuckLakeViewEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	switch (info.type) {
	case AlterType::SET_COMMENT: {
		auto &alter = info.Cast<SetCommentInfo>();
		auto info = GetInfo();
		info->comment = alter.comment_value;
		auto &view_info = info->Cast<CreateViewInfo>();
		return make_uniq<DuckLakeViewEntry>(*this, view_info, LocalChangeType::SET_COMMENT);
	}
	case AlterType::ALTER_VIEW: {
		auto &alter_view = info.Cast<AlterViewInfo>();
		switch (alter_view.alter_view_type) {
		case AlterViewType::RENAME_VIEW: {
			auto &rename_view = alter_view.Cast<RenameViewInfo>();
			auto create_info = GetInfo();
			auto &view_info = create_info->Cast<CreateViewInfo>();
			view_info.view_name = rename_view.new_view_name;
			// create a complete copy of this view with only the name changed
			return make_uniq<DuckLakeViewEntry>(*this, view_info, LocalChangeType::RENAMED);
		}
		default:
			throw NotImplementedException("Unsupported ALTER VIEW type in DuckLake");
		}
	}
	default:
		throw NotImplementedException("Unsupported ALTER type for VIEW");
	}
}

unique_ptr<CreateInfo> DuckLakeViewEntry::GetInfo() const {
	auto info = ViewCatalogEntry::GetInfo();
	auto &view_info = info->Cast<CreateViewInfo>();
	if (!view_info.query) {
		view_info.query = ParseSelectStatement();
	}
	return info;
}

string DuckLakeViewEntry::ToSQL() const {
	return GetInfo()->ToString();
}

unique_ptr<CatalogEntry> DuckLakeViewEntry::Copy(ClientContext &context) const {
	D_ASSERT(!internal);
	auto create_info = GetInfo();

	return make_uniq<DuckLakeViewEntry>(catalog, schema, create_info->Cast<CreateViewInfo>(), view_id, view_uuid,
	                                    query_sql, local_change);
}

unique_ptr<SelectStatement> DuckLakeViewEntry::ParseSelectStatement() const {
	Parser parser;
	parser.ParseQuery(query_sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InvalidInputException("Invalid input for view - view must have a single SELECT statement: \"%s\"",
		                            query_sql);
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

const SelectStatement &DuckLakeViewEntry::GetQuery() {
	lock_guard<mutex> l(lock);
	if (!query) {
		// parse the query
		query = ParseSelectStatement();
	}
	return *query;
}

string DuckLakeViewEntry::GetQuerySQL() {
	return query_sql;
}

} // namespace duckdb
