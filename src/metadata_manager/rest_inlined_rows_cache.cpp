#include "metadata_manager/rest_inlined_rows_cache.hpp"

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

static RESTInlinedField ToInlinedField(const DuckLakeProjectedColumn &column) {
	RESTInlinedField result;
	result.all = column.children.empty();
	for (auto &child : column.children) {
		result.fields[child.field_index.index] = ToInlinedField(child);
	}
	return result;
}

RESTInlinedProjection RESTInlinedProjection::From(optional_ptr<const vector<DuckLakeProjectedColumn>> projection) {
	RESTInlinedProjection result;
	if (projection) {
		result.all = false;
		for (auto &column : *projection) {
			result.columns[column.field_index.index] = ToInlinedField(column);
		}
	}
	return result;
}

bool RESTInlinedField::Covers(const RESTInlinedField &other) const {
	if (all) {
		return true;
	}
	if (other.all) {
		return false;
	}
	for (auto &entry : other.fields) {
		auto field = fields.find(entry.first);
		if (field == fields.end() || !field->second.Covers(entry.second)) {
			return false;
		}
	}
	return true;
}

bool RESTInlinedProjection::Covers(const RESTInlinedProjection &other) const {
	if (all) {
		return true;
	}
	if (other.all) {
		return false;
	}
	for (auto &entry : other.columns) {
		auto column = columns.find(entry.first);
		if (column == columns.end() || !column->second.Covers(entry.second)) {
			return false;
		}
	}
	return true;
}

bool RESTInlinedRowsKey::operator<(const RESTInlinedRowsKey &other) const {
	return std::tie(kind, start_snapshot_id, snapshot_id, table_id) <
	       std::tie(other.kind, other.start_snapshot_id, other.snapshot_id, other.table_id);
}

//! Whether the rows have the column to read, which is a column name that is optionally cast
static bool HasColumn(const RESTInlinedRows &rows, const string &column_to_read) {
	auto expressions = Parser::GetBuiltinParser().ParseExpressionList(column_to_read);
	if (expressions.size() != 1) {
		return false;
	}
	optional_ptr<const ParsedExpression> column_ref = expressions[0].get();
	if (column_ref->GetExpressionClass() == ExpressionClass::CAST) {
		column_ref = column_ref->Cast<CastExpression>().Child();
	}
	if (column_ref->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		return false;
	}
	auto &name = column_ref->Cast<ColumnRefExpression>().GetColumnName();
	for (auto &column : rows.columns) {
		if (Identifier(column.name) == name) {
			return true;
		}
	}
	return false;
}

bool RESTInlinedRowsCache::Covers(const RESTInlinedRowsKey &key, const RESTInlinedProjection &projection) const {
	lock_guard<mutex> guard(lock);
	auto entry = responses.find(key);
	if (entry == responses.end()) {
		return false;
	}
	for (auto &response : entry->second) {
		if (response->projection.Covers(projection)) {
			return true;
		}
	}
	return false;
}

bool RESTInlinedRowsCache::HasResponse(const RESTInlinedRowsKey &key) const {
	lock_guard<mutex> guard(lock);
	auto entry = responses.find(key);
	return entry != responses.end() && !entry->second.empty();
}

bool RESTInlinedRowsCache::HasInlinedRows(idx_t snapshot_id, idx_t table_id, bool &has_inlined_rows) const {
	lock_guard<mutex> guard(lock);
	for (auto &entry : responses) {
		if (entry.first.snapshot_id != snapshot_id || entry.first.table_id != table_id || entry.second.empty()) {
			continue;
		}
		has_inlined_rows = entry.second.back()->rows != nullptr;
		return true;
	}
	return false;
}

shared_ptr<RESTInlinedRowsResponse> RESTInlinedRowsCache::Add(const RESTInlinedRowsKey &key,
                                                              const RESTInlinedProjection &projection,
                                                              unique_ptr<RESTInlinedRows> inlined_data) {
	auto response = make_shared_ptr<RESTInlinedRowsResponse>();
	response->projection = projection;
	if (inlined_data) {
		response->rows = make_shared_ptr<RESTCachedInlinedRows>();
		response->rows->rows = std::move(*inlined_data);
	}
	lock_guard<mutex> guard(lock);
	auto &entries = responses[key];
	for (auto &existing : entries) {
		if (existing->projection.Covers(projection)) {
			// a scan that ran at the same time has the columns already
			return response;
		}
	}
	for (idx_t i = entries.size(); i > 0; i--) {
		if (projection.Covers(entries[i - 1]->projection)) {
			entries.erase_at(i - 1);
		}
	}
	entries.push_back(response);
	return response;
}

shared_ptr<RESTCachedInlinedRows> RESTInlinedRowsCache::Find(const RESTInlinedRowsKey &key,
                                                             const vector<string> &columns_to_read) const {
	lock_guard<mutex> guard(lock);
	auto entry = responses.find(key);
	if (entry == responses.end()) {
		return nullptr;
	}
	for (idx_t i = entry->second.size(); i > 0; i--) {
		auto &rows = entry->second[i - 1]->rows;
		if (!rows) {
			continue;
		}
		bool has_columns = true;
		for (auto &column : columns_to_read) {
			if (!HasColumn(rows->rows, column)) {
				has_columns = false;
				break;
			}
		}
		if (has_columns) {
			return rows;
		}
	}
	return nullptr;
}

} // namespace duckdb
