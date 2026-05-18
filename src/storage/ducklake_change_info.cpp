#include "storage/ducklake_change_info.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

template <>
string ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INTO>::Serialize(const string &suffix) const {
	string sql;
	sql += StringUtil::Format("CREATE TEMP TABLE IF NOT EXISTS ducklake_staged_change_touch_%s("
	                          "kind VARCHAR, entity_id BIGINT);",
	                          suffix);
	for (auto &table_id : data) {
		sql += StringUtil::Format("INSERT INTO ducklake_staged_change_touch_%s VALUES ('inserted_into', %llu);", suffix,
		                          static_cast<unsigned long long>(table_id.index));
	}
	return sql;
}

template <>
string ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INTO>::Drop(const string &suffix) const {
	return StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_change_touch_%s;", suffix);
}

} // namespace duckdb
