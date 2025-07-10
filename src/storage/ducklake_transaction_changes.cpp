#include "storage/ducklake_transaction_changes.hpp"
#include "common/ducklake_util.hpp"

namespace duckdb {

enum class ChangeType {
	CREATED_TABLE,
	CREATED_VIEW,
	CREATED_SCHEMA,
	DROPPED_SCHEMA,
	DROPPED_TABLE,
	DROPPED_VIEW,
	INSERTED_INTO_TABLE,
	DELETED_FROM_TABLE,
	INSERTED_INTO_TABLE_INLINED,
	DELETED_FROM_TABLE_INLINED,
	FLUSHED_INLINE_DATA_FOR_TABLE,
	ALTERED_TABLE,
	ALTERED_VIEW,
	COMPACTED_TABLE
};

struct ChangeInfo {
	ChangeType change_type;
	string change_value;
};

ChangeType ParseChangeType(const string &changes_made, idx_t &pos) {
	idx_t start_pos = pos;
	for (; pos < changes_made.size(); pos++) {
		if (changes_made[pos] == ':') {
			break;
		}
	}
	auto change_type_str = changes_made.substr(start_pos, pos - start_pos);
	if (StringUtil::CIEquals(change_type_str, "created_table")) {
		return ChangeType::CREATED_TABLE;
	} else if (StringUtil::CIEquals(change_type_str, "created_view")) {
		return ChangeType::CREATED_VIEW;
	} else if (StringUtil::CIEquals(change_type_str, "created_schema")) {
		return ChangeType::CREATED_SCHEMA;
	} else if (StringUtil::CIEquals(change_type_str, "dropped_schema")) {
		return ChangeType::DROPPED_SCHEMA;
	} else if (StringUtil::CIEquals(change_type_str, "dropped_table")) {
		return ChangeType::DROPPED_TABLE;
	} else if (StringUtil::CIEquals(change_type_str, "dropped_view")) {
		return ChangeType::DROPPED_VIEW;
	} else if (StringUtil::CIEquals(change_type_str, "inserted_into_table")) {
		return ChangeType::INSERTED_INTO_TABLE;
	} else if (StringUtil::CIEquals(change_type_str, "altered_table")) {
		return ChangeType::ALTERED_TABLE;
	} else if (StringUtil::CIEquals(change_type_str, "altered_view")) {
		return ChangeType::ALTERED_VIEW;
	} else if (StringUtil::CIEquals(change_type_str, "deleted_from_table")) {
		return ChangeType::DELETED_FROM_TABLE;
	} else if (StringUtil::CIEquals(change_type_str, "compacted_table")) {
		return ChangeType::COMPACTED_TABLE;
	} else if (StringUtil::CIEquals(change_type_str, "inlined_insert")) {
		return ChangeType::INSERTED_INTO_TABLE_INLINED;
	} else if (StringUtil::CIEquals(change_type_str, "inlined_delete")) {
		return ChangeType::DELETED_FROM_TABLE_INLINED;
	} else if (StringUtil::CIEquals(change_type_str, "flushed_inlined")) {
		return ChangeType::FLUSHED_INLINE_DATA_FOR_TABLE;
	} else {
		throw InvalidInputException("Unsupported change type %s", change_type_str);
	}
}

string ParseChangeValue(const string &changes_made, idx_t &pos) {
	// parse until we find an unquoted comma
	bool in_quotes = false;
	idx_t start_pos = pos;
	for (; pos < changes_made.size(); pos++) {
		if (!in_quotes && changes_made[pos] == ',') {
			// found an unquoted comma
			break;
		}
		if (changes_made[pos] == '"') {
			in_quotes = !in_quotes;
		}
	}
	return changes_made.substr(start_pos, pos - start_pos);
}

ChangeInfo ParseChangeEntry(const string &changes_made, idx_t &pos) {
	ChangeInfo info;
	info.change_type = ParseChangeType(changes_made, pos);
	if (pos >= changes_made.size() || changes_made[pos] != ':') {
		throw InvalidInputException("Expected a colon after the change type");
	}
	pos++;
	info.change_value = ParseChangeValue(changes_made, pos);
	return info;
}

vector<ChangeInfo> ParseChangesList(const string &changes_made) {
	vector<ChangeInfo> result;
	idx_t pos = 0;
	while (pos < changes_made.size()) {
		result.push_back(ParseChangeEntry(changes_made, pos));
		if (pos >= changes_made.size()) {
			break;
		}
		if (changes_made[pos] != ',') {
			throw InvalidInputException("Expected a comma separating the change entry");
		}
		pos++;
	}
	return result;
}

SnapshotChangeInformation SnapshotChangeInformation::ParseChangesMade(const string &changes_made) {
	auto change_list = ParseChangesList(changes_made);

	SnapshotChangeInformation result;
	for (auto &entry : change_list) {
		switch (entry.change_type) {
		case ChangeType::CREATED_TABLE: {
			auto catalog_value = DuckLakeUtil::ParseCatalogEntry(entry.change_value);
			result.created_tables[catalog_value.schema].insert(make_pair(std::move(catalog_value.name), "table"));
			break;
		}
		case ChangeType::CREATED_VIEW: {
			auto catalog_value = DuckLakeUtil::ParseCatalogEntry(entry.change_value);
			result.created_tables[catalog_value.schema].insert(make_pair(std::move(catalog_value.name), "view"));
			break;
		}
		case ChangeType::CREATED_SCHEMA: {
			idx_t pos = 0;
			auto schema_name = DuckLakeUtil::ParseQuotedValue(entry.change_value, pos);
			result.created_schemas.insert(std::move(schema_name));
			break;
		}
		case ChangeType::DROPPED_SCHEMA:
			result.dropped_schemas.insert(SchemaIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::DROPPED_TABLE:
			result.dropped_tables.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::DROPPED_VIEW:
			result.dropped_views.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::INSERTED_INTO_TABLE:
			result.inserted_tables.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::DELETED_FROM_TABLE:
			result.tables_deleted_from.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::ALTERED_TABLE:
			result.altered_tables.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::ALTERED_VIEW:
			result.altered_views.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::COMPACTED_TABLE:
			result.tables_compacted.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::INSERTED_INTO_TABLE_INLINED:
			result.tables_inserted_inlined.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::DELETED_FROM_TABLE_INLINED:
			result.tables_deleted_inlined.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::FLUSHED_INLINE_DATA_FOR_TABLE:
			result.tables_flushed_inlined.insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		default:
			throw InternalException("Unsupported change type in ParseChangesMade");
		}
	}
	return result;
}

} // namespace duckdb
