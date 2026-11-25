#include "storage/ducklake_transaction_changes.hpp"
#include "common/ducklake_util.hpp"

namespace duckdb {

namespace {

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
	COMPACTED_TABLE,
	MERGE_ADJACENT,
	REWRITE_DELETE,
	CREATED_SCALAR_MACRO,
	CREATED_TABLE_MACRO,
	DROPPED_SCALAR_MACRO,
	DROPPED_TABLE_MACRO,
	CREATED_SNAPSHOT_TAG,
	DROPPED_SNAPSHOT_TAG,
	EXPIRED_SNAPSHOT
};

constexpr StringUtil::EnumStringLiteral CHANGE_TYPE_NAMES[] = {
    {static_cast<uint32_t>(ChangeType::CREATED_TABLE), "created_table"},
    {static_cast<uint32_t>(ChangeType::CREATED_VIEW), "created_view"},
    {static_cast<uint32_t>(ChangeType::CREATED_SCALAR_MACRO), "created_scalar_macro"},
    {static_cast<uint32_t>(ChangeType::CREATED_TABLE_MACRO), "created_table_macro"},
    {static_cast<uint32_t>(ChangeType::CREATED_SCHEMA), "created_schema"},
    {static_cast<uint32_t>(ChangeType::DROPPED_SCHEMA), "dropped_schema"},
    {static_cast<uint32_t>(ChangeType::DROPPED_TABLE), "dropped_table"},
    {static_cast<uint32_t>(ChangeType::DROPPED_VIEW), "dropped_view"},
    {static_cast<uint32_t>(ChangeType::INSERTED_INTO_TABLE), "inserted_into_table"},
    {static_cast<uint32_t>(ChangeType::DROPPED_SCALAR_MACRO), "dropped_scalar_macro"},
    {static_cast<uint32_t>(ChangeType::DROPPED_TABLE_MACRO), "dropped_table_macro"},
    {static_cast<uint32_t>(ChangeType::ALTERED_TABLE), "altered_table"},
    {static_cast<uint32_t>(ChangeType::ALTERED_VIEW), "altered_view"},
    {static_cast<uint32_t>(ChangeType::DELETED_FROM_TABLE), "deleted_from_table"},
    {static_cast<uint32_t>(ChangeType::COMPACTED_TABLE), "compacted_table"},
    {static_cast<uint32_t>(ChangeType::MERGE_ADJACENT), "merge_adjacent"},
    {static_cast<uint32_t>(ChangeType::REWRITE_DELETE), "rewrite_delete"},
    {static_cast<uint32_t>(ChangeType::INSERTED_INTO_TABLE_INLINED), "inlined_insert"},
    {static_cast<uint32_t>(ChangeType::DELETED_FROM_TABLE_INLINED), "inlined_delete"},
    {static_cast<uint32_t>(ChangeType::FLUSHED_INLINE_DATA_FOR_TABLE), "flushed_inlined"},
    {static_cast<uint32_t>(ChangeType::FLUSHED_INLINE_DATA_FOR_TABLE), "inline_flush"},
    {static_cast<uint32_t>(ChangeType::CREATED_SNAPSHOT_TAG), "created_tag"},
    {static_cast<uint32_t>(ChangeType::DROPPED_SNAPSHOT_TAG), "dropped_tag"},
    {static_cast<uint32_t>(ChangeType::EXPIRED_SNAPSHOT), "expired_snapshot"}};

//! A change that names a table or view by its id, with the set that collects it
struct TableChange {
	ChangeType type;
	set<TableIndex> SnapshotChangeInformation::*tables;
};

constexpr TableChange TABLE_CHANGES[] = {
    {ChangeType::DROPPED_TABLE, &SnapshotChangeInformation::dropped_tables},
    {ChangeType::DROPPED_VIEW, &SnapshotChangeInformation::dropped_views},
    {ChangeType::INSERTED_INTO_TABLE, &SnapshotChangeInformation::inserted_tables},
    {ChangeType::DELETED_FROM_TABLE, &SnapshotChangeInformation::tables_deleted_from},
    {ChangeType::ALTERED_TABLE, &SnapshotChangeInformation::altered_tables},
    {ChangeType::ALTERED_VIEW, &SnapshotChangeInformation::altered_views},
    {ChangeType::COMPACTED_TABLE, &SnapshotChangeInformation::tables_compacted},
    {ChangeType::MERGE_ADJACENT, &SnapshotChangeInformation::tables_merge_adjacent},
    {ChangeType::REWRITE_DELETE, &SnapshotChangeInformation::tables_rewrite_delete},
    {ChangeType::INSERTED_INTO_TABLE_INLINED, &SnapshotChangeInformation::tables_inserted_inlined},
    {ChangeType::DELETED_FROM_TABLE_INLINED, &SnapshotChangeInformation::tables_deleted_inlined},
    {ChangeType::FLUSHED_INLINE_DATA_FOR_TABLE, &SnapshotChangeInformation::tables_flushed_inlined}};

set<TableIndex> &ChangedTables(SnapshotChangeInformation &changes, ChangeType type) {
	for (auto &change : TABLE_CHANGES) {
		if (change.type == type) {
			return changes.*change.tables;
		}
	}
	throw InternalException("Unsupported change type in ParseChangesMade");
}

void AddCreatedEntry(case_insensitive_map_t<identifier_map_t<string>> &created, const string &value, const char *type) {
	auto catalog_value = DuckLakeUtil::ParseCatalogEntry(value);
	created[catalog_value.SchemaKey()].insert(make_pair(std::move(catalog_value.name), type));
}

struct ParsedChange {
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
	try {
		return static_cast<ChangeType>(
		    StringUtil::StringToEnum(CHANGE_TYPE_NAMES, sizeof(CHANGE_TYPE_NAMES) / sizeof(CHANGE_TYPE_NAMES[0]),
		                             "ChangeType", change_type_str.c_str()));
	} catch (NotImplementedException &) {
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

ParsedChange ParseChangeEntry(const string &changes_made, idx_t &pos) {
	ParsedChange info;
	info.change_type = ParseChangeType(changes_made, pos);
	if (pos >= changes_made.size() || changes_made[pos] != ':') {
		throw InvalidInputException("Expected a colon after the change type");
	}
	pos++;
	info.change_value = ParseChangeValue(changes_made, pos);
	return info;
}

vector<ParsedChange> ParseChangesList(const string &changes_made) {
	vector<ParsedChange> result;
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

} // namespace

SnapshotChangeInformation SnapshotChangeInformation::ParseChangesMade(const string &changes_made) {
	auto change_list = ParseChangesList(changes_made);

	SnapshotChangeInformation result;
	for (auto &entry : change_list) {
		switch (entry.change_type) {
		case ChangeType::CREATED_TABLE:
			AddCreatedEntry(result.created_tables, entry.change_value, "table");
			break;
		case ChangeType::CREATED_SCALAR_MACRO:
			AddCreatedEntry(result.created_scalar_macros, entry.change_value, "scalar_macro");
			break;
		case ChangeType::CREATED_TABLE_MACRO:
			AddCreatedEntry(result.created_table_macros, entry.change_value, "table_macro");
			break;
		case ChangeType::CREATED_VIEW:
			AddCreatedEntry(result.created_tables, entry.change_value, "view");
			break;
		case ChangeType::CREATED_SCHEMA: {
			auto schema_path = StringsToIdentifiers(DuckLakeUtil::ParseQuotedList(entry.change_value, '.'));
			result.created_schemas.insert(DuckLakeUtil::ToQuotedList(schema_path, '.'));
			break;
		}
		case ChangeType::DROPPED_SCHEMA:
			result.dropped_schemas.insert(SchemaIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::DROPPED_SCALAR_MACRO:
			result.dropped_scalar_macros.insert(MacroIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::DROPPED_TABLE_MACRO:
			result.dropped_table_macros.insert(MacroIndex(StringUtil::ToUnsigned(entry.change_value)));
			break;
		case ChangeType::CREATED_SNAPSHOT_TAG: {
			idx_t pos = 0;
			result.created_snapshot_tags.insert(DuckLakeUtil::ParseQuotedValue(entry.change_value, pos));
			break;
		}
		case ChangeType::DROPPED_SNAPSHOT_TAG: {
			idx_t pos = 0;
			result.dropped_snapshot_tags.insert(DuckLakeUtil::ParseQuotedValue(entry.change_value, pos));
			break;
		}
		case ChangeType::EXPIRED_SNAPSHOT:
			result.expired_snapshots.insert(StringUtil::ToUnsigned(entry.change_value));
			break;
		default:
			ChangedTables(result, entry.change_type).insert(TableIndex(StringUtil::ToUnsigned(entry.change_value)));
		}
	}
	return result;
}

} // namespace duckdb
