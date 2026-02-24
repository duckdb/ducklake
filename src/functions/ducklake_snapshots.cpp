#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

template <class T>
Value IDListToValue(const set<T> &id_list) {
	vector<Value> list_values;
	for (auto &id_entry : id_list) {
		list_values.emplace_back(to_string(id_entry.index));
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list_values));
}

Value NameListToValue(const case_insensitive_set_t &list_val) {
	vector<Value> list_values;
	for (auto &entry_name : list_val) {
		list_values.emplace_back(entry_name);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list_values));
}

Value CatalogListToValue(const case_insensitive_map_t<case_insensitive_set_t> &list_val) {
	vector<Value> list_values;
	for (auto &entry : list_val) {
		auto schema = KeywordHelper::WriteOptionallyQuoted(entry.first);
		for (auto &entry_name : entry.second) {
			auto table = KeywordHelper::WriteOptionallyQuoted(entry_name);
			list_values.emplace_back(schema + "." + table);
		}
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list_values));
}

void DuckLakeSnapshotsFunction::GetSnapshotTypes(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("snapshot_time");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("schema_version");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("changes");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)));

	names.emplace_back("author");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("commit_message");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("commit_extra_info");
	return_types.emplace_back(LogicalType::VARCHAR);
}

template <class T>
void PushIDChangeList(vector<Value> &change_keys, vector<Value> &change_values, const set<T> &id_list,
                      const char *key) {
	if (id_list.empty()) {
		return;
	}
	change_keys.emplace_back(key);
	change_values.push_back(IDListToValue(id_list));
}

vector<Value> DuckLakeSnapshotsFunction::GetSnapshotValues(const DuckLakeSnapshotInfo &snapshot) {
	vector<Value> row_values;
	row_values.push_back(Value::BIGINT(NumericCast<int64_t>(snapshot.id)));
	row_values.push_back(Value::TIMESTAMPTZ(snapshot.time));
	row_values.push_back(Value::BIGINT(NumericCast<int64_t>(snapshot.schema_version)));

	auto other_changes = SnapshotChangeInformation::ParseChangesMade(snapshot.change_info.changes_made);
	vector<Value> change_keys;
	vector<Value> change_values;
	if (!other_changes.created_schemas.empty()) {
		change_keys.emplace_back("schemas_created");
		change_values.push_back(NameListToValue(other_changes.created_schemas));
	}
	PushIDChangeList(change_keys, change_values, other_changes.dropped_schemas, "schemas_dropped");

	case_insensitive_map_t<case_insensitive_set_t> created_tables;
	case_insensitive_map_t<case_insensitive_set_t> created_views;
	case_insensitive_map_t<case_insensitive_set_t> created_table_macros;
	case_insensitive_map_t<case_insensitive_set_t> created_scalar_macros;
	for (auto &entry : other_changes.created_tables) {
		for (auto &sub_entry : entry.second) {
			if (sub_entry.second == "table") {
				created_tables[entry.first].insert(sub_entry.first);
			} else {
				created_views[entry.first].insert(sub_entry.first);
			}
		}
	}
	for (auto &entry : other_changes.created_scalar_macros) {
		for (auto &sub_entry : entry.second) {
			if (sub_entry.second == "scalar_macro") {
				created_scalar_macros[entry.first].insert(sub_entry.first);
			} else {
				throw InternalException("Unexpected type for macro in GetSnapshotValues");
			}
		}
	}
	for (auto &entry : other_changes.created_table_macros) {
		for (auto &sub_entry : entry.second) {
			if (sub_entry.second == "table_macro") {
				created_table_macros[entry.first].insert(sub_entry.first);
			} else {
				throw InternalException("Unexpected type for macro in GetSnapshotValues");
			}
		}
	}

	if (!created_tables.empty()) {
		change_keys.emplace_back("tables_created");
		change_values.push_back(CatalogListToValue(created_tables));
	}
	if (!created_views.empty()) {
		change_keys.emplace_back("views_created");
		change_values.push_back(CatalogListToValue(created_views));
	}
	if (!created_scalar_macros.empty()) {
		change_keys.emplace_back("scalar_macros_created");
		change_values.push_back(CatalogListToValue(created_scalar_macros));
	}
	if (!created_table_macros.empty()) {
		change_keys.emplace_back("table_macros_created");
		change_values.push_back(CatalogListToValue(created_table_macros));
	}
	PushIDChangeList(change_keys, change_values, other_changes.dropped_tables, "tables_dropped");
	PushIDChangeList(change_keys, change_values, other_changes.altered_tables, "tables_altered");
	PushIDChangeList(change_keys, change_values, other_changes.inserted_tables, "tables_inserted_into");
	PushIDChangeList(change_keys, change_values, other_changes.tables_deleted_from, "tables_deleted_from");
	PushIDChangeList(change_keys, change_values, other_changes.dropped_views, "views_dropped");
	PushIDChangeList(change_keys, change_values, other_changes.dropped_scalar_macros, "scalar_macros_dropped");
	PushIDChangeList(change_keys, change_values, other_changes.dropped_table_macros, "table_macros_dropped");
	PushIDChangeList(change_keys, change_values, other_changes.altered_views, "views_altered");
	PushIDChangeList(change_keys, change_values, other_changes.tables_inserted_inlined, "inlined_insert");
	PushIDChangeList(change_keys, change_values, other_changes.tables_deleted_inlined, "inlined_delete");
	PushIDChangeList(change_keys, change_values, other_changes.tables_flushed_inlined, "flushed_inlined");
	PushIDChangeList(change_keys, change_values, other_changes.tables_merge_adjacent, "merge_adjacent");
	PushIDChangeList(change_keys, change_values, other_changes.tables_rewrite_delete, "rewrite_delete");

	row_values.push_back(Value::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR),
	                                std::move(change_keys), std::move(change_values)));
	row_values.push_back(snapshot.author);
	row_values.push_back(snapshot.commit_message);
	row_values.push_back(snapshot.commit_extra_info);
	return row_values;
}

static unique_ptr<FunctionData> DuckLakeSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();
	auto snapshots = metadata_manager.GetAllSnapshots();
	auto result = make_uniq<MetadataBindData>();
	for (auto &snapshot : snapshots) {
		auto row_values = DuckLakeSnapshotsFunction::GetSnapshotValues(snapshot);
		result->rows.push_back(std::move(row_values));
	}
	DuckLakeSnapshotsFunction::GetSnapshotTypes(return_types, names);
	return std::move(result);
}

DuckLakeSnapshotsFunction::DuckLakeSnapshotsFunction()
    : DuckLakeBaseMetadataFunction("ducklake_snapshots", DuckLakeSnapshotsBind) {
}

} // namespace duckdb
