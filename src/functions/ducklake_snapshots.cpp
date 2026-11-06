#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

template <class T>
static Value IDListToValue(const set<T> &id_list) {
	vector<Value> list_values;
	for (auto &id_entry : id_list) {
		list_values.emplace_back(to_string(id_entry.index));
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list_values));
}

static Value NameListToValue(const case_insensitive_set_t &list_val) {
	vector<Value> list_values;
	for (auto &schema_key : list_val) {
		list_values.emplace_back(QualifiedName::Parse(schema_key).ToString());
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list_values));
}

using CatalogNameMap = case_insensitive_map_t<identifier_set_t>;

static void SplitCreatedEntries(const case_insensitive_map_t<identifier_map_t<string>> &created, const string &type,
                                CatalogNameMap &matching, optional_ptr<CatalogNameMap> other) {
	for (auto &entry : created) {
		for (auto &sub_entry : entry.second) {
			if (sub_entry.second == type) {
				matching[entry.first].insert(sub_entry.first);
			} else if (other) {
				(*other)[entry.first].insert(sub_entry.first);
			} else {
				throw InternalException("Unexpected type for macro in GetSnapshotValues");
			}
		}
	}
}

static Value CatalogListToValue(const CatalogNameMap &list_val) {
	vector<Value> list_values;
	for (auto &entry : list_val) {
		auto schema = QualifiedName::Parse(entry.first).ToString();
		for (auto &entry_name : entry.second) {
			auto table = SQLIdentifier::ToString(entry_name);
			list_values.emplace_back(schema + "." + table);
		}
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list_values));
}

void DuckLakeSnapshotsFunction::GetSnapshotTypes(vector<LogicalType> &return_types, vector<Identifier> &names) {
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
static void PushIDChangeList(vector<Value> &change_keys, vector<Value> &change_values, const set<T> &id_list,
                             const char *key) {
	if (id_list.empty()) {
		return;
	}
	change_keys.emplace_back(key);
	change_values.push_back(IDListToValue(id_list));
}

static void PushNameChangeList(vector<Value> &change_keys, vector<Value> &change_values,
                               const case_insensitive_set_t &names, const char *key) {
	if (names.empty()) {
		return;
	}
	vector<string> sorted_names(names.begin(), names.end());
	std::sort(sorted_names.begin(), sorted_names.end());
	vector<Value> list_values;
	for (auto &name : sorted_names) {
		list_values.emplace_back(name);
	}
	change_keys.emplace_back(key);
	change_values.push_back(Value::LIST(LogicalType::VARCHAR, std::move(list_values)));
}

static Value ChangesMadeToValue(const string &changes_made) {
	auto other_changes = SnapshotChangeInformation::ParseChangesMade(changes_made);
	vector<Value> change_keys;
	vector<Value> change_values;
	if (!other_changes.created_schemas.empty()) {
		change_keys.emplace_back("schemas_created");
		change_values.push_back(NameListToValue(other_changes.created_schemas));
	}
	PushIDChangeList(change_keys, change_values, other_changes.dropped_schemas, "schemas_dropped");

	CatalogNameMap created_tables;
	CatalogNameMap created_views;
	CatalogNameMap created_table_macros;
	CatalogNameMap created_scalar_macros;
	SplitCreatedEntries(other_changes.created_tables, "table", created_tables, created_views);
	SplitCreatedEntries(other_changes.created_scalar_macros, "scalar_macro", created_scalar_macros, nullptr);
	SplitCreatedEntries(other_changes.created_table_macros, "table_macro", created_table_macros, nullptr);

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
	PushIDChangeList(change_keys, change_values, other_changes.tables_compacted, "compacted_table");
	PushNameChangeList(change_keys, change_values, other_changes.created_snapshot_tags, "tags_created");
	PushNameChangeList(change_keys, change_values, other_changes.dropped_snapshot_tags, "tags_dropped");
	if (!other_changes.expired_snapshots.empty()) {
		vector<Value> expired;
		for (auto &snapshot_id : other_changes.expired_snapshots) {
			expired.emplace_back(std::to_string(snapshot_id));
		}
		change_keys.emplace_back("snapshots_expired");
		change_values.push_back(Value::LIST(LogicalType::VARCHAR, std::move(expired)));
	}

	return Value::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR), std::move(change_keys),
	                  std::move(change_values));
}

vector<Value> DuckLakeSnapshotsFunction::GetSnapshotValues(const DuckLakeSnapshotInfo &snapshot) {
	vector<Value> row_values;
	row_values.push_back(Value::BIGINT(NumericCast<int64_t>(snapshot.id)));
	row_values.push_back(Value::TIMESTAMPTZ(snapshot.time));
	row_values.push_back(Value::BIGINT(NumericCast<int64_t>(snapshot.schema_version)));
	row_values.push_back(ChangesMadeToValue(snapshot.change_info.changes_made));
	row_values.push_back(snapshot.author);
	row_values.push_back(snapshot.commit_message);
	row_values.push_back(snapshot.commit_extra_info);
	return row_values;
}

struct DuckLakeSnapshotsBindData : public TableFunctionData {
	unique_ptr<ColumnDataCollection> snapshots;
};

struct DuckLakeSnapshotsState : public GlobalTableFunctionState {
	ColumnDataScanState scan_state;
	DataChunk scan_chunk;
};

static unique_ptr<FunctionData> DuckLakeSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();
	auto result = make_uniq<DuckLakeSnapshotsBindData>();
	auto query_result = metadata_manager.ReadSnapshots(
	    StringUtil::Format("snapshot_id <= %llu", transaction.GetNewestVisibleSnapshotId()));
	result->snapshots = query_result->TakeCollection();
	DuckLakeSnapshotsFunction::GetSnapshotTypes(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeSnapshotsInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DuckLakeSnapshotsBindData>();
	auto result = make_uniq<DuckLakeSnapshotsState>();
	bind_data.snapshots->InitializeScan(result->scan_state, input.column_ids);
	bind_data.snapshots->InitializeScanChunk(result->scan_state, result->scan_chunk);
	return std::move(result);
}

static void DuckLakeSnapshotsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	static constexpr idx_t CHANGES_COLUMN = 3;
	auto &data = data_p.bind_data->Cast<DuckLakeSnapshotsBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeSnapshotsState>();
	auto &chunk = state.scan_chunk;
	data.snapshots->Scan(state.scan_state, chunk);
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		if (state.scan_state.column_ids[col_idx] == CHANGES_COLUMN) {
			for (idx_t row = 0; row < chunk.size(); row++) {
				auto changes_made = chunk.GetValue(col_idx, row);
				output.data[col_idx].Append(
				    ChangesMadeToValue(changes_made.IsNull() ? string() : changes_made.GetValue<string>()));
			}
		} else {
			VectorOperations::Cast(context, chunk.data[col_idx], output.data[col_idx], chunk.size());
		}
	}
	output.SetChildCardinality(chunk.size());
}

DuckLakeSnapshotsFunction::DuckLakeSnapshotsFunction()
    : DuckLakeBaseMetadataFunction("ducklake_snapshots", DuckLakeSnapshotsBind) {
	function = DuckLakeSnapshotsExecute;
	init_global = DuckLakeSnapshotsInit;
	projection_pushdown = true;
}

} // namespace duckdb
