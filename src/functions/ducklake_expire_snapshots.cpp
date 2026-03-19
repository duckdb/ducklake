#include "duckdb/common/operator/subtract.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct ExpireSnapshotsBindData : public TableFunctionData {
	explicit ExpireSnapshotsBindData(Catalog &catalog) : catalog(catalog) {
	}

	Catalog &catalog;
	vector<DuckLakeSnapshotInfo> snapshots;
	bool dry_run = false;
	bool valid = true;
};

static unique_ptr<FunctionData> DuckLakeExpireSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto result = make_uniq<ExpireSnapshotsBindData>(catalog);
	timestamp_tz_t from_timestamp;
	string snapshot_list;
	bool has_timestamp = false;
	bool has_versions = false;
	auto &ducklake_catalog = reinterpret_cast<DuckLakeCatalog &>(catalog);
	DuckLakeSnapshotsFunction::GetSnapshotTypes(return_types, names);

	const auto older_than_default = ducklake_catalog.GetConfigOption<string>("expire_older_than", {}, {}, "");

	for (auto &entry : input.named_parameters) {
		if (StringUtil::CIEquals(entry.first, "dry_run")) {
			result->dry_run = BooleanValue::Get(entry.second);
		} else if (StringUtil::CIEquals(entry.first, "versions")) {
			has_versions = true;
			for (auto &snapshot_id : ListValue::GetChildren(entry.second)) {
				if (!snapshot_list.empty()) {
					snapshot_list += ", ";
				}
				snapshot_list += snapshot_id.ToString();
			}
		} else if (StringUtil::CIEquals(entry.first, "older_than")) {
			from_timestamp = entry.second.GetValue<timestamp_tz_t>();
			has_timestamp = true;
		} else {
			throw InternalException("Unsupported named parameter for ducklake_expire_snapshots");
		}
	}
	if ((has_versions == has_timestamp && has_versions == true) ||
	    (has_versions == has_timestamp && has_versions == false && older_than_default.empty())) {
		result->valid = false;
		return std::move(result);
	}

	string filter;
	// we can never delete the most recent snapshot
	filter = "snapshot_id != (SELECT MAX(snapshot_id) FROM {METADATA_CATALOG}.ducklake_snapshot) AND ";
	if (has_timestamp) {
		auto timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampISO8601(timestamp_t(from_timestamp.value));
		filter += StringUtil::Format("snapshot_time < '%s'", timestamp_filter);
	} else if (!has_versions && !older_than_default.empty()) {
		interval_t interval;
		if (!Interval::FromString(older_than_default, interval)) {
			throw InvalidInputException("Failed to parse interval: '%s'", older_than_default);
		}
		auto current_time = Timestamp::GetCurrentTimestamp();
		auto target_timestamp =
		    SubtractOperator::Operation<timestamp_t, interval_t, timestamp_t>(current_time, interval);
		auto timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampISO8601(target_timestamp);
		filter += StringUtil::Format("snapshot_time < '%s'", timestamp_filter);
	} else {
		filter += StringUtil::Format("snapshot_id IN (%s)", snapshot_list);
	}
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	auto &metadata_manager = transaction.GetMetadataManager();
	result->snapshots = metadata_manager.GetAllSnapshots(filter);

	return std::move(result);
}

struct DuckLakeExpireSnapshotsData : public GlobalTableFunctionState {
	DuckLakeExpireSnapshotsData() : offset(0), executed(false) {
	}

	idx_t offset;
	bool executed;
};

unique_ptr<GlobalTableFunctionState> DuckLakeExpireSnapshotsInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<DuckLakeExpireSnapshotsData>();
	return std::move(result);
}

void DuckLakeExpireSnapshotsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<ExpireSnapshotsBindData>();
	if (!data.valid) {
		return;
	}
	auto &state = data_p.global_state->Cast<DuckLakeExpireSnapshotsData>();
	if (state.offset >= data.snapshots.size()) {
		return;
	}
	if (!state.executed && !data.dry_run) {
		auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
		transaction.DeleteSnapshots(data.snapshots);
	}

	idx_t count = 0;
	while (state.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		auto row_values = DuckLakeSnapshotsFunction::GetSnapshotValues(data.snapshots[state.offset++]);
		for (idx_t col_idx = 0; col_idx < row_values.size(); col_idx++) {
			output.SetValue(col_idx, count, row_values[col_idx]);
		}
		count++;
	}
	output.SetCardinality(count);
}

DuckLakeExpireSnapshotsFunction::DuckLakeExpireSnapshotsFunction()
    : TableFunction("ducklake_expire_snapshots", {LogicalType::VARCHAR}, DuckLakeExpireSnapshotsExecute,
                    DuckLakeExpireSnapshotsBind, DuckLakeExpireSnapshotsInit) {
	named_parameters["older_than"] = LogicalType::TIMESTAMP_TZ;
	named_parameters["versions"] = LogicalType::LIST(LogicalType::UBIGINT);
	named_parameters["dry_run"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
