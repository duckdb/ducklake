#include "duckdb/catalog/catalog.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct ExpireSnapshotsBindData : public MetadataBindData {
	explicit ExpireSnapshotsBindData(DuckLakeCatalog &catalog) : catalog(catalog) {
	}

	DuckLakeCatalog &catalog;
	vector<DuckLakeSnapshotInfo> snapshots;
	bool dry_run = false;
};

static unique_ptr<FunctionData> DuckLakeExpireSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	auto &ducklake_catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto result = make_uniq<ExpireSnapshotsBindData>(ducklake_catalog);
	DuckLakeSnapshotsFunction::GetSnapshotTypes(return_types, names);

	const auto older_than_default = ducklake_catalog.GetConfigOption<string>("expire_older_than", {}, {}, "");

	Value dry_run;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "dry_run", "boolean", dry_run)) {
		result->dry_run = BooleanValue::Get(dry_run);
	}
	vector<string> snapshot_ids;
	auto versions_entry = input.named_parameters.find("versions");
	bool has_versions = versions_entry != input.named_parameters.end();
	if (has_versions && !versions_entry->second.IsNull()) {
		for (auto &snapshot_id : ListValue::GetChildren(versions_entry->second)) {
			if (snapshot_id.IsNull()) {
				continue;
			}
			if (BigIntValue::Get(snapshot_id) < 0) {
				throw BinderException("The versions option must only contain non-negative snapshot ids.");
			}
			snapshot_ids.push_back(snapshot_id.ToString());
		}
	}
	Value older_than;
	bool has_timestamp = DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "older_than", "timestamp", older_than);
	if (has_versions && has_timestamp) {
		throw InvalidInputException(
		    "ducklake_expire_snapshots: cannot specify both 'versions' and 'older_than' parameters at the "
		    "same time. Please use only one criterion.");
	}
	// An explicitly empty version set expires no snapshots.
	if (has_versions && snapshot_ids.empty()) {
		return std::move(result);
	}
	// No criteria given and no global default: silently no-op.
	if (!has_versions && !has_timestamp && older_than_default.empty()) {
		return std::move(result);
	}

	string filter;
	// we can never delete the most recent snapshot
	filter = "snapshot_id != (SELECT MAX(snapshot_id) FROM {METADATA_CATALOG}.ducklake_snapshot) AND ";
	if (has_timestamp) {
		auto from_timestamp = older_than.GetValue<timestamp_tz_t>();
		auto timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampISO8601(timestamp_t(from_timestamp.value));
		filter += StringUtil::Format("snapshot_time::TIMESTAMPTZ < '%s'", timestamp_filter);
	} else if (!has_versions) {
		auto timestamp_filter = DuckLakeTableFunctionUtil::FormatTimestampOlderThan(older_than_default);
		filter += StringUtil::Format("snapshot_time::TIMESTAMPTZ < '%s'", timestamp_filter);
	} else {
		filter += StringUtil::Format("snapshot_id IN (%s)", StringUtil::Join(snapshot_ids, ", "));
	}
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);
	auto &metadata_manager = transaction.GetMetadataManager();
	result->snapshots = metadata_manager.GetAllSnapshots(filter);
	for (auto &snapshot : result->snapshots) {
		result->rows.push_back(DuckLakeSnapshotsFunction::GetSnapshotValues(snapshot));
	}
	return std::move(result);
}

void DuckLakeExpireSnapshotsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<ExpireSnapshotsBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	if (state.offset >= data.rows.size()) {
		return;
	}
	if (!state.executed && !data.dry_run) {
		auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
		transaction.DeleteSnapshots(data.snapshots);
		for (auto &snapshot : data.snapshots) {
			data.catalog.InvalidateSchemaCache(snapshot.schema_version);
		}
		state.executed = true;
	}
	DuckLakeBaseMetadataFunction::ScanRows(data.rows, state.offset, output);
}

DuckLakeExpireSnapshotsFunction::DuckLakeExpireSnapshotsFunction()
    : TableFunction("ducklake_expire_snapshots", FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR),
                    DuckLakeExpireSnapshotsExecute, DuckLakeExpireSnapshotsBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options
		    .Add("older_than", LogicalType::TIMESTAMP_TZ)
		    // BIGINT rather than UBIGINT: a caller writes "versions => [2]", which is a list of signed literals, and
		    // an unsigned parameter has no implicit cast from one. The range is checked below instead.
		    .Add("versions", LogicalType::LIST(LogicalType::BIGINT))
		    .Add("dry_run", LogicalType::BOOLEAN);
	});
}

} // namespace duckdb
