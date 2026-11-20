#include "duckdb/catalog/catalog.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct ExpireSnapshotsBindData : public MetadataBindData {
	explicit ExpireSnapshotsBindData(DuckLakeCatalog &catalog) : catalog(catalog) {
	}

	DuckLakeCatalog &catalog;
	//! The criterion of the expiry
	DuckLakeSnapshotFilter filter;
	bool dry_run = false;
};

struct ExpireSnapshotsGlobalState : public GlobalTableFunctionState {
	bool executed = false;
	idx_t offset = 0;
	//! The expired snapshots, or the candidates of a dry run
	vector<vector<Value>> rows;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<ExpireSnapshotsGlobalState>();
	}
};

static unique_ptr<FunctionData> DuckLakeExpireSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	auto &ducklake_catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	if (DuckLakeTransaction::Get(context, ducklake_catalog).GetMetadataManager().IsRemote()) {
		throw NotImplementedException("DuckLake REST catalogs do not support expiring snapshots: run the maintenance "
		                              "functions on the DuckLake that the server serves");
	}
	auto result = make_uniq<ExpireSnapshotsBindData>(ducklake_catalog);
	DuckLakeSnapshotsFunction::GetSnapshotTypes(return_types, names);

	const auto older_than_default = ducklake_catalog.GetConfigOption<string>("expire_older_than", {}, {}, "");

	Value dry_run;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "dry_run", "boolean", dry_run)) {
		result->dry_run = BooleanValue::Get(dry_run);
	}
	if (!result->dry_run) {
		ducklake_catalog.VerifyWritable();
	}
	auto &filter = result->filter;
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
			filter.snapshot_ids.push_back(NumericCast<idx_t>(BigIntValue::Get(snapshot_id)));
		}
	}
	Value older_than;
	bool has_timestamp = DuckLakeTableFunctionUtil::TryGetTimestampOption(input, "older_than", older_than);
	if (has_versions && has_timestamp) {
		throw InvalidInputException(
		    "ducklake_expire_snapshots: cannot specify both 'versions' and 'older_than' parameters at the "
		    "same time. Please use only one criterion.");
	}
	// an empty version set, or no criterion and no global default, expires no snapshots
	if ((has_versions && filter.snapshot_ids.empty()) ||
	    (!has_versions && !has_timestamp && older_than_default.empty())) {
		// the expiry still drops the tags whose expiry time has passed, an empty id list selects no snapshots
		filter.snapshot_ids.clear();
		filter.has_snapshot_ids = true;
		return std::move(result);
	}

	// we can never delete the most recent snapshot
	filter.exclude_latest = true;
	if (has_timestamp) {
		auto from_timestamp = older_than.GetValue<timestamp_tz_t>();
		filter.older_than = DuckLakeTableFunctionUtil::FormatTimestampISO8601(timestamp_t(from_timestamp.value));
	} else if (!has_versions) {
		filter.older_than =
		    DuckLakeTableFunctionUtil::FormatTimestampOlderThan("expire_older_than", older_than_default);
	} else {
		filter.has_snapshot_ids = true;
	}
	if (result->dry_run) {
		auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);
		for (auto &snapshot : transaction.ExpirableSnapshots(filter)) {
			result->rows.push_back(DuckLakeSnapshotsFunction::GetSnapshotValues(snapshot));
		}
	}
	return std::move(result);
}

void DuckLakeExpireSnapshotsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<ExpireSnapshotsBindData>();
	auto &state = data_p.global_state->Cast<ExpireSnapshotsGlobalState>();
	if (!state.executed) {
		state.executed = true;
		if (data.dry_run) {
			state.rows = data.rows;
		} else {
			auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
			for (auto &snapshot : transaction.ExpireSnapshots(data.filter)) {
				state.rows.push_back(DuckLakeSnapshotsFunction::GetSnapshotValues(snapshot));
			}
		}
	}
	if (state.offset >= state.rows.size()) {
		return;
	}
	DuckLakeBaseMetadataFunction::ScanRows(state.rows, state.offset, output);
}

DuckLakeExpireSnapshotsFunction::DuckLakeExpireSnapshotsFunction()
    : TableFunction("ducklake_expire_snapshots", FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR),
                    DuckLakeExpireSnapshotsExecute, DuckLakeExpireSnapshotsBind, ExpireSnapshotsGlobalState::Init) {
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
