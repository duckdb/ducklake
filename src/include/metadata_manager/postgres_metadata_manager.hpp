//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class PostgresMetadataManager : public DuckLakeMetadataManager {
public:
	explicit PostgresMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<PostgresMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool SupportsInlining(const LogicalType &type) override;
	bool SupportsAppender() const override {
		return false;
	}
	idx_t MaxIdentifierLength() const override {
		return 63;
	}

	string GetColumnTypeInternal(const LogicalType &type) override;
	shared_ptr<DuckLakeInlinedData> TransformInlinedData(QueryResult &result,
	                                                     const vector<LogicalType> &expected_types) override;

	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;

	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;

	idx_t AllocateNextSnapshotId(idx_t current_snapshot_id) override;
	idx_t AllocateNextCatalogId(idx_t current_next_catalog_id) override;
	idx_t AllocateNextFileId(idx_t current_next_file_id) override;
	idx_t AllocateNextSchemaVersion(idx_t current_schema_version) override;
	idx_t PeekSchemaVersion(idx_t current_schema_version) override;
	void AcquireCommitLock(const TransactionChangeInformation &changes) override;

	void EnsureIdSequences() override;

protected:
	string GetLatestSnapshotQuery() const override;
	string GetSnapshotAndStatsAndChangesQuery() override;
	string GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id) override;

private:
	unique_ptr<QueryResult> ExecuteQuery(DuckLakeSnapshot snapshot, string &query, string command);

	idx_t FetchScalarSequenceValue(const string &seq_name);
	idx_t EnsureCatalogClassid();
	bool BootstrapObjectsPresent();

	static constexpr int32_t DUCKLAKE_COMMIT_ADVISORY_SUBKEY = 0x44754C4B;
	static constexpr int32_t DUCKLAKE_BOOTSTRAP_ADVISORY_SUBKEY = 0x42535452;
	optional_idx catalog_classid;
};

} // namespace duckdb
