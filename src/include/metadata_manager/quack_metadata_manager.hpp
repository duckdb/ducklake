//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/quack_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class QuackMetadataManager : public DuckLakeMetadataManager {
public:
	explicit QuackMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<QuackMetadataManager>(transaction);
	}

	bool SupportsAppender() const override {
		return false;
	}
	void ProbeServerCapabilities() override;
	bool CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const override;
	void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                            const TransactionChangeInformation &transaction_changes,
	                            const DuckLakeRetryConfig &retry_config) override;
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(string &query) override;
	unique_ptr<QueryResult> AttachMetadata(const string &attach_query) override;
	void ClearCache() override;

	bool MetadataExists() override;

protected:
	string MetadataExistsQuery() const override;

private:
	//! Serializes metadata statements on the shared metadata connection. A failing statement leaves the
	//! server-side transaction aborted until Query() has rolled it back, so the failure and the recovery
	//! must not be interleaved with another thread's statement.
	mutex query_lock;
};

} // namespace duckdb
