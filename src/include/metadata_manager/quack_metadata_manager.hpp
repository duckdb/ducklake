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
	bool CommitsEachStatement() const override {
		return true;
	}
	void ProbeServerCapabilities() override;
	bool CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const override;
	void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                            const TransactionChangeInformation &transaction_changes,
	                            const DuckLakeRetryConfig &retry_config) override;
	DuckLakeSQLPlaceholders QueryPlaceholders() const override;
	unique_ptr<QueryResult> ExecuteBatch(const string &query) override;
	unique_ptr<QueryResult> ExecuteInTransaction(const string &query) override;
	unique_ptr<QueryResult> Query(const string &query) override;
	unique_ptr<QueryResult> AttachMetadata(const string &attach_query) override;
	void ClearCache() override;

	bool MetadataExists() override;
	bool InlinedDeletionTableExists(const string &table_name) override;

protected:
	string MetadataExistsQuery() const override;
};

} // namespace duckdb
