//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_staged_commit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "common/ducklake_snapshot.hpp"

namespace duckdb {
class DuckLakeMetadataManager;
class DuckLakeTransaction;
struct TransactionChangeInformation;

class DuckLakeStagedCommit {
public:
	DuckLakeStagedCommit(DuckLakeMetadataManager &manager, string commit_uuid);

	void Write(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	           const TransactionChangeInformation &transaction_changes);
	void Drop();

	const string &CommitUUID() const {
		return commit_uuid;
	}

private:
	DuckLakeMetadataManager &manager;
	string commit_uuid;
	//! Dash-stripped UUID
	string identifier_suffix;
};

} // namespace duckdb
