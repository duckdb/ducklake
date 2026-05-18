//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_staged_commit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class DuckLakeMetadataManager;

//! Transforms the per-commit information to temporary staging tables and hand them off to the server
//! via `ducklake_commit`.
class DuckLakeStagedCommit {
public:
	DuckLakeStagedCommit(DuckLakeMetadataManager &manager, string commit_uuid);

	//! Creates the staging tables
	void Write();
	//! Drops the staging tables
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
