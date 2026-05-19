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
class DuckLakeTransaction;
struct TransactionChangeInformation;

class DuckLakeStagedCommit {
public:
	explicit DuckLakeStagedCommit(string commit_uuid);

	//! Builds and returns the SQL of the tables with necessary data changes to perform the commit
	string Build(DuckLakeTransaction &transaction, const TransactionChangeInformation &transaction_changes,
	             DuckLakeSnapshot transaction_snapshot) const;

private:
	string commit_uuid;
	//! Dash-stripped UUID
	string identifier_suffix;
};

} // namespace duckdb
