#include "storage/ducklake_staged_commit.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "storage/ducklake_change_info.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

DuckLakeStagedCommit::DuckLakeStagedCommit(DuckLakeMetadataManager &manager, string commit_uuid)
    : manager(manager), commit_uuid(std::move(commit_uuid)),
      identifier_suffix(StringUtil::Replace(this->commit_uuid, "-", "")) {
}

void DuckLakeStagedCommit::Write(DuckLakeTransaction &transaction,
                                 const TransactionChangeInformation &transaction_changes) {
	string batch;
	batch += transaction.GetCommitInfo().Serialize(identifier_suffix);
	batch += transaction_changes.tables_inserted_into.Serialize(identifier_suffix);
	batch += transaction.GetLocalChanges().Serialize(identifier_suffix);

	auto result = manager.Query(batch);
	if (result && result->HasError()) {
		result->GetErrorObject().Throw("Failed to create DuckLake staging tables: ");
	}
}

void DuckLakeStagedCommit::Drop() {
	string batch;
	batch += ChangeInfo<DuckLakeSnapshotCommit, ChangeKind::COMMIT_HEADER>().Drop(identifier_suffix);
	batch += ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INTO>().Drop(identifier_suffix);
	batch += ChangeInfo<LocalTableChanges, ChangeKind::DATA_FILES>().Drop(identifier_suffix);
	auto result = manager.Query(batch);
	if (result && result->HasError()) {
		result->GetErrorObject().Throw("Failed to drop DuckLake staging tables: ");
	}
}

} // namespace duckdb
