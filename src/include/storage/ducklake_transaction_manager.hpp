//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storageducklake_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"

namespace duckdb {

class DuckLakeTransactionManager : public TransactionManager {
public:
	DuckLakeTransactionManager(AttachedDatabase &db_p, DuckLakeCatalog &ducklake_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;
	void GetWrittenFiles(vector<string> &result);

private:
	//! Takes the transaction out, it is destroyed after the lock as its metadata connection can end one of ours
	shared_ptr<DuckLakeTransaction> RemoveTransaction(Transaction &transaction);

	DuckLakeCatalog &ducklake_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, shared_ptr<DuckLakeTransaction>> transactions;
	bool get_snapshot = true;
};

} // namespace duckdb
