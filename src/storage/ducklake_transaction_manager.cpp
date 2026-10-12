#include "storage/ducklake_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "duckdb/main/settings.hpp"

namespace duckdb {

DuckLakeTransactionManager::DuckLakeTransactionManager(AttachedDatabase &db_p, DuckLakeCatalog &ducklake_catalog)
    : TransactionManager(db_p), ducklake_catalog(ducklake_catalog) {
}

Transaction &DuckLakeTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_shared_ptr<DuckLakeTransaction>(ducklake_catalog, *this, context);
	transaction->Start();
	if (Settings::Get<ImmediateTransactionModeSetting>(context) && get_snapshot && ducklake_catalog.IsInitialized() &&
	    !context.registered_state->Get<DuckLakeInternalConnectionState>(DuckLakeInternalConnectionState::KEY)) {
		get_snapshot = false;
		// no snapshot loaded yet for this transaction - load it
		transaction->GetSnapshot();
		get_snapshot = true;
	}
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData DuckLakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ErrorData error;
	try {
		ducklake_transaction.Commit();
	} catch (std::exception &ex) {
		error = ErrorData(ex);
	}
	// a transaction whose commit failed is not rolled back afterwards, so it is done either way
	// it is destroyed after the lock is released, closing its connection can roll back other transactions
	shared_ptr<DuckLakeTransaction> finished_transaction;
	{
		lock_guard<mutex> l(transaction_lock);
		if (error.HasError() && ducklake_transaction.CommitOutcomeUnknown()) {
			ducklake_transaction.GetWrittenFiles(in_doubt_files);
		}
		auto entry = transactions.find(transaction);
		if (entry != transactions.end()) {
			finished_transaction = std::move(entry->second);
			transactions.erase(entry);
		}
	}
	return error;
}

void DuckLakeTransactionManager::GetWrittenFiles(vector<string> &result) {
	lock_guard<mutex> l(transaction_lock);
	result.insert(result.end(), in_doubt_files.begin(), in_doubt_files.end());
	for (auto &entry : transactions) {
		entry.second->GetWrittenFiles(result);
	}
}

void DuckLakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ducklake_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

} // namespace duckdb
