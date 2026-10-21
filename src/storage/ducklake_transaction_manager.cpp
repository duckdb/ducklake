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

shared_ptr<DuckLakeTransaction> DuckLakeTransactionManager::RemoveTransaction(Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	auto entry = transactions.find(transaction);
	if (entry == transactions.end()) {
		return nullptr;
	}
	auto result = std::move(entry->second);
	transactions.erase(entry);
	return result;
}

ErrorData DuckLakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ErrorData error;
	try {
		ducklake_transaction.Commit();
	} catch (std::exception &ex) {
		error = ErrorData(ex);
	} catch (...) {
		RemoveTransaction(transaction);
		throw;
	}
	// the caller does not roll back a failed commit, so it is released here without removing its files
	RemoveTransaction(transaction);
	return error;
}

void DuckLakeTransactionManager::GetWrittenFiles(vector<string> &result) {
	lock_guard<mutex> l(transaction_lock);
	for (auto &entry : transactions) {
		entry.second->GetWrittenFiles(result);
	}
}

void DuckLakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	try {
		ducklake_transaction.Rollback();
	} catch (...) {
		// the caller counts it as rolled back either way
		RemoveTransaction(transaction);
		throw;
	}
	RemoveTransaction(transaction);
}

} // namespace duckdb
