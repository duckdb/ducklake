#include "ducklake_extension.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/ducklake_transaction_manager.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

void DuckLakeTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto conn = make_uniq<Connection>(ducklake_catalog.GetDatabase());
	// 1. We first flush inlined data, since these can generate many files, which would be important for compaction
	// 2. We expire snapshots since these can create more compaction opportunities.
	// 3. We call the compaction functions, merge_adjacent and rewrite, unclear what is the best order here.
	// 4. We call the functions that delete files last, since these mostly result from compaction or expired snapshots.
	const vector<string> checkpoint_queries {
	    "CALL ducklake_flush_inlined_data({CATALOG})",   "CALL ducklake_expire_snapshots({CATALOG})",
	    "CALL ducklake_merge_adjacent_files({CATALOG})", "CALL ducklake_rewrite_data_files({CATALOG})",
	    "CALL ducklake_cleanup_old_files({CATALOG})",    "CALL ducklake_delete_orphaned_files({CATALOG})"};

	for (const auto &query : checkpoint_queries) {
		auto checkpoint_query =
		    StringUtil::Replace(query, "{CATALOG}", KeywordHelper::WriteQuoted(ducklake_catalog.GetName(), '\''));
		auto res = conn->Query(checkpoint_query);
		if (res->HasError()) {
			res->GetErrorObject().Throw("Failed to perform CHECKPOINT; in DuckLake:  ");
		}
	}
}

} // namespace duckdb
