//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/rest_inlined_rows_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_api.hpp"
#include "common/index.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//! Which inlined rows a response holds: the rows of a scan, or the rows that were inserted or deleted in a range
enum class RESTInlinedRowsKind : uint8_t { SCAN, INSERTIONS, DELETIONS };

//! A column of inlined rows, or a field of a struct: all of it, or the fields of the struct that are read
struct RESTInlinedField {
	bool Covers(const RESTInlinedField &other) const;

	bool all = true;
	map<idx_t, RESTInlinedField> fields;
};

//! The columns of inlined rows: all columns, or the field ids of top-level columns
struct RESTInlinedProjection {
	static RESTInlinedProjection From(optional_ptr<const vector<DuckLakeProjectedColumn>> projection);

	bool Covers(const RESTInlinedProjection &other) const;

	bool all = true;
	map<idx_t, RESTInlinedField> columns;
};

//! The inlined rows of a table
struct RESTCachedInlinedRows {
	RESTInlinedRows rows;
	//! Scans of the same table read the rows at the same time
	mutex read_lock;
};

//! One response of the server: the inlined rows of the table, none if the table has no inlined rows
struct RESTInlinedRowsResponse {
	RESTInlinedProjection projection;
	shared_ptr<RESTCachedInlinedRows> rows;
};

//! The rows to be read: the table at a snapshot, or the changes of the table in a range of snapshots
struct RESTInlinedRowsKey {
	RESTInlinedRowsKind kind;
	//! The first snapshot of the range, 0 for scans
	idx_t start_snapshot_id;
	idx_t snapshot_id;
	idx_t table_id;

	bool operator<(const RESTInlinedRowsKey &other) const;
};

//! The inlined rows that came along with the file lists of the scans of a transaction. Scans read the columns they
//! need from them. The rows at a snapshot do not change, so the responses are only added to the cache: a response
//! is never replaced, and a scan that holds one can always finish reading it.
class RESTInlinedRowsCache {
public:
	//! Whether a response has the columns
	bool Covers(const RESTInlinedRowsKey &key, const RESTInlinedProjection &projection) const;
	//! Whether any response is known
	bool HasResponse(const RESTInlinedRowsKey &key) const;
	//! Whether a table has inlined rows at a snapshot, if a response of any kind knows
	bool HasInlinedRows(idx_t snapshot_id, idx_t table_id, bool &has_inlined_rows) const;
	//! Adds a response, which takes over the responses that have a subset of its columns
	shared_ptr<RESTInlinedRowsResponse> Add(const RESTInlinedRowsKey &key, const RESTInlinedProjection &projection,
	                                        unique_ptr<RESTInlinedRows> inlined_data);
	//! The rows of a response that has the columns to read, which are column names that are optionally cast
	shared_ptr<RESTCachedInlinedRows> Find(const RESTInlinedRowsKey &key, const vector<string> &columns_to_read) const;

private:
	mutable mutex lock;
	map<RESTInlinedRowsKey, vector<shared_ptr<RESTInlinedRowsResponse>>> responses;
};

} // namespace duckdb
