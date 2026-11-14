//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_branch_chain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct DuckLakeBranchInfo;

//! A branch of the chain with the last snapshot of it the chain reads, unset for the attached branch
struct DuckLakeBranchSegment {
	idx_t branch_id;
	optional_idx last_snapshot;
};

//! The branches a branch reads through, from the root to the branch itself
class DuckLakeBranchChain {
public:
	//! The chain of main alone, one segment without a cap
	static DuckLakeBranchChain Main();
	//! Walks the parents of the branch to the root, fails on a cycle or a missing parent
	static DuckLakeBranchChain Resolve(const vector<DuckLakeBranchInfo> &branches, idx_t branch_id);

	//! The attached branch
	idx_t BranchId() const;
	bool OnBranch() const;
	//! The segments from the root to the attached branch
	const vector<DuckLakeBranchSegment> &Segments() const;
	//! The cap of the segment at the position, unset for the attached branch
	optional_idx SegmentCap(idx_t position) const;
	//! The read snapshot clamped to the cap of the segment at the position, unclamped without a position or a cap
	idx_t SnapshotBound(optional_idx position, idx_t read_snapshot) const;
	//! The predicate selecting the rows of the chain view, each segment capped by its cap
	//! Prefix "" gives bare column names, "v" gives v.branch_id and v.snapshot_id, both columns can be renamed
	string Predicate(const string &prefix, const string &snapshot_column = "snapshot_id",
	                 const string &branch_column = "branch_id") const;
	//! The chain predicate with every segment also capped by read_snapshot, an SQL expression such as a placeholder
	string ViewPredicate(const string &prefix, const string &snapshot_column, const string &read_snapshot,
	                     const string &branch_column = "branch_id") const;
	//! The CASE mapping the branch column of a row to the position of its segment, root first
	string SegmentPosition(const string &prefix, const string &branch_column = "branch_id") const;
	//! Whether the view of the chain holds the snapshot of the branch
	bool Contains(idx_t branch_id, idx_t snapshot_id) const;
	//! The branch whose segment holds the snapshot id
	idx_t OwnerOf(idx_t snapshot_id) const;
	//! The first catalog and file id a branch allocates, so the ids of branches never collide
	static idx_t AllocatorBase(idx_t branch_id);
	//! The branch ids the allocator base keeps apart
	static constexpr idx_t MAXIMUM_BRANCH_ID = idx_t(1) << 23;

private:
	explicit DuckLakeBranchChain(vector<DuckLakeBranchSegment> segments);
	const DuckLakeBranchSegment &SegmentOf(idx_t snapshot_id) const;

	vector<DuckLakeBranchSegment> segments;
};

} // namespace duckdb
