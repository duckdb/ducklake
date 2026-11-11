#include "common/ducklake_branch_chain.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

DuckLakeBranchChain::DuckLakeBranchChain(vector<DuckLakeBranchSegment> segments_p) : segments(std::move(segments_p)) {
}

DuckLakeBranchChain DuckLakeBranchChain::Main() {
	vector<DuckLakeBranchSegment> segments;
	segments.push_back({0, optional_idx()});
	return DuckLakeBranchChain(std::move(segments));
}

static optional_ptr<const DuckLakeBranchInfo> FindBranchInfo(const vector<DuckLakeBranchInfo> &branches,
                                                             idx_t branch_id) {
	for (auto &branch : branches) {
		if (branch.branch_id == branch_id) {
			return &branch;
		}
	}
	return nullptr;
}

DuckLakeBranchChain DuckLakeBranchChain::Resolve(const vector<DuckLakeBranchInfo> &branches, idx_t branch_id) {
	vector<DuckLakeBranchSegment> segments;
	// the cap of a branch is the parent snapshot its child continues from, clamped by the caps below it
	// a branch forked below the fork of its parent reads nothing of the parent itself
	optional_idx cap;
	auto current = branch_id;
	while (true) {
		if (segments.size() > branches.size()) {
			throw InvalidInputException("Corrupt DuckLake - the parents of branch %llu form a cycle", branch_id);
		}
		auto branch = FindBranchInfo(branches, current);
		if (!branch) {
			throw InvalidInputException("Corrupt DuckLake - branch %llu in the chain of branch %llu does not exist",
			                            current, branch_id);
		}
		segments.push_back({current, cap});
		if (!branch->parent_id.IsValid()) {
			break;
		}
		if (!branch->parent_branch_snapshot.IsValid()) {
			throw InvalidInputException("Corrupt DuckLake - branch %llu has a parent but no parent snapshot", current);
		}
		auto parent_cap = branch->parent_branch_snapshot.GetIndex();
		if (!cap.IsValid() || parent_cap < cap.GetIndex()) {
			cap = parent_cap;
		}
		current = branch->parent_id.GetIndex();
	}
	std::reverse(segments.begin(), segments.end());
	return DuckLakeBranchChain(std::move(segments));
}

idx_t DuckLakeBranchChain::BranchId() const {
	return segments.back().branch_id;
}

bool DuckLakeBranchChain::OnBranch() const {
	return BranchId() != 0;
}

const vector<DuckLakeBranchSegment> &DuckLakeBranchChain::Segments() const {
	return segments;
}

optional_idx DuckLakeBranchChain::RootCap() const {
	return segments.front().last_snapshot;
}

static string ColumnPrefix(const string &prefix) {
	return prefix.empty() ? string() : prefix + ".";
}

string DuckLakeBranchChain::Predicate(const string &prefix, const string &snapshot_column) const {
	auto column_prefix = ColumnPrefix(prefix);
	vector<string> arms;
	for (auto &segment : segments) {
		auto &cap = segment.last_snapshot;
		if (cap.IsValid()) {
			arms.push_back(StringUtil::Format("(%sbranch_id = %llu AND %s%s <= %llu)", column_prefix, segment.branch_id,
			                                  column_prefix, snapshot_column, cap.GetIndex()));
		} else {
			arms.push_back(StringUtil::Format("%sbranch_id = %llu", column_prefix, segment.branch_id));
		}
	}
	return StringUtil::Join(arms, " OR ");
}

string DuckLakeBranchChain::ViewPredicate(const string &prefix, const string &snapshot_column,
                                          const string &read_snapshot) const {
	// the read snapshot caps every arm of the chain predicate
	return StringUtil::Format("(%s) AND %s%s <= %s", Predicate(prefix, snapshot_column), ColumnPrefix(prefix),
	                          snapshot_column, read_snapshot);
}

const DuckLakeBranchSegment &DuckLakeBranchChain::SegmentOf(idx_t snapshot_id) const {
	// the ids of a chain are contiguous, so the first segment whose cap reaches the id holds it
	for (auto &segment : segments) {
		if (!segment.last_snapshot.IsValid() || snapshot_id <= segment.last_snapshot.GetIndex()) {
			return segment;
		}
	}
	// LCOV_EXCL_START
	return segments.back();
	// LCOV_EXCL_STOP
}

idx_t DuckLakeBranchChain::OwnerOf(idx_t snapshot_id) const {
	return SegmentOf(snapshot_id).branch_id;
}

optional_idx DuckLakeBranchChain::CapOf(idx_t snapshot_id) const {
	return SegmentOf(snapshot_id).last_snapshot;
}

idx_t DuckLakeBranchChain::AllocatorBase(idx_t branch_id) {
	// a commit on a branch allocates catalog and file ids from the maximum of its head and this base,
	// which keeps the ids of every branch apart without a read across branches, main keeps the plain ids
	if (branch_id >= MAXIMUM_BRANCH_ID) {
		throw InvalidInputException("Corrupt DuckLake - branch id %llu exceeds the maximum of 2^23 - 1", branch_id);
	}
	return branch_id << 40;
}

} // namespace duckdb
