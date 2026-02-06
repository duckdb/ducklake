//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_extra_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "common/index.hpp"

namespace duckdb {
class BaseStatistics;
struct DuckLakeColumnStatsInfo;
struct DuckLakeFileInfo;

struct DuckLakeColumnStats;

enum class DuckLakeExtraStatsType { GEOMETRY, VARIANT };

struct DuckLakeColumnExtraStats {
	explicit DuckLakeColumnExtraStats(DuckLakeExtraStatsType stats_type);
	virtual ~DuckLakeColumnExtraStats() = default;

	virtual void Merge(const DuckLakeColumnExtraStats &new_stats) = 0;
	virtual unique_ptr<DuckLakeColumnExtraStats> Copy() const = 0;

	DuckLakeExtraStatsType GetStatsType() const {
		return stats_type;
	}

	virtual bool ParseStats(const string &stats_name, const vector<Value> &children) = 0;

	// Convert the stats into a string representation for global stats storage (e.g. JSON)
	virtual bool TrySerialize(string &result) const {
		// by default: cannot convert to stats
		return false;
	}
	//! Convert the stats into file-specific stats
	virtual void Serialize(DuckLakeColumnStatsInfo &column_stats) const = 0;
	// Parse the stats from a string
	virtual void Deserialize(const string &stats) = 0;

	template <class TARGET>
	TARGET &Cast() {
		if (stats_type != TARGET::TYPE) {
			throw InternalException("Failed to cast DuckLakeColumnExtraStats to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		if (stats_type != TARGET::TYPE) {
			throw InternalException("Failed to cast DuckLakeColumnExtraStats to type - type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

private:
	DuckLakeExtraStatsType stats_type;
};

} // namespace duckdb
