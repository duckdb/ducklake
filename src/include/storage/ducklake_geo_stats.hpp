//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_geo_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_stats.hpp"

namespace duckdb {

struct DuckLakeColumnGeoStats final : public DuckLakeColumnExtraStats {
	static constexpr const DuckLakeExtraStatsType TYPE = DuckLakeExtraStatsType::GEOMETRY;

	DuckLakeColumnGeoStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;
	bool ParseStats(const string &stats_name, const vector<Value> &stats_children) override;

	bool TrySerialize(string &result) const override;
	void Serialize(DuckLakeColumnStatsInfo &column_stats) const override;
	void Deserialize(const string &stats) override;

public:
	double xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax;
	set<string> geo_types;
};

} // namespace duckdb
