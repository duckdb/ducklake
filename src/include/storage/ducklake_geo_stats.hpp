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
	DuckLakeColumnGeoStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;
	bool ParseStats(const string &stats_name, const vector<Value> &stats_children) override;
	string Serialize() const override;
	void Deserialize(const string &stats) override;

public:
	double xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax;
	set<string> geo_types;
};

} // namespace duckdb
