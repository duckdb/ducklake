//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_variant_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_stats.hpp"

namespace duckdb {

struct DuckLakeVariantStats {
	DuckLakeVariantStats(LogicalType shredded_type, DuckLakeColumnStats field_stats);

	LogicalType shredded_type;
	DuckLakeColumnStats field_stats;
};

struct DuckLakeColumnVariantStats final : public DuckLakeColumnExtraStats {
	static constexpr const DuckLakeExtraStatsType TYPE = DuckLakeExtraStatsType::VARIANT;

	DuckLakeColumnVariantStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;

	bool ParseStats(const string &stats_name, const vector<Value> &stats_children) override;

	bool TrySerialize(string &result) const override;
	void Serialize(DuckLakeColumnStatsInfo &column_stats) const override;
	void Deserialize(const string &stats) override;

	unique_ptr<BaseStatistics> ToStats() const;

public:
	// map of field name -> field stats
	unordered_map<string, DuckLakeVariantStats> shredded_field_stats;
	LogicalType variant_type;
};

//! Helper class for constructing variant stats from the stats returned by the Parquet writer
struct PartialVariantStats {
public:
	PartialVariantStats();

	void ParseVariantStats(const vector<string> &path, idx_t variant_field_start, const vector<Value> &col_stats);
	DuckLakeColumnStats Finalize();

private:
	DuckLakeColumnStats result;
	LogicalType variant_type;
	map<vector<string>, DuckLakeVariantStats> shredded_field_stats;
	map<vector<string>, string> field_names;
	set<vector<string>> fully_shredded_fields;
};

} // namespace duckdb
