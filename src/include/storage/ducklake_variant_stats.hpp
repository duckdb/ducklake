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
	DuckLakeColumnVariantStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;

	bool ParseStats(const string &stats_name, const vector<Value> &stats_children) override;

	string Serialize() const override;
	void Deserialize(const string &stats) override;

public:
	// map of field name -> field stats
	unordered_map<string, DuckLakeVariantStats> shredded_field_stats;
	LogicalType variant_type;
};

} // namespace duckdb
