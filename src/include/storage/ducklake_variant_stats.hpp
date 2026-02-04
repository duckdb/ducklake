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
	string field_path;
	string shredded_type;
	DuckLakeColumnStats field_stats;
};

struct DuckLakeColumnVariantStats final : public DuckLakeColumnExtraStats {
	DuckLakeColumnVariantStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;

	string Serialize() const override;
	void Deserialize(const string &stats) override;

public:
	vector<DuckLakeVariantStats> variant_stats;
};

} // namespace duckdb
