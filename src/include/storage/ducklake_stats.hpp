//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_extra_stats.hpp"

namespace duckdb {
class BaseStatistics;

//! Returns true for types that require value-based (not lexicographic string) comparison for min/max stats
inline bool RequiresValueComparison(const LogicalType &type) {
	return type.IsNumeric() || type.IsTemporal();
}

struct DuckLakeColumnStats;

struct DuckLakeColumnStats {
	explicit DuckLakeColumnStats(LogicalType type_p);

	// Copy constructor
	DuckLakeColumnStats(const DuckLakeColumnStats &other);
	DuckLakeColumnStats &operator=(const DuckLakeColumnStats &other);
	DuckLakeColumnStats(DuckLakeColumnStats &&other) noexcept = default;
	DuckLakeColumnStats &operator=(DuckLakeColumnStats &&other) noexcept = default;

	LogicalType type;
	string min;
	string max;
	idx_t null_count = 0;
	idx_t num_values = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_num_values = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	bool has_contains_nan = false;

	bool AnyValid() const {
		if (has_num_values && has_null_count) {
			return num_values > null_count;
		}
		return any_valid;
	}

	unique_ptr<DuckLakeColumnExtraStats> extra_stats;

public:
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const DuckLakeColumnStats &new_stats);

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
	unique_ptr<BaseStatistics> CreateVariantStats() const;
};

//! These are the global, table-wide stats
struct DuckLakeTableStats {
	idx_t record_count = 0;
	idx_t table_size_bytes = 0;
	idx_t next_row_id = 0;
	map<FieldIndex, DuckLakeColumnStats> column_stats;

	void MergeStats(FieldIndex col_id, const DuckLakeColumnStats &file_stats);
};

struct DuckLakeStats {
	map<TableIndex, unique_ptr<DuckLakeTableStats>> table_stats;
};

} // namespace duckdb
