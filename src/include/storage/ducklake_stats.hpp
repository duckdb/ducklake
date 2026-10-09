//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_extra_stats.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include <functional>

namespace duckdb {
class BaseStatistics;
struct DuckLakeDataFile;
struct DuckLakeGlobalStatsInfo;

//! Returns true for types that require value-based (not lexicographic string) comparison for min/max stats
inline bool RequiresValueComparison(const LogicalType &type) {
	return type.IsNumeric() || type.IsTemporal() || type.id() == LogicalTypeId::BOOLEAN;
}

//! Finite bounds need an explicit UTC offset
inline bool StatsBoundsRequireOffset(const LogicalType &type) {
	return type.id() == LogicalTypeId::TIMESTAMP_TZ || type.id() == LogicalTypeId::TIMESTAMP_TZ_NS;
}

struct DuckLakeColumnStats;
struct DuckLakeGlobalColumnStatsInfo;

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
	//! Invalidated bounds must never be reseeded
	bool bounds_unknown = false;
	bool has_contains_nan = false;
	bool min_is_exact = false;
	bool max_is_exact = false;

	bool AnyValid() const {
		if (has_num_values && has_null_count) {
			return num_values > null_count;
		}
		return any_valid;
	}
	//! Strings can have truncated min/max stats, other types are always exact
	bool EffectiveMinIsExact() const {
		return has_min && (min_is_exact || RequiresValueComparison(type));
	}
	bool EffectiveMaxIsExact() const {
		return has_max && (max_is_exact || RequiresValueComparison(type));
	}

	unique_ptr<DuckLakeColumnExtraStats> extra_stats;

public:
	static DuckLakeColumnStats FromGlobalStats(const LogicalType &type, const DuckLakeGlobalColumnStatsInfo &col,
	                                           bool table_has_rows);
	//! The statistics of count values that are all the given value
	static DuckLakeColumnStats FromConstant(const LogicalType &type, const Value &value, idx_t count);
	//! Discards the min/max bounds, leaving the counts intact
	void ClearBounds();
	//! Rewrite FLOAT bounds as the DOUBLE values they widen to
	void WidenFloatBounds();
	void CopyMinFrom(const DuckLakeColumnStats &other);
	void CopyMaxFrom(const DuckLakeColumnStats &other);
	static bool BoundsSurviveTypePromotion(const LogicalType &source, const LogicalType &target);
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const DuckLakeColumnStats &new_stats);

private:
	void MergeBound(const DuckLakeColumnStats &new_stats, bool is_min);
	void SetValidity(BaseStatistics &stats) const;
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
	unique_ptr<BaseStatistics> CreateVariantStats() const;
	unique_ptr<BaseStatistics> CreateGeometryStats() const;
};

class DuckLakeFieldId;

//! A leaf field that rows written without it read as NULL or as its default
struct DuckLakeMissingField {
	FieldIndex field_index;
	LogicalType field_type;
	bool reads_null;
	//! Whether the column is inside a list or map element, so it does not have one value per row
	bool repeated;

	//! Collects the leaf fields of a missing field, whose own value reads as NULL if reads_null is set
	static void Collect(const DuckLakeFieldId &field_id, bool reads_null, bool repeated,
	                    vector<DuckLakeMissingField> &result);
	//! Adds the statistics of count rows without the field, which are unknown unless it reads as NULL
	void AddStats(idx_t count, map<FieldIndex, DuckLakeColumnStats> &result) const;
};

//! These are the global, table-wide stats
struct DuckLakeTableStats {
	idx_t record_count = 0;
	bool record_count_unknown = false;
	idx_t table_size_bytes = 0;
	idx_t next_row_id = 0;
	map<FieldIndex, DuckLakeColumnStats> column_stats;

	void MergeStats(FieldIndex col_id, const DuckLakeColumnStats &file_stats);

	//! Merges a file with the given column stats, which can differ from the file's own
	void MergeFileStats(const DuckLakeDataFile &file, const map<FieldIndex, DuckLakeColumnStats> &column_stats);

	//! Skips columns whose type lookup returns nullptr
	static unique_ptr<DuckLakeTableStats>
	FromGlobalStats(const DuckLakeGlobalStatsInfo &stats,
	                const std::function<optional_ptr<const LogicalType>(FieldIndex)> &get_column_type);
};

struct DuckLakeStats {
	map<TableIndex, unique_ptr<DuckLakeTableStats>> table_stats;
};

} // namespace duckdb
