#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_geo_stats.hpp"
#include "storage/ducklake_variant_stats.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"

#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

DuckLakeColumnExtraStats::DuckLakeColumnExtraStats(DuckLakeExtraStatsType stats_type) : stats_type(stats_type) {
}

DuckLakeColumnStats::DuckLakeColumnStats(LogicalType type_p) : type(std::move(type_p)) {
	if (DuckLakeTypes::IsGeoType(type)) {
		extra_stats = make_uniq<DuckLakeColumnGeoStats>();
	}
	if (type.id() == LogicalTypeId::VARIANT) {
		extra_stats = make_uniq<DuckLakeColumnVariantStats>();
	}
}

DuckLakeColumnStats::DuckLakeColumnStats(const DuckLakeColumnStats &other) {
	type = other.type;
	min = other.min;
	max = other.max;
	null_count = other.null_count;
	num_values = other.num_values;
	column_size_bytes = other.column_size_bytes;
	contains_nan = other.contains_nan;
	has_null_count = other.has_null_count;
	has_num_values = other.has_num_values;
	has_min = other.has_min;
	has_max = other.has_max;
	any_valid = other.any_valid;
	has_contains_nan = other.has_contains_nan;

	if (other.extra_stats) {
		extra_stats = other.extra_stats->Copy();
	}
}

DuckLakeColumnStats &DuckLakeColumnStats::operator=(const DuckLakeColumnStats &other) {
	if (this == &other) {
		return *this;
	}
	type = other.type;
	min = other.min;
	max = other.max;
	null_count = other.null_count;
	num_values = other.num_values;
	column_size_bytes = other.column_size_bytes;
	contains_nan = other.contains_nan;
	has_null_count = other.has_null_count;
	has_min = other.has_min;
	has_max = other.has_max;
	has_num_values = other.has_num_values;
	any_valid = other.any_valid;
	has_contains_nan = other.has_contains_nan;

	if (other.extra_stats) {
		extra_stats = other.extra_stats->Copy();
	} else {
		extra_stats.reset();
	}
	return *this;
}

void DuckLakeColumnStats::MergeStats(const DuckLakeColumnStats &new_stats) {
	if (type != new_stats.type) {
		// handle type promotion - adopt the new type
		type = new_stats.type;
	}
	if (!new_stats.has_null_count) {
		has_null_count = false;
	} else if (has_null_count) {
		// both stats have a null count - add them up
		null_count += new_stats.null_count;
	}
	if (!new_stats.has_num_values) {
		has_num_values = false;
	} else if (has_num_values) {
		// both stats have a null count - add them up
		num_values += new_stats.num_values;
	}
	column_size_bytes += new_stats.column_size_bytes;
	if (!new_stats.has_contains_nan) {
		has_contains_nan = false;
	} else if (has_contains_nan) {
		// both stats have a null count - add them up
		if (new_stats.contains_nan) {
			contains_nan = true;
		}
	}

	if (!new_stats.AnyValid()) {
		// all values in the source are NULL - don't update min/max
		return;
	}
	if (!AnyValid()) {
		// all values in the current stats are null - copy the min/max
		min = new_stats.min;
		has_min = new_stats.has_min;
		max = new_stats.max;
		has_max = new_stats.has_max;
		any_valid = true;
		return;
	}
	if (!new_stats.has_min) {
		has_min = false;
	} else if (has_min) {
		// both stats have a min - select the smallest
		if (RequiresValueComparison(type)) {
			// for numerics/temporals we need to parse the stats
			auto current_min = Value(min).DefaultCastAs(type);
			auto new_min = Value(new_stats.min).DefaultCastAs(type);
			if (new_min < current_min) {
				min = new_stats.min;
			}
		} else if (new_stats.min < min) {
			// for other types we can compare the strings directly
			min = new_stats.min;
		}
	}

	if (!new_stats.has_max) {
		has_max = false;
	} else if (has_max) {
		// both stats have a max - select the largest
		if (RequiresValueComparison(type)) {
			// for numerics/temporals we need to parse the stats
			auto current_max = Value(max).DefaultCastAs(type);
			auto new_max = Value(new_stats.max).DefaultCastAs(type);
			if (new_max > current_max) {
				max = new_stats.max;
			}
		} else if (new_stats.max > max) {
			// for other types we can compare the strings directly
			max = new_stats.max;
		}
	}

	if (new_stats.extra_stats) {
		if (extra_stats) {
			extra_stats->Merge(*new_stats.extra_stats);
		} else {
			extra_stats = new_stats.extra_stats->Copy();
		}
	}
}

void DuckLakeTableStats::MergeStats(FieldIndex col_id, const DuckLakeColumnStats &file_stats) {
	auto entry = column_stats.find(col_id);
	if (entry == column_stats.end()) {
		column_stats.insert(make_pair(col_id, file_stats));
		return;
	}
	// merge the stats
	auto &current_stats = entry->second;
	current_stats.MergeStats(file_stats);
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::CreateNumericStats() const {
	auto stats = NumericStats::CreateEmpty(type);
	if (has_min) {
		// set min
		Value min_val(min);
		NumericStats::SetMin(stats, min_val.DefaultCastAs(type));
	}
	if (has_max) {
		// set max
		Value max_val(max);
		NumericStats::SetMax(stats, max_val.DefaultCastAs(type));
	}
	if (!has_min && !has_max) {
		stats = NumericStats::CreateUnknown(type);
	}

	// set null count
	if (!has_null_count || null_count > 0) {
		stats.SetHasNullFast();
	}
	if (!has_null_count || !has_num_values || null_count != num_values) {
		//! Not *all* values are NULL, set HasNoNull
		stats.SetHasNoNullFast();
	}
	return stats.ToUnique();
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::CreateVariantStats() const {
	if (!extra_stats) {
		throw InternalException("Variant DuckLakeColumnStats without extra_stats?");
	}
	auto &variant_stats = extra_stats->Cast<DuckLakeColumnVariantStats>();
	return variant_stats.ToStats();
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::CreateStringStats() const {
	auto stats = StringStats::CreateEmpty(type);
	if (has_min && has_max) {
		StringStats::Update(stats, string_t(max));
		StringStats::Update(stats, string_t(min));
		StringStats::ResetMaxStringLength(stats);
		StringStats::SetContainsUnicode(stats);
	} else if (has_min) {
		stats = StringStats::CreateUnknown(type);
		StringStats::SetMin(stats, string_t(min));
	} else if (has_max) {
		stats = StringStats::CreateUnknown(type);
		StringStats::SetMax(stats, string_t(max));
	}

	// set null count
	if (!has_null_count || null_count > 0) {
		stats.SetHasNullFast();
	}
	if (!has_null_count || !has_num_values || null_count != num_values) {
		//! Not *all* values are NULL, set HasNoNull
		stats.SetHasNoNullFast();
	}
	return stats.ToUnique();
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::ToStats() const {
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return CreateNumericStats();
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		// we only create stats if we know there are no NaN values
		// FIXME: we can just set Max to NaN instead
		if (has_contains_nan && !contains_nan) {
			return CreateNumericStats();
		}
		return nullptr;
	case LogicalTypeId::VARCHAR:
		return CreateStringStats();
	case LogicalTypeId::VARIANT:
		return CreateVariantStats();
	default:
		return nullptr;
	}
}

} // namespace duckdb
