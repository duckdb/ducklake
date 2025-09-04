#include "storage/ducklake_stats.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"

#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

DuckLakeColumnStats::DuckLakeColumnStats(const DuckLakeColumnStats &other) {
	type = other.type;
	min = other.min;
	max = other.max;
	null_count = other.null_count;
	column_size_bytes = other.column_size_bytes;
	contains_nan = other.contains_nan;
	has_null_count = other.has_null_count;
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
	column_size_bytes = other.column_size_bytes;
	contains_nan = other.contains_nan;
	has_null_count = other.has_null_count;
	has_min = other.has_min;
	has_max = other.has_max;
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
	column_size_bytes += new_stats.column_size_bytes;
	if (!new_stats.has_contains_nan) {
		has_contains_nan = false;
	} else if (has_contains_nan) {
		// both stats have a null count - add them up
		if (new_stats.contains_nan) {
			contains_nan = true;
		}
	}

	if (!new_stats.any_valid) {
		// all values in the source are NULL - don't update min/max
		return;
	}
	if (!any_valid) {
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
		if (type.IsNumeric()) {
			// for numerics we need to parse the stats
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
		// both stats have a min - select the smallest
		if (type.IsNumeric()) {
			// for numerics we need to parse the stats
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
	if (!has_min || !has_max) {
		return nullptr;
	}
	auto stats = NumericStats::CreateEmpty(type);
	// set min
	Value min_val(min);
	NumericStats::SetMin(stats, min_val.DefaultCastAs(type));
	// set max
	Value max_val(max);
	NumericStats::SetMax(stats, max_val.DefaultCastAs(type));
	// set null count
	if (!has_null_count || null_count > 0) {
		stats.SetHasNull();
	}
	stats.SetHasNoNull();
	return stats.ToUnique();
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::CreateStringStats() const {
	if (!has_min || !has_max) {
		return nullptr;
	}
	auto stats = StringStats::CreateEmpty(type);

	StringStats::Update(stats, string_t(min));
	StringStats::Update(stats, string_t(max));
	StringStats::ResetMaxStringLength(stats);
	StringStats::SetContainsUnicode(stats);
	// set null count
	if (!has_null_count || null_count > 0) {
		stats.SetHasNull();
	}
	stats.SetHasNoNull();
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
	default:
		return nullptr;
	}
}

DuckLakeColumnGeoStats::DuckLakeColumnGeoStats() : DuckLakeColumnExtraStats() {
	xmin = NumericLimits<double>::Maximum();
	xmax = NumericLimits<double>::Minimum();
	ymin = NumericLimits<double>::Maximum();
	ymax = NumericLimits<double>::Minimum();
	zmin = NumericLimits<double>::Maximum();
	zmax = NumericLimits<double>::Minimum();
	mmin = NumericLimits<double>::Maximum();
	mmax = NumericLimits<double>::Minimum();
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnGeoStats::Copy() const {
	return make_uniq<DuckLakeColumnGeoStats>(*this);
}

void DuckLakeColumnGeoStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &geo_stats = new_stats.Cast<DuckLakeColumnGeoStats>();

	xmin = MinValue(xmin, geo_stats.xmin);
	xmax = MaxValue(xmax, geo_stats.xmax);
	ymin = MinValue(ymin, geo_stats.ymin);
	ymax = MaxValue(ymax, geo_stats.ymax);
	zmin = MinValue(zmin, geo_stats.zmin);
	zmax = MaxValue(zmax, geo_stats.zmax);
	mmin = MinValue(mmin, geo_stats.mmin);
	mmax = MaxValue(mmax, geo_stats.mmax);

	geo_types.insert(geo_stats.geo_types.begin(), geo_stats.geo_types.end());
}

string DuckLakeColumnGeoStats::Serialize() const {

	// Format as JSON
	auto xmin_val = xmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(xmin);
	auto xmax_val = xmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(xmax);
	auto ymin_val = ymin == NumericLimits<double>::Maximum() ? "null" : std::to_string(ymin);
	auto ymax_val = ymax == NumericLimits<double>::Minimum() ? "null" : std::to_string(ymax);
	auto zmin_val = zmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(zmin);
	auto zmax_val = zmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(zmax);
	auto mmin_val = mmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(mmin);
	auto mmax_val = mmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(mmax);

	auto bbox = StringUtil::Format(
	    R"({"xmin": %s, "xmax": %s, "ymin": %s, "ymax": %s, "zmin": %s, "zmax": %s, "mmin": %s, "mmax": %s})", xmin_val,
	    xmax_val, ymin_val, ymax_val, zmin_val, zmax_val, mmin_val, mmax_val);

	string types = "[";
	for (auto &type : geo_types) {
		if (types.size() > 1) {
			types += ", ";
		}
		types += StringUtil::Format("\"%s\"", type);
	}
	types += "]";

	return StringUtil::Format(R"('{"bbox": %s, "types": %s}')", bbox, types);
}

void DuckLakeColumnGeoStats::Deserialize(const string &stats) {
	auto doc = yyjson_read(stats.c_str(), stats.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse geo stats JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid geo stats JSON");
	}

	auto bbox_json = yyjson_obj_get(root, "bbox");
	if (yyjson_is_obj(bbox_json)) {
		auto xmin_json = yyjson_obj_get(bbox_json, "xmin");
		if (yyjson_is_num(xmin_json)) {
			xmin = yyjson_get_real(xmin_json);
		}
		auto xmax_json = yyjson_obj_get(bbox_json, "xmax");
		if (yyjson_is_num(xmax_json)) {
			xmax = yyjson_get_real(xmax_json);
		}
		auto ymin_json = yyjson_obj_get(bbox_json, "ymin");
		if (yyjson_is_num(ymin_json)) {
			ymin = yyjson_get_real(ymin_json);
		}
		auto ymax_json = yyjson_obj_get(bbox_json, "ymax");
		if (yyjson_is_num(ymax_json)) {
			ymax = yyjson_get_real(ymax_json);
		}
		auto zmin_json = yyjson_obj_get(bbox_json, "zmin");
		if (yyjson_is_num(zmin_json)) {
			zmin = yyjson_get_real(zmin_json);
		}
		auto zmax_json = yyjson_obj_get(bbox_json, "zmax");
		if (yyjson_is_num(zmax_json)) {
			zmax = yyjson_get_real(zmax_json);
		}
		auto mmin_json = yyjson_obj_get(bbox_json, "mmin");
		if (yyjson_is_num(mmin_json)) {
			mmin = yyjson_get_real(mmin_json);
		}
		auto mmax_json = yyjson_obj_get(bbox_json, "mmax");
		if (yyjson_is_num(mmax_json)) {
			mmax = yyjson_get_real(mmax_json);
		}
	}

	auto types_json = yyjson_obj_get(root, "types");
	if (yyjson_is_arr(types_json)) {
		yyjson_arr_iter iter;
		yyjson_arr_iter_init(types_json, &iter);
		yyjson_val *type_json;
		while ((type_json = yyjson_arr_iter_next(&iter))) {
			if (yyjson_is_str(type_json)) {
				geo_types.insert(yyjson_get_str(type_json));
			}
		}
	}
	yyjson_doc_free(doc);
}

} // namespace duckdb
