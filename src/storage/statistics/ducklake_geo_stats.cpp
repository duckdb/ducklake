#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_geo_stats.hpp"
#include "duckdb/common/types/value.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

DuckLakeColumnGeoStats::DuckLakeColumnGeoStats() : DuckLakeColumnExtraStats(DuckLakeExtraStatsType::GEOMETRY) {
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

bool DuckLakeColumnGeoStats::TrySerialize(string &result) const {
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

	result = StringUtil::Format(R"('{"bbox": %s, "types": %s}')", bbox, types);
	return true;
}

void DuckLakeColumnGeoStats::Serialize(DuckLakeColumnStatsInfo &column_stats) const {
	TrySerialize(column_stats.extra_stats);
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

bool DuckLakeColumnGeoStats::ParseStats(const string &stats_name, const vector<Value> &stats_children) {
	if (stats_name == "bbox_xmax") {
		xmax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_xmin") {
		xmin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_ymax") {
		ymax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_ymin") {
		ymin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_zmax") {
		zmax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_zmin") {
		zmin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_mmax") {
		mmax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_mmin") {
		mmin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "geo_types") {
		auto list_value = stats_children[1].DefaultCastAs(LogicalType::LIST(LogicalType::VARCHAR));
		for (const auto &child : ListValue::GetChildren(list_value)) {
			geo_types.insert(StringValue::Get(child));
		}
	} else {
		return false;
	}
	return true;
}

} // namespace duckdb
