#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_geo_stats.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/json_document.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

DuckLakeColumnGeoStats::DuckLakeColumnGeoStats() : DuckLakeColumnExtraStats(DuckLakeExtraStatsType::GEOMETRY) {
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnGeoStats::Copy() const {
	return make_uniq<DuckLakeColumnGeoStats>(*this);
}

void DuckLakeColumnGeoStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &geo_stats = new_stats.Cast<DuckLakeColumnGeoStats>();

	extent.Merge(geo_stats.extent);
	geo_types.insert(geo_stats.geo_types.begin(), geo_stats.geo_types.end());
}

bool DuckLakeColumnGeoStats::TrySerialize(string &result) const {
	// Format as JSON
	auto xmin_val = extent.x_min == GeometryExtent::EMPTY_MIN ? "null" : std::to_string(extent.x_min);
	auto xmax_val = extent.x_max == GeometryExtent::EMPTY_MAX ? "null" : std::to_string(extent.x_max);
	auto ymin_val = extent.y_min == GeometryExtent::EMPTY_MIN ? "null" : std::to_string(extent.y_min);
	auto ymax_val = extent.y_max == GeometryExtent::EMPTY_MAX ? "null" : std::to_string(extent.y_max);
	auto zmin_val = extent.z_min == GeometryExtent::EMPTY_MIN ? "null" : std::to_string(extent.z_min);
	auto zmax_val = extent.z_max == GeometryExtent::EMPTY_MAX ? "null" : std::to_string(extent.z_max);
	auto mmin_val = extent.m_min == GeometryExtent::EMPTY_MIN ? "null" : std::to_string(extent.m_min);
	auto mmax_val = extent.m_max == GeometryExtent::EMPTY_MAX ? "null" : std::to_string(extent.m_max);

	auto bbox = StringUtil::Format(
	    R"({"xmin": %s, "xmax": %s, "ymin": %s, "ymax": %s, "zmin": %s, "zmax": %s, "mmin": %s, "mmax": %s})", xmin_val,
	    xmax_val, ymin_val, ymax_val, zmin_val, zmax_val, mmin_val, mmax_val);

	auto types = geo_types.empty() ? "" : "\"" + StringUtil::Join(geo_types, "\", \"") + "\"";
	result = StringUtil::Format(R"('{"bbox": %s, "types": [%s]}')", bbox, types);
	return true;
}

void DuckLakeColumnGeoStats::Serialize(DuckLakeColumnStatsInfo &column_stats) const {
	TrySerialize(column_stats.extra_stats);
}

void DuckLakeColumnGeoStats::Deserialize(const string &stats) {
	JSONParseError error;
	auto doc = JSONDocument::TryParse(stats.c_str(), stats.size(), error);
	if (!doc) {
		throw InvalidInputException("Failed to parse geo stats JSON");
	}
	auto root = doc->GetRoot();
	if (!root.IsObject()) {
		throw InvalidInputException("Invalid geo stats JSON");
	}

	auto bbox = root.GetMember("bbox");
	const pair<const char *, double *> bounds[] = {
	    {"xmin", &extent.x_min}, {"xmax", &extent.x_max}, {"ymin", &extent.y_min}, {"ymax", &extent.y_max},
	    {"zmin", &extent.z_min}, {"zmax", &extent.z_max}, {"mmin", &extent.m_min}, {"mmax", &extent.m_max}};
	for (auto &bound : bounds) {
		auto bound_val = bbox.GetMember(bound.first);
		if (bound_val.IsNumber()) {
			*bound.second = bound_val.GetNumber();
		}
	}

	root.GetMember("types").IterateArray([&](JSONValue type_val) {
		if (type_val.IsString()) {
			geo_types.insert(type_val.GetString());
		}
	});
}

bool DuckLakeColumnGeoStats::ParseStats(const string &stats_name, const vector<Value> &stats_children) {
	if (stats_name == "bbox_xmax") {
		extent.x_max = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_xmin") {
		extent.x_min = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_ymax") {
		extent.y_max = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_ymin") {
		extent.y_min = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_zmax") {
		extent.z_max = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_zmin") {
		extent.z_min = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_mmax") {
		extent.m_max = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_mmin") {
		extent.m_min = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
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

unique_ptr<BaseStatistics> DuckLakeColumnGeoStats::ToStats() const {
	auto stats = GeometryStats::CreateEmpty(LogicalType::GEOMETRY());

	GeometryStats::GetExtent(stats) = extent;

	auto &types = GeometryStats::GetTypes(stats);
	for (auto &type : geo_types) {
		types.TryAdd(type);
	}

	// DuckLake doesn't store flags for empty/non-empty geometry/parts, so assume the worst.
	auto &flags = GeometryStats::GetFlags(stats);
	flags.SetHasEmptyGeometry();
	flags.SetHasEmptyPart();
	flags.SetHasNonEmptyGeometry();
	flags.SetHasNonEmptyPart();

	return stats.ToUnique();
}

} // namespace duckdb
