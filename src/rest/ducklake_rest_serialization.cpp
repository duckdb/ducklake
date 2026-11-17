#include "rest/ducklake_rest_api.hpp"

#include "common/ducklake_util.hpp"
#include "storage/ducklake_extra_stats.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Column Statistics
//===--------------------------------------------------------------------===//
// the statistics are constructed for their type, and their extra statistics are serialized by the statistics
JSONMutableValue DuckLakeRESTSerializer::Write(JSONWriter &writer, const DuckLakeColumnStats &value) {
	auto result = writer.CreateObject();
	result.Add("type", RESTJson::TypeToJson(writer, value.type));
	if (value.has_min) {
		result.AddString("min_value", value.min);
	}
	if (value.has_max) {
		result.AddString("max_value", value.max);
	}
	if (value.has_null_count) {
		result.Add("null_count", writer.CreateUnsignedInteger(value.null_count));
	}
	if (value.has_num_values) {
		result.Add("value_count", writer.CreateUnsignedInteger(value.num_values));
	}
	result.Add("column_size_bytes", writer.CreateUnsignedInteger(value.column_size_bytes));
	if (value.has_contains_nan) {
		result.Add("contains_nan", writer.CreateBoolean(value.contains_nan));
	}
	result.Add("any_valid", writer.CreateBoolean(value.any_valid));
	result.Add("bounds_unknown", writer.CreateBoolean(value.bounds_unknown));
	result.Add("min_is_exact", writer.CreateBoolean(value.min_is_exact));
	result.Add("max_is_exact", writer.CreateBoolean(value.max_is_exact));
	string extra_stats;
	if (value.extra_stats && value.extra_stats->TrySerialize(extra_stats)) {
		result.AddString("extra_stats", DuckLakeUtil::UnquoteSQLString(extra_stats));
	}
	return result;
}

template <>
DuckLakeColumnStats DuckLakeRESTSerializer::Read(RESTJsonIn in) {
	DuckLakeColumnStats result(RESTJson::TypeFromJson(in.Get("type")));
	auto min_val = in.TryGet("min_value");
	result.has_min = !min_val.IsNull();
	result.min = result.has_min ? min_val.Str() : string();
	auto max_val = in.TryGet("max_value");
	result.has_max = !max_val.IsNull();
	result.max = result.has_max ? max_val.Str() : string();
	auto null_count = in.TryGet("null_count");
	result.has_null_count = !null_count.IsNull();
	result.null_count = result.has_null_count ? null_count.UInt() : 0;
	auto value_count = in.TryGet("value_count");
	result.has_num_values = !value_count.IsNull();
	result.num_values = result.has_num_values ? value_count.UInt() : 0;
	result.column_size_bytes = in.UInt("column_size_bytes", 0);
	auto contains_nan = in.TryGet("contains_nan");
	result.has_contains_nan = !contains_nan.IsNull();
	result.contains_nan = result.has_contains_nan && contains_nan.Bool();
	result.any_valid = in.Bool("any_valid", true);
	result.bounds_unknown = in.Bool("bounds_unknown", false);
	result.min_is_exact = in.Bool("min_is_exact", false);
	result.max_is_exact = in.Bool("max_is_exact", false);
	auto extra_stats = in.TryGet("extra_stats");
	if (!extra_stats.IsNull() && result.extra_stats) {
		result.extra_stats->Deserialize(extra_stats.Str());
	}
	return result;
}

} // namespace duckdb
