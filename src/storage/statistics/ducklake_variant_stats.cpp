#include "storage/ducklake_variant_stats.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

DuckLakeColumnVariantStats::DuckLakeColumnVariantStats() : DuckLakeColumnExtraStats(DuckLakeExtraStatsType::VARIANT) {
}

DuckLakeVariantStats::DuckLakeVariantStats(LogicalType shredded_type_p, DuckLakeColumnStats field_stats_p)
    : shredded_type(std::move(shredded_type_p)), field_stats(std::move(field_stats_p)) {
}

void DuckLakeColumnVariantStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	throw InternalException("eek");
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnVariantStats::Copy() const {
	auto result = make_uniq<DuckLakeColumnVariantStats>();
	result->shredded_field_stats = shredded_field_stats;
	return std::move(result);
}

string DuckLakeColumnVariantStats::Serialize() const {
	throw InternalException("eek");
}

void DuckLakeColumnVariantStats::Deserialize(const string &stats) {
	throw InternalException("eek");
}

bool DuckLakeColumnVariantStats::ParseStats(const string &stats_name, const vector<Value> &stats_children) {
	if (stats_name == "variant_type") {
		auto type_str = StringValue::Get(stats_children[1]);
		variant_type = UnboundType::TryParseAndDefaultBind(type_str);
		return true;
	}
	return false;
}

} // namespace duckdb
