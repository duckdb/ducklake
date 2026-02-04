#include "storage/ducklake_variant_stats.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/common/type_visitor.hpp"


namespace duckdb {

DuckLakeColumnVariantStats::DuckLakeColumnVariantStats()
    : DuckLakeColumnExtraStats(DuckLakeExtraStatsType::VARIANT) {
}

void DuckLakeColumnVariantStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
    throw InternalException("eek");
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnVariantStats::Copy() const {
    throw InternalException("eek");
}

string DuckLakeColumnVariantStats::Serialize() const {
    throw InternalException("eek");
}

void DuckLakeColumnVariantStats::Deserialize(const string &stats) {
    throw InternalException("eek");
}

bool DuckLakeColumnVariantStats::ParseStats(const string &stats_name, const vector<Value> &stats_children) {
    return false;
}

} // namespace duckdb
