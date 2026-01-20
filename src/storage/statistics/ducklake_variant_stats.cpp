#include "storage/ducklake_stats.hpp"

#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

DuckLakeColumnVariantFieldStats::DuckLakeColumnVariantFieldStats(
    DuckLakeVariantStatsArena<DuckLakeColumnVariantFieldStats> &field_arena,
    DuckLakeVariantStatsArena<DuckLakeColumnStats> &stats_arena, idx_t index, const LogicalType &type)
    : field_arena(field_arena), stats_arena(stats_arena), index(index), stats(type) {
}

DuckLakeColumnVariantStats::DuckLakeColumnVariantStats() : DuckLakeColumnExtraStats() {
	//! TODO: initialize the variant stats with a default
}

void DuckLakeColumnVariantStats::BuildInternal(idx_t parent_index, const LogicalType &parent_type) {
	auto id = parent_type.id();
	if (id == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(parent_type);
		for (auto &entry : child_types) {
			auto &child_name = entry.first;
			auto &child_type = entry.second;

			auto new_index = field_arena.size();
			field_arena[parent_index].children.emplace(child_name, new_index);
			field_arena.emplace(field_arena, stats_arena, new_index, child_type);
			BuildInternal(new_index, child_type);
		}
	} else if (id == LogicalTypeId::LIST) {
		auto new_index = field_arena.size();
		auto &child_type = ListType::GetChildType(parent_type);
		field_arena[parent_index].children.emplace("element", new_index);
		field_arena.emplace(field_arena, stats_arena, new_index, child_type);
		BuildInternal(new_index, child_type);
	}
	//! Primitives (leafs) are already handled by the parent
}

void DuckLakeColumnVariantStats::Build(const LogicalType &shredded_internal_type) {
	//! Create the root stats
	field_arena.emplace(field_arena, stats_arena, 0, shredded_internal_type);
	BuildInternal(0, shredded_internal_type);
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnVariantStats::Copy() const {
	auto res = make_uniq<DuckLakeColumnVariantStats>();
	res->shredding_state = this->shredding_state;
	// res->shredded_stats = this->shredded_stats->Copy();
	return res;
}

void DuckLakeColumnVariantStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &variant_stats = new_stats.Cast<DuckLakeColumnVariantStats>();
	//! TODO: copy the MergeShredding method implementation
	throw NotImplementedException("DuckLakeColumnVariantStats::Merge");
}

string DuckLakeColumnVariantStats::Serialize() const {
	//! TODO: Format as JSON
	//! As Parquet's internal format or DuckDBs ???
	throw NotImplementedException("DuckLakeColumnVariantStats::Serialize");
}

void DuckLakeColumnVariantStats::Deserialize(const string &stats) {
	auto doc = yyjson_read(stats.c_str(), stats.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse variant stats JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid variant stats JSON");
	}

	//! TODO: implement deserialize
	throw NotImplementedException("DuckLakeColumnVariantStats::Deserialize");
	yyjson_doc_free(doc);
}

} // namespace duckdb
