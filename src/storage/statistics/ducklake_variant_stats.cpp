#include "storage/ducklake_variant_stats.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "storage/ducklake_insert.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

DuckLakeColumnVariantStats::DuckLakeColumnVariantStats() : DuckLakeColumnExtraStats(DuckLakeExtraStatsType::VARIANT) {
}

DuckLakeVariantStats::DuckLakeVariantStats(LogicalType shredded_type_p, DuckLakeColumnStats field_stats_p)
    : shredded_type(std::move(shredded_type_p)), field_stats(std::move(field_stats_p)) {
}

void DuckLakeColumnVariantStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	// FIXME: we can merge overlapping shredded stats
	shredded_field_stats.clear();
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnVariantStats::Copy() const {
	auto result = make_uniq<DuckLakeColumnVariantStats>();
	result->shredded_field_stats = shredded_field_stats;
	return std::move(result);
}

void DuckLakeColumnVariantStats::Serialize(DuckLakeColumnStatsInfo &column_stats) const {
	for (auto &entry : shredded_field_stats) {
		DuckLakeVariantStatsInfo shredded_stats;
		shredded_stats.field_name = entry.first;
		shredded_stats.shredded_type = DuckLakeTypes::ToString(entry.second.shredded_type);
		shredded_stats.field_stats =
		    DuckLakeColumnStatsInfo::FromColumnStats(column_stats.column_id, entry.second.field_stats);
		column_stats.variant_stats.push_back(std::move(shredded_stats));
	}
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

string QuoteVariantFieldName(const string &field_name) {
	return KeywordHelper::WriteQuoted(field_name, '"');
}

vector<string> ExtractVariantFieldNames(const vector<string> &path, idx_t variant_field_start) {
	vector<string> field_names;
	for (idx_t i = variant_field_start; i + 1 < path.size(); i += 2) {
		if (path[i] != "typed_value") {
			throw InvalidInputException("Expected typed_value at position %d in path %s", i,
			                            StringUtil::Join(path, "."));
		}
		field_names.push_back(path[i + 1]);
	}
	return field_names;
}

LogicalType ExtractVariantType(const LogicalType &variant_type, const vector<string> &field_names,
                               string &variant_field_name, idx_t field_idx = 0) {
	if (field_idx == 0 && field_names.empty()) {
		variant_field_name = "root";
	}
	if (variant_type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException(
		    "Expected variant type to be struct at this layer while looking for field %s - but found %s",
		    StringUtil::Join(field_names, "."), variant_type);
	}
	// find the "typed_value" within this entry
	for (auto &entry : StructType::GetChildTypes(variant_type)) {
		if (entry.first == "typed_value") {
			// found!
			if (field_idx >= field_names.size()) {
				// reached the final type - this is the type
				return entry.second;
			}
			auto &field_name = field_names[field_idx];
			if (entry.second.id() == LogicalTypeId::LIST) {
				if (field_name != "element") {
					throw InvalidInputException(
					    "Found a list at this layer - expected a field named \"element\" but got a field named \"%s\"",
					    field_name);
				}
				if (!variant_field_name.empty()) {
					variant_field_name += ".";
				}
				variant_field_name += "element";
				return ExtractVariantType(ListType::GetChildType(entry.second), field_names, variant_field_name,
				                          field_idx + 1);
			}
			if (entry.second.id() != LogicalTypeId::STRUCT) {
				throw InvalidInputException(
				    "Expected variant type to be struct at this layer while looking for nested field %s - but found %s",
				    StringUtil::Join(field_names, "."), variant_type);
			}
			// not the final field - recurse to find the field
			for (auto &typed_child : StructType::GetChildTypes(entry.second)) {
				if (typed_child.first == field_name) {
					// found the field to recurse on
					if (!variant_field_name.empty()) {
						variant_field_name += ".";
					}
					variant_field_name += QuoteVariantFieldName(field_name);
					return ExtractVariantType(typed_child.second, field_names, variant_field_name, field_idx + 1);
				}
			}
			throw InvalidInputException("Could not find field %s in type %s", field_name, variant_type);
		}
	}
	throw InvalidInputException("Could not find typed_value field in type %s", variant_type);
}

PartialVariantStats::PartialVariantStats() : result(LogicalTypeId::VARIANT) {
}

void PartialVariantStats::ParseVariantStats(const vector<string> &path, idx_t variant_field_start,
                                            const vector<Value> &col_stats) {
	if (path.size() == variant_field_start + 1 && path.back() == "metadata") {
		// metadata provides the top-level metadata, together with the shredding types
		auto metadata_stats = DuckLakeInsert::ParseColumnStats(LogicalTypeId::VARIANT, col_stats);
		// propagate the top-level stats
		if (metadata_stats.has_null_count) {
			result.has_null_count = true;
			result.null_count = metadata_stats.null_count;
		}
		if (metadata_stats.has_num_values) {
			result.has_num_values = true;
			result.num_values = metadata_stats.num_values;
		}
		result.column_size_bytes += metadata_stats.column_size_bytes;

		variant_type = std::move(metadata_stats.extra_stats->Cast<DuckLakeColumnVariantStats>().variant_type);
		return;
	}
	// this is information about a field within the variant
	// this must be either a "value" (untyped info) or "typed_value" (shredded info)
	auto variant_field_names = ExtractVariantFieldNames(path, variant_field_start);
	if (path.back() == "typed_value") {
		// typed info - extract the shredded type from the variant type
		string variant_field_name;
		auto shredded_type = ExtractVariantType(variant_type, variant_field_names, variant_field_name);
		auto shredded_stats = DuckLakeInsert::ParseColumnStats(shredded_type, col_stats);
		result.column_size_bytes += shredded_stats.column_size_bytes;
		DuckLakeVariantStats variant_field_stats(std::move(shredded_type), std::move(shredded_stats));
		field_names[variant_field_names] = std::move(variant_field_name);
		shredded_field_stats.insert(make_pair(variant_field_names, std::move(variant_field_stats)));
		return;
	}
	if (path.back() == "value") {
		// untyped info - blob stats
		auto untyped_stats = DuckLakeInsert::ParseColumnStats(LogicalTypeId::BLOB, col_stats);
		result.column_size_bytes += untyped_stats.column_size_bytes;
		// check if there are any untyped values - if they are ALL null this is fully shredded
		// for partially shredded data we don't write any stats to ducklake
		if (untyped_stats.has_null_count && untyped_stats.has_num_values &&
		    untyped_stats.null_count == untyped_stats.num_values) {
			fully_shredded_fields.insert(variant_field_names);
		}
		return;
	}
	throw InvalidInputException("Variant stats - unknown path element %s - expected metadata, typed_value or value",
	                            path.back());
}

DuckLakeColumnStats PartialVariantStats::Finalize() {
	// for any fully shredded variant - copy over the stats
	auto &variant_stats = result.extra_stats->Cast<DuckLakeColumnVariantStats>();
	for (auto &entry : fully_shredded_fields) {
		auto shredded_entry = shredded_field_stats.find(entry);
		if (shredded_entry != shredded_field_stats.end()) {
			auto field_name_entry = field_names.find(shredded_entry->first);
			if (field_name_entry == field_names.end()) {
				throw InternalException("Found shredded stats but not shredded field name");
			}
			variant_stats.shredded_field_stats.emplace(std::move(field_name_entry->second),
			                                           std::move(shredded_entry->second));
			field_names.erase(field_name_entry);
			shredded_field_stats.erase(entry);
		}
	}
	return std::move(result);
}

unique_ptr<BaseStatistics> DuckLakeColumnVariantStats::ToStats() const {
	return nullptr;
}

} // namespace duckdb
