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
#include "yyjson.hpp"

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

static DuckLakeVariantStats DeserializeShreddedStats(const LogicalType &shredded_type, duckdb_yyjson::yyjson_val *obj) {
	DuckLakeColumnStats column_stats(shredded_type);
	DuckLakeVariantStats variant_stats(shredded_type, std::move(column_stats));

	auto &stats = variant_stats.field_stats;

	auto *null_count_val = yyjson_obj_get(obj, "null_count");
	if (null_count_val) {
	        stats.has_null_count = true;
	        stats.null_count = (idx_t)yyjson_get_int(null_count_val);
	}

	auto *min_val = yyjson_obj_get(obj, "min");
	if (min_val) {
	        stats.has_min = true;
	        stats.min = string(yyjson_get_str(min_val), yyjson_get_len(min_val));
	}

	auto *max_val = yyjson_obj_get(obj, "max");
	if (max_val) {
	        stats.has_max = true;
	        stats.max = string(yyjson_get_str(max_val), yyjson_get_len(max_val));
	}

	auto *num_values_val = yyjson_obj_get(obj, "num_values");
	if (num_values_val) {
	        stats.has_num_values = true;
	        stats.num_values = (idx_t)yyjson_get_int(num_values_val);
	}

	auto *contains_nan_val = yyjson_obj_get(obj, "contains_nan");
	if (contains_nan_val) {
	        stats.has_contains_nan = true;
	        stats.contains_nan = yyjson_get_bool(contains_nan_val);
	}

	auto *column_size_val = yyjson_obj_get(obj, "column_size_bytes");
	if (column_size_val) {
	        stats.column_size_bytes = (idx_t)yyjson_get_int(column_size_val);
	}

	auto *any_valid_val = yyjson_obj_get(obj, "any_valid");
	if (any_valid_val) {
	        stats.any_valid = yyjson_get_bool(any_valid_val);
	}

	if (stats.extra_stats) {
		auto *extra_stats_val = yyjson_obj_get(obj, "extra_stats");
		if (extra_stats_val) {
		        string extra_stats_str = yyjson_get_str(extra_stats_val);
			stats.extra_stats->Deserialize(extra_stats_str);
		}
	}

	return variant_stats;
	}

void DuckLakeColumnVariantStats::Deserialize(const string &stats) {
	duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(stats.c_str(), stats.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse VARIANT stats JSON \"%s\"", stats);
	}
	duckdb_yyjson::yyjson_val *root = yyjson_doc_get_root(doc);
	size_t idx, max;
	duckdb_yyjson::yyjson_val *obj;
	yyjson_arr_foreach(root, idx, max, obj) {
		auto *field_name_val = yyjson_obj_get(obj, "field_name");
		if (!field_name_val) {
			throw InvalidInputException("Missing field_name in VARIANT stats JSON \"%s\"", stats);
		}
		string field_name = yyjson_get_str(field_name_val);

		auto *shredded_type_val = yyjson_obj_get(obj, "shredded_type");
		if (!shredded_type_val) {
			throw InvalidInputException("Missing shredded_type in VARIANT stats JSON \"%s\"", stats);
		}
		auto shredded_type = DuckLakeTypes::FromString(yyjson_get_str(shredded_type_val));
		auto variant_stats = DeserializeShreddedStats(shredded_type, obj);

		shredded_field_stats.insert(make_pair(std::move(field_name), std::move(variant_stats)));
	}

	yyjson_doc_free(doc);
}

static void SerializeShreddedStats(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *obj, const string &field_name, const DuckLakeVariantStats &variant_stats) {
	yyjson_mut_obj_add_strcpy(doc, obj, "field_name", field_name.c_str());

	yyjson_mut_obj_add_strcpy(doc, obj, "shredded_type", DuckLakeTypes::ToString(variant_stats.shredded_type).c_str());

	auto &stats = variant_stats.field_stats;
	if (stats.has_null_count) {
		yyjson_mut_obj_add_int(doc, obj, "null_count", (int64_t)stats.null_count);
	}

	if (stats.has_min) {
		yyjson_mut_obj_add_strncpy(doc, obj, "min", stats.min.c_str(), stats.min.size());
	}

	if (stats.has_max) {
		yyjson_mut_obj_add_strncpy(doc, obj, "max", stats.max.c_str(), stats.max.size());
	}

	if (stats.has_num_values) {
		yyjson_mut_obj_add_int(doc, obj, "num_values", (int64_t)stats.num_values);
	}

	if (stats.has_contains_nan) {
		yyjson_mut_obj_add_bool(doc, obj, "contains_nan", stats.contains_nan);
	}

	yyjson_mut_obj_add_int(doc, obj, "column_size_bytes", (int64_t)stats.column_size_bytes);
	yyjson_mut_obj_add_bool(doc, obj, "any_valid", stats.any_valid);

	if (stats.extra_stats) {
		string extra_stats_str;
		if (stats.extra_stats->TrySerialize(extra_stats_str)) {
			yyjson_mut_obj_add_strcpy(doc, obj, "extra_stats", extra_stats_str.c_str());
		}
	}
}

bool DuckLakeColumnVariantStats::TrySerialize(string &result) const {
	if (shredded_field_stats.empty()) {
		// no shredded stats
		return false;
	}

	duckdb_yyjson::yyjson_mut_doc *doc = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
	duckdb_yyjson::yyjson_mut_val *root = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root);

	for(auto &entry : shredded_field_stats) {
		auto child_obj = yyjson_mut_obj(doc);
		SerializeShreddedStats(doc, child_obj, entry.first, entry.second);
		yyjson_mut_arr_append(root, child_obj);
	}

	// serialize to string
	size_t len = 0;
	char *json = yyjson_mut_write(doc, 0, &len);
	if (!json) {
		throw InternalException("Failed to serialize the VARIANT stats to JSON");
	}
	string out(json, len);
	free(json);
	yyjson_mut_doc_free(doc);
	result = "'" + out + "'";
	return true;
}

unique_ptr<BaseStatistics> DuckLakeColumnVariantStats::ToStats() const {
	if (shredded_field_stats.empty()) {
		return nullptr;
	}
	throw InternalException("To Stats");
//	// create the "shredded" type
//	if (shredding_state != VariantStatsShreddingState::SHREDDED) {
//		return nullptr;
//	}
//	auto &child_types = StructType::GetChildTypes(shredded_type);
//	idx_t index = DConstants::INVALID_INDEX;
//	for (idx_t i = 0; i < child_types.size(); i++) {
//		if (child_types[i].first == "typed_value") {
//			index = i;
//			break;
//		}
//	}
//	if (index == DConstants::INVALID_INDEX) {
//		throw InternalException("State says SHREDDED but there is no 'typed_value' ???");
//	}
//
//	//! STRUCT(metadata, value, typed_value) -> STRUCT(untyped_value_index, typed_value)
//	LogicalType logical_type;
//	if (!TypedValueLayoutToType(child_types[index].second, logical_type)) {
//		return nullptr;
//	}
//	auto shredding_type = TypeVisitor::VisitReplace(logical_type, [](const LogicalType &type) {
//		return LogicalType::STRUCT({{"untyped_value_index", LogicalType::UINTEGER}, {"typed_value", type}});
//	});
//
//	auto variant_stats = VariantStats::CreateShredded(shredding_type);
//
//	//! Take the root stats
//	auto &shredded_stats = VariantStats::GetShreddedStats(variant_stats);
//	if (!ConvertStats(0, shredded_stats)) {
//		return nullptr;
//	}
//
//	variant_stats.SetHasNull();
//	variant_stats.SetHasNoNull();
//	return variant_stats.ToUnique();
//	return nullptr;
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
		bool is_fully_shredded = false;
		if (untyped_stats.has_null_count && untyped_stats.has_num_values &&
		    untyped_stats.null_count == untyped_stats.num_values) {
			// all untyped values are NULL - fully shredded
			is_fully_shredded = true;
		}
		if (untyped_stats.has_min && untyped_stats.has_max && untyped_stats.min == "00" && untyped_stats.max == "00") {
			// we have untyped values - but they are all NULL
			is_fully_shredded = true;
		}
		if (is_fully_shredded) {
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

} // namespace duckdb
