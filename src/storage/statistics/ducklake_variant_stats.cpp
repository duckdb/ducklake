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

void DuckLakeColumnVariantStats::Merge(const DuckLakeColumnExtraStats &new_stats_p) {
	if (shredded_field_stats.empty()) {
		// nothing to merge
		return;
	}
	auto &new_stats = new_stats_p.Cast<DuckLakeColumnVariantStats>();
	vector<string> stats_to_erase;
	for (auto &entry : shredded_field_stats) {
		auto other_entry = new_stats.shredded_field_stats.find(entry.first);
		if (other_entry == new_stats.shredded_field_stats.end()) {
			// no stats - erase it
			stats_to_erase.push_back(entry.first);
			continue;
		}
		// merge stats
		entry.second.field_stats.MergeStats(other_entry->second.field_stats);
	}
	// erase any stats that do not occur in both
	for (auto &entry : stats_to_erase) {
		shredded_field_stats.erase(entry);
	}
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

static void SerializeShreddedStats(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *obj,
                                   const string &field_name, const DuckLakeVariantStats &variant_stats) {
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

	for (auto &entry : shredded_field_stats) {
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

struct NestedVariantStats {
	// field name, if leaf
	string full_field_name;
	// child stats, if any
	vector<NestedVariantStats> child_stats;
	vector<string> child_names;

	LogicalType ToType(const unordered_map<string, DuckLakeVariantStats> &field_stats) {
		if (!full_field_name.empty()) {
			// field, find it in the stats
			auto entry = field_stats.find(full_field_name);
			if (entry == field_stats.end()) {
				throw InternalException("NestedVariantStats failed to find field name");
			}
			return entry->second.shredded_type;
		}
		if (child_names.empty()) {
			// list
			if (child_stats.size() != 1) {
				throw InternalException("List should have a single child");
			}
			return LogicalType::LIST(child_stats[0].ToType(field_stats));
		}
		if (child_names.size() != child_stats.size()) {
			throw InternalException("Unaligned struct stats");
		}
		// not a leaf - this is a struct
		// recurse into the children
		child_list_t<LogicalType> children;
		for (idx_t i = 0; i < child_names.size(); i++) {
			children.emplace_back(child_names[i], child_stats[i].ToType(field_stats));
		}
		return LogicalType::STRUCT(std::move(children));
	}

	void ConvertStats(const unordered_map<string, DuckLakeVariantStats> &field_stats, BaseStatistics &main_stats) {
		auto &stats = StructStats::GetChildStats(main_stats, VariantStats::TYPED_VALUE_INDEX);

		auto &untyped_value_index_stats = StructStats::GetChildStats(main_stats, VariantStats::UNTYPED_VALUE_INDEX);
		untyped_value_index_stats.SetHasNull();
		if (!full_field_name.empty()) {
			// field, find it in the stats
			auto entry = field_stats.find(full_field_name);
			if (entry == field_stats.end()) {
				throw InternalException("NestedVariantStats failed to find field name");
			}
			auto field_stats = entry->second.field_stats.ToStats();
			if (field_stats) {
				stats.Copy(*field_stats);
			}
			return;
		}
		if (child_names.empty()) {
			// list
			child_stats[0].ConvertStats(field_stats, ListStats::GetChildStats(stats));
			return;
		}
		// struct - recurse into children
		for (idx_t struct_idx = 0; struct_idx < child_stats.size(); struct_idx++) {
			auto &struct_child_stats = StructStats::GetChildStats(stats, struct_idx);
			child_stats[struct_idx].ConvertStats(field_stats, struct_child_stats);
		}
	}
};

void ToNestedVariantStats(const string &field_name, reference<NestedVariantStats> stats) {
	if (field_name == "root") {
		// special case: top-level variant - no need to parse
		stats.get().full_field_name = field_name;
		return;
	}
	idx_t pos = 0;
	while (pos < field_name.size()) {
		if (field_name[pos] != '"') {
			// find the dot
			idx_t start_pos = pos;
			for (; pos < field_name.size(); pos++) {
				if (field_name[pos] == '.') {
					break;
				}
			}
			auto subfield = field_name.substr(start_pos, pos - start_pos);
			if (subfield != "element") {
				throw InvalidInputException("Invalid unquoted field %s, expected element or root", subfield);
			}
			if (stats.get().child_stats.empty()) {
				stats.get().child_stats.emplace_back();
			}
			stats = stats.get().child_stats.back();
			if (pos >= field_name.size()) {
				// leaf level list - push field name into child and finish
				stats.get().full_field_name = field_name;
				return;
			} else {
				// nested list - continue
				pos++;
			}
			continue;
		}
		// quoted field - this is a field name
		string current_field_name;
		idx_t next_pos;
		for (next_pos = pos + 1; next_pos < field_name.size(); next_pos++) {
			auto c = field_name[next_pos];
			if (c == '"') {
				// found a quote
				// check if this is an escaped quote
				if (next_pos + 1 < field_name.size() && field_name[next_pos + 1] == '"') {
					// escaped quote - add a single quote and skip
					current_field_name += c;
					next_pos++;
				} else {
					// not an escaped quote - we are done
					break;
				}
			}
			current_field_name += c;
		}
		// we have the current field name
		// check if the field name already exists
		auto &child_names = stats.get().child_names;
		auto &child_stats = stats.get().child_stats;
		idx_t struct_idx;
		for (struct_idx = 0; struct_idx < child_names.size(); struct_idx++) {
			if (child_names[struct_idx] == current_field_name) {
				// found it!
				break;
			}
		}
		if (struct_idx >= child_names.size()) {
			// did not find it - push the field name
			child_names.push_back(current_field_name);
			child_stats.emplace_back();
		}
		stats = child_stats[struct_idx];
		// have we reached the end?
		if (next_pos + 1 >= field_name.size()) {
			// we have! add the full field name and return
			stats.get().full_field_name = field_name;
			return;
		} else {
			// we have not - this is nested - continue with the next field name
			if (field_name[next_pos + 1] != '.') {
				// invalid format - expected a dot here
				break;
			}
			pos = next_pos + 2;
		}
	}
	throw InvalidInputException("Incorrectly formatted field name %s", field_name);
}

unique_ptr<BaseStatistics> DuckLakeColumnVariantStats::ToStats() const {
	if (shredded_field_stats.empty()) {
		return nullptr;
	}
	// create a nested structure that holds all the stats in the original nested order
	NestedVariantStats nested_stats;
	for (auto &entry : shredded_field_stats) {
		auto &full_field_name = entry.first;
		ToNestedVariantStats(full_field_name, nested_stats);
	}
	// get the type
	auto shredded_type = nested_stats.ToType(shredded_field_stats);
	auto full_shredding_type = TypeVisitor::VisitReplace(shredded_type, [](const LogicalType &type) {
		return LogicalType::STRUCT({{"typed_value", type}, {"untyped_value_index", LogicalType::UINTEGER}});
	});
	auto variant_stats = VariantStats::CreateShredded(full_shredding_type);

	auto &shredded_stats = VariantStats::GetShreddedStats(variant_stats);
	nested_stats.ConvertStats(shredded_field_stats, shredded_stats);

	variant_stats.SetHasNull();
	variant_stats.SetHasNoNull();
	return variant_stats.ToUnique();
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
