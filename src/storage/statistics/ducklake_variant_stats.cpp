#include "storage/ducklake_stats.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/printer.hpp"

#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

DuckLakeColumnVariantStats::DuckLakeColumnVariantStats()
	: DuckLakeColumnExtraStats(), shredding_state(VariantStatsShreddingState::UNINITIALIZED) {
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
			field_arena.emplace(new_index);
			BuildInternal(new_index, child_type);
		}
	} else if (id == LogicalTypeId::LIST) {
		auto new_index = field_arena.size();
		auto &child_type = ListType::GetChildType(parent_type);
		field_arena[parent_index].children.emplace("element", new_index);
		field_arena.emplace(new_index);
		BuildInternal(new_index, child_type);
	}
	//! Primitives (leafs) are already handled by the parent
}

void DuckLakeColumnVariantStats::Build(const LogicalType &shredded_internal_type) {
	//! Create the root stats
	shredding_state = VariantStatsShreddingState::SHREDDED;
	shredded_type = shredded_internal_type;
	field_arena.emplace(0);
	BuildInternal(0, shredded_internal_type);
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnVariantStats::Copy() const {
	auto res = make_uniq<DuckLakeColumnVariantStats>();
	res->shredding_state = this->shredding_state;
	res->field_arena = this->field_arena;
	res->stats_arena = this->stats_arena;
	res->shredded_type = this->shredded_type;
	return res;
}

void DuckLakeColumnVariantStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &variant_stats = new_stats.Cast<DuckLakeColumnVariantStats>();
	//! TODO: copy the MergeShredding method implementation
	throw NotImplementedException("DuckLakeColumnVariantStats::Merge");
}

static void SerializeShreddedStats(yyjson_mut_doc *doc, yyjson_mut_val *obj, const duckdb::DuckLakeColumnStats &stats) {
	yyjson_mut_obj_add_strcpy(doc, obj, "type", stats.type.ToString().c_str());

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
		yyjson_mut_obj_add_strcpy(doc, obj, "extra_stats", stats.extra_stats->Serialize().c_str());
	}
}

static void SerializeShreddedChildren(const DuckLakeColumnVariantStats &variant, yyjson_mut_doc *doc, yyjson_mut_val *obj,
								   idx_t parent_field, const LogicalType &type) {
	auto &stats_arena = variant.stats_arena;
	auto &field_arena = variant.field_arena;
	auto &parent = field_arena[parent_field];

	auto serialize_child = [&](idx_t field_index, yyjson_mut_val *container, const string &name, const LogicalType &child_type) {
		if (field_index >= field_arena.size()) {
			throw InternalException("VariantStats::Serialize: field_index out of range for field_arena");
		}
		auto &field = field_arena[field_index];
		auto name_str = unsafe_yyjson_mut_strncpy(doc, name.c_str(), name.size());
		auto child_obj = yyjson_mut_obj_add_obj(doc, container, name_str);
		if (!field.children.empty()) {
			SerializeShreddedChildren(variant, doc, child_obj, field_index, child_type);
			return;
		}

		if (field.stats_index.IsValid()) {
			auto stats_index = field.stats_index.GetIndex();
			if (stats_index >= stats_arena.size()) {
				throw InternalException("VariantStats::Serialize: stats_index out of range for stats_arena");
			}
			SerializeShreddedStats(doc, child_obj, stats_arena[stats_index]);
		}
	};

	if (parent.children.empty()) {
		return;
	}
	auto type_id = type.id();
	yyjson_mut_obj_add_strcpy(doc, obj, "type", EnumUtil::ToString(type_id).c_str());
	auto children = yyjson_mut_obj_add_obj(doc, obj, "children");

	if (type_id == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &child_entry = child_types[i];
			auto &name = child_entry.first;
			auto &child_type = child_entry.second;

			auto child_it = parent.children.find(name);
			if (child_it == parent.children.end()) {
				throw InternalException("VariantStats::Serialize: Can't find child '%s' in parent", name);
			}
			auto field_index = child_it->second;
			serialize_child(field_index, children, name, child_type);
		}
	} else if (type_id == LogicalTypeId::LIST) {
		auto &child_type = ListType::GetChildType(type);
		auto child_it = parent.children.find("element");
		if (child_it == parent.children.end()) {
			throw InternalException("VariantStats::Serialize: Can't find child 'element' in parent");
		}
		auto field_index = child_it->second;
		serialize_child(field_index, children, "element", child_type);
	}
}

static void SerializeShredded(const DuckLakeColumnVariantStats &variant, yyjson_mut_doc *doc, yyjson_mut_val *root) {
	SerializeShreddedChildren(variant, doc, root, 0, variant.shredded_type);
}

string DuckLakeColumnVariantStats::Serialize() const {
	// create document
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// state
	auto state_str = EnumUtil::ToString(shredding_state);
	yyjson_mut_obj_add_strcpy(doc, root, "state", state_str.c_str());

	if (shredding_state == VariantStatsShreddingState::SHREDDED) {
		if (stats_arena.size() == 0) {
			throw InternalException("Variant is SHREDDED but there are 0 stats for it");
		}
		SerializeShredded(*this, doc, root);
	}

	// serialize to string
	size_t len = 0;
	char *json = yyjson_mut_write(doc, 0, &len);
	if (!json) {
		throw InternalException("Failed to serialize the VARIANT stats to JSON");
	}
	string out(json, len);
	Printer::Print(out);
	free(json);
	yyjson_mut_doc_free(doc);
	return "'" + out + "'";
}

static idx_t BuildField(
	DuckLakeColumnVariantStats &variant,
	yyjson_val *node,
	LogicalType &out_type
) {
	auto type_val = yyjson_obj_get(node, "type");
	string type_str = yyjson_get_str(type_val);

	idx_t index = variant.field_arena.size();
	variant.field_arena.emplace(index);

	LogicalType result_type;

	if (type_str == "STRUCT") {
		// build object members
		auto children = yyjson_obj_get(node, "children");
		child_list_t<LogicalType> struct_children;

		yyjson_obj_iter iter;
		yyjson_obj_iter_init(children, &iter);
		yyjson_val *key, *val;
		while ((key = yyjson_obj_iter_next(&iter))) {
			val = yyjson_obj_iter_get_val(key);

			string child_name = yyjson_get_str(key);
			LogicalType child_type;
			auto field_index = BuildField(variant, val, child_type);
			variant.field_arena[index].children.emplace(child_name, field_index);
			struct_children.emplace_back(child_name, child_type);
		}
		result_type = LogicalType::STRUCT(std::move(struct_children));
	}
	else if (type_str == "LIST") {
		// list -> children.element
		auto children = yyjson_obj_get(node, "children");
		auto elem = yyjson_obj_get(children, "element");
		LogicalType child_type;
		auto field_index = BuildField(variant, elem, child_type);
		variant.field_arena[index].children.emplace("element", field_index);
		result_type = LogicalType::LIST(std::move(child_type));
	}
	else {
		// leaf scalar => allocate stats and bind
		result_type = TransformStringToLogicalType(type_str);

		idx_t stats_idx = variant.stats_arena.emplace(result_type);
		variant.field_arena[index].stats_index = stats_idx;

		auto &stats = variant.stats_arena[stats_idx];

		// fill stats if present
		auto null_count = yyjson_obj_get(node, "null_count");
		stats.null_count = yyjson_get_uint(null_count);
		stats.has_null_count = true;

		auto min_val = yyjson_obj_get(node, "min");
		if (min_val) {
			stats.min = yyjson_get_str(min_val);
			stats.has_min = true;
		}
		auto max_val = yyjson_obj_get(node, "max");
		if (max_val) {
			stats.max = yyjson_get_str(max_val);
			stats.has_max = true;
		}

		auto num_values = yyjson_obj_get(node, "num_values");
		if (num_values) {
			stats.has_num_values = true;
			stats.num_values = yyjson_get_uint(num_values);
		}

		auto contains_nan = yyjson_obj_get(node, "contains_nan");
		if (contains_nan) {
			stats.has_contains_nan = true;
			stats.contains_nan = yyjson_get_bool(contains_nan);
		}

		auto extra_stats = yyjson_obj_get(node, "extra_stats");
		if (extra_stats) {
			auto extra_stats_str = yyjson_get_str(extra_stats);
			stats.extra_stats->Deserialize(extra_stats_str);
		}

		auto column_size_bytes = yyjson_obj_get(node, "column_size_bytes");
		if (column_size_bytes) {
			stats.column_size_bytes = yyjson_get_uint(column_size_bytes);
		}

		auto any_valid = yyjson_obj_get(node, "any_valid");
		if (any_valid) {
			stats.any_valid = yyjson_get_bool(any_valid);
		}
	}

	out_type = result_type;
	return index;
}

static LogicalType DeserializeShredded(DuckLakeColumnVariantStats &variant, yyjson_val *val) {
	LogicalType shredded_type;
	BuildField(variant, val, shredded_type);
	return shredded_type;
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
	auto state_val = yyjson_obj_get(root, "state");
	auto state = EnumUtil::FromString<VariantStatsShreddingState>(yyjson_get_str(state_val));
	if (state == VariantStatsShreddingState::SHREDDED) {
		shredded_type = DeserializeShredded(*this, root);
	}
	yyjson_doc_free(doc);
}

} // namespace duckdb
