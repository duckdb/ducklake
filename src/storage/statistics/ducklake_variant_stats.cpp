#include "storage/ducklake_stats.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/common/type_visitor.hpp"

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

//! Copied from VariantColumnReader::TypedValueLayoutToType
static bool TypedValueLayoutToType(const LogicalType &typed_value, LogicalType &output) {
	if (!typed_value.IsNested()) {
		output = typed_value;
		return true;
	}
	auto type_id = typed_value.id();
	if (type_id == LogicalTypeId::STRUCT) {
		//! OBJECT (...)
		auto &object_fields = StructType::GetChildTypes(typed_value);
		child_list_t<LogicalType> children;
		for (auto &object_field : object_fields) {
			auto &name = object_field.first;
			auto &field = object_field.second;
			//! <name>: {
			//! 	value: BLOB,
			//! 	typed_value: <type>
			//! }
			auto &field_children = StructType::GetChildTypes(field);
			idx_t index = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < field_children.size(); i++) {
				if (field_children[i].first == "typed_value") {
					index = i;
					break;
				}
			}
			if (index == DConstants::INVALID_INDEX) {
				//! FIXME: we might be able to just omit this field from the OBJECT, instead of flat-out failing the
				//! conversion No 'typed_value' field, so we can't assign a structured type to this field at all
				return false;
			}
			LogicalType child_type;
			if (!TypedValueLayoutToType(field_children[index].second, child_type)) {
				return false;
			}
			children.emplace_back(name, child_type);
		}
		output = LogicalType::STRUCT(std::move(children));
		return true;
	}
	if (type_id == LogicalTypeId::LIST) {
		//! ARRAY
		auto &element = ListType::GetChildType(typed_value);
		//! element: {
		//! 	value: BLOB,
		//! 	typed_value: <type>
		//! }
		auto &element_children = StructType::GetChildTypes(element);
		idx_t index = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < element_children.size(); i++) {
			if (element_children[i].first == "typed_value") {
				index = i;
				break;
			}
		}
		if (index == DConstants::INVALID_INDEX) {
			//! This *might* be allowed by the spec, it's hard to reason about..
			return false;
		}
		LogicalType child_type;
		if (!TypedValueLayoutToType(element_children[index].second, child_type)) {
			return false;
		}
		output = LogicalType::LIST(child_type);
		return true;
	}
	throw InvalidInputException("VARIANT typed value has to be a primitive/struct/list, not %s",
	                            typed_value.ToString());
}

static bool ConvertUnshreddedStats(BaseStatistics &result, BaseStatistics &input) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::UINTEGER);

	D_ASSERT(input.GetType().id() == LogicalTypeId::BLOB);
	result.CopyValidity(input);

	auto min = StringStats::Min(input);
	auto max = StringStats::Max(input);

	if (!result.CanHaveNoNull()) {
		return true;
	}

	bool is_null = min.empty() && max.empty();
	if (!is_null) {
		is_null = min == "00" && max == "00";
	}
	if (is_null) {
		//! All non-shredded values are NULL or VARIANT_NULL, set the stats to indicate this
		NumericStats::SetMin<uint32_t>(result, 0);
		NumericStats::SetMax<uint32_t>(result, 0);
		result.SetHasNoNull();
	}
	return true;
}

bool DuckLakeColumnVariantStats::ConvertStats(idx_t field_index, BaseStatistics &result) const {
	auto &untyped_value_index_stats = StructStats::GetChildStats(result, 0);
	auto &typed_value_stats = StructStats::GetChildStats(result, 1);

	auto &field = field_arena[field_index];
	D_ASSERT(!field.children.empty());

	auto value_it = field.children.find("value");
	auto typed_value_it = field.children.find("typed_value");

	if (value_it == field.children.end() || typed_value_it == field.children.end()) {
		return false;
	}
	auto &value = field_arena[value_it->second];
	auto &typed_value = field_arena[typed_value_it->second];

	auto &type = typed_value_stats.GetType();
	auto type_id = type.id();
	if (type_id == LogicalTypeId::STRUCT) {
		typed_value_stats.SetHasNoNullFast();
		typed_value_stats.SetHasNullFast();

		auto &child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &child_stats = StructStats::GetChildStats(typed_value_stats, i);
			auto child_it = typed_value.children.find(child_types[i].first);
			if (child_it == typed_value.children.end()) {
				throw InternalException("STRUCT is missing its child?");
			}
			if (!ConvertStats(child_it->second, child_stats)) {
				return false;
			}
		}
	} else if (type_id == LogicalTypeId::LIST) {
		typed_value_stats.SetHasNoNullFast();
		typed_value_stats.SetHasNullFast();

		auto &element = ListStats::GetChildStats(typed_value_stats);
		auto element_it = typed_value.children.find("element");
		if (element_it == typed_value.children.end()) {
			throw InternalException("LIST without an 'element' field?");
		}
		if (!ConvertStats(element_it->second, element)) {
			return false;
		}
	} else {
		if (!typed_value.stats_index.IsValid()) {
			//! No stats for the leaf
			return false;
		}
		auto &typed = stats_arena[typed_value.stats_index.GetIndex()];
		auto typed_stats = typed.ToStats();
		StructStats::SetChildStats(result, 1, std::move(typed_stats));
	}

	//! Initialize stats as UNKNOWN, in case we can't convert them
	untyped_value_index_stats = BaseStatistics::CreateEmpty(untyped_value_index_stats.GetType());
	unique_ptr<BaseStatistics> value_stats;
	do {
		if (!value.stats_index.IsValid()) {
			//! No stats for the leaf 'value', can't convert
			break;
		}
		auto &untyped = stats_arena[value.stats_index.GetIndex()];
		value_stats = untyped.ToStats();
	} while (false);

	if (value_stats) {
		ConvertUnshreddedStats(untyped_value_index_stats, *value_stats);
	} else {
		untyped_value_index_stats.SetHasNoNullFast();
		untyped_value_index_stats.SetHasNullFast();
	}
	return true;
}

unique_ptr<BaseStatistics> DuckLakeColumnVariantStats::ToStats() const {
	if (shredding_state != VariantStatsShreddingState::SHREDDED) {
		return nullptr;
	}
	auto &child_types = StructType::GetChildTypes(shredded_type);
	idx_t index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < child_types.size(); i++) {
		if (child_types[i].first == "typed_value") {
			index = i;
			break;
		}
	}
	if (index == DConstants::INVALID_INDEX) {
		throw InternalException("State says SHREDDED but there is no 'typed_value' ???");
	}

	//! STRUCT(metadata, value, typed_value) -> STRUCT(untyped_value_index, typed_value)
	LogicalType logical_type;
	if (!TypedValueLayoutToType(child_types[index].second, logical_type)) {
		return nullptr;
	}
	auto shredding_type = TypeVisitor::VisitReplace(logical_type, [](const LogicalType &type) {
		return LogicalType::STRUCT({{"untyped_value_index", LogicalType::UINTEGER}, {"typed_value", type}});
	});

	auto variant_stats = VariantStats::CreateShredded(shredding_type);

	//! Take the root stats
	auto &shredded_stats = VariantStats::GetShreddedStats(variant_stats);
	if (!ConvertStats(0, shredded_stats)) {
		return nullptr;
	}

	variant_stats.SetHasNull();
	variant_stats.SetHasNoNull();
	return variant_stats.ToUnique();
}

//! Logic copied from VariantStats::MergeShredding
static bool MergeShredding(DuckLakeColumnVariantStats &a_stats, const LogicalType &a_type, idx_t a_field_index,
                           const DuckLakeColumnVariantStats &b_stats, const LogicalType &b_type, idx_t b_field_index,
                           LogicalType &out_type) {
	auto a_type_id = a_type.id();
	auto b_type_id = b_type.id();

	if (a_type_id != b_type_id) {
		return false;
	}

	auto &a_field = a_stats.field_arena[a_field_index];
	auto &b_field = b_stats.field_arena[b_field_index];
	if (a_type_id == LogicalTypeId::LIST) {
		auto &a_child_type = ListType::GetChildType(a_type);
		auto &b_child_type = ListType::GetChildType(b_type);

		D_ASSERT(a_field.children.size() == 1 && b_field.children.size() == 1);
		auto a_element = a_field.children.begin();
		auto b_element = b_field.children.begin();
		D_ASSERT(a_element->first == "element");
		D_ASSERT(b_element->first == "element");

		LogicalType child_type;
		if (!MergeShredding(a_stats, a_child_type, a_element->second, b_stats, b_child_type, b_element->second,
		                    child_type)) {
			return false;
		}
		out_type = LogicalType::LIST(child_type);
		return true;
	} else if (a_type_id == LogicalTypeId::STRUCT) {
		auto &a_child_types = StructType::GetChildTypes(a_type);
		auto &b_child_types = StructType::GetChildTypes(b_type);

		//! Map field name to index, for 'b'
		case_insensitive_map_t<idx_t> key_to_index;
		for (idx_t i = 0; i < b_child_types.size(); i++) {
			auto &b_child_type = b_child_types[i];
			key_to_index.emplace(b_child_type.first, i);
		}

		//! Attempt to merge all overlapping fields, only keep the fields that were able to be merged
		child_list_t<LogicalType> new_children;
		for (idx_t i = 0; i < a_child_types.size(); i++) {
			auto &a_child_type = a_child_types[i];
			auto other_it = key_to_index.find(a_child_type.first);
			if (other_it == key_to_index.end()) {
				//! Delete the child from the result
				a_field.children.erase(a_child_type.first);
				continue;
			}
			auto &b_child_type = b_child_types[other_it->second];
			if (b_child_type.second.id() != a_child_type.second.id()) {
				//! Delete the child from the result
				a_field.children.erase(a_child_type.first);
				continue;
			}

			auto a_child = a_field.children.find(a_child_type.first);
			auto b_child = b_field.children.find(a_child_type.first);

			LogicalType child_type;
			if (!MergeShredding(a_stats, a_child_type.second, a_child->second, b_stats, b_child_type.second,
			                    b_child->second, child_type)) {
				//! Delete the child from the result
				a_field.children.erase(a_child_type.first);
				continue;
			}
			new_children.emplace_back(a_child_type.first, child_type);
		}
		if (new_children.empty()) {
			//! No fields remaining, demote to unshredded
			return false;
		}

		//! Create new stats out of the remaining fields
		out_type = LogicalType::STRUCT(std::move(new_children));
		return true;
	} else {
		if (!a_field.stats_index.IsValid()) {
			return false;
		}
		if (!b_field.stats_index.IsValid()) {
			return false;
		}
		auto &a_stats_item = a_stats.stats_arena[a_field.stats_index.GetIndex()];
		auto &b_stats_item = b_stats.stats_arena[b_field.stats_index.GetIndex()];
		a_stats_item.MergeStats(b_stats_item);
		out_type = a_type;
		return true;
	}
}

void DuckLakeColumnVariantStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &new_variant_stats = new_stats.Cast<DuckLakeColumnVariantStats>();

	auto other_shredding_state = new_variant_stats.shredding_state;
	switch (shredding_state) {
	case VariantStatsShreddingState::INCONSISTENT: {
		//! INCONSISTENT + ANY -> INCONSISTENT
		return;
	}
	case VariantStatsShreddingState::UNINITIALIZED: {
		switch (other_shredding_state) {
		case VariantStatsShreddingState::SHREDDED:
			shredded_type = new_variant_stats.shredded_type;
			field_arena = new_variant_stats.field_arena;
			stats_arena = new_variant_stats.stats_arena;
			break;
		default:
			break;
		}
		//! UNINITIALIZED + ANY -> ANY
		shredding_state = other_shredding_state;
		break;
	}
	case VariantStatsShreddingState::NOT_SHREDDED: {
		if (other_shredding_state == VariantStatsShreddingState::NOT_SHREDDED) {
			return;
		}
		//! NOT_SHREDDED + !NOT_SHREDDED -> INCONSISTENT
		shredding_state = VariantStatsShreddingState::INCONSISTENT;
		shredded_type = LogicalType::INVALID;
		break;
	}
	case VariantStatsShreddingState::SHREDDED: {
		switch (other_shredding_state) {
		case VariantStatsShreddingState::SHREDDED: {
			LogicalType merged_shredded_type;
			if (!MergeShredding(*this, shredded_type, 0, new_variant_stats, new_variant_stats.shredded_type, 0,
			                    merged_shredded_type)) {
				//! SHREDDED(T1) + SHREDDED(T2) -> INCONSISTENT
				shredding_state = VariantStatsShreddingState::INCONSISTENT;
				shredded_type = LogicalType::INVALID;
				break;
			}
			shredded_type = merged_shredded_type;
			break;
		}
		default:
			//! SHREDDED + !SHREDDED -> INCONSISTENT
			shredding_state = VariantStatsShreddingState::INCONSISTENT;
			shredded_type = LogicalType::INVALID;
			break;
		}
		break;
	}
	}
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

static void SerializeShreddedChildren(const DuckLakeColumnVariantStats &variant, yyjson_mut_doc *doc,
                                      yyjson_mut_val *obj, idx_t parent_field, const LogicalType &type) {
	auto &stats_arena = variant.stats_arena;
	auto &field_arena = variant.field_arena;
	auto &parent = field_arena[parent_field];

	auto serialize_child = [&](idx_t field_index, yyjson_mut_val *container, const string &name,
	                           const LogicalType &child_type) {
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
	free(json);
	yyjson_mut_doc_free(doc);
	return "'" + out + "'";
}

static idx_t BuildField(DuckLakeColumnVariantStats &variant, yyjson_val *node, LogicalType &out_type) {
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
	} else if (type_str == "LIST") {
		// list -> children.element
		auto children = yyjson_obj_get(node, "children");
		auto elem = yyjson_obj_get(children, "element");
		LogicalType child_type;
		auto field_index = BuildField(variant, elem, child_type);
		variant.field_arena[index].children.emplace("element", field_index);
		result_type = LogicalType::LIST(std::move(child_type));
	} else {
		// leaf scalar => allocate stats and bind
		result_type = UnboundType::TryParseAndDefaultBind(type_str);

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
		shredding_state = state;
	}
	yyjson_doc_free(doc);
}

} // namespace duckdb
