#include "common/ducklake_name_map.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

hash_t DuckLakeNameMapEntry::GetHash() const {
	hash_t result = Hash(source_name.c_str(), source_name.size());
	for (auto &entry : child_entries) {
		result ^= entry->GetHash();
	}
	return result;
}

bool DuckLakeNameMapEntry::ListIsCompatible(const vector<unique_ptr<DuckLakeNameMapEntry>> &left,
                                            const vector<unique_ptr<DuckLakeNameMapEntry>> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	// names must be identical in both sets
	unordered_map<string, idx_t> right_map;
	for (idx_t right_idx = 0; right_idx < right.size(); ++right_idx) {
		right_map.emplace(right[right_idx]->source_name, right_idx);
	}
	for (auto &left_entry : left) {
		auto entry = right_map.find(left_entry->source_name);
		if (entry == right_map.end()) {
			return false;
		}
		auto &right_entry = right[entry->second];
		if (!left_entry->IsCompatibleWith(*right_entry)) {
			return false;
		}
		right_map.erase(left_entry->source_name);
	}
	return right_map.empty();
}

bool DuckLakeNameMapEntry::IsCompatibleWith(const DuckLakeNameMapEntry &other) const {
	if (source_name != other.source_name) {
		return false;
	}
	if (target_field_id != other.target_field_id) {
		return false;
	}
	if (hive_partition != other.hive_partition) {
		return false;
	}
	return ListIsCompatible(child_entries, other.child_entries);
}

hash_t DuckLakeNameMap::GetHash() const {
	hash_t result = Hash(table_id.index);
	for (auto &entry : column_maps) {
		result ^= entry->GetHash();
	}
	return result;
}

bool DuckLakeNameMap::IsCompatibleWith(const DuckLakeNameMap &other) const {
	if (table_id.index != other.table_id.index) {
		return false;
	}
	return DuckLakeNameMapEntry::ListIsCompatible(column_maps, other.column_maps);
}

MappingIndex DuckLakeNameMapSet::TryGetCompatibleNameMap(const DuckLakeNameMap &name_map) {
	// try to find a compatible set
	auto entry = name_map_compatibility_set.find(name_map);
	if (entry != name_map_compatibility_set.end()) {
		return entry->get().id;
	}
	return MappingIndex();
}

void DuckLakeNameMapSet::Add(unique_ptr<DuckLakeNameMap> mapping) {
	auto mapping_id = mapping->id;
	auto shared_mapping = shared_ptr<DuckLakeNameMap>(std::move(mapping));
	auto &ref = *shared_mapping;
	name_maps.emplace(mapping_id, std::move(shared_mapping));
	name_map_compatibility_set.insert(ref);
}

void DuckLakeNameMapSet::Remove(MappingIndex mapping_id) {
	auto entry = name_maps.find(mapping_id);
	if (entry == name_maps.end()) {
		return;
	}
	auto compatibility_entry = name_map_compatibility_set.find(*entry->second);
	if (compatibility_entry != name_map_compatibility_set.end() && compatibility_entry->get().id == mapping_id) {
		name_map_compatibility_set.erase(compatibility_entry);
	}
	name_maps.erase(entry);
}

vector<unique_ptr<DuckLakeNameMapEntry>>
DuckLakeNameMap::CreatePositionalMapping(const vector<string> &source_names,
                                         const vector<FieldIndex> &target_field_ids) {
	vector<unique_ptr<DuckLakeNameMapEntry>> result;
	auto count = MinValue(source_names.size(), target_field_ids.size());
	for (idx_t i = 0; i < count; i++) {
		auto entry = make_uniq<DuckLakeNameMapEntry>();
		entry->source_name = source_names[i];
		entry->target_field_id = target_field_ids[i];
		result.push_back(std::move(entry));
	}
	return result;
}

unique_ptr<DuckLakeNameMap> DuckLakeNameMap::FromColumnMapping(DuckLakeColumnMappingInfo column_mapping) {
	if (column_mapping.map_type != "map_by_name") {
		throw InvalidInputException("Unsupported column mapping type \"%s\"", column_mapping.map_type);
	}
	auto result = make_uniq<DuckLakeNameMap>();
	result->id = column_mapping.mapping_id;
	result->table_id = column_mapping.table_id;

	unordered_map<idx_t, reference<DuckLakeNameMapEntry>> column_id_map;
	for (auto &col : column_mapping.map_columns) {
		auto map_entry = make_uniq<DuckLakeNameMapEntry>();
		map_entry->source_name = std::move(col.source_name);
		map_entry->target_field_id = col.target_field_id;
		map_entry->hive_partition = col.hive_partition;
		column_id_map.emplace(col.column_id, *map_entry);
		if (!col.parent_column.IsValid()) {
			result->column_maps.push_back(std::move(map_entry));
			continue;
		}
		auto parent_entry = column_id_map.find(col.parent_column.GetIndex());
		if (parent_entry == column_id_map.end()) {
			throw InvalidInputException("Parent column %d not found when converting name map with id %d",
			                            col.parent_column.GetIndex(), column_mapping.mapping_id.index);
		}
		parent_entry->second.get().child_entries.push_back(std::move(map_entry));
	}
	return result;
}

static void FlattenNameMapEntry(const DuckLakeNameMapEntry &entry, optional_idx parent_idx,
                                vector<DuckLakeNameMapColumnInfo> &result) {
	auto column_id = result.size();
	DuckLakeNameMapColumnInfo column_info;
	column_info.column_id = column_id;
	column_info.source_name = entry.source_name;
	column_info.target_field_id = entry.target_field_id;
	column_info.hive_partition = entry.hive_partition;
	column_info.parent_column = parent_idx;
	result.push_back(std::move(column_info));
	for (auto &child : entry.child_entries) {
		FlattenNameMapEntry(*child, column_id, result);
	}
}

vector<DuckLakeNameMapColumnInfo> DuckLakeNameMap::FlattenColumns() const {
	vector<DuckLakeNameMapColumnInfo> result;
	for (auto &column : column_maps) {
		FlattenNameMapEntry(*column, optional_idx(), result);
	}
	return result;
}

} // namespace duckdb
