#include "common/ducklake_name_map.hpp"

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
	auto &ref = *mapping;
	name_maps.emplace(mapping_id, std::move(mapping));
	name_map_compatibility_set.insert(ref);
}

} // namespace duckdb
