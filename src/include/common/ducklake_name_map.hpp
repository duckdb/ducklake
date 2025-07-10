//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_name_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/hash.hpp"
#include "common/index.hpp"

namespace duckdb {

struct DuckLakeNameMapEntry {
	string source_name;
	FieldIndex target_field_id;
	bool hive_partition = false;
	vector<unique_ptr<DuckLakeNameMapEntry>> child_entries;

	hash_t GetHash() const;
	bool IsCompatibleWith(const DuckLakeNameMapEntry &other) const;
	static bool ListIsCompatible(const vector<unique_ptr<DuckLakeNameMapEntry>> &left,
	                             const vector<unique_ptr<DuckLakeNameMapEntry>> &right);
};

struct DuckLakeNameMap {
	MappingIndex id;
	TableIndex table_id;
	vector<unique_ptr<DuckLakeNameMapEntry>> column_maps;

	hash_t GetHash() const;
	bool IsCompatibleWith(const DuckLakeNameMap &other) const;
};

struct NameMapHashFunction {
	uint64_t operator()(const const_reference<DuckLakeNameMap> &value) const {
		return (uint64_t)value.get().GetHash();
	}
};

struct NameMapIsCompatible {
	bool operator()(const const_reference<DuckLakeNameMap> &a, const const_reference<DuckLakeNameMap> &b) const {
		return a.get().IsCompatibleWith(b.get());
	}
};

using ducklake_name_map_compatibility_set =
    unordered_set<const_reference<DuckLakeNameMap>, NameMapHashFunction, NameMapIsCompatible>;

struct DuckLakeNameMapSet {
	map<MappingIndex, unique_ptr<DuckLakeNameMap>> name_maps;
	ducklake_name_map_compatibility_set name_map_compatibility_set;

	//! Try to find a compatible name map that already exists in the set
	MappingIndex TryGetCompatibleNameMap(const DuckLakeNameMap &name_map);
	void Add(unique_ptr<DuckLakeNameMap> name_map);
};

} // namespace duckdb
