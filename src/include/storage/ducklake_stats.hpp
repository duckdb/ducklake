//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "common/index.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"

namespace duckdb {
class BaseStatistics;

struct DuckLakeColumnStats;

struct DuckLakeColumnExtraStats {
	virtual ~DuckLakeColumnExtraStats() = default;

	virtual void Merge(const DuckLakeColumnExtraStats &new_stats) = 0;
	virtual unique_ptr<DuckLakeColumnExtraStats> Copy() const = 0;

	// Convert the stats into a string representation for storage (e.g. JSON)
	virtual string Serialize() const = 0;
	// Parse the stats from a string
	virtual void Deserialize(const string &stats) = 0;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct DuckLakeColumnGeoStats final : public DuckLakeColumnExtraStats {
	DuckLakeColumnGeoStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;

	string Serialize() const override;
	void Deserialize(const string &stats) override;

public:
	double xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax;
	set<string> geo_types;
};

template <class T>
class DuckLakeVariantStatsArena {
public:
	DuckLakeVariantStatsArena() = default;

	// create a new T with arbitrary constructor args
	template <class... Args>
	idx_t emplace(Args &&...args) {
		idx_t id = static_cast<idx_t>(storage.size());
		storage.emplace_back(std::forward<Args>(args)...);
		return id;
	}

	T &operator[](idx_t i) {
		return storage[i];
	}
	const T &operator[](idx_t i) const {
		return storage[i];
	}

	size_t size() const {
		return storage.size();
	}

private:
	vector<T> storage;
};

struct DuckLakeColumnStats {
	explicit DuckLakeColumnStats(LogicalType type_p);

	// Copy constructor
	DuckLakeColumnStats(const DuckLakeColumnStats &other);
	DuckLakeColumnStats &operator=(const DuckLakeColumnStats &other);
	DuckLakeColumnStats(DuckLakeColumnStats &&other) noexcept = default;
	DuckLakeColumnStats &operator=(DuckLakeColumnStats &&other) noexcept = default;

	LogicalType type;
	string min;
	string max;
	idx_t null_count = 0;
	idx_t num_values = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_num_values = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	bool has_contains_nan = false;

	unique_ptr<DuckLakeColumnExtraStats> extra_stats;

public:
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const DuckLakeColumnStats &new_stats);

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
};

struct DuckLakeColumnStats;

struct DuckLakeColumnVariantFieldStats {
public:
	explicit DuckLakeColumnVariantFieldStats(idx_t index);

public:
	//! Index in the tree of the current node
	idx_t index;

	optional_idx stats_index;

	//! Children of this column (their index to find them in the tree)
	case_insensitive_map_t<idx_t> children;
};

struct DuckLakeColumnVariantStats final : public DuckLakeColumnExtraStats {
public:
	DuckLakeColumnVariantStats();

public:
	void Build(const LogicalType &shredded_internal_type);

public:
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;

	string Serialize() const override;
	void Deserialize(const string &stats) override;

private:
	void BuildInternal(idx_t parent, const LogicalType &shredded_internal_type);

public:
	VariantStatsShreddingState shredding_state = VariantStatsShreddingState::UNINITIALIZED;
	LogicalType shredded_type = LogicalType::INVALID;
	DuckLakeVariantStatsArena<DuckLakeColumnVariantFieldStats> field_arena;
	DuckLakeVariantStatsArena<DuckLakeColumnStats> stats_arena;
};

//! These are the global, table-wide stats
struct DuckLakeTableStats {
	idx_t record_count = 0;
	idx_t table_size_bytes = 0;
	idx_t next_row_id = 0;
	map<FieldIndex, DuckLakeColumnStats> column_stats;

	void MergeStats(FieldIndex col_id, const DuckLakeColumnStats &file_stats);
};

struct DuckLakeStats {
	map<TableIndex, unique_ptr<DuckLakeTableStats>> table_stats;
};

} // namespace duckdb
