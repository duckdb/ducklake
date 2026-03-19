//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_field_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "common/index.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
struct AlterTableInfo;
struct SetPartitionedByInfo;
class DuckLakeTransaction;
class ColumnDefinition;
class ColumnList;

struct DuckLakeColumnData {
	FieldIndex id;
	Value initial_default;
	unique_ptr<ParsedExpression> default_value;
	DuckLakeColumnData Copy() const {
		DuckLakeColumnData copy;
		copy.id = id;
		copy.initial_default = initial_default;

		if (default_value) {
			copy.default_value = default_value->Copy();
		}
		return copy;
	}
};

class DuckLakeFieldId {
public:
	DuckLakeFieldId(DuckLakeColumnData column_data, string name, LogicalType type);
	DuckLakeFieldId(DuckLakeColumnData column_data, string name, LogicalType type,
	                vector<unique_ptr<DuckLakeFieldId>> children);

	FieldIndex GetFieldIndex() const {
		return column_data.id;
	}
	const DuckLakeColumnData &GetColumnData() const {
		return column_data;
	}
	DuckLakeColumnData &GetColumnDataMutable() {
		return column_data;
	}
	const string &Name() const {
		return name;
	}
	const LogicalType &Type() const {
		return type;
	}
	bool HasChildren() const {
		return !children.empty();
	}
	const vector<unique_ptr<DuckLakeFieldId>> &Children() const {
		return children;
	}
	const DuckLakeFieldId &GetChildByIndex(idx_t index) const;
	optional_ptr<const DuckLakeFieldId> GetChildByName(const string &name) const;
	unique_ptr<DuckLakeFieldId> Copy() const;
	unique_ptr<ParsedExpression> GetDefault() const;

	static unique_ptr<DuckLakeFieldId> FieldIdFromColumn(const ColumnDefinition &col, idx_t &column_id,
	                                                     bool add_column = false);
	static unique_ptr<DuckLakeFieldId> FieldIdFromType(const string &name, const LogicalType &type,
	                                                   optional_ptr<const ParsedExpression> default_expr,
	                                                   idx_t &column_id, bool add_column);
	static unique_ptr<DuckLakeFieldId> Rename(const DuckLakeFieldId &field_id, const string &new_name);
	static unique_ptr<DuckLakeFieldId> SetDefault(const DuckLakeFieldId &field_id,
	                                              optional_ptr<const ParsedExpression> default_expr);
	unique_ptr<DuckLakeFieldId> AddField(const vector<string> &column_path, unique_ptr<DuckLakeFieldId> new_child,
	                                     idx_t depth = 1) const;
	unique_ptr<DuckLakeFieldId> RemoveField(const vector<string> &column_path, idx_t depth = 1) const;
	unique_ptr<DuckLakeFieldId> RenameField(const vector<string> &column_path, const string &new_name,
	                                        idx_t depth = 1) const;

private:
	DuckLakeColumnData column_data;
	string name;
	LogicalType type;
	vector<unique_ptr<DuckLakeFieldId>> children;
	case_insensitive_map_t<idx_t> child_map;
	optional_ptr<DuckLakeFieldId> parent;
};

class DuckLakeFieldData {
public:
	DuckLakeFieldData() = default;
	// disable copy constructors
	DuckLakeFieldData(const DuckLakeFieldData &other) = delete;
	DuckLakeFieldData &operator=(const DuckLakeFieldData &) = delete;
	//! enable move constructors
	DuckLakeFieldData(DuckLakeFieldData &&other) noexcept = default;
	DuckLakeFieldData &operator=(DuckLakeFieldData &&) noexcept = default;

public:
	void Add(unique_ptr<DuckLakeFieldId> field_info);
	const DuckLakeFieldId &GetByRootIndex(PhysicalIndex id) const;
	optional_ptr<const DuckLakeFieldId> GetByFieldIndex(FieldIndex id) const;
	optional_ptr<const DuckLakeFieldId> GetByNames(PhysicalIndex id, const vector<string> &column_names,
	                                               optional_ptr<optional_idx> name_offset = nullptr) const;
	idx_t GetColumnCount() {
		return field_ids.size();
	}
	const vector<unique_ptr<DuckLakeFieldId>> &GetFieldIds() const {
		return field_ids;
	}

	static shared_ptr<DuckLakeFieldData> FromColumns(const ColumnList &columns);
	static shared_ptr<DuckLakeFieldData> FromColumns(const ColumnList &columns, idx_t &column_id);
	static shared_ptr<DuckLakeFieldData> RenameColumn(const DuckLakeFieldData &field_data, FieldIndex rename_index,
	                                                  const string &new_name);
	static shared_ptr<DuckLakeFieldData> DropColumn(const DuckLakeFieldData &field_data, FieldIndex drop_index);
	static shared_ptr<DuckLakeFieldData> AddColumn(const DuckLakeFieldData &field_data, const ColumnDefinition &new_col,
	                                               idx_t &next_column_id);
	static shared_ptr<DuckLakeFieldData> SetDefault(const DuckLakeFieldData &field_data, FieldIndex field_index,
	                                                const ColumnDefinition &new_col, bool add_column);

private:
	vector<unique_ptr<DuckLakeFieldId>> field_ids;
	map<FieldIndex, const_reference<DuckLakeFieldId>> field_references;
};

} // namespace duckdb
