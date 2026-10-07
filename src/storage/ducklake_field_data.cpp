#include "storage/ducklake_field_data.hpp"
#include "common/ducklake_util.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

void DuckLakeFieldData::Add(unique_ptr<DuckLakeFieldId> field_info) {
	// add field references' to the map
	vector<const_reference<DuckLakeFieldId>> active_fields {*field_info};
	for (idx_t i = 0; i < active_fields.size(); i++) {
		auto &current_field = active_fields[i].get();
		for (auto &child_field : current_field.Children()) {
			active_fields.push_back(const_reference<DuckLakeFieldId>(*child_field));
		}
		field_references.insert(
		    make_pair(current_field.GetFieldIndex(), const_reference<DuckLakeFieldId>(current_field)));
	}

	field_ids.push_back(std::move(field_info));
}

DuckLakeFieldId::DuckLakeFieldId(DuckLakeColumnData column_data_p, string name_p, LogicalType type_p)
    : column_data(std::move(column_data_p)), name(std::move(name_p)), type(std::move(type_p)) {
}

DuckLakeFieldId::DuckLakeFieldId(DuckLakeColumnData column_data_p, string name_p, LogicalType type_p,
                                 vector<unique_ptr<DuckLakeFieldId>> children_p)
    : column_data(std::move(column_data_p)), name(std::move(name_p)), type(std::move(type_p)),
      children(std::move(children_p)) {
	for (idx_t child_idx = 0; child_idx < children.size(); ++child_idx) {
		auto &child = children[child_idx];
		auto entry = child_map.find(child->name);
		if (entry != child_map.end()) {
			throw InvalidInputException("Duplicate child name \"%s\" found in column \"%s\"", child->name, name);
		}
		child->parent = this;
		child_map.insert(make_pair(child->name, child_idx));
	}
}

static unique_ptr<ParsedExpression> ExtractDefaultExpression(optional_ptr<const ParsedExpression> default_expr,
                                                             const LogicalType &type) {
	if (!default_expr) {
		return ConstantExpression::FromValue(Value(type));
	}
	if (default_expr->HasSubquery()) {
		throw NotImplementedException("Expressions with subqueries are not yet supported as default expressions");
	}
	if (default_expr->IsWindow()) {
		throw NotImplementedException("Expressions with window functions are not yet supported as default expressions");
	}
	return default_expr->Copy();
}

static Value ExtractInitialValue(optional_ptr<const ParsedExpression> initial_expr, const LogicalType &type,
                                 bool add_column) {
	if (!initial_expr) {
		return Value(type);
	}
	Value literal_value;
	if (!DuckLakeUtil::TryGetLiteralValue(*initial_expr, literal_value)) {
		if (!add_column) {
			return Value(type);
		}
		throw NotImplementedException("We cannot add a column with a non-literal default value. Add the column and "
		                              "then explicitly set the default for new values using \"ALTER ... SET DEFAULT\"");
	}
	return literal_value.DefaultCastAs(type);
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::FieldIdFromType(const string &name, const LogicalType &type,
                                                             optional_ptr<const ParsedExpression> default_expr,
                                                             idx_t &column_id, bool add_column) {
	DuckLakeColumnData column_data;
	column_data.id = FieldIndex(column_id++);
	vector<unique_ptr<DuckLakeFieldId>> field_children;
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::MAP:
		if (default_expr) {
			auto type_id = type.id() == LogicalTypeId::ARRAY ? LogicalTypeId::LIST : type.id();
			throw NotImplementedException("Default value for %s type not supported", EnumUtil::ToString(type_id));
		}
		for (auto &child : LogicalType::GetNamedChildTypes(type)) {
			field_children.push_back(
			    FieldIdFromType(child.first.GetIdentifierName(), child.second, nullptr, column_id, add_column));
		}
		break;
	default:
		break;
	}
	column_data.initial_default = ExtractInitialValue(default_expr, type, add_column);
	if (default_expr) {
		column_data.default_value = default_expr->Copy();
	}

	return make_uniq<DuckLakeFieldId>(std::move(column_data), name, type, std::move(field_children));
}

unique_ptr<ParsedExpression> DuckLakeFieldId::GetDefault() const {
	if (column_data.default_value) {
		return column_data.default_value->Copy();
	}
	return nullptr;
}

unique_ptr<ParsedExpression> DuckLakeFieldId::GetInitialDefault() const {
	if (column_data.initial_default.IsNull()) {
		return ConstantExpression::FromValue(Value(type));
	}
	return ConstantExpression::FromValue(column_data.initial_default);
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::FieldIdFromColumn(const ColumnDefinition &col, idx_t &column_id,
                                                               bool add_column) {
	auto default_val = col.HasDefaultValue() ? optional_ptr<const ParsedExpression>(col.DefaultValue()) : nullptr;
	return DuckLakeFieldId::FieldIdFromType(col.Name().GetIdentifierName(), col.Type(), default_val, column_id,
	                                        add_column);
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::FromColumns(const ColumnList &columns) {
	// generate field ids based on the column ids
	idx_t column_id = 1;
	return FromColumns(columns, column_id);
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::FromColumns(const ColumnList &columns, idx_t &column_id) {
	auto field_data = make_shared_ptr<DuckLakeFieldData>();
	for (auto &col : columns.Logical()) {
		auto field_id = DuckLakeFieldId::FieldIdFromColumn(col, column_id);
		field_data->Add(std::move(field_id));
	}
	return field_data;
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::Copy() const {
	vector<unique_ptr<DuckLakeFieldId>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	return make_uniq<DuckLakeFieldId>(column_data.Copy(), name, type, std::move(new_children));
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::Rename(const DuckLakeFieldId &field_id, const string &new_name) {
	auto result = field_id.Copy();
	result->name = new_name;
	return result;
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::SetDefault(const DuckLakeFieldId &field_id,
                                                        optional_ptr<const ParsedExpression> default_expr) {
	auto result = field_id.Copy();
	result->column_data.default_value = ExtractDefaultExpression(default_expr, field_id.Type());
	return result;
}

LogicalType GetNewNestedType(const LogicalType &type, const vector<unique_ptr<DuckLakeFieldId>> &new_children) {
	child_list_t<LogicalType> child_types;
	for (auto &child : new_children) {
		child_types.emplace_back(child->Name(), child->Type());
	}
	return LogicalType::ConstructNestedType(type, std::move(child_types));
}

template <class FUNC>
static unique_ptr<DuckLakeFieldId> ReplaceChild(const DuckLakeFieldId &field_id, const Identifier &child_name,
                                                const char *function_name, FUNC &&transform) {
	vector<unique_ptr<DuckLakeFieldId>> new_children;
	bool found = false;
	for (auto &child : field_id.Children()) {
		if (found || child->Name() != child_name) {
			new_children.push_back(child->Copy());
			continue;
		}
		found = true;
		auto new_child = transform(*child);
		if (new_child) {
			new_children.push_back(std::move(new_child));
		}
	}
	if (!found) {
		throw InternalException("DuckLakeFieldId::%s - child not found in struct path", function_name);
	}
	auto new_type = GetNewNestedType(field_id.Type(), new_children);
	return make_uniq<DuckLakeFieldId>(field_id.GetColumnData().Copy(), field_id.Name(), std::move(new_type),
	                                  std::move(new_children));
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::AddField(const vector<Identifier> &column_path,
                                                      unique_ptr<DuckLakeFieldId> new_child, idx_t depth) const {
	if (depth < column_path.size()) {
		return ReplaceChild(*this, column_path[depth], "AddField", [&](const DuckLakeFieldId &child) {
			return child.AddField(column_path, std::move(new_child), depth + 1);
		});
	}
	vector<unique_ptr<DuckLakeFieldId>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	new_children.push_back(std::move(new_child));
	auto new_type = GetNewNestedType(type, new_children);
	return make_uniq<DuckLakeFieldId>(column_data.Copy(), Name(), std::move(new_type), std::move(new_children));
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::RemoveField(const vector<Identifier> &column_path, idx_t depth) const {
	auto remove_child = [&](const DuckLakeFieldId &child) -> unique_ptr<DuckLakeFieldId> {
		if (column_path.size() == 2 && (type.id() == LogicalTypeId::MAP || type.id() == LogicalTypeId::LIST)) {
			throw CatalogException("Cannot drop field %s from column %s - it's not a struct", Identifier(child.Name()),
			                       Identifier(name));
		}
		if (depth + 1 >= column_path.size()) {
			return nullptr;
		}
		return child.RemoveField(column_path, depth + 1);
	};
	return ReplaceChild(*this, column_path[depth], "RemoveField", remove_child);
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::RenameField(const vector<Identifier> &column_path, const string &new_name,
                                                         idx_t depth) const {
	return ReplaceChild(*this, column_path[depth], "RenameField", [&](const DuckLakeFieldId &child) {
		if (depth + 1 >= column_path.size()) {
			return Rename(child, new_name);
		}
		return child.RenameField(column_path, new_name, depth + 1);
	});
}

template <class FUNC>
static shared_ptr<DuckLakeFieldData> RebuildRootFields(const DuckLakeFieldData &field_data, FieldIndex field_index,
                                                       FUNC &&transform) {
	auto result = make_shared_ptr<DuckLakeFieldData>();
	for (auto &existing_id : field_data.GetFieldIds()) {
		if (existing_id->GetFieldIndex() != field_index) {
			result->Add(existing_id->Copy());
			continue;
		}
		auto field_id = transform(*existing_id);
		if (field_id) {
			result->Add(std::move(field_id));
		}
	}
	return result;
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::RenameColumn(const DuckLakeFieldData &field_data,
                                                              FieldIndex rename_index, const string &new_name) {
	return RebuildRootFields(field_data, rename_index, [&](const DuckLakeFieldId &field_id) {
		return DuckLakeFieldId::Rename(field_id, new_name);
	});
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::AddColumn(const DuckLakeFieldData &field_data,
                                                           const ColumnDefinition &new_col, idx_t &next_column_id) {
	auto result = make_shared_ptr<DuckLakeFieldData>();
	for (auto &existing_id : field_data.field_ids) {
		result->Add(existing_id->Copy());
	}
	auto field_id = DuckLakeFieldId::FieldIdFromColumn(new_col, next_column_id, true);
	result->Add(std::move(field_id));
	return result;
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::DropColumn(const DuckLakeFieldData &field_data,
                                                            FieldIndex drop_index) {
	return RebuildRootFields(field_data, drop_index,
	                         [](const DuckLakeFieldId &) { return unique_ptr<DuckLakeFieldId>(); });
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::ReplaceRootField(const DuckLakeFieldData &field_data,
                                                                  PhysicalIndex root_index,
                                                                  unique_ptr<DuckLakeFieldId> new_field) {
	auto field_index = field_data.GetByRootIndex(root_index).GetFieldIndex();
	return RebuildRootFields(field_data, field_index, [&](const DuckLakeFieldId &) { return std::move(new_field); });
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::SetDefault(const DuckLakeFieldData &field_data, FieldIndex field_index,
                                                            const ColumnDefinition &new_col, bool add_column) {
	auto new_default =
	    new_col.HasDefaultValue() ? optional_ptr<const ParsedExpression>(new_col.DefaultValue()) : nullptr;
	if (new_default && new_default->GetExpressionType() != ExpressionType::VALUE_CONSTANT && add_column) {
		throw NotImplementedException("We cannot add a column with a non-literal default value. Add the column and "
		                              "then explicitly set the default for new values using \"ALTER ... SET DEFAULT\"");
	}
	return RebuildRootFields(field_data, field_index, [&](const DuckLakeFieldId &field_id) {
		return DuckLakeFieldId::SetDefault(field_id, new_default);
	});
}

const DuckLakeFieldId &DuckLakeFieldData::GetByRootIndex(PhysicalIndex id) const {
	return *field_ids[id.index];
}

optional_ptr<const DuckLakeFieldId> DuckLakeFieldData::GetByFieldIndex(FieldIndex id) const {
	auto entry = field_references.find(id);
	if (entry == field_references.end()) {
		return nullptr;
	}
	return entry->second.get();
}

optional_ptr<const DuckLakeFieldId> DuckLakeFieldId::GetChildByName(const string &child_name) const {
	auto entry = child_map.find(child_name);
	if (entry == child_map.end()) {
		return nullptr;
	}
	return *children[entry->second];
}

const DuckLakeFieldId &DuckLakeFieldId::GetChildByIndex(idx_t index) const {
	return *children[index];
}

optional_ptr<const DuckLakeFieldId> DuckLakeFieldData::GetByNames(PhysicalIndex id,
                                                                  const vector<Identifier> &column_names,
                                                                  optional_ptr<optional_idx> name_offset) const {
	const_reference<DuckLakeFieldId> result = GetByRootIndex(id);
	for (idx_t i = 1; i <= column_names.size(); ++i) {
		if (result.get().Type().id() == LogicalTypeId::VARIANT) {
			if (name_offset) {
				*name_offset = i;
				return result.get();
			}
			throw InvalidInputException(
			    "Column path %s points to child of variant column %s - but no name_offset is provided",
			    StringUtil::Join(IdentifiersToStrings(column_names), "."), result.get().Name());
		}
		if (i >= column_names.size()) {
			break;
		}
		auto &current = result.get();
		auto next_child = current.GetChildByName(column_names[i].GetIdentifierName());
		if (!next_child) {
			return nullptr;
		}
		result = *next_child;
	}
	return result.get();
}

} // namespace duckdb
