#include "storage/ducklake_field_data.hpp"

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
		return make_uniq<ConstantExpression>(Value(type));
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
	if (initial_expr->type != ExpressionType::VALUE_CONSTANT) {
		if (!add_column) {
			return Value(type);
		}
		throw NotImplementedException("We cannot add a column with a non-literal default value. Add the column and "
		                              "then explicitly set the default for new values using \"ALTER ... SET DEFAULT\"");
	}
	auto &const_default = initial_expr->Cast<ConstantExpression>();
	return const_default.value.DefaultCastAs(type);
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::FieldIdFromType(const string &name, const LogicalType &type,
                                                             optional_ptr<const ParsedExpression> default_expr,
                                                             idx_t &column_id, bool add_column) {
	DuckLakeColumnData column_data;
	column_data.id = FieldIndex(column_id++);
	vector<unique_ptr<DuckLakeFieldId>> field_children;
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		// FIXME: check for struct pack
		if (default_expr) {
			throw NotImplementedException("Default value for STRUCT type not supported");
		}
		for (auto &entry : StructType::GetChildTypes(type)) {
			field_children.push_back(FieldIdFromType(entry.first, entry.second, nullptr, column_id, add_column));
		}
		break;
	}
	case LogicalTypeId::LIST:
		if (default_expr) {
			throw NotImplementedException("Default value for LIST type not supported");
		}
		field_children.push_back(
		    FieldIdFromType("element", ListType::GetChildType(type), nullptr, column_id, add_column));
		break;
	case LogicalTypeId::ARRAY:
		if (default_expr) {
			throw NotImplementedException("Default value for LIST type not supported");
		}
		field_children.push_back(
		    FieldIdFromType("element", ArrayType::GetChildType(type), nullptr, column_id, add_column));
		break;
	case LogicalTypeId::MAP:
		if (default_expr) {
			throw NotImplementedException("Default value for MAP type not supported");
		}
		field_children.push_back(FieldIdFromType("key", MapType::KeyType(type), nullptr, column_id, add_column));
		field_children.push_back(FieldIdFromType("value", MapType::ValueType(type), nullptr, column_id, add_column));
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

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::FieldIdFromColumn(const ColumnDefinition &col, idx_t &column_id,
                                                               bool add_column) {
	auto default_val = col.HasDefaultValue() ? optional_ptr<const ParsedExpression>(col.DefaultValue()) : nullptr;
	return DuckLakeFieldId::FieldIdFromType(col.Name(), col.Type(), default_val, column_id, add_column);
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

LogicalType GetStructType(const vector<unique_ptr<DuckLakeFieldId>> &new_children) {
	child_list_t<LogicalType> child_types;
	for (auto &child : new_children) {
		child_types.emplace_back(child->Name(), child->Type());
	}
	return LogicalType::STRUCT(std::move(child_types));
}

LogicalType GetNewNestedType(const LogicalType &type, const vector<unique_ptr<DuckLakeFieldId>> &new_children) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return LogicalType::LIST(new_children[0]->Type());
	case LogicalTypeId::STRUCT:
		return GetStructType(new_children);
	case LogicalTypeId::MAP:
		return LogicalType::MAP(new_children[0]->Type(), new_children[1]->Type());
	default:
		throw InternalException("Unsupported type for AddField");
	}
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::AddField(const vector<string> &column_path,
                                                      unique_ptr<DuckLakeFieldId> new_child, idx_t depth) const {
	vector<unique_ptr<DuckLakeFieldId>> new_children;
	if (depth >= column_path.size()) {
		// leaf - add the column at this level
		// copy over all the other columns as-is
		for (auto &child : children) {
			new_children.push_back(child->Copy());
		}
		new_children.push_back(std::move(new_child));
	} else {
		// not the leaf - find the child to add it to and recurse
		bool found = false;
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child = *children[child_idx];
			if (!found && StringUtil::CIEquals(child.Name(), column_path[depth])) {
				// found it!
				auto new_field = child.AddField(column_path, std::move(new_child), depth + 1);
				new_child.reset();
				new_children.push_back(std::move(new_field));
				found = true;
			} else {
				// this entry can be copied as-is
				new_children.push_back(child.Copy());
			}
		}
		if (!found) {
			throw InternalException("DuckLakeFieldId::AddField - child not found in struct path");
		}
	}
	LogicalType new_type = GetNewNestedType(type, new_children);
	return make_uniq<DuckLakeFieldId>(column_data.Copy(), Name(), std::move(new_type), std::move(new_children));
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::RemoveField(const vector<string> &column_path, idx_t depth) const {
	vector<unique_ptr<DuckLakeFieldId>> new_children;
	bool found = false;
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		if (StringUtil::CIEquals(child.Name(), column_path[depth])) {
			if (column_path.size() == 2 && (type.id() == LogicalTypeId::MAP || type.id() == LogicalTypeId::LIST)) {
				throw CatalogException("Cannot drop field '%s' from column '%s' - it's not a struct", child.Name(),
				                       name);
			}
			// found it!
			found = true;
			if (depth + 1 >= column_path.size()) {
				// leaf - remove the column at this level
				continue;
			} else {
				// not the leaf - find the child to drop it from and recurse
				new_children.push_back(child.RemoveField(column_path, depth + 1));
			}
		} else {
			// this entry can be copied as-is
			new_children.push_back(child.Copy());
		}
	}
	if (!found) {
		throw InternalException("DuckLakeFieldId::AddField - child not found in struct path");
	}
	LogicalType new_type = GetNewNestedType(type, new_children);
	return make_uniq<DuckLakeFieldId>(column_data.Copy(), Name(), std::move(new_type), std::move(new_children));
}

unique_ptr<DuckLakeFieldId> DuckLakeFieldId::RenameField(const vector<string> &column_path, const string &new_name,
                                                         idx_t depth) const {
	vector<unique_ptr<DuckLakeFieldId>> new_children;
	bool found = false;
	idx_t child_idx;
	for (child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		if (StringUtil::CIEquals(child.Name(), column_path[depth])) {
			// found it!
			found = true;
			if (depth + 1 >= column_path.size()) {
				// leaf - rename the column at this level
				auto copied_entry = child.Copy();
				auto renamed_entry =
				    make_uniq<DuckLakeFieldId>(copied_entry->column_data.Copy(), new_name,
				                               std::move(copied_entry->type), std::move(copied_entry->children));
				new_children.push_back(std::move(renamed_entry));
			} else {
				// not the leaf - find the child to rename it and recurse
				new_children.push_back(child.RenameField(column_path, new_name, depth + 1));
			}
		} else {
			// this entry can be copied as-is
			new_children.push_back(child.Copy());
		}
	}
	if (!found) {
		throw InternalException("DuckLakeFieldId::AddField - child not found in struct path");
	}
	auto new_type = GetStructType(new_children);
	return make_uniq<DuckLakeFieldId>(column_data.Copy(), Name(), std::move(new_type), std::move(new_children));
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::RenameColumn(const DuckLakeFieldData &field_data,
                                                              FieldIndex rename_index, const string &new_name) {
	auto result = make_shared_ptr<DuckLakeFieldData>();
	for (auto &existing_id : field_data.field_ids) {
		unique_ptr<DuckLakeFieldId> field_id;
		if (existing_id->GetFieldIndex() == rename_index) {
			field_id = DuckLakeFieldId::Rename(*existing_id, new_name);
		} else {
			field_id = existing_id->Copy();
		}
		result->Add(std::move(field_id));
	}
	return result;
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
	auto result = make_shared_ptr<DuckLakeFieldData>();
	for (auto &existing_id : field_data.field_ids) {
		if (existing_id->GetFieldIndex() == drop_index) {
			continue;
		}
		result->Add(existing_id->Copy());
	}
	return result;
}

shared_ptr<DuckLakeFieldData> DuckLakeFieldData::SetDefault(const DuckLakeFieldData &field_data, FieldIndex field_index,
                                                            const ColumnDefinition &new_col, bool add_column) {
	auto result = make_shared_ptr<DuckLakeFieldData>();
	auto new_default =
	    new_col.HasDefaultValue() ? optional_ptr<const ParsedExpression>(new_col.DefaultValue()) : nullptr;
	if (new_default && new_default->type != ExpressionType::VALUE_CONSTANT && add_column) {
		throw NotImplementedException("We cannot add a column with a non-literal default value. Add the column and "
		                              "then explicitly set the default for new values using \"ALTER ... SET DEFAULT\"");
	}
	for (auto &existing_id : field_data.field_ids) {
		unique_ptr<DuckLakeFieldId> field_id;
		if (existing_id->GetFieldIndex() == field_index) {
			field_id = DuckLakeFieldId::SetDefault(*existing_id, new_default);
		} else {
			field_id = existing_id->Copy();
		}
		result->Add(std::move(field_id));
	}
	return result;
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

optional_ptr<const DuckLakeFieldId> DuckLakeFieldData::GetByNames(PhysicalIndex id, const vector<string> &column_names,
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
			    StringUtil::Join(column_names, "."), result.get().Name());
		}
		if (i >= column_names.size()) {
			break;
		}
		auto &current = result.get();
		auto next_child = current.GetChildByName(column_names[i]);
		if (!next_child) {
			return nullptr;
		}
		result = *next_child;
	}
	return result.get();
}

} // namespace duckdb
