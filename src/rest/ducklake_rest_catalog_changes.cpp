#include "rest/ducklake_rest_catalog_changes.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

static vector<string> WritePath(const vector<Identifier> &path) {
	vector<string> result;
	for (auto &name : path) {
		result.push_back(name.GetIdentifierName());
	}
	return result;
}

static vector<Identifier> ReadPath(const vector<string> &path) {
	vector<Identifier> result;
	for (auto &name : path) {
		result.emplace_back(name);
	}
	return result;
}

static unique_ptr<ParsedExpression> ReadExpression(const string &expression) {
	if (expression.empty()) {
		return nullptr;
	}
	return Parser::GetBuiltinParser().ParseSingleExpression(expression);
}

RESTColumnDefinition DuckLakeRESTCatalogChanges::WriteColumn(const ColumnDefinition &column, bool not_null) {
	RESTColumnDefinition result;
	result.column_name = column.GetName().GetIdentifierName();
	result.column_type = column.GetType();
	if (column.HasDefaultValue()) {
		result.default_value = column.DefaultValue().ToString();
	}
	if (!column.Comment().IsNull()) {
		result.comment = column.Comment().ToString();
	}
	result.not_null = not_null;
	return result;
}

ColumnDefinition DuckLakeRESTCatalogChanges::ReadColumn(ClientContext &context, const RESTColumnDefinition &column) {
	auto type = column.column_type;
	ColumnDefinition result(Identifier(column.column_name), std::move(type));
	if (!column.default_value.empty()) {
		result.SetDefaultValue(Parser::GetBuiltinParser().ParseSingleExpression(column.default_value));
	}
	if (column.comment.has_value) {
		result.SetComment(Value(column.comment.value));
	}
	return result;
}

unique_ptr<RESTAlteration> DuckLakeRESTCatalogChanges::WriteAlter(const AlterInfo &info) {
	switch (info.type) {
	case AlterType::ALTER_TABLE: {
		auto &alter = info.Cast<AlterTableInfo>();
		switch (alter.alter_table_type) {
		case AlterTableType::RENAME_TABLE: {
			auto result = make_uniq<RESTRenameTable>();
			result->new_name = alter.Cast<RenameTableInfo>().new_table_name.GetIdentifierName();
			return std::move(result);
		}
		case AlterTableType::RENAME_COLUMN: {
			auto &rename = alter.Cast<RenameColumnInfo>();
			auto result = make_uniq<RESTRenameColumn>();
			result->old_name = rename.old_name.GetIdentifierName();
			result->new_name = rename.new_name.GetIdentifierName();
			return std::move(result);
		}
		case AlterTableType::RENAME_FIELD: {
			auto &rename = alter.Cast<RenameFieldInfo>();
			auto result = make_uniq<RESTRenameField>();
			result->column_path = WritePath(rename.column_path);
			result->new_name = rename.new_name.GetIdentifierName();
			return std::move(result);
		}
		case AlterTableType::ADD_COLUMN: {
			auto &add = alter.Cast<AddColumnInfo>();
			auto result = make_uniq<RESTAddColumn>();
			result->column = WriteColumn(add.new_column, add.add_column_constraints.add_not_null);
			result->if_not_exists = add.if_column_not_exists;
			return std::move(result);
		}
		case AlterTableType::ADD_FIELD: {
			auto &add = alter.Cast<AddFieldInfo>();
			auto result = make_uniq<RESTAddField>();
			result->column_path = WritePath(add.column_path);
			result->field = WriteColumn(add.new_field, false);
			result->if_not_exists = add.if_field_not_exists;
			return std::move(result);
		}
		case AlterTableType::REMOVE_COLUMN: {
			auto &remove = alter.Cast<RemoveColumnInfo>();
			auto result = make_uniq<RESTRemoveColumn>();
			result->column_name = remove.removed_column.GetIdentifierName();
			result->if_exists = remove.if_column_exists;
			result->cascade = remove.cascade;
			return std::move(result);
		}
		case AlterTableType::REMOVE_FIELD: {
			auto &remove = alter.Cast<RemoveFieldInfo>();
			auto result = make_uniq<RESTRemoveField>();
			result->column_path = WritePath(remove.column_path);
			result->if_exists = remove.if_column_exists;
			result->cascade = remove.cascade;
			return std::move(result);
		}
		case AlterTableType::ALTER_COLUMN_TYPE: {
			auto &change = alter.Cast<ChangeColumnTypeInfo>();
			auto result = make_uniq<RESTChangeColumnType>();
			result->column_path = WritePath(change.column_path);
			result->column_type = change.target_type;
			if (change.expression) {
				result->expression = change.expression->ToString();
			}
			return std::move(result);
		}
		case AlterTableType::SET_DEFAULT: {
			auto &set_default = alter.Cast<SetDefaultInfo>();
			auto result = make_uniq<RESTSetDefault>();
			result->column_path = WritePath(set_default.column_path);
			if (set_default.expression) {
				result->expression = set_default.expression->ToString();
			}
			return std::move(result);
		}
		case AlterTableType::SET_NOT_NULL: {
			auto result = make_uniq<RESTSetNotNull>();
			result->column_path = WritePath(alter.Cast<SetNotNullInfo>().column_path);
			return std::move(result);
		}
		case AlterTableType::DROP_NOT_NULL: {
			auto result = make_uniq<RESTDropNotNull>();
			result->column_path = WritePath(alter.Cast<DropNotNullInfo>().column_path);
			return std::move(result);
		}
		case AlterTableType::SET_PARTITIONED_BY: {
			auto result = make_uniq<RESTSetPartitionedBy>();
			for (auto &key : alter.Cast<SetPartitionedByInfo>().partition_keys) {
				result->partitioned_by.push_back(key->ToString());
			}
			return std::move(result);
		}
		case AlterTableType::SET_SORTED_BY: {
			auto result = make_uniq<RESTSetSortedBy>();
			for (auto &order : alter.Cast<SetSortedByInfo>().orders) {
				result->sorted_by.push_back(order.ToString());
			}
			return std::move(result);
		}
		case AlterTableType::SET_TABLE_OPTIONS: {
			auto result = make_uniq<RESTSetOptions>();
			for (auto &option : alter.Cast<SetTableOptionsInfo>().table_options) {
				result->options[option.first] = option.second->ToString();
			}
			return std::move(result);
		}
		case AlterTableType::RESET_TABLE_OPTIONS: {
			auto result = make_uniq<RESTResetOptions>();
			for (auto &option : alter.Cast<ResetTableOptionsInfo>().table_options) {
				result->options.push_back(option.GetIdentifierName());
			}
			return std::move(result);
		}
		default:
			break;
		}
		throw NotImplementedException("DuckLake REST catalogs do not support ALTER TABLE type %s",
		                              EnumUtil::ToString(alter.alter_table_type));
	}
	case AlterType::ALTER_VIEW: {
		auto &alter = info.Cast<AlterViewInfo>();
		if (alter.alter_view_type != AlterViewType::RENAME_VIEW) {
			break;
		}
		auto result = make_uniq<RESTRenameView>();
		result->new_name = alter.Cast<RenameViewInfo>().new_view_name.GetIdentifierName();
		return std::move(result);
	}
	case AlterType::SET_COMMENT: {
		auto &comment = info.Cast<SetCommentInfo>();
		auto result = make_uniq<RESTSetComment>();
		result->entry_type = comment.entry_catalog_type;
		result->comment = comment.comment_value;
		return std::move(result);
	}
	case AlterType::SET_COLUMN_COMMENT: {
		auto &comment = info.Cast<SetColumnCommentInfo>();
		auto result = make_uniq<RESTSetColumnComment>();
		result->entry_type = comment.catalog_entry_type;
		result->column_name = comment.column_name.GetIdentifierName();
		result->comment = comment.comment_value;
		return std::move(result);
	}
	default:
		break;
	}
	throw NotImplementedException("DuckLake REST catalogs do not support ALTER type %s", EnumUtil::ToString(info.type));
}

unique_ptr<AlterInfo> DuckLakeRESTCatalogChanges::ReadAlter(ClientContext &context, const RESTAlteration &alteration,
                                                            const AlterEntryData &entry) {
	auto parser = Parser::GetBuiltinParser();
	switch (alteration.type) {
	case RESTAlterationType::RENAME_TABLE:
		return make_uniq<RenameTableInfo>(entry, Identifier(alteration.Cast<RESTRenameTable>().new_name));
	case RESTAlterationType::RENAME_VIEW:
		return make_uniq<RenameViewInfo>(entry, Identifier(alteration.Cast<RESTRenameView>().new_name));
	case RESTAlterationType::RENAME_COLUMN: {
		auto &rename = alteration.Cast<RESTRenameColumn>();
		return make_uniq<RenameColumnInfo>(entry, Identifier(rename.old_name), Identifier(rename.new_name));
	}
	case RESTAlterationType::RENAME_FIELD: {
		auto &rename = alteration.Cast<RESTRenameField>();
		return make_uniq<RenameFieldInfo>(entry, ReadPath(rename.column_path), Identifier(rename.new_name));
	}
	case RESTAlterationType::ADD_COLUMN: {
		auto &add = alteration.Cast<RESTAddColumn>();
		AddColumnConstraints constraints;
		constraints.add_not_null = add.column.not_null;
		return make_uniq<AddColumnInfo>(entry, ReadColumn(context, add.column), add.if_not_exists, constraints);
	}
	case RESTAlterationType::ADD_FIELD: {
		auto &add = alteration.Cast<RESTAddField>();
		return make_uniq<AddFieldInfo>(entry, ReadPath(add.column_path), ReadColumn(context, add.field),
		                               add.if_not_exists);
	}
	case RESTAlterationType::REMOVE_COLUMN: {
		auto &remove = alteration.Cast<RESTRemoveColumn>();
		return make_uniq<RemoveColumnInfo>(entry, remove.column_name, remove.if_exists, remove.cascade);
	}
	case RESTAlterationType::REMOVE_FIELD: {
		auto &remove = alteration.Cast<RESTRemoveField>();
		return make_uniq<RemoveFieldInfo>(entry, ReadPath(remove.column_path), remove.if_exists, remove.cascade);
	}
	case RESTAlterationType::CHANGE_COLUMN_TYPE: {
		auto &change = alteration.Cast<RESTChangeColumnType>();
		auto target_type = change.column_type;
		return make_uniq<ChangeColumnTypeInfo>(entry, ReadPath(change.column_path), std::move(target_type),
		                                       ReadExpression(change.expression));
	}
	case RESTAlterationType::SET_DEFAULT: {
		auto &set_default = alteration.Cast<RESTSetDefault>();
		return make_uniq<SetDefaultInfo>(entry, ReadPath(set_default.column_path),
		                                 ReadExpression(set_default.expression));
	}
	case RESTAlterationType::SET_NOT_NULL:
		return make_uniq<SetNotNullInfo>(entry, ReadPath(alteration.Cast<RESTSetNotNull>().column_path));
	case RESTAlterationType::DROP_NOT_NULL:
		return make_uniq<DropNotNullInfo>(entry, ReadPath(alteration.Cast<RESTDropNotNull>().column_path));
	case RESTAlterationType::SET_PARTITIONED_BY: {
		vector<unique_ptr<ParsedExpression>> keys;
		for (auto &key : alteration.Cast<RESTSetPartitionedBy>().partitioned_by) {
			keys.push_back(parser.ParseSingleExpression(key));
		}
		return make_uniq<SetPartitionedByInfo>(entry, std::move(keys));
	}
	case RESTAlterationType::SET_SORTED_BY: {
		vector<OrderByNode> orders;
		for (auto &key : alteration.Cast<RESTSetSortedBy>().sorted_by) {
			for (auto &order : parser.ParseOrderList(key)) {
				orders.push_back(std::move(order));
			}
		}
		return make_uniq<SetSortedByInfo>(entry, std::move(orders));
	}
	case RESTAlterationType::SET_OPTIONS: {
		case_insensitive_map_t<unique_ptr<ParsedExpression>> options;
		for (auto &option : alteration.Cast<RESTSetOptions>().options) {
			options[option.first] = parser.ParseSingleExpression(option.second);
		}
		return make_uniq<SetTableOptionsInfo>(entry, std::move(options));
	}
	case RESTAlterationType::RESET_OPTIONS: {
		identifier_set_t options;
		for (auto &option : alteration.Cast<RESTResetOptions>().options) {
			options.insert(Identifier(option));
		}
		return make_uniq<ResetTableOptionsInfo>(entry, std::move(options));
	}
	case RESTAlterationType::SET_COMMENT: {
		auto &comment = alteration.Cast<RESTSetComment>();
		auto result = make_uniq<SetCommentInfo>(comment.entry_type, Identifier(), Identifier(), Identifier(),
		                                        comment.comment, entry.if_not_found);
		result->SetQualifiedName(entry.qualified_name);
		return std::move(result);
	}
	case RESTAlterationType::SET_COLUMN_COMMENT: {
		auto &comment = alteration.Cast<RESTSetColumnComment>();
		auto result =
		    make_uniq<SetColumnCommentInfo>(Identifier(), Identifier(), Identifier(), Identifier(comment.column_name),
		                                    comment.comment, entry.if_not_found);
		result->catalog_entry_type = comment.entry_type;
		result->SetQualifiedName(entry.qualified_name);
		return std::move(result);
	}
	default:
		throw InternalException("Unsupported RESTAlterationType");
	}
}

} // namespace duckdb
