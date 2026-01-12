#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

string DuckLakeUtil::ParseQuotedValue(const string &input, idx_t &pos) {
	if (pos >= input.size() || input[pos] != '"') {
		throw InvalidInputException("Failed to parse quoted value - expected a quote");
	}
	string result;
	pos++;
	for (; pos < input.size(); pos++) {
		if (input[pos] == '"') {
			pos++;
			// check if this is an escaped quote
			if (pos < input.size() && input[pos] == '"') {
				// escaped quote
				result += '"';
				continue;
			}
			return result;
		}
		result += input[pos];
	}
	throw InvalidInputException("Failed to parse quoted value - unterminated quote");
}

string DuckLakeUtil::ToQuotedList(const vector<string> &input, char list_separator) {
	string result;
	for (auto &str : input) {
		if (!result.empty()) {
			result += list_separator;
		}
		result += KeywordHelper::WriteQuoted(str, '"');
	}
	return result;
}

vector<string> DuckLakeUtil::ParseQuotedList(const string &input, char list_separator) {
	vector<string> result;
	if (input.empty()) {
		return result;
	}
	idx_t pos = 0;
	while (true) {
		result.push_back(ParseQuotedValue(input, pos));
		if (pos >= input.size()) {
			break;
		}
		if (input[pos] != list_separator) {
			throw InvalidInputException("Failed to parse list - expected a %s", string(1, list_separator));
		}
		pos++;
	}
	return result;
}

ParsedCatalogEntry DuckLakeUtil::ParseCatalogEntry(const string &input) {
	ParsedCatalogEntry result_data;
	idx_t pos = 0;
	result_data.schema = DuckLakeUtil::ParseQuotedValue(input, pos);
	if (pos >= input.size() || input[pos] != '.') {
		throw InvalidInputException("Failed to parse catalog entry - expected a dot");
	}
	pos++;
	result_data.name = DuckLakeUtil::ParseQuotedValue(input, pos);
	if (pos < input.size()) {
		throw InvalidInputException("Failed to parse catalog entry - trailing data after quoted value");
	}
	return result_data;
}

string DuckLakeUtil::SQLIdentifierToString(const string &text) {
	return "\"" + StringUtil::Replace(text, "\"", "\"\"") + "\"";
}

string DuckLakeUtil::SQLLiteralToString(const string &text) {
	return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

string DuckLakeUtil::StatsToString(const string &text) {
	for (auto c : text) {
		if (c == '\0') {
			return "NULL";
		}
	}
	return DuckLakeUtil::SQLLiteralToString(text);
}

string ToSQLString(DuckLakeMetadataManager &metadata_manager, const Value &value) {
	if (value.IsNull()) {
		return value.ToString();
	}
	string value_type = value.type().ToString();
	if (!metadata_manager.TypeIsNativelySupported(value.type())) {
		value_type = "VARCHAR";
	} else {
		value_type = metadata_manager.GetColumnTypeInternal(value.type());
	}
	switch (value.type().id()) {
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::BLOB:
		return "'" + value.ToString() + "'::" + value_type;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM: {
		auto str_val = value.ToString();
		if (str_val.size() == 1 && str_val[0] == '\0') {
			return "chr(0)";
		}
		return "'" + StringUtil::Replace(value.ToString(), "'", "''") + "'";
	}
	case LogicalTypeId::VARIANT:
	case LogicalTypeId::STRUCT: {
		bool is_unnamed = StructType::IsUnnamed(value.type());
		string ret = is_unnamed ? "(" : "{";
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &struct_values = StructValue::GetChildren(value);
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_values[i];
			if (is_unnamed) {
				ret += child.ToSQLString();
			} else {
				ret += "'" + name + "': " + child.ToSQLString();
			}
			if (i < struct_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += is_unnamed ? ")" : "}";
		return ret;
	}
	case LogicalTypeId::FLOAT:
		if (!value.FloatIsFinite(FloatValue::Get(value))) {
			return "'" + value.ToString() + "'::" + value_type;
		}
		return value.ToString();
	case LogicalTypeId::DOUBLE: {
		double val = DoubleValue::Get(value);
		if (!value.DoubleIsFinite(val)) {
			if (!Value::IsNan(val)) {
				// to infinity and beyond
				return val < 0 ? "-1e1000" : "1e1000";
			}
			return "'" + value.ToString() + "'::" + value_type;
		}
		return value.ToString();
	}
	case LogicalTypeId::LIST: {
		string ret = "[";
		auto &list_values = ListValue::GetChildren(value);
		for (idx_t i = 0; i < list_values.size(); i++) {
			auto &child = list_values[i];
			ret += child.ToSQLString();
			if (i < list_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	case LogicalTypeId::ARRAY: {
		string ret = "[";
		auto &array_values = ArrayValue::GetChildren(value);
		for (idx_t i = 0; i < array_values.size(); i++) {
			auto &child = array_values[i];
			ret += child.ToSQLString();
			if (i < array_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	case LogicalTypeId::UNION: {
		string ret = "union_value(";
		auto union_tag = UnionValue::GetTag(value);
		auto &tag_name = UnionType::GetMemberName(value.type(), union_tag);
		ret += tag_name + " := ";
		ret += UnionValue::GetValue(value).ToSQLString();
		ret += ")";
		return ret;
	}
	default:
		return value.ToString();
	}
}

string DuckLakeUtil::ValueToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, const Value &val) {
	// FIXME: this should be upstreamed
	if (val.IsNull()) {
		return val.ToSQLString();
	}
	if (val.type().HasAlias()) {
		// extension type: cast to string
		auto str_val = val.CastAs(context, LogicalType::VARCHAR);
		return ValueToSQL(metadata_manager, context, str_val);
	}
	string result;
	switch (val.type().id()) {
	case LogicalTypeId::VARCHAR: {
		auto &str_val = StringValue::Get(val);
		string ret;
		bool concat = false;
		for (auto c : str_val) {
			switch (c) {
			case '\0':
				// need to concat to place a null byte
				concat = true;
				ret += "', chr(0), '";
				break;
			case '\'':
				ret += "''";
				break;
			default:
				ret += c;
				break;
			}
		}
		if (concat) {
			return "CONCAT('" + ret + "')";
		} else {
			return "'" + ret + "'";
		}
	}
	case LogicalTypeId::MAP: {
		if (metadata_manager.TypeIsNativelySupported(val.type())) {
			string ret = "MAP(";
			auto &map_values = MapValue::GetChildren(val);
			// keys
			ret += "[";
			for (idx_t i = 0; i < map_values.size(); i++) {
				if (i > 0) {
					ret += ", ";
				}
				auto &map_children = StructValue::GetChildren(map_values[i]);
				ret += ToSQLString(metadata_manager, map_children[0]);
			}
			ret += "], [";
			// values
			for (idx_t i = 0; i < map_values.size(); i++) {
				if (i > 0) {
					ret += ", ";
				}
				auto &map_children = StructValue::GetChildren(map_values[i]);
				ret += ToSQLString(metadata_manager, map_children[1]);
			}
			ret += "])";
			result = ret;
		} else {
			result = ToSQLString(metadata_manager, val);
		}
		break;
	}
	default:
		result = ToSQLString(metadata_manager, val);
	}
	if (metadata_manager.TypeIsNativelySupported(val.type()) || !val.type().IsNested()) {
		return result;
	}
	return StringUtil::Format("%s", SQLString(result));
}

string DuckLakeUtil::JoinPath(FileSystem &fs, const string &a, const string &b) {
	auto sep = fs.PathSeparator(a);
	if (StringUtil::EndsWith(a, sep)) {
		return a + b;
	} else {
		return a + sep + b;
	}
}

} // namespace duckdb
