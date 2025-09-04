#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/file_system.hpp"

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

string DuckLakeUtil::ValueToSQL(ClientContext &context, const Value &val) {
	// FIXME: this should be upstreamed
	if (val.IsNull()) {
		return val.ToSQLString();
	}
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
		string ret = "MAP(";
		auto &map_values = MapValue::GetChildren(val);
		// keys
		ret += "[";
		for (idx_t i = 0; i < map_values.size(); i++) {
			if (i > 0) {
				ret += ", ";
			}
			auto &map_children = StructValue::GetChildren(map_values[i]);
			ret += map_children[0].ToSQLString();
		}
		ret += "], [";
		// values
		for (idx_t i = 0; i < map_values.size(); i++) {
			if (i > 0) {
				ret += ", ";
			}
			auto &map_children = StructValue::GetChildren(map_values[i]);
			ret += map_children[1].ToSQLString();
		}
		ret += "])";
		return ret;
	}
	case LogicalTypeId::BLOB: {
		if (val.type().HasAlias() && val.type().GetAlias() == "GEOMETRY") {
			// geometry - cast to string
			auto str_val = val.CastAs(context, LogicalType::VARCHAR);
			return DuckLakeUtil::ValueToSQL(context, str_val);
		}
		return val.ToSQLString();
	}
	default:
		return val.ToSQLString();
	}
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
