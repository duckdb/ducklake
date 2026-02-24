#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <cmath>

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

static string EscapeVarcharForSQL(const string &str_val) {
	string ret;
	bool concat = false;
	for (auto c : str_val) {
		switch (c) {
		case '\0':
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
	}
	return "'" + ret + "'";
}

string ToSQLString(DuckLakeMetadataManager &metadata_manager, const Value &value) {
	if (value.IsNull()) {
		return value.ToString();
	}
	string value_type = value.type().ToString();
	bool use_native_type = metadata_manager.TypeIsNativelySupported(value.type());
	if (!use_native_type) {
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
	case LogicalTypeId::BLOB:
	case LogicalTypeId::GEOMETRY:
		return "'" + value.ToString() + "'::" + value_type;
	case LogicalTypeId::INTERVAL: {
		auto interval = IntervalValue::Get(value);
		return StringUtil::Format("'%d months %d days %lld microseconds'::%s", interval.months, interval.days,
		                          interval.micros, value_type);
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM:
		return EscapeVarcharForSQL(value.ToString());
	case LogicalTypeId::VARIANT: {
		Vector tmp(value);
		RecursiveUnifiedVectorFormat format;
		Vector::RecursiveToUnifiedFormat(tmp, 1, format);
		UnifiedVariantVectorData vector_data(format);
		auto val = VariantUtils::ConvertVariantToValue(vector_data, 0, 0);
		if (!use_native_type) {
			throw NotImplementedException("Variant types cannot be inlined in this catalog type yet");
		}
		// variant can just be stored as a variant
		return ToSQLString(metadata_manager, val);
	}
	case LogicalTypeId::STRUCT: {
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &struct_values = StructValue::GetChildren(value);
		if (struct_values.empty()) {
			return "NULL";
		}
		bool is_unnamed = StructType::IsUnnamed(value.type());
		string ret = is_unnamed ? "(" : "{";
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_values[i];
			if (is_unnamed) {
				ret += ToSQLString(metadata_manager, child);
			} else {
				ret += "'" + StringUtil::Replace(name, "'", "''") + "': " + ToSQLString(metadata_manager, child);
			}
			if (i < struct_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += is_unnamed ? ")" : "}";
		return ret;
	}
	case LogicalTypeId::FLOAT: {
		float fval = FloatValue::Get(value);
		if (!Value::FloatIsFinite(fval) || (fval == 0.0f && std::signbit(fval))) {
			return "'" + value.ToString() + "'::" + value_type;
		}
		return value.ToString();
	}
	case LogicalTypeId::DOUBLE: {
		double val = DoubleValue::Get(value);
		if (!Value::DoubleIsFinite(val) || (val == 0.0 && std::signbit(val))) {
			return "'" + value.ToString() + "'::" + value_type;
		}
		return value.ToString();
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		if (!metadata_manager.TypeIsNativelySupported(value.type())) {
			// Stored as VARCHAR text - use ToString() which produces parseable format
			return value.ToString();
		}
		auto &children =
		    value.type().id() == LogicalTypeId::LIST ? ListValue::GetChildren(value) : ArrayValue::GetChildren(value);
		string ret = "[";
		for (idx_t i = 0; i < children.size(); i++) {
			ret += ToSQLString(metadata_manager, children[i]);
			if (i < children.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	case LogicalTypeId::MAP: {
		if (!metadata_manager.TypeIsNativelySupported(value.type())) {
			return value.ToString();
		}
		string ret = "MAP(";
		auto &map_values = MapValue::GetChildren(value);
		ret += "[";
		for (idx_t i = 0; i < map_values.size(); i++) {
			if (i > 0) {
				ret += ", ";
			}
			auto &map_children = StructValue::GetChildren(map_values[i]);
			ret += ToSQLString(metadata_manager, map_children[0]);
		}
		ret += "], [";
		for (idx_t i = 0; i < map_values.size(); i++) {
			if (i > 0) {
				ret += ", ";
			}
			auto &map_children = StructValue::GetChildren(map_values[i]);
			ret += ToSQLString(metadata_manager, map_children[1]);
		}
		ret += "])";
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

string ToByteaHexLiteral(const string &raw_bytes) {
	string hex;
	for (unsigned char c : raw_bytes) {
		hex += StringUtil::Format("%02x", static_cast<int>(c));
	}
	return "'\\x" + hex + "'";
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
		if (!metadata_manager.TypeIsNativelySupported(LogicalType::VARCHAR)) {
			return ToByteaHexLiteral(str_val);
		}
		return EscapeVarcharForSQL(str_val);
	}
	case LogicalTypeId::BLOB: {
		if (!metadata_manager.TypeIsNativelySupported(LogicalType::BLOB)) {
			return ToByteaHexLiteral(StringValue::Get(val));
		}
		result = ToSQLString(metadata_manager, val);
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

DynamicFilter *DuckLakeUtil::GetOptionalDynamicFilter(const TableFilter &filter) {
	if (filter.filter_type != TableFilterType::OPTIONAL_FILTER) {
		return nullptr;
	}
	auto &optional = filter.Cast<OptionalFilter>();
	if (!optional.child_filter || optional.child_filter->filter_type != TableFilterType::DYNAMIC_FILTER) {
		return nullptr;
	}
	auto &dynamic = optional.child_filter->Cast<DynamicFilter>();
	if (!dynamic.filter_data || !dynamic.filter_data->filter) {
		return nullptr;
	}
	return &dynamic;
}

bool DuckLakeUtil::IsInlinedSystemColumn(const string &name) {
	return StringUtil::CIEquals(name, "row_id") || StringUtil::CIEquals(name, "begin_snapshot") ||
	       StringUtil::CIEquals(name, "end_snapshot");
}

bool DuckLakeUtil::HasInlinedSystemColumnConflict(const ColumnList &columns) {
	for (auto &col : columns.Logical()) {
		if (IsInlinedSystemColumn(col.Name())) {
			return true;
		}
	}
	return false;
}

bool DuckLakeUtil::HasInlinedSystemColumnConflict(const vector<DuckLakeColumnInfo> &columns) {
	for (auto &col : columns) {
		if (IsInlinedSystemColumn(col.name)) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
