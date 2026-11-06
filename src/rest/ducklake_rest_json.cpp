#include "rest/ducklake_rest_json.hpp"
#include "common/ducklake_types.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include <cmath>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Writer
//===--------------------------------------------------------------------===//
RESTJsonWriter::RESTJsonWriter() {
	root = writer.CreateObject();
	writer.SetRoot(root);
}

//===--------------------------------------------------------------------===//
// Reader
//===--------------------------------------------------------------------===//
RESTJsonReader::RESTJsonReader(const string &json) {
	JSONParseError error;
	doc = JSONDocument::TryParse(json.c_str(), json.size(), error, JSONReadFlags::ALLOW_INVALID_UNICODE);
	if (!doc) {
		throw InvalidInputException("Failed to parse DuckLake REST JSON: %s at position %llu", error.message,
		                            error.position);
	}
}

bool RESTJsonIn::IsNull() const {
	return !val.IsValid() || val.IsNull();
}

bool RESTJsonIn::IsObject() const {
	return val.IsValid() && val.IsObject();
}

bool RESTJsonIn::IsArray() const {
	return val.IsValid() && val.IsArray();
}

bool RESTJsonIn::IsString() const {
	return val.IsValid() && val.IsString();
}

bool RESTJsonIn::Has(const string &key) const {
	return IsObject() && val.GetMember(key).IsValid();
}

RESTJsonIn RESTJsonIn::Get(const string &key) const {
	if (!IsObject()) {
		throw InvalidInputException("DuckLake REST: expected a JSON object when reading \"%s\"", key);
	}
	auto result = val.GetMember(key);
	if (!result.IsValid()) {
		throw InvalidInputException("DuckLake REST: missing required field \"%s\"", key);
	}
	return RESTJsonIn(result);
}

RESTJsonIn RESTJsonIn::TryGet(const string &key) const {
	if (!IsObject()) {
		return RESTJsonIn();
	}
	return RESTJsonIn(val.GetMember(key));
}

vector<RESTJsonIn> RESTJsonIn::Items() const {
	vector<RESTJsonIn> result;
	if (IsNull()) {
		return result;
	}
	if (!IsArray()) {
		throw InvalidInputException("DuckLake REST: expected a JSON array");
	}
	val.IterateArray([&](JSONValue entry) { result.emplace_back(entry); });
	return result;
}

vector<pair<string, RESTJsonIn>> RESTJsonIn::Members() const {
	vector<pair<string, RESTJsonIn>> result;
	if (IsNull()) {
		return result;
	}
	if (!IsObject()) {
		throw InvalidInputException("DuckLake REST: expected a JSON object");
	}
	val.IterateObject([&](const string &key, JSONValue entry) { result.emplace_back(key, RESTJsonIn(entry)); });
	return result;
}

string RESTJsonIn::Str() const {
	if (!IsString()) {
		throw InvalidInputException("DuckLake REST: expected a JSON string");
	}
	return val.GetString();
}

idx_t RESTJsonIn::UInt() const {
	if (!val.IsValid() || !val.IsInteger()) {
		throw InvalidInputException("DuckLake REST: expected a JSON integer");
	}
	if (val.GetType() == JSONValueType::SIGNED_INTEGER) {
		auto result = val.GetSignedInteger();
		if (result < 0) {
			throw InvalidInputException("DuckLake REST: expected a non-negative JSON integer");
		}
		return UnsafeNumericCast<idx_t>(result);
	}
	return val.GetUnsignedInteger();
}

int64_t RESTJsonIn::Int() const {
	if (!val.IsValid() || !val.IsInteger()) {
		throw InvalidInputException("DuckLake REST: expected a JSON integer");
	}
	if (val.GetType() == JSONValueType::UNSIGNED_INTEGER) {
		return NumericCast<int64_t>(val.GetUnsignedInteger());
	}
	return val.GetSignedInteger();
}

double RESTJsonIn::Double() const {
	if (!val.IsValid() || !val.IsNumber()) {
		throw InvalidInputException("DuckLake REST: expected a JSON number");
	}
	return val.GetNumber();
}

bool RESTJsonIn::Bool() const {
	if (!val.IsValid() || val.GetType() != JSONValueType::BOOLEAN) {
		throw InvalidInputException("DuckLake REST: expected a JSON boolean");
	}
	return val.GetBoolean();
}

string RESTJsonIn::Str(const string &key) const {
	return Get(key).Str();
}

string RESTJsonIn::Str(const string &key, const string &default_value) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? default_value : entry.Str();
}

idx_t RESTJsonIn::UInt(const string &key) const {
	return Get(key).UInt();
}

idx_t RESTJsonIn::UInt(const string &key, idx_t default_value) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? default_value : entry.UInt();
}

int64_t RESTJsonIn::Int(const string &key) const {
	return Get(key).Int();
}

bool RESTJsonIn::Bool(const string &key, bool default_value) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? default_value : entry.Bool();
}

optional_idx RESTJsonIn::Opt(const string &key) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? optional_idx() : optional_idx(entry.UInt());
}

//===--------------------------------------------------------------------===//
// Values and types
//===--------------------------------------------------------------------===//
JSONMutableValue RESTJson::TypeToJson(JSONWriter &writer, const LogicalType &type) {
	auto result = writer.CreateObject();
	// arrays are stored as lists
	auto ducklake_type = type.id() == LogicalTypeId::ARRAY ? LogicalType::LIST(ArrayType::GetChildType(type)) : type;
	auto name = DuckLakeTypes::ToString(ducklake_type);
	result.AddString("id", name.substr(0, name.find('(')));
	switch (ducklake_type.id()) {
	case LogicalTypeId::GEOMETRY:
		if (GeoType::HasCRS(ducklake_type)) {
			result.AddString("crs", GeoType::GetCRS(ducklake_type).GetDefinition());
		}
		break;
	case LogicalTypeId::DECIMAL:
		result.Add("width", writer.CreateUnsignedInteger(DecimalType::GetWidth(ducklake_type)));
		result.Add("scale", writer.CreateUnsignedInteger(DecimalType::GetScale(ducklake_type)));
		break;
	case LogicalTypeId::LIST:
		result.Add("child", TypeToJson(writer, ListType::GetChildType(ducklake_type)));
		break;
	case LogicalTypeId::MAP:
		result.Add("key", TypeToJson(writer, MapType::KeyType(ducklake_type)));
		result.Add("value", TypeToJson(writer, MapType::ValueType(ducklake_type)));
		break;
	case LogicalTypeId::STRUCT: {
		auto children = writer.CreateArray();
		for (auto &child : StructType::GetChildTypes(ducklake_type)) {
			auto child_obj = writer.CreateObject();
			child_obj.AddString("name", child.first.GetIdentifierName());
			child_obj.Add("type", TypeToJson(writer, child.second));
			children.Append(child_obj);
		}
		result.Add("children", children);
		break;
	}
	default:
		break;
	}
	return result;
}

LogicalType RESTJson::TypeFromJson(RESTJsonIn json) {
	auto id = json.Str("id");
	if (StringUtil::CIEquals(id, "geometry")) {
		return LogicalType::GEOMETRY(json.Str("crs", string()));
	}
	if (StringUtil::CIEquals(id, "decimal")) {
		return LogicalType::DECIMAL(NumericCast<uint8_t>(json.UInt("width")), NumericCast<uint8_t>(json.UInt("scale")));
	}
	auto type = DuckLakeTypes::FromString(id);
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return LogicalType::LIST(TypeFromJson(json.Get("child")));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(TypeFromJson(json.Get("key")), TypeFromJson(json.Get("value")));
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &child : json.Get("children").Items()) {
			children.emplace_back(Identifier(child.Str("name")), TypeFromJson(child.Get("type")));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	default:
		return type;
	}
}

//! Variants keep the types of the values they hold, also of nested values
static Value VariantFromValue(const Value &value) {
	if (value.IsNull()) {
		return Value(LogicalType::VARIANT());
	}
	switch (value.type().id()) {
	case LogicalTypeId::STRUCT: {
		child_list_t<Value> children;
		auto &values = StructValue::GetChildren(value);
		for (idx_t i = 0; i < values.size(); i++) {
			children.emplace_back(StructType::GetChildName(value.type(), i), VariantFromValue(values[i]));
		}
		return Value::STRUCT(std::move(children)).DefaultCastAs(LogicalType::VARIANT());
	}
	case LogicalTypeId::LIST: {
		vector<Value> children;
		for (auto &child : ListValue::GetChildren(value)) {
			children.push_back(VariantFromValue(child));
		}
		return Value::LIST(LogicalType::VARIANT(), std::move(children)).DefaultCastAs(LogicalType::VARIANT());
	}
	default:
		return value.DefaultCastAs(LogicalType::VARIANT());
	}
}

} // namespace duckdb
