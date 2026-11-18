//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_codec.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_json.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/typed_index.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
class ColumnDataCollection;

//! The codecs that the generated code of the REST API (ducklake_rest_api.hpp) uses to convert values to and from
//! JSON. A codec implements a type of the API specification (src/rest/api/*.json), its overloads implement the C++
//! types that can be stored with that type - storing a value with a type it is not specified as does not compile.

//! A value that may be absent, for types that have no null state of their own
template <class T>
struct RESTOptional {
	RESTOptional() : has_value(false), value() {
	}
	RESTOptional(T value_p) : has_value(true), value(std::move(value_p)) { // NOLINT: allow implicit conversion
	}

	bool has_value;
	T value;
};

//! Rows of typed values, which are owned when they were read
struct RESTRows {
	RESTRows();
	explicit RESTRows(ColumnDataCollection &rows);
	explicit RESTRows(unique_ptr<ColumnDataCollection> rows);
	RESTRows(RESTRows &&other) noexcept;
	RESTRows &operator=(RESTRows &&other) noexcept;
	~RESTRows();

	unique_ptr<ColumnDataCollection> owned_rows;
	optional_ptr<ColumnDataCollection> rows;
};

//===--------------------------------------------------------------------===//
// Null values
//===--------------------------------------------------------------------===//
//! Nullable fields are left out if their value is null
inline bool RESTIsNull(const optional_idx &value) {
	return !value.IsValid();
}
template <class T>
bool RESTIsNull(const TypedIndex<T> &value) {
	return !value.IsValid();
}
inline bool RESTIsNull(const string &value) {
	return value.empty();
}
inline bool RESTIsNull(const Value &value) {
	return value.IsNull();
}
template <class T>
bool RESTIsNull(const vector<T> &value) {
	return value.empty();
}
template <class T>
bool RESTIsNull(const set<T> &value) {
	return value.empty();
}
template <class K, class V>
bool RESTIsNull(const map<K, V> &value) {
	return value.empty();
}
template <class K, class V>
bool RESTIsNull(const unordered_map<K, V> &value) {
	return value.empty();
}
template <class T>
bool RESTIsNull(const unique_ptr<T> &value) {
	return !value;
}
template <class T>
bool RESTIsNull(const RESTOptional<T> &value) {
	return !value.has_value;
}

//===--------------------------------------------------------------------===//
// Codecs
//===--------------------------------------------------------------------===//
template <class CODEC>
struct RESTCodec {
	template <class T>
	static T Get(RESTJsonIn in) {
		T result;
		CODEC::Read(in, result);
		return result;
	}
	//! Codecs of polymorphic types hide this to read the right subclass
	template <class T>
	static void ReadPtr(RESTJsonIn in, unique_ptr<T> &result) {
		result = make_uniq<T>(CODEC::template Get<T>(in));
	}
};

//! Writes / reads a value with a codec, values of nullable fields can be wrapped
template <class CODEC, class T>
JSONMutableValue RESTWriteValue(JSONWriter &writer, const T &value) {
	return CODEC::Write(writer, value);
}
template <class CODEC, class T>
JSONMutableValue RESTWriteValue(JSONWriter &writer, const unique_ptr<T> &value) {
	return CODEC::Write(writer, *value);
}
template <class CODEC, class T>
JSONMutableValue RESTWriteValue(JSONWriter &writer, const RESTOptional<T> &value) {
	return CODEC::Write(writer, value.value);
}
template <class CODEC, class T>
void RESTReadValue(RESTJsonIn in, T &result) {
	CODEC::Read(in, result);
}
template <class CODEC, class T>
void RESTReadValue(RESTJsonIn in, unique_ptr<T> &result) {
	CODEC::ReadPtr(in, result);
}
template <class CODEC, class T>
void RESTReadValue(RESTJsonIn in, RESTOptional<T> &result) {
	result = RESTOptional<T>(CODEC::template Get<T>(in));
}

//! A value of an enum of the REST API and its name
template <class T>
struct RESTEnumValue {
	T value;
	const char *name;
};
[[noreturn]] void RESTThrowInvalidEnum(const string &name, const char *type_name, const vector<string> &names);
//! The name of a value of an enum
template <class T, idx_t N>
const char *RESTEnumName(const RESTEnumValue<T> (&values)[N], T value, const char *type_name) {
	for (auto &entry : values) {
		if (entry.value == value) {
			return entry.name;
		}
	}
	throw InternalException("%d is not a value of the REST API type %s", static_cast<int64_t>(value), type_name);
}
template <idx_t N>
const char *RESTEnumName(const char *const (&names)[N], const string &value, const char *type_name) {
	for (auto name : names) {
		if (value == name) {
			return name;
		}
	}
	throw InternalException("\"%s\" is not a value of the REST API type %s", value, type_name);
}
//! The value of an enum with a name, throws if it is not one of its names
template <class T, idx_t N>
T RESTEnumFromName(const RESTEnumValue<T> (&values)[N], const string &name, const char *type_name) {
	vector<string> names;
	for (auto &entry : values) {
		if (name == entry.name) {
			return entry.value;
		}
		names.push_back(entry.name);
	}
	RESTThrowInvalidEnum(name, type_name, names);
}
template <idx_t N>
string RESTEnumFromName(const char *const (&names)[N], const string &name, const char *type_name) {
	vector<string> all_names;
	for (auto entry : names) {
		if (name == entry) {
			return name;
		}
		all_names.push_back(entry);
	}
	RESTThrowInvalidEnum(name, type_name, all_names);
}
//! Throws unless the JSON value of an object type is an object (or absent)
void RESTExpectObject(RESTJsonIn in, const char *type_name);
//! Reads a field that can be absent, returns whether it is there
template <class CODEC, class T>
bool RESTReadOptional(RESTJsonIn in, const char *name, T &result) {
	auto entry = in.TryGet(name);
	if (entry.IsNull()) {
		return false;
	}
	RESTReadValue<CODEC>(entry, result);
	return true;
}
//! Reads a field that has a default when it is absent
template <class CODEC, class T, class DEFAULT>
void RESTReadOptional(RESTJsonIn in, const char *name, T &result, DEFAULT &&default_value) {
	if (!RESTReadOptional<CODEC>(in, name, result)) {
		result = std::forward<DEFAULT>(default_value);
	}
}
//! Writes a field of an object if it has a value
template <class CODEC, class T>
void RESTWriteOptional(JSONWriter &writer, JSONMutableValue &object, const char *name, const T &value, bool has_value) {
	if (has_value) {
		object.Add(name, RESTWriteValue<CODEC>(writer, value));
	}
}

struct RESTStringCodec : public RESTCodec<RESTStringCodec> {
	static JSONMutableValue Write(JSONWriter &writer, const string &value) {
		return writer.CreateString(value);
	}
	static void Read(RESTJsonIn in, string &result) {
		result = in.Str();
	}
	static JSONMutableValue Write(JSONWriter &writer, const Identifier &value) {
		return writer.CreateString(value.GetIdentifierName());
	}
	static void Read(RESTJsonIn in, Identifier &result) {
		result = Identifier(in.Str());
	}
};

//! Maps paths between the data path of a client and the data path of the server, for clients that have another view of
//! the data path (e.g. another mount point). Paths outside of the data paths are not mapped.
class RESTPathMapper {
public:
	RESTPathMapper(string client_path, string server_path);

	string ToServer(const string &path) const;
	string ToClient(const string &path) const;

private:
	static string Map(const string &path, const string &from, const string &to);

private:
	string client_path;
	string server_path;
};

//! The path mapper for the paths that the current thread writes and reads, see DuckLakeRESTClientAPI
class RESTPathScope {
public:
	explicit RESTPathScope(optional_ptr<const RESTPathMapper> mapper);
	~RESTPathScope();

	static optional_ptr<const RESTPathMapper> Current();

private:
	optional_ptr<const RESTPathMapper> previous;
};

//! Paths of files and directories, which are mapped to and from the data path of the server by clients
struct RESTPathCodec : public RESTCodec<RESTPathCodec> {
	static JSONMutableValue Write(JSONWriter &writer, const string &value);
	static void Read(RESTJsonIn in, string &result);
};

struct RESTBase64Codec : public RESTCodec<RESTBase64Codec> {
	static JSONMutableValue Write(JSONWriter &writer, const string &value);
	static void Read(RESTJsonIn in, string &result);
};

struct RESTBoolCodec : public RESTCodec<RESTBoolCodec> {
	static JSONMutableValue Write(JSONWriter &writer, bool value) {
		return writer.CreateBoolean(value);
	}
	static void Read(RESTJsonIn in, bool &result) {
		result = in.Bool();
	}
};

struct RESTUInt64Codec : public RESTCodec<RESTUInt64Codec> {
	static JSONMutableValue Write(JSONWriter &writer, idx_t value) {
		return writer.CreateUnsignedInteger(value);
	}
	static JSONMutableValue Write(JSONWriter &writer, const optional_idx &value) {
		return writer.CreateUnsignedInteger(value.GetIndex());
	}
	template <class T>
	static JSONMutableValue Write(JSONWriter &writer, const TypedIndex<T> &value) {
		return writer.CreateUnsignedInteger(value.index);
	}
	static void Read(RESTJsonIn in, idx_t &result) {
		result = in.UInt();
	}
	static void Read(RESTJsonIn in, optional_idx &result) {
		auto value = in.UInt();
		if (value == DConstants::INVALID_INDEX) {
			throw InvalidInputException("DuckLake REST: %llu is not a valid id", value);
		}
		result = value;
	}
	template <class T>
	static void Read(RESTJsonIn in, TypedIndex<T> &result) {
		result.index = in.UInt();
	}
};

struct RESTInt64Codec : public RESTCodec<RESTInt64Codec> {
	static JSONMutableValue Write(JSONWriter &writer, int64_t value) {
		return writer.CreateSignedInteger(value);
	}
	static void Read(RESTJsonIn in, int64_t &result) {
		result = in.Int();
	}
};

struct RESTDoubleCodec : public RESTCodec<RESTDoubleCodec> {
	static JSONMutableValue Write(JSONWriter &writer, double value) {
		return writer.CreateDouble(value);
	}
	static void Read(RESTJsonIn in, double &result) {
		result = in.Double();
	}
};

//! Timestamps with time zone, as strings
struct RESTTimestampCodec : public RESTCodec<RESTTimestampCodec> {
	static JSONMutableValue Write(JSONWriter &writer, timestamp_tz_t value);
	static JSONMutableValue Write(JSONWriter &writer, const string &value);
	static void Read(RESTJsonIn in, timestamp_tz_t &result);
	//! Timestamps that are kept as strings are validated
	static void Read(RESTJsonIn in, string &result);
};

//! Values as text, which are strings (VARCHAR values) in the API
struct RESTTextCodec : public RESTCodec<RESTTextCodec> {
	static JSONMutableValue Write(JSONWriter &writer, const Value &value) {
		return writer.CreateString(value.type().id() == LogicalTypeId::VARCHAR ? StringValue::Get(value)
		                                                                       : value.ToString());
	}
	static void Read(RESTJsonIn in, Value &result) {
		result = Value(in.Str());
	}
};

struct RESTLogicalTypeCodec : public RESTCodec<RESTLogicalTypeCodec> {
	static JSONMutableValue Write(JSONWriter &writer, const LogicalType &value) {
		return RESTJson::TypeToJson(writer, value);
	}
	static void Read(RESTJsonIn in, LogicalType &result) {
		result = RESTJson::TypeFromJson(in);
	}
};

//! Rows as arrays of values, whose types are given by another field
template <class CODEC>
struct RESTListCodec : public RESTCodec<RESTListCodec<CODEC>> {
	template <class T>
	static JSONMutableValue Write(JSONWriter &writer, const vector<T> &value) {
		return WriteItems(writer, value);
	}
	template <class T>
	static JSONMutableValue Write(JSONWriter &writer, const set<T> &value) {
		return WriteItems(writer, value);
	}
	template <class T>
	static JSONMutableValue Write(JSONWriter &writer, const unordered_set<T> &value) {
		return WriteItems(writer, value);
	}
	template <class T>
	static void Read(RESTJsonIn in, vector<T> &result) {
		for (auto &item : in.Items()) {
			result.push_back(CODEC::template Get<T>(item));
		}
	}
	template <class T>
	static void Read(RESTJsonIn in, set<T> &result) {
		for (auto &item : in.Items()) {
			result.insert(CODEC::template Get<T>(item));
		}
	}
	template <class T>
	static void Read(RESTJsonIn in, unordered_set<T> &result) {
		for (auto &item : in.Items()) {
			result.insert(CODEC::template Get<T>(item));
		}
	}

private:
	template <class CONTAINER>
	static JSONMutableValue WriteItems(JSONWriter &writer, const CONTAINER &value) {
		auto result = writer.CreateArray();
		for (auto &item : value) {
			result.Append(RESTWriteValue<CODEC>(writer, item));
		}
		return result;
	}
};

//! JSON objects with arbitrary keys
template <class CODEC>
struct RESTDictCodec : public RESTCodec<RESTDictCodec<CODEC>> {
	template <class T>
	static JSONMutableValue Write(JSONWriter &writer, const map<string, T> &value) {
		auto result = writer.CreateObject();
		for (auto &entry : value) {
			result.Add(entry.first, RESTWriteValue<CODEC>(writer, entry.second));
		}
		return result;
	}
	template <class T>
	static void Read(RESTJsonIn in, map<string, T> &result) {
		for (auto &member : in.Members()) {
			result.emplace(member.first, CODEC::template Get<T>(member.second));
		}
	}
};

} // namespace duckdb
