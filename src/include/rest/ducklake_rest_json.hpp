//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_json.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! Builds a JSON document
class RESTJsonWriter {
public:
	RESTJsonWriter();

	void SetRoot(JSONMutableValue value) {
		root = value;
		writer.SetRoot(value);
	}
	string ToString() const {
		return writer.ToString();
	}

	JSONWriter writer;
	JSONMutableValue root;
};

//! A handle to an immutable JSON value that is owned by a JSONDocument
class RESTJsonIn {
public:
	RESTJsonIn() {
	}
	explicit RESTJsonIn(JSONValue val) : val(val) {
	}

	bool IsNull() const;
	bool IsObject() const;
	bool IsArray() const;
	bool IsString() const;
	bool Has(const string &key) const;
	//! Throws if the key is missing
	RESTJsonIn Get(const string &key) const;
	//! Returns a null value if the key is missing
	RESTJsonIn TryGet(const string &key) const;
	vector<RESTJsonIn> Items() const;
	vector<pair<string, RESTJsonIn>> Members() const;

	string Str() const;
	idx_t UInt() const;
	int64_t Int() const;
	double Double() const;
	bool Bool() const;

	string Str(const string &key) const;
	string Str(const string &key, const string &default_value) const;
	idx_t UInt(const string &key) const;
	idx_t UInt(const string &key, idx_t default_value) const;
	int64_t Int(const string &key) const;
	bool Bool(const string &key, bool default_value = false) const;
	optional_idx Opt(const string &key) const;

	JSONValue val;
};

//! Parses a JSON document
class RESTJsonReader {
public:
	explicit RESTJsonReader(const string &json);

	RESTJsonIn Root() const {
		return RESTJsonIn(doc->GetRoot());
	}

	unique_ptr<JSONDocument> doc;
};

struct RESTJson {
	//! Logical types as structured JSON
	static JSONMutableValue TypeToJson(JSONWriter &writer, const LogicalType &type);
	static LogicalType TypeFromJson(RESTJsonIn json);
};

} // namespace duckdb
