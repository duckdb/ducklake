#include "rest/ducklake_rest_codec.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

void RESTThrowInvalidEnum(const string &name, const char *type_name, const vector<string> &names) {
	throw InvalidInputException("DuckLake REST: \"%s\" is not a valid %s (expected one of %s)", name, type_name,
	                            StringUtil::Join(names, ", "));
}

void RESTExpectObject(RESTJsonIn in, const char *type_name) {
	if (!in.IsNull() && !in.IsObject()) {
		throw InvalidInputException("DuckLake REST: expected a JSON object for %s", type_name);
	}
}

RESTRows::RESTRows() {
}

RESTRows::RESTRows(ColumnDataCollection &rows_p) : rows(rows_p) {
}

RESTRows::RESTRows(unique_ptr<ColumnDataCollection> rows_p) : owned_rows(std::move(rows_p)), rows(owned_rows.get()) {
}

RESTRows::RESTRows(RESTRows &&other) noexcept : owned_rows(std::move(other.owned_rows)), rows(other.rows) {
	other.rows = nullptr;
}

RESTRows &RESTRows::operator=(RESTRows &&other) noexcept {
	owned_rows = std::move(other.owned_rows);
	rows = other.rows;
	other.rows = nullptr;
	return *this;
}

RESTRows::~RESTRows() {
}

//===--------------------------------------------------------------------===//
// Paths
//===--------------------------------------------------------------------===//
static string WithTrailingSeparator(string path) {
	// local paths on Windows end in a backslash
	if (!path.empty() && path.back() != '/' && path.back() != '\\') {
		path += '/'; // LCOV_EXCL_LINE: the data paths of the catalog end in a separator
	}
	return path;
}

RESTPathMapper::RESTPathMapper(string client_path_p, string server_path_p)
    : client_path(WithTrailingSeparator(std::move(client_path_p))),
      server_path(WithTrailingSeparator(std::move(server_path_p))) {
}

string RESTPathMapper::Map(const string &path, const string &from, const string &to) {
	if (from.empty() || to.empty()) {
		return path; // LCOV_EXCL_LINE: a mapper is only set with both data paths
	}
	// the data path without its trailing separator, a path can continue it with either separator (e.g. on Windows)
	auto prefix = from.substr(0, from.size() - 1);
	if (!StringUtil::StartsWith(path, prefix)) {
		return path;
	}
	if (path.size() == prefix.size()) {
		return to.substr(0, to.size() - 1); // LCOV_EXCL_LINE: the paths of the API are below the data path
	}
	if (path[prefix.size()] == '/' || path[prefix.size()] == '\\') {
		return to + path.substr(prefix.size() + 1);
	}
	return path; // LCOV_EXCL_LINE: a path that merely starts with the name of the data path
}

string RESTPathMapper::ToServer(const string &path) const {
	return Map(path, client_path, server_path);
}

string RESTPathMapper::ToClient(const string &path) const {
	return Map(path, server_path, client_path);
}

static thread_local optional_ptr<const RESTPathMapper> current_path_mapper;

RESTPathScope::RESTPathScope(optional_ptr<const RESTPathMapper> mapper) : previous(current_path_mapper) {
	current_path_mapper = mapper;
}

RESTPathScope::~RESTPathScope() {
	current_path_mapper = previous;
}

optional_ptr<const RESTPathMapper> RESTPathScope::Current() {
	return current_path_mapper;
}

JSONMutableValue RESTPathCodec::Write(JSONWriter &writer, const string &value) {
	auto mapper = RESTPathScope::Current();
	return writer.CreateString(mapper ? mapper->ToServer(value) : value);
}

void RESTPathCodec::Read(RESTJsonIn in, string &result) {
	auto mapper = RESTPathScope::Current();
	result = in.Str();
	if (mapper) {
		result = mapper->ToClient(result);
	}
}

JSONMutableValue RESTBase64Codec::Write(JSONWriter &writer, const string &value) {
	return writer.CreateString(Blob::ToBase64(string_t(value)));
}

void RESTBase64Codec::Read(RESTJsonIn in, string &result) {
	result = Blob::FromBase64(string_t(in.Str()));
}

JSONMutableValue RESTTimestampCodec::Write(JSONWriter &writer, timestamp_tz_t value) {
	return writer.CreateString(Value::TIMESTAMPTZ(value).ToString());
}

JSONMutableValue RESTTimestampCodec::Write(JSONWriter &writer, const string &value) {
	return writer.CreateString(value);
}

void RESTTimestampCodec::Read(RESTJsonIn in, timestamp_tz_t &result) {
	result = Value(in.Str()).DefaultCastAs(LogicalType::TIMESTAMP_TZ).GetValue<timestamp_tz_t>();
}

void RESTTimestampCodec::Read(RESTJsonIn in, string &result) {
	// timestamps that are kept as strings end up in metadata queries
	result = in.Str();
	Value(result).DefaultCastAs(LogicalType::TIMESTAMP_TZ);
}

} // namespace duckdb
