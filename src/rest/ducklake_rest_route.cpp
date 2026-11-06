#include "rest/ducklake_rest_api.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static bool IsUnreserved(char c) {
	return StringUtil::CharacterIsAlpha(c) || StringUtil::CharacterIsDigit(c) || c == '-' || c == '_' || c == '.' ||
	       c == '~';
}

static string UrlEncode(const string &value) {
	static constexpr const char *HEX = "0123456789ABCDEF";
	string result;
	for (auto c : value) {
		if (IsUnreserved(c)) {
			result += c;
			continue;
		}
		auto byte = static_cast<uint8_t>(c);
		result += '%';
		result += HEX[byte >> 4];
		result += HEX[byte & 15];
	}
	return result;
}

//! The text of a scalar of a request, as it is written in a URL
static string ScalarText(const RESTJsonIn &value) {
	return value.val.IsString() ? value.val.GetString() : value.val.ToString();
}

string RESTRoute::ClientTarget(const string &request_json, const string &prefix, string &body) const {
	RESTJsonReader document(request_json);
	auto root = document.Root();
	if (!root.IsObject()) {
		throw InternalException("DuckLake REST: the request of \"%s\" is not an object", endpoint);
	}
	// the paths start with the version
	string target = prefix.empty() || no_prefix ? path : "/v1/" + UrlEncode(prefix) + path.substr(3);
	string query;
	RESTJsonWriter writer;
	auto remaining = writer.writer.CreateObject();
	idx_t remaining_count = 0;
	for (auto &member : root.Members()) {
		const RESTRouteParameter *parameter = nullptr;
		for (auto &candidate : parameters) {
			if (candidate.name == member.first) {
				parameter = &candidate;
				break;
			}
		}
		if (!parameter) {
			remaining.Add(member.first, writer.writer.CreateCopy(member.second.val));
			remaining_count++;
			continue;
		}
		if (member.second.IsNull()) {
			continue;
		}
		if (parameter->query_name.empty()) {
			target = StringUtil::Replace(target, "{" + parameter->name + "}", UrlEncode(ScalarText(member.second)));
			continue;
		}
		vector<string> values;
		if (member.second.IsArray()) {
			for (auto &item : member.second.Items()) {
				values.push_back(ScalarText(item));
			}
		} else {
			values.push_back(ScalarText(member.second));
		}
		for (auto &value : values) {
			query += query.empty() ? "?" : "&";
			query += parameter->query_name + "=" + UrlEncode(value);
		}
	}
	if (method == "GET") {
		if (remaining_count != 0) {
			throw InternalException("DuckLake REST: the request of \"%s\" has members that are not parameters",
			                        endpoint);
		}
		body.clear();
	} else {
		writer.SetRoot(remaining);
		body = writer.ToString();
	}
	return target + query;
}

string RESTRoute::PathPattern(bool with_prefix) const {
	string result;
	idx_t pos = 0;
	while (pos < path.size()) {
		auto start = path.find('{', pos);
		if (start == string::npos) {
			result += path.substr(pos);
			break;
		}
		result += path.substr(pos, start - pos);
		result += "([^/]+)";
		pos = path.find('}', start) + 1;
	}
	if (with_prefix) {
		// the prefix of the catalog follows the version
		result = "/v1/([^/]+)" + result.substr(3);
	}
	return result;
}

static void ThrowInvalidParameter(const string &name, const string &value) {
	throw InvalidInputException("DuckLake REST: \"%s\" is not a valid value for the parameter \"%s\"", value, name);
}

static JSONMutableValue ParameterToJson(JSONWriter &writer, const RESTRouteParameter &parameter, const string &value) {
	switch (parameter.type) {
	case RESTParameterType::STRING:
		return writer.CreateString(value);
	case RESTParameterType::UINT: {
		uint64_t result;
		if (!TryCast::Operation<string_t, uint64_t>(string_t(value), result)) {
			ThrowInvalidParameter(parameter.name, value);
		}
		return writer.CreateUnsignedInteger(result);
	}
	case RESTParameterType::INT: {
		int64_t result;
		if (!TryCast::Operation<string_t, int64_t>(string_t(value), result)) {
			ThrowInvalidParameter(parameter.name, value);
		}
		return writer.CreateSignedInteger(result);
	}
	case RESTParameterType::BOOL:
		if (value != "true" && value != "false") {
			ThrowInvalidParameter(parameter.name, value);
		}
		return writer.CreateBoolean(value == "true");
	default:
		throw InternalException("Parameter of a list type outside of a list");
	}
}

string RESTRoute::ServerRequest(const vector<string> &path_values, const vector<pair<string, string>> &query,
                                const string &body) const {
	RESTJsonWriter writer;
	auto result = writer.writer.CreateObject();
	idx_t path_index = 0;
	for (auto &parameter : parameters) {
		if (parameter.query_name.empty()) {
			if (path_index >= path_values.size()) {
				throw InternalException("DuckLake REST: the path of \"%s\" has no value for \"%s\"", endpoint,
				                        parameter.name);
			}
			result.Add(parameter.name, ParameterToJson(writer.writer, parameter, path_values[path_index++]));
			continue;
		}
		vector<string> values;
		for (auto &entry : query) {
			if (entry.first == parameter.query_name) {
				values.push_back(entry.second);
			}
		}
		if (values.empty()) {
			continue;
		}
		if (parameter.type == RESTParameterType::UINT_LIST) {
			auto list = writer.writer.CreateArray();
			RESTRouteParameter element {parameter.name, RESTParameterType::UINT, string()};
			for (auto &value : values) {
				list.Append(ParameterToJson(writer.writer, element, value));
			}
			result.Add(parameter.name, list);
		} else {
			result.Add(parameter.name, ParameterToJson(writer.writer, parameter, values[0]));
		}
	}
	if (!body.empty()) {
		RESTJsonReader document(body);
		auto root = document.Root();
		if (!root.IsObject()) {
			throw InvalidInputException("DuckLake REST: the body of a request has to be a JSON object");
		}
		for (auto &member : root.Members()) {
			result.Add(member.first, writer.writer.CreateCopy(member.second.val));
		}
	}
	writer.SetRoot(result);
	return writer.ToString();
}

} // namespace duckdb
