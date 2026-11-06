#include "rest/ducklake_rest_multipart.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>

namespace duckdb {

const char *const REST_ARROW_STREAM = "application/vnd.apache.arrow.stream";

RESTMultipartWriter::RESTMultipartWriter() {
	static atomic<idx_t> counter {0};
	RandomEngine random(counter++ ^ NumericCast<idx_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
	boundary = StringUtil::Format("ducklake-%llx-%llx", random.NextRandomInteger(), random.NextRandomInteger());
}

void RESTMultipartWriter::AddPart(const string &name, const string &content_type, const string &content) {
	body += "--" + boundary + "\r\n";
	string headers = "Content-Type: " + content_type + "\r\nContent-Disposition: form-data; name=\"" + name + "\"\r\n";
	auto data_offset = body.size() + headers.size() + 2;
	auto padding = (8 - data_offset % 8) % 8;
	if (padding > 0) {
		// a header of at least 6 bytes: "X:" and the line break
		padding += padding < 5 ? 8 : 0;
		headers += "X:" + string(padding - 4, ' ') + "\r\n";
	}
	body += headers + "\r\n" + content + "\r\n";
}

string RESTMultipartWriter::Finish(string &content_type) {
	body += "--" + boundary + "--\r\n";
	content_type = "multipart/mixed; boundary=" + boundary;
	return std::move(body);
}

bool RESTMultipart::IsMultipart(const string &content_type) {
	return StringUtil::StartsWith(content_type, "multipart/mixed");
}

vector<RESTMultipartPart> RESTMultipart::Parse(const string &body, const string &content_type) {
	auto boundary_start = content_type.find("boundary=");
	if (boundary_start == string::npos) {
		throw InvalidInputException("The multipart message has no boundary");
	}
	auto boundary = content_type.substr(boundary_start + 9);
	if (!boundary.empty() && boundary.front() == '"') {
		boundary = boundary.substr(1, boundary.find('"', 1) - 1);
	}
	auto delimiter = "--" + boundary;
	vector<RESTMultipartPart> parts;
	auto position = body.find(delimiter);
	while (position != string::npos) {
		position += delimiter.size();
		if (body.compare(position, 2, "--") == 0) {
			break;
		}
		position = body.find("\r\n", position);
		if (position == string::npos) {
			break;
		}
		position += 2;
		auto headers_end = body.find("\r\n\r\n", position - 2);
		if (headers_end == string::npos) {
			throw InvalidInputException("The multipart message is malformed");
		}
		RESTMultipartPart part;
		auto headers = body.substr(position, headers_end + 2 - position);
		for (auto &line : StringUtil::Split(headers, "\r\n")) {
			auto colon = line.find(':');
			if (colon == string::npos) {
				continue;
			}
			auto key = StringUtil::Lower(line.substr(0, colon));
			auto value = line.substr(colon + 1);
			StringUtil::Trim(value);
			if (key == "content-type") {
				part.content_type = value;
			} else if (key == "content-disposition") {
				auto name_start = value.find("name=\"");
				if (name_start != string::npos) {
					name_start += 6;
					part.name = value.substr(name_start, value.find('"', name_start) - name_start);
				}
			}
		}
		part.offset = headers_end + 4;
		auto next = body.find("\r\n" + delimiter, part.offset);
		if (next == string::npos) {
			throw InvalidInputException("The multipart message is malformed");
		}
		part.size = next - part.offset;
		parts.push_back(std::move(part));
		position = next + 2;
	}
	return parts;
}

optional_ptr<const RESTMultipartPart> RESTMultipart::TryFind(const vector<RESTMultipartPart> &parts,
                                                             const string &name) {
	for (auto &part : parts) {
		if (part.name == name) {
			return part;
		}
	}
	return nullptr;
}

const RESTMultipartPart &RESTMultipart::Find(const vector<RESTMultipartPart> &parts, const string &name) {
	auto part = TryFind(parts, name);
	if (!part) {
		throw InvalidInputException("The multipart message has no part \"%s\"", name);
	}
	return *part;
}

} // namespace duckdb
