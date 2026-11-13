//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_multipart.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! The type of the content of the parts that hold tables
extern const char *const REST_ARROW_STREAM;

//! A part of a multipart/mixed message: a region of the body
struct RESTMultipartPart {
	string name;
	string content_type;
	idx_t offset;
	idx_t size;
};

//! Writes a multipart/mixed message in memory. The content of each part starts at a multiple of 8 bytes of the message,
//! as Arrow arrays have to be aligned
class RESTMultipartWriter {
public:
	RESTMultipartWriter();

	void AddPart(const string &name, const string &content_type, const string &content);
	//! The message, and its content type
	string Finish(string &content_type);

private:
	string boundary;
	string body;
};

struct RESTMultipart {
	//! Whether a content type is the one of a multipart message
	static bool IsMultipart(const string &content_type);
	//! The parts of a message
	static vector<RESTMultipartPart> Parse(const string &body, const string &content_type);
	static optional_ptr<const RESTMultipartPart> TryFind(const vector<RESTMultipartPart> &parts, const string &name);
	static const RESTMultipartPart &Find(const vector<RESTMultipartPart> &parts, const string &name);
};

} // namespace duckdb
