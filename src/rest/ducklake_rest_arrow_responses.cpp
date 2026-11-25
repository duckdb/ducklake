#include "rest/ducklake_rest_arrow_responses.hpp"

#include "rest/ducklake_rest_arrow.hpp"
#include "rest/ducklake_rest_arrow_files.hpp"
#include "rest/ducklake_rest_multipart.hpp"
#include "rest/ducklake_rest_server.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {

//! The Arrow IPC stream of inlined rows
string WriteInlinedRows(ClientContext &context, const RESTInlinedRows &rows) {
	vector<string> names;
	for (auto &column : rows.columns) {
		names.push_back(column.name);
	}
	return RESTArrow::Write(context, names, *rows.rows.rows);
}

unique_ptr<RESTInlinedRows> ReadInlinedRows(ClientContext &context, const shared_ptr<const string> &body,
                                            const RESTMultipartPart &part) {
	auto result = make_uniq<RESTInlinedRows>();
	vector<string> names;
	auto collection = RESTArrow::Read(context, body, part.offset, part.size, {}, names);
	for (idx_t i = 0; i < names.size(); i++) {
		RESTResultColumn column;
		column.name = names[i];
		column.type = collection->Types()[i];
		result->columns.push_back(std::move(column));
	}
	result->rows = RESTRows(std::move(collection));
	return result;
}

//! The response with two lists of files: a multipart response with a part for each list, if the client accepts them
template <class RESPONSE, class FIRST, class SECOND>
string WriteLists(DuckLakeRESTRequest &request, RESPONSE &value, vector<FIRST> RESPONSE::*first_member,
                  const char *first_name, vector<SECOND> RESPONSE::*second_member, const char *second_name) {
	bool has_lists = !(value.*first_member).empty() || !(value.*second_member).empty() || value.inlined_data;
	if (!request.accepts_arrow || !has_lists) {
		return DuckLakeRESTSerializer::ToJSON(value);
	}
	auto &context = *request.connection.context;
	RESTMultipartWriter multipart;
	vector<FIRST> first;
	first.swap(value.*first_member);
	vector<SECOND> second;
	second.swap(value.*second_member);
	multipart.AddPart("response", "application/json", DuckLakeRESTSerializer::ToJSON(value));
	if (!first.empty()) {
		multipart.AddPart(first_name, REST_ARROW_STREAM, RESTArrowFiles::Write(context, first));
	}
	if (!second.empty()) {
		multipart.AddPart(second_name, REST_ARROW_STREAM, RESTArrowFiles::Write(context, second));
	}
	if (value.inlined_data) {
		multipart.AddPart("inlined_data", REST_ARROW_STREAM, WriteInlinedRows(context, *value.inlined_data));
	}
	return multipart.Finish(request.content_type);
}

template <class RESPONSE, class FIRST, class SECOND>
RESPONSE ReadLists(RESTHttpResponse &response, vector<FIRST> RESPONSE::*first_member, const char *first_name,
                   vector<SECOND> RESPONSE::*second_member, const char *second_name) {
	if (!RESTMultipart::IsMultipart(response.content_type)) {
		RESTJsonReader document(response.body);
		return DuckLakeRESTSerializer::Read<RESPONSE>(document.Root());
	}
	auto body = make_shared_ptr<const string>(std::move(response.body));
	auto parts = RESTMultipart::Parse(*body, response.content_type);
	auto &json_part = RESTMultipart::Find(parts, "response");
	RESTJsonReader document(body->substr(json_part.offset, json_part.size));
	auto result = DuckLakeRESTSerializer::Read<RESPONSE>(document.Root());
	auto &context = *response.context;
	auto first = RESTMultipart::TryFind(parts, first_name);
	if (first) {
		RESTArrowFiles::Read(context, body, first->offset, first->size, result.*first_member);
	}
	auto second = RESTMultipart::TryFind(parts, second_name);
	if (second) {
		RESTArrowFiles::Read(context, body, second->offset, second->size, result.*second_member);
	}
	auto inlined_data = RESTMultipart::TryFind(parts, "inlined_data");
	if (inlined_data) {
		result.inlined_data = ReadInlinedRows(context, body, *inlined_data);
	}
	return result;
}

} // namespace

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
template <>
string DuckLakeRESTSerializer::ToResponse<RESTSelectResponse>(DuckLakeRESTRequest &request, RESTSelectResponse &value) {
	return WriteLists(request, value, &RESTSelectResponse::files, "files", &RESTSelectResponse::extended_files,
	                  "extended_files");
}

template <>
RESTSelectResponse DuckLakeRESTSerializer::ReadResponse<RESTSelectResponse>(RESTHttpResponse &response) {
	return ReadLists(response, &RESTSelectResponse::files, "files", &RESTSelectResponse::extended_files,
	                 "extended_files");
}

//===--------------------------------------------------------------------===//
// TableChanges
//===--------------------------------------------------------------------===//
template <>
string DuckLakeRESTSerializer::ToResponse<RESTTableChangesResponse>(DuckLakeRESTRequest &request,
                                                                    RESTTableChangesResponse &value) {
	return WriteLists(request, value, &RESTTableChangesResponse::insertions, "insertions",
	                  &RESTTableChangesResponse::deletions, "deletions");
}

template <>
RESTTableChangesResponse DuckLakeRESTSerializer::ReadResponse<RESTTableChangesResponse>(RESTHttpResponse &response) {
	return ReadLists(response, &RESTTableChangesResponse::insertions, "insertions",
	                 &RESTTableChangesResponse::deletions, "deletions");
}

//===--------------------------------------------------------------------===//
// CommitTransaction
//===--------------------------------------------------------------------===//
template <>
void DuckLakeRESTSerializer::AddParts<RESTCommitTransactionRequest>(ClientContext &context,
                                                                    const RESTCommitTransactionRequest &value,
                                                                    vector<RESTBodyPart> &parts) {
	for (idx_t i = 0; i < value.requests.size(); i++) {
		auto &request = *value.requests[i];
		if (request.type != RESTInsertRowsRequest::TYPE) {
			continue;
		}
		auto &rows = *request.Cast<RESTInsertRowsRequest>().rows.rows;
		vector<string> names;
		for (idx_t column = 0; column < rows.ColumnCount(); column++) {
			names.push_back("column" + to_string(column));
		}
		parts.push_back({"rows." + to_string(i), RESTArrow::Write(context, names, rows)});
	}
}

template <>
RESTCommitTransactionRequest
DuckLakeRESTSerializer::ReadRequest<RESTCommitTransactionRequest>(DuckLakeRESTRequest &request, const string &body) {
	RESTJsonReader document(body);
	auto result = Read<RESTCommitTransactionRequest>(document.Root());
	for (idx_t i = 0; i < result.requests.size(); i++) {
		auto &commit_request = *result.requests[i];
		if (commit_request.type != RESTInsertRowsRequest::TYPE) {
			continue;
		}
		auto part = RESTMultipart::TryFind(request.request_parts, "rows." + to_string(i));
		if (!part) {
			throw InvalidInputException("The request %llu inserts rows, and has no part \"rows.%llu\" with the rows", i,
			                            i);
		}
		vector<string> names;
		commit_request.Cast<RESTInsertRowsRequest>().rows = RESTRows(
		    RESTArrow::Read(*request.connection.context, request.request_data, part->offset, part->size, {}, names));
	}
	return result;
}

} // namespace duckdb
