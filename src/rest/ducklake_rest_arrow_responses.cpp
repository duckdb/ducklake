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

//! The JSON document of a response in its part of a multipart response
template <class RESPONSE>
RESPONSE ReadJSONPart(const shared_ptr<const string> &body, const RESTMultipartPart &part) {
	RESTJsonReader document(body->substr(part.offset, part.size));
	return DuckLakeRESTSerializer::Read<RESPONSE>(document.Root());
}

} // namespace

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
template <>
string DuckLakeRESTSerializer::ToResponse<RESTSelectResponse>(DuckLakeRESTRequest &request, RESTSelectResponse &value) {
	bool has_lists = !value.files.empty() || !value.extended_files.empty() || value.inlined_data;
	if (!request.accepts_arrow || !has_lists) {
		return ToJSON(value);
	}
	auto &context = *request.connection.context;
	RESTMultipartWriter multipart;
	vector<DuckLakeFileListEntry> files;
	files.swap(value.files);
	vector<DuckLakeFileListExtendedEntry> extended_files;
	extended_files.swap(value.extended_files);
	multipart.AddPart("response", "application/json", ToJSON(value));
	if (!files.empty()) {
		multipart.AddPart("files", REST_ARROW_STREAM, RESTArrowFiles::Write(context, files));
	}
	if (!extended_files.empty()) {
		multipart.AddPart("extended_files", REST_ARROW_STREAM, RESTArrowFiles::Write(context, extended_files));
	}
	if (value.inlined_data) {
		multipart.AddPart("inlined_data", REST_ARROW_STREAM, WriteInlinedRows(context, *value.inlined_data));
	}
	return multipart.Finish(request.content_type);
}

template <>
RESTSelectResponse DuckLakeRESTSerializer::ReadResponse<RESTSelectResponse>(RESTHttpResponse &response) {
	if (!RESTMultipart::IsMultipart(response.content_type)) {
		RESTJsonReader document(response.body);
		return Read<RESTSelectResponse>(document.Root());
	}
	auto body = make_shared_ptr<const string>(std::move(response.body));
	auto parts = RESTMultipart::Parse(*body, response.content_type);
	auto result = ReadJSONPart<RESTSelectResponse>(body, RESTMultipart::Find(parts, "response"));
	auto &context = *response.context;
	auto files = RESTMultipart::TryFind(parts, "files");
	if (files) {
		RESTArrowFiles::Read(context, body, files->offset, files->size, result.files);
	}
	auto extended_files = RESTMultipart::TryFind(parts, "extended_files");
	if (extended_files) {
		RESTArrowFiles::Read(context, body, extended_files->offset, extended_files->size, result.extended_files);
	}
	auto inlined_data = RESTMultipart::TryFind(parts, "inlined_data");
	if (inlined_data) {
		result.inlined_data = ReadInlinedRows(context, body, *inlined_data);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// TableChanges
//===--------------------------------------------------------------------===//
template <>
string DuckLakeRESTSerializer::ToResponse<RESTTableChangesResponse>(DuckLakeRESTRequest &request,
                                                                    RESTTableChangesResponse &value) {
	bool has_lists = !value.insertions.empty() || !value.deletions.empty() || value.inlined_data;
	if (!request.accepts_arrow || !has_lists) {
		return ToJSON(value);
	}
	auto &context = *request.connection.context;
	RESTMultipartWriter multipart;
	vector<DuckLakeFileListEntry> insertions;
	insertions.swap(value.insertions);
	vector<DuckLakeDeleteScanEntry> deletions;
	deletions.swap(value.deletions);
	multipart.AddPart("response", "application/json", ToJSON(value));
	if (!insertions.empty()) {
		multipart.AddPart("insertions", REST_ARROW_STREAM, RESTArrowFiles::Write(context, insertions));
	}
	if (!deletions.empty()) {
		multipart.AddPart("deletions", REST_ARROW_STREAM, RESTArrowFiles::Write(context, deletions));
	}
	if (value.inlined_data) {
		multipart.AddPart("inlined_data", REST_ARROW_STREAM, WriteInlinedRows(context, *value.inlined_data));
	}
	return multipart.Finish(request.content_type);
}

template <>
RESTTableChangesResponse DuckLakeRESTSerializer::ReadResponse<RESTTableChangesResponse>(RESTHttpResponse &response) {
	if (!RESTMultipart::IsMultipart(response.content_type)) {
		RESTJsonReader document(response.body);
		return Read<RESTTableChangesResponse>(document.Root());
	}
	auto body = make_shared_ptr<const string>(std::move(response.body));
	auto parts = RESTMultipart::Parse(*body, response.content_type);
	auto result = ReadJSONPart<RESTTableChangesResponse>(body, RESTMultipart::Find(parts, "response"));
	auto &context = *response.context;
	auto insertions = RESTMultipart::TryFind(parts, "insertions");
	if (insertions) {
		RESTArrowFiles::Read(context, body, insertions->offset, insertions->size, result.insertions);
	}
	auto deletions = RESTMultipart::TryFind(parts, "deletions");
	if (deletions) {
		RESTArrowFiles::Read(context, body, deletions->offset, deletions->size, result.deletions);
	}
	auto inlined_data = RESTMultipart::TryFind(parts, "inlined_data");
	if (inlined_data) {
		result.inlined_data = ReadInlinedRows(context, body, *inlined_data);
	}
	return result;
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
