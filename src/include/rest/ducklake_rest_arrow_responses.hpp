//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_arrow_responses.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_api.hpp"

namespace duckdb {

//! The responses with lists of files and rows, which are sent as multipart responses when the client accepts them:
//! the first part is the JSON document of the response without the lists, a part for each list is an Arrow IPC stream
template <>
string DuckLakeRESTSerializer::ToResponse<RESTSelectResponse>(DuckLakeRESTRequest &request, RESTSelectResponse &value);
template <>
RESTSelectResponse DuckLakeRESTSerializer::ReadResponse<RESTSelectResponse>(RESTHttpResponse &response);
template <>
string DuckLakeRESTSerializer::ToResponse<RESTTableChangesResponse>(DuckLakeRESTRequest &request,
                                                                    RESTTableChangesResponse &value);
template <>
RESTTableChangesResponse DuckLakeRESTSerializer::ReadResponse<RESTTableChangesResponse>(RESTHttpResponse &response);

//! The requests with tables: the JSON document of the commit of a transaction and the rows to insert as parts of a
//! multipart request
template <>
void DuckLakeRESTSerializer::AddParts<RESTCommitTransactionRequest>(ClientContext &context,
                                                                    const RESTCommitTransactionRequest &value,
                                                                    vector<RESTBodyPart> &parts);
template <>
RESTCommitTransactionRequest
DuckLakeRESTSerializer::ReadRequest<RESTCommitTransactionRequest>(DuckLakeRESTRequest &request, const string &body);

} // namespace duckdb
