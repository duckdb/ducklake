//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_arrow_files.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class ClientContext;

//! The lists of files of a scan, as the Arrow tables that the REST API sends them as. Columns whose values are the
//! default are NULL
struct RESTArrowFiles {
	static string Write(ClientContext &context, const vector<DuckLakeFileListEntry> &files);
	static string Write(ClientContext &context, const vector<DuckLakeFileListExtendedEntry> &files);
	static string Write(ClientContext &context, const vector<DuckLakeDeleteScanEntry> &files);

	//! Reads the files of an Arrow IPC stream from a region of the data, the paths are mapped to the paths of the
	//! client
	static void Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
	                 vector<DuckLakeFileListEntry> &result);
	static void Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
	                 vector<DuckLakeFileListExtendedEntry> &result);
	static void Read(ClientContext &context, shared_ptr<const string> data, idx_t offset, idx_t size,
	                 vector<DuckLakeDeleteScanEntry> &result);
};

} // namespace duckdb
