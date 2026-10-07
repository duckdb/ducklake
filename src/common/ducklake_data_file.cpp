#include "common/ducklake_data_file.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string DeleteFileFormatToString(DeleteFileFormat format) {
	switch (format) {
	case DeleteFileFormat::PARQUET:
		return "parquet";
	case DeleteFileFormat::PUFFIN:
		return "puffin";
	default:
		throw InternalException("Unknown DeleteFileFormat");
	}
}

DeleteFileFormat DeleteFileFormatFromString(const string &str) {
	if (StringUtil::CIEquals(str, "parquet")) {
		return DeleteFileFormat::PARQUET;
	} else if (StringUtil::CIEquals(str, "puffin")) {
		return DeleteFileFormat::PUFFIN;
	}
	throw InvalidInputException("Unknown delete file format: %s", str);
}

} // namespace duckdb
