#include "common/ducklake_version.hpp"
#include "duckdb.hpp"

namespace duckdb {

DuckLakeVersion DuckLakeVersionFromString(const string &version_str) {
	if (version_str == "1.0") {
		return DuckLakeVersion::V1_0;
	}
	throw InvalidInputException("Unsupported ducklake_version '%s'. Only '1.0' is currently supported", version_str);
}

string DuckLakeVersionToString(DuckLakeVersion version) {
	switch (version) {
	case DuckLakeVersion::V1_0:
		return "1.0";
	default:
		throw InternalException("DuckLakeVersionToString: unknown version");
	}
}

} // namespace duckdb
