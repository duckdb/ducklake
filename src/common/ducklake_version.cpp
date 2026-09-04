#include "common/ducklake_version.hpp"
#include "duckdb.hpp"

namespace duckdb {

DuckLakeVersion DuckLakeVersionFromString(const string &version_str) {
	if (version_str == "0.1") {
		return DuckLakeVersion::V0_1;
	}
	if (version_str == "0.2") {
		return DuckLakeVersion::V0_2;
	}
	if (version_str == "0.3-dev1") {
		return DuckLakeVersion::V0_3_DEV1;
	}
	if (version_str == "0.3") {
		return DuckLakeVersion::V0_3;
	}
	if (version_str == "0.4-dev1") {
		return DuckLakeVersion::V0_4_DEV1;
	}
	if (version_str == "0.4") {
		return DuckLakeVersion::V0_4;
	}
	if (version_str == "1.0") {
		return DuckLakeVersion::V1_0;
	}
	if (version_str == "1.1-dev1") {
		return DuckLakeVersion::V1_1_DEV_1;
	}
	throw InvalidInputException("Unsupported ducklake_version '%s'", version_str);
}

string DuckLakeVersionToString(DuckLakeVersion version) {
	switch (version) {
	case DuckLakeVersion::V0_1:
		return "0.1";
	case DuckLakeVersion::V0_2:
		return "0.2";
	case DuckLakeVersion::V0_3_DEV1:
		return "0.3-dev1";
	case DuckLakeVersion::V0_3:
		return "0.3";
	case DuckLakeVersion::V0_4_DEV1:
		return "0.4-dev1";
	case DuckLakeVersion::V0_4:
		return "0.4";
	case DuckLakeVersion::V1_0:
		return "1.0";
	case DuckLakeVersion::V1_1_DEV_1:
		return "1.1-dev1";
	default:
		throw InternalException("DuckLakeVersionToString: unknown version");
	}
}

void ThrowUnsupportedByVersion(DuckLakeVersion version, const string &feature) {
	throw InvalidInputException("DuckLake %s does not support %s - attach with AUTOMATIC_MIGRATION set to TRUE to "
	                            "migrate the catalog to a newer version",
	                            DuckLakeVersionToString(version), feature);
}

} // namespace duckdb
