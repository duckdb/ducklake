#include "common/ducklake_version.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static constexpr StringUtil::EnumStringLiteral DUCKLAKE_VERSIONS[] = {
    {static_cast<uint32_t>(DuckLakeVersion::V0_1), "0.1"},
    {static_cast<uint32_t>(DuckLakeVersion::V0_2), "0.2"},
    {static_cast<uint32_t>(DuckLakeVersion::V0_3_DEV1), "0.3-dev1"},
    {static_cast<uint32_t>(DuckLakeVersion::V0_3), "0.3"},
    {static_cast<uint32_t>(DuckLakeVersion::V0_4_DEV1), "0.4-dev1"},
    {static_cast<uint32_t>(DuckLakeVersion::V0_4), "0.4"},
    {static_cast<uint32_t>(DuckLakeVersion::V1_0), "1.0"},
    {static_cast<uint32_t>(DuckLakeVersion::V1_1_DEV_1), "1.1-dev1"}};

DuckLakeVersion DuckLakeVersionFromString(const string &version_str) {
	for (auto &entry : DUCKLAKE_VERSIONS) {
		if (version_str == entry.string) {
			return static_cast<DuckLakeVersion>(entry.number);
		}
	}
	throw InvalidInputException("Unsupported ducklake_version '%s'", version_str);
}

DuckLakeVersion ParseWritableDuckLakeVersion(const string &version_str, const string &option_name) {
	auto version = DuckLakeVersionFromString(version_str);
	if (version < DuckLakeVersion::V1_0) {
		throw InvalidInputException("%s must be >= '1.0', got '%s'", option_name, version_str);
	}
	return version;
}

string DuckLakeVersionToString(DuckLakeVersion version) {
	return StringUtil::EnumToString(DUCKLAKE_VERSIONS, sizeof(DUCKLAKE_VERSIONS) / sizeof(DUCKLAKE_VERSIONS[0]),
	                                "DuckLakeVersion", static_cast<uint32_t>(version));
}

void ThrowUnsupportedByVersion(DuckLakeVersion version, const string &feature) {
	throw InvalidInputException("DuckLake %s does not support %s - attach with AUTOMATIC_MIGRATION set to TRUE to "
	                            "migrate the catalog to a newer version",
	                            DuckLakeVersionToString(version), feature);
}

} // namespace duckdb
