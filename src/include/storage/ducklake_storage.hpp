//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "storage/ducklake_metadata_profile.hpp"

namespace duckdb {

class DuckLakeStorageExtension : public StorageExtension {
public:
	DuckLakeStorageExtension();

	DuckLakeMetadataProfiles metadata_profiles;
};

} // namespace duckdb
