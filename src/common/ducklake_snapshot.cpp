#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void DuckLakeSnapshot::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "snapshot_id", snapshot_id);
	serializer.WriteProperty(101, "schema_version", schema_version);
	serializer.WriteProperty(102, "next_catalog_id", next_catalog_id);
	serializer.WriteProperty(103, "next_file_id", next_file_id);
	serializer.WritePropertyWithDefault<idx_t>(104, "branch_id", branch_id, 0);
}

DuckLakeSnapshot DuckLakeSnapshot::Deserialize(Deserializer &deserializer) {
	DuckLakeSnapshot result;
	result.snapshot_id = deserializer.ReadProperty<idx_t>(100, "snapshot_id");
	result.schema_version = deserializer.ReadProperty<idx_t>(101, "schema_version");
	result.next_catalog_id = deserializer.ReadProperty<idx_t>(102, "next_catalog_id");
	result.next_file_id = deserializer.ReadProperty<idx_t>(103, "next_file_id");
	result.branch_id = deserializer.ReadPropertyWithExplicitDefault<idx_t>(104, "branch_id", 0);
	return result;
}

} // namespace duckdb
