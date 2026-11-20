#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_profile.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeSettingsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &ducklake_catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);

	names.emplace_back("catalog_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("extension_version");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("data_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<MetadataBindData>();
	vector<Value> row_values;

	// catalog_type: the catalog type of the metadata database, unless its profile reports another
	auto catalog_type = ducklake_catalog.MetadataCatalogType();
	auto &metadata_type = ducklake_catalog.MetadataType();
	if (metadata_type == "md" || metadata_type == "motherduck" || metadata_type == "md_server") {
		catalog_type = "duckdb";
	}
	auto &reported_catalog_type = ducklake_catalog.MetadataProfile().reported_catalog_type;
	if (!reported_catalog_type.empty()) {
		catalog_type = reported_catalog_type;
	}
	row_values.push_back(Value(catalog_type));

#ifdef EXT_VERSION_DUCKLAKE
	row_values.push_back(Value(EXT_VERSION_DUCKLAKE));
#else
	row_values.push_back(Value(""));
#endif

	row_values.push_back(Value(ducklake_catalog.DataPath()));

	result->rows.push_back(std::move(row_values));
	return std::move(result);
}

DuckLakeSettingsFunction::DuckLakeSettingsFunction()
    : DuckLakeBaseMetadataFunction("ducklake_settings", DuckLakeSettingsBind) {
}

} // namespace duckdb
