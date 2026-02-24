#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeSettingsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();

	names.emplace_back("catalog_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("extension_version");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("data_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<MetadataBindData>();
	vector<Value> row_values;

	// catalog_type: normalize the metadata type to a user-friendly name
	auto catalog_type = ducklake_catalog.MetadataType();
	if (catalog_type == "postgres_scanner") {
		catalog_type = "postgres";
	} else if (catalog_type == "sqlite_scanner") {
		catalog_type = "sqlite";
	} else if (catalog_type.empty() || catalog_type == "motherduck" || catalog_type == "md_server") {
		// default to "duckdb" since DuckDB is the default metadata storage
		catalog_type = "duckdb";
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
