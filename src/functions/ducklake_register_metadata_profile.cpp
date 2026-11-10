#include "functions/ducklake_table_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_metadata_profile.hpp"

namespace duckdb {

struct DuckLakeRegisterMetadataProfileData final : public TableFunctionData {
	DuckLakeRegisterMetadataProfileData(string name_p, string profile_p, bool replace_p)
	    : name(std::move(name_p)), profile(std::move(profile_p)), replace(replace_p) {
	}

	string name;
	string profile;
	bool replace;
};

static unique_ptr<FunctionData> DuckLakeRegisterMetadataProfileBind(ClientContext &context,
                                                                    TableFunctionBindInput &input,
                                                                    vector<LogicalType> &return_types,
                                                                    vector<Identifier> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("ducklake_register_metadata_profile does not accept NULL arguments");
		}
	}
	bool replace = true;
	Value replace_value;
	if (DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "replace", "boolean", replace_value)) {
		replace = BooleanValue::Get(replace_value);
	}
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	return make_uniq<DuckLakeRegisterMetadataProfileData>(StringValue::Get(input.inputs[0]),
	                                                      StringValue::Get(input.inputs[1]), replace);
}

static void DuckLakeRegisterMetadataProfileExecute(ClientContext &context, TableFunctionInput &data_p,
                                                   DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<DuckLakeRegisterMetadataProfileData>();
	DuckLakeMetadataProfiles::Get(*context.db).Register(bind_data.name, bind_data.profile, bind_data.replace);
}

DuckLakeRegisterMetadataProfileFunction::DuckLakeRegisterMetadataProfileFunction()
    : TableFunction("ducklake_register_metadata_profile",
                    FunctionSignature()
                        .AddPositionalOnly("name", LogicalType::VARCHAR)
                        .AddPositionalOnly("profile", LogicalType::VARCHAR),
                    DuckLakeRegisterMetadataProfileExecute, DuckLakeRegisterMetadataProfileBind) {
	GetSignature().WithTypedKwargs("options",
	                               [&](TypedKwargs &options) { options.Add("replace", LogicalType::BOOLEAN); });
}

} // namespace duckdb
