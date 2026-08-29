#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_encryption_provider.hpp"

namespace duckdb {

struct SelfTestBindData : public TableFunctionData {
	optional_ptr<DuckLakeCatalog> catalog;
};

struct SelfTestGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> DuckLakeSelfTestBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<SelfTestBindData>();

	if (input.inputs.size() > 1) {
		throw InvalidInputException("ducklake_self_test takes at most one argument: the name of an attached DuckLake");
	}
	if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
		auto db_name = input.inputs[0].GetValue<string>();
		auto &db_manager = DatabaseManager::Get(context);
		auto db = db_manager.GetDatabase(context, Identifier(db_name));
		if (!db) {
			throw InvalidInputException("ducklake_self_test: failed to find attached database \"%s\"", db_name);
		}
		auto &catalog_obj = db->GetCatalog();
		if (catalog_obj.GetCatalogType() != "ducklake") {
			throw InvalidInputException("ducklake_self_test: \"%s\" is a %s catalog, not a ducklake catalog", db_name,
			                            catalog_obj.GetCatalogType());
		}
		result->catalog = catalog_obj.Cast<DuckLakeCatalog>();
	}

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("extension_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("extension_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("duckdb_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("provider_kind");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("load_ok");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeSelfTestInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_uniq<SelfTestGlobalState>();
}

static void DuckLakeSelfTestExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<SelfTestBindData>();
	auto &gstate = data_p.global_state->Cast<SelfTestGlobalState>();
	if (gstate.finished) {
		output.SetChildCardinality(0);
		return;
	}
	gstate.finished = true;

#ifdef EXT_VERSION_DUCKLAKE
	string extension_version = EXT_VERSION_DUCKLAKE;
#else
	string extension_version = "(dev)";
#endif
	// With no DuckLake named this only reports that the extension is loaded and callable.
	string provider_kind = "not_checked";
	bool load_ok = true;

	if (data.catalog) {
		auto provider = data.catalog->EncryptionProvider();
		if (!provider) {
			// A lake with no envelope is healthy; there is nothing to check.
			provider_kind = "catalog_has_no_provider";
		} else {
			try {
				provider_kind = provider->SelfTest();
			} catch (std::exception &e) {
				provider_kind = StringUtil::Format("error: %s", e.what());
				load_ok = false;
			}
		}
	}

	output.data[0].Append(Value("ducklake"));
	output.data[1].Append(Value(extension_version));
	output.data[2].Append(Value(DuckDB::LibraryVersion()));
	output.data[3].Append(Value(provider_kind));
	output.data[4].Append(Value::BOOLEAN(load_ok));
	output.SetChildCardinality(1);
}

DuckLakeSelfTestFunction::DuckLakeSelfTestFunction()
    : TableFunction("ducklake_self_test", {}, DuckLakeSelfTestExecute, DuckLakeSelfTestBind, DuckLakeSelfTestInit) {
	varargs = LogicalType::VARCHAR;
}

} // namespace duckdb
