#include "functions/ducklake_table_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

Catalog &DuckLakeBaseMetadataFunction::GetCatalog(ClientContext &context, TableFunctionBindInput &input) {
	if (input.binder) {
		// the bind result depends on the transaction so prepared statements are rebound on every execution
		input.binder->SetAlwaysRequireRebind();
	}
	auto &catalog_name = input.inputs[0];
	if (catalog_name.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = catalog_name.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "ducklake") {
		throw BinderException("Attached database \"%s\" does not refer to a DuckLake database", db_name);
	}
	return catalog;
}

struct MetadataFunctionData : public GlobalTableFunctionState {
	MetadataFunctionData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<GlobalTableFunctionState> MetadataFunctionInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<MetadataFunctionData>();
	return std::move(result);
}

void DuckLakeBaseMetadataFunction::ScanRows(const vector<vector<Value>> &rows, idx_t &offset, DataChunk &output) {
	idx_t count = 0;
	while (offset < rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = rows[offset++];
		if (entry.size() != output.ColumnCount()) {
			throw InternalException("Unaligned metadata row in result");
		}
		for (idx_t c = 0; c < entry.size(); c++) {
			output.data[c].Append(entry[c]);
		}
		count++;
	}
	output.SetChildCardinality(count);
}

static void MetadataFunctionExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<MetadataBindData>();
	auto &state = data_p.global_state->Cast<MetadataFunctionData>();
	DuckLakeBaseMetadataFunction::ScanRows(data.rows, state.offset, output);
}

unique_ptr<GlobalTableFunctionState> DuckLakeRunOnceState::Init(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DuckLakeRunOnceState>();
}

DuckLakeBaseMetadataFunction::DuckLakeBaseMetadataFunction(Identifier name_p, table_function_bind_t bind)
    : TableFunction(std::move(name_p), FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR),
                    MetadataFunctionExecute, bind, MetadataFunctionInit) {
}

} // namespace duckdb
