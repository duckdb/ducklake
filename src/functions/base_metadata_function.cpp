#include "functions/ducklake_table_functions.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

Catalog &DuckLakeBaseMetadataFunction::GetCatalog(ClientContext &context, const Value &input) {
	if (input.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = input.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
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

static void MetadataFunctionExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<MetadataBindData>();
	auto &state = data_p.global_state->Cast<MetadataFunctionData>();
	if (state.offset >= data.rows.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (state.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.rows[state.offset++];
		if (entry.size() != output.ColumnCount()) {
			throw InternalException("Unaligned metadata row in result");
		}

		for (idx_t c = 0; c < entry.size(); c++) {
			output.SetValue(c, count, entry[c]);
		}
		count++;
	}
	output.SetCardinality(count);
}

DuckLakeBaseMetadataFunction::DuckLakeBaseMetadataFunction(string name_p, table_function_bind_t bind)
    : TableFunction(std::move(name_p), {LogicalType::VARCHAR}, MetadataFunctionExecute, bind, MetadataFunctionInit) {
}

} // namespace duckdb
