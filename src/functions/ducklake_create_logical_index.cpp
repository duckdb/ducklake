#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

enum class LogicalIndexOperation : uint8_t { CREATE, INVALIDATE, DROP };

struct LogicalIndexBindData : public TableFunctionData {
	LogicalIndexBindData(LogicalIndexOperation operation_p, string catalog_p, string schema_p, string table_p,
	                     string column_p = string())
	    : operation(operation_p), catalog(std::move(catalog_p)), schema(std::move(schema_p)),
	      table(std::move(table_p)), column(std::move(column_p)) {
	}

	LogicalIndexOperation operation;
	string catalog;
	string schema;
	string table;
	string column;
};

struct LogicalIndexState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> BindLogicalIndexOperation(TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names,
                                                          LogicalIndexOperation operation) {
	if (operation == LogicalIndexOperation::CREATE) {
		return_types.emplace_back(LogicalType::UBIGINT);
		names.emplace_back("index_id");
	} else {
		return_types.emplace_back(LogicalType::BOOLEAN);
		names.emplace_back("success");
	}
	auto column = input.inputs.size() > 3 ? StringValue::Get(input.inputs[3]) : string();
	return make_uniq<LogicalIndexBindData>(operation, StringValue::Get(input.inputs[0]),
	                                        StringValue::Get(input.inputs[1]), StringValue::Get(input.inputs[2]),
	                                        std::move(column));
}

static unique_ptr<FunctionData> CreateLogicalIndexBind(ClientContext &, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	return BindLogicalIndexOperation(input, return_types, names, LogicalIndexOperation::CREATE);
}

static unique_ptr<FunctionData> InvalidateLogicalIndexBind(ClientContext &, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return BindLogicalIndexOperation(input, return_types, names, LogicalIndexOperation::INVALIDATE);
}

static unique_ptr<FunctionData> DropLogicalIndexesBind(ClientContext &, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	return BindLogicalIndexOperation(input, return_types, names, LogicalIndexOperation::DROP);
}

static unique_ptr<GlobalTableFunctionState> LogicalIndexInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<LogicalIndexState>();
}

static void LogicalIndexExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<LogicalIndexState>();
	if (state.finished) {
		return;
	}
	state.finished = true;
	auto &bind = input.bind_data->Cast<LogicalIndexBindData>();
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, Value(bind.catalog));
	auto entry = catalog.GetEntry<TableCatalogEntry>(context, bind.schema, bind.table, OnEntryNotFound::THROW_EXCEPTION);
	auto &table = entry->Cast<DuckLakeTableEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	auto &metadata_manager = transaction.GetMetadataManager();

	output.SetCardinality(1);
	switch (bind.operation) {
	case LogicalIndexOperation::CREATE: {
		auto index_id = metadata_manager.CreateBigIntLogicalIndex(context, table, bind.column);
		output.SetValue(0, 0, Value::UBIGINT(index_id));
		break;
	}
	case LogicalIndexOperation::INVALIDATE:
		metadata_manager.InvalidateLogicalIndex(table, bind.column);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	case LogicalIndexOperation::DROP:
		metadata_manager.DropLogicalIndexes(table);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	}
}

DuckLakeCreateLogicalIndexFunction::DuckLakeCreateLogicalIndexFunction()
    : TableFunction("ducklake_create_logical_index",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    LogicalIndexExecute, CreateLogicalIndexBind, LogicalIndexInit) {
}

DuckLakeInvalidateLogicalIndexFunction::DuckLakeInvalidateLogicalIndexFunction()
    : TableFunction("ducklake_invalidate_logical_index",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    LogicalIndexExecute, InvalidateLogicalIndexBind, LogicalIndexInit) {
}

DuckLakeDropLogicalIndexesFunction::DuckLakeDropLogicalIndexesFunction()
    : TableFunction("ducklake_drop_logical_indexes",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalIndexExecute,
                    DropLogicalIndexesBind, LogicalIndexInit) {
}

} // namespace duckdb
