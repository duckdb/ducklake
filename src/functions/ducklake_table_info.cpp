#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeTableInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();
	auto tables = metadata_manager.GetTableSizes(transaction.GetSnapshot());
	auto result = make_uniq<MetadataBindData>();
	for (auto &table_info : tables) {
		vector<Value> row_values;
		row_values.push_back(Value(table_info.table_name));
		row_values.push_back(Value::BIGINT(NumericCast<int64_t>(table_info.schema_id.index)));
		row_values.push_back(Value::BIGINT(NumericCast<int64_t>(table_info.table_id.index)));
		row_values.push_back(Value::UUID(table_info.table_uuid));
		row_values.push_back(Value::BIGINT(NumericCast<int64_t>(table_info.file_count)));
		row_values.push_back(Value::BIGINT(NumericCast<int64_t>(table_info.file_size_bytes)));
		row_values.push_back(Value::BIGINT(NumericCast<int64_t>(table_info.delete_file_count)));
		row_values.push_back(Value::BIGINT(NumericCast<int64_t>(table_info.delete_file_size_bytes)));
		result->rows.push_back(std::move(row_values));
	}

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("table_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("table_uuid");
	return_types.emplace_back(LogicalType::UUID);

	names.emplace_back("file_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("file_size_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("delete_file_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("delete_file_size_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	return std::move(result);
}

DuckLakeTableInfoFunction::DuckLakeTableInfoFunction()
    : DuckLakeBaseMetadataFunction("ducklake_table_info", DuckLakeTableInfoBind) {
}

} // namespace duckdb
