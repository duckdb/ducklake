#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeMetadataTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	if (input.inputs[1].IsNull()) {
		throw BinderException("ducklake_metadata_table: the table name cannot be NULL");
	}
	auto query_result = transaction.GetMetadataManager().ReadMetadataTable(StringValue::Get(input.inputs[1]));
	names = query_result->GetNames();
	return_types = query_result->GetTypes();
	auto result = make_uniq<MetadataBindData>();
	for (auto &row : *query_result) {
		vector<Value> values;
		for (idx_t col_idx = 0; col_idx < return_types.size(); col_idx++) {
			values.push_back(row.GetBaseValue(col_idx));
		}
		result->rows.push_back(std::move(values));
	}
	return std::move(result);
}

DuckLakeMetadataTableFunction::DuckLakeMetadataTableFunction()
    : DuckLakeBaseMetadataFunction("ducklake_metadata_table", DuckLakeMetadataTableBind) {
	GetSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR);
}

} // namespace duckdb
