#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

static LogicalType OptionType() {
	child_list_t<LogicalType> children;
	children.emplace_back("type", LogicalType::VARCHAR);
	children.emplace_back("value", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(children));
}

//! The type and the text of every option, ordered by name
static Value OptionsToValue(const unordered_map<string, Value> &options) {
	vector<string> names;
	for (auto &option : options) {
		names.push_back(option.first);
	}
	std::sort(names.begin(), names.end());
	vector<Value> keys;
	vector<Value> values;
	for (auto &name : names) {
		auto &option = options.at(name);
		child_list_t<Value> children;
		children.emplace_back("type", Value(option.type().ToString()));
		children.emplace_back("value", option.IsNull() ? Value(LogicalType::VARCHAR) : Value(option.ToString()));
		keys.emplace_back(name);
		values.push_back(Value::STRUCT(std::move(children)));
	}
	return Value::MAP(LogicalType::VARCHAR, OptionType(), std::move(keys), std::move(values));
}

static DuckLakeScanType GetScanType(const TableFunctionBindInput &input) {
	auto scan_type = StringUtil::Lower(DuckLakeTableFunctionUtil::GetStringOption(input, "scan_type"));
	if (scan_type.empty() || scan_type == "table") {
		return DuckLakeScanType::SCAN_TABLE;
	}
	if (scan_type == "insertions") {
		return DuckLakeScanType::SCAN_INSERTIONS;
	}
	if (scan_type == "deletions") {
		return DuckLakeScanType::SCAN_DELETIONS;
	}
	throw BinderException("The scan_type option must be one of table, insertions or deletions, not '%s'", scan_type);
}

//! The file list after pushing the dynamic filter of a descending Top-N on a column into it
static unique_ptr<MultiFileList> PushTopNFilter(ClientContext &context, const MultiFileBindData &bind_data,
                                                const string &column_name) {
	optional_idx column_idx;
	for (idx_t i = 0; i < bind_data.names.size(); i++) {
		if (StringUtil::CIEquals(bind_data.names[i].GetIdentifierName(), column_name)) {
			column_idx = i;
			break;
		}
	}
	if (!column_idx.IsValid()) {
		throw BinderException("The top_n_column option '%s' is not a column of the table", column_name);
	}
	auto &type = bind_data.types[column_idx.GetIndex()];
	auto boundary = type.InternalType() == PhysicalType::VARCHAR ? Value("") : Value::MinimumValue(type);
	auto filter_data = make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_GREATERTHAN, std::move(boundary));
	TableFilterSet filters;
	filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(
	                                           CreateDynamicFilterExpression(filter_data, type), type)));
	vector<ColumnIndex> column_indexes {ColumnIndex(column_idx.GetIndex())};
	MultiFileDynamicPushdownInfo pushdown_info(context, bind_data.file_options, bind_data.names, bind_data.types,
	                                           column_indexes, filters);
	return bind_data.multi_file_reader->DynamicFilterPushdown(*bind_data.file_list, pushdown_info);
}

static unique_ptr<FunctionData> DuckLakeScanMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("file_options");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, OptionType()));

	names.emplace_back("scan_options");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, OptionType()));

	auto schema = DuckLakeTableFunctionUtil::GetStringOption(input, "schema");
	auto scan_type = GetScanType(input);
	auto at_clause = DuckLakeTableFunctionUtil::GetSnapshotAtClause(input);
	Value start_snapshot;
	bool has_start_snapshot = DuckLakeTableFunctionUtil::TryGetNonNullOption(
	    input, "start_snapshot", "snapshot id, tag name or timestamp", start_snapshot);
	if (has_start_snapshot == (scan_type == DuckLakeScanType::SCAN_TABLE)) {
		throw BinderException("The start_snapshot option is required for scans of table changes, and only for them");
	}
	if (has_start_snapshot) {
		at_clause = make_uniq<BoundAtClause>(DuckLakeTableFunctionUtil::ChangesEndAtClause(transaction, at_clause));
	}
	auto table_name = DuckLakeTableFunctionUtil::GetTableName(input.inputs[1]);
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)), at_clause.get(),
	                             QueryErrorContext());
	auto &table = DuckLakeBaseMetadataFunction::GetTableEntry(context, catalog, schema, table_lookup);

	// bind the scan and list its files like an external reader would
	unique_ptr<FunctionData> scan_bind_data;
	auto scan_function = table.GetScanFunction(context, scan_bind_data, table_lookup);
	if (has_start_snapshot) {
		auto &function_info = scan_function.function_info->Cast<DuckLakeFunctionInfo>();
		auto start_at_clause = DuckLakeTableFunctionUtil::AtClauseFromValue(start_snapshot);
		function_info.start_snapshot =
		    make_uniq<DuckLakeSnapshot>(transaction.GetSnapshot(start_at_clause, SnapshotBound::LOWER_BOUND));
		function_info.scan_type = scan_type;
	}
	TableFunctionGetBindInfoInput bind_info_input(context, scan_bind_data.get());
	auto scan_options = OptionsToValue(scan_function.get_bind_info(bind_info_input).options);
	auto &multi_file_data = scan_bind_data->Cast<MultiFileBindData>();
	unique_ptr<MultiFileList> filtered_list;
	auto top_n_column = DuckLakeTableFunctionUtil::GetStringOption(input, "top_n_column");
	if (!top_n_column.empty()) {
		filtered_list = PushTopNFilter(context, multi_file_data, top_n_column);
	}
	auto &file_list = filtered_list ? *filtered_list : *multi_file_data.file_list;

	auto result = make_uniq<MetadataBindData>();
	for (auto &file : file_list.GetAllFiles()) {
		vector<Value> row_values;
		row_values.emplace_back(file.path);
		row_values.push_back(OptionsToValue(file.extended_info->options));
		row_values.push_back(scan_options);
		result->rows.push_back(std::move(row_values));
	}
	return std::move(result);
}

DuckLakeScanMetadataFunction::DuckLakeScanMetadataFunction()
    : DuckLakeBaseMetadataFunction("ducklake_scan_metadata", DuckLakeScanMetadataBind) {
	GetSignature()
	    .AddPositionalOnly("table_name", LogicalType::VARCHAR)
	    .WithTypedKwargs("options", [&](TypedKwargs &options) {
		    options.Add("schema", LogicalType::VARCHAR)
		        .Add("snapshot_version", LogicalType::ANY)
		        .Add("snapshot_time", LogicalType::TIMESTAMP_TZ)
		        .Add("scan_type", LogicalType::VARCHAR)
		        .Add("start_snapshot", LogicalType::ANY)
		        .Add("top_n_column", LogicalType::VARCHAR);
	    });
}

} // namespace duckdb
