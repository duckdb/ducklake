#include "common/ducklake_util.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

// DeferredFilters implementation
DeferredFilters DeferredFilters::Copy() const {
	DeferredFilters result;
	result.evaluated = evaluated;
	result.column_ids = column_ids;
	result.context = context;

	// Copy complex filters
	for (auto &filter : pending_complex_filters) {
		result.pending_complex_filters.push_back(filter->Copy());
	}

	// Copy dynamic filters
	for (auto &entry : pending_dynamic_filters.filters) {
		result.pending_dynamic_filters.filters[entry.first] = entry.second->Copy();
	}

	// Copy processed table filters
	for (auto &entry : processed_table_filters.filters) {
		result.processed_table_filters.filters[entry.first] = entry.second->Copy();
	}

	return result;
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeDataFile> transaction_local_files_p,
                                             shared_ptr<DuckLakeInlinedData> transaction_local_data_p, string filter_p,
                                             string cte_section_p)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info), read_file_list(false),
      transaction_local_files(std::move(transaction_local_files_p)),
      transaction_local_data(std::move(transaction_local_data_p)), filter(std::move(filter_p)),
      cte_section(std::move(cte_section_p)) {
	deferred_filters.evaluated = !filter.empty() || !cte_section.empty();
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeFileListEntry> files_to_scan)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info),
      files(std::move(files_to_scan)), read_file_list(true) {
	deferred_filters.evaluated = true;
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             const DuckLakeInlinedTableInfo &inlined_table)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info), read_file_list(true) {
	deferred_filters.evaluated = true;
	DuckLakeFileListEntry file_entry;
	file_entry.file.path = inlined_table.table_name;
	file_entry.row_id_start = 0;
	file_entry.data_type = DuckLakeDataType::INLINED_DATA;
	files.push_back(std::move(file_entry));
	inlined_data_tables.push_back(inlined_table);
}

struct CTERequirement {
	idx_t column_field_index;
	unordered_set<string> referenced_stats;
	idx_t reference_count = 1;

	CTERequirement(idx_t column_idx, unordered_set<string> stats)
	    : column_field_index(column_idx), referenced_stats(std::move(stats)) {
	}
};

static void MergeCTERequirementsMap(unordered_map<idx_t, CTERequirement> &target,
                                    const unordered_map<idx_t, CTERequirement> &source) {
	for (const auto &entry : source) {
		const auto &req = entry.second;
		auto it = target.find(req.column_field_index);
		if (it != target.end()) {
			// Merge stats and add reference counts
			it->second.referenced_stats.insert(req.referenced_stats.begin(), req.referenced_stats.end());
			it->second.reference_count += req.reference_count;
		} else {
			// New column, add it with its reference count
			target.emplace(req.column_field_index, req);
		}
	}
}

struct ComplexFilterPushdownResult {
	bool can_pushdown = false;
	bool is_unsatisfiable = false;
	string sql_condition;
	unordered_map<idx_t, CTERequirement> required_ctes;

	ComplexFilterPushdownResult() : can_pushdown(false), is_unsatisfiable(false) {
	}

	void MergeCTERequirements(const ComplexFilterPushdownResult &other) {
		MergeCTERequirementsMap(required_ctes, other.required_ctes);
	}
};

static ComplexFilterPushdownResult ProcessAndConjunction(const BoundConjunctionExpression &and_expr,
                                                         const vector<column_t> &column_ids, ClientContext &context,
                                                         const DuckLakeFunctionInfo &read_info,
                                                         const vector<ColumnIndex> &column_indexes);

static ComplexFilterPushdownResult ProcessOrDisjunction(const BoundConjunctionExpression &or_expr,
                                                        const vector<column_t> &column_ids, ClientContext &context,
                                                        const DuckLakeFunctionInfo &read_info,
                                                        const vector<ColumnIndex> &column_indexes);

struct FilterSQLResult {
	string where_conditions;                            // WHERE clause using CTEs
	unordered_map<idx_t, CTERequirement> required_ctes; // CTE requirements for further processing

	FilterSQLResult() = default;
	FilterSQLResult(string conditions) : where_conditions(std::move(conditions)) {
	}
};

static ComplexFilterPushdownResult ProcessComplexFilter(const Expression &filter, const vector<column_t> &column_ids,
                                                        ClientContext &context, const DuckLakeFunctionInfo &read_info,
                                                        const vector<ColumnIndex> &column_indexes);

static FilterSQLResult ConvertTableFilterSetToSQL(const TableFilterSet &table_filters,
                                                  const vector<column_t> &column_ids,
                                                  const DuckLakeFunctionInfo &read_info);

static string GenerateFilterPushdown(const TableFilter &filter, unordered_set<string> &referenced_stats);

static string GenerateCTESectionFromRequirements(const unordered_map<idx_t, CTERequirement> &requirements,
                                                 const DuckLakeFunctionInfo &read_info) {
	if (requirements.empty()) {
		return "";
	}

	string cte_section = "WITH ";
	bool first_cte = true;

	for (const auto &entry : requirements) {
		const auto &req = entry.second;
		if (!first_cte) {
			cte_section += ",\n";
		}
		first_cte = false;

		string select_list = "data_file_id";
		for (const auto &stat : req.referenced_stats) {
			select_list += ", " + stat;
		}

		// Only add MATERIALIZED hint if the CTE is referenced multiple times
		string materialized_hint = (req.reference_count > 1) ? " AS MATERIALIZED" : " AS";

		cte_section += StringUtil::Format("col_%d_stats%s (\n", req.column_field_index, materialized_hint);
		cte_section += StringUtil::Format("  SELECT %s\n", select_list);
		cte_section += "  FROM {METADATA_CATALOG}.ducklake_file_column_stats\n";
		cte_section += StringUtil::Format("  WHERE column_id = %d AND table_id = %d\n", req.column_field_index,
		                                  read_info.table_id.index);
		cte_section += ")";
	}

	return cte_section + "\n";
}

void DuckLakeMultiFileList::EvaluateDeferredFilters() {
	if (deferred_filters.evaluated || !deferred_filters.context) {
		return;
	}

	string combined_filter;
	unordered_map<idx_t, CTERequirement> all_required_ctes;

	// Process dynamic filters first (simple table filters)
	if (!deferred_filters.pending_dynamic_filters.filters.empty()) {
		auto dynamic_result = ConvertTableFilterSetToSQL(deferred_filters.pending_dynamic_filters,
		                                                 deferred_filters.column_ids, read_info);
		if (!dynamic_result.where_conditions.empty()) {
			combined_filter = dynamic_result.where_conditions;
			all_required_ctes = std::move(dynamic_result.required_ctes);
		}
	}

	// Process complex filters if any
	if (!deferred_filters.pending_complex_filters.empty()) {
		// Construct AND conjunction from the vector of expressions
		auto and_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &filter : deferred_filters.pending_complex_filters) {
			and_expr->children.push_back(filter->Copy());
		}
		// Create column indexes from column IDs
		vector<ColumnIndex> column_indexes;
		for (idx_t i = 0; i < deferred_filters.column_ids.size(); i++) {
			column_indexes.push_back(ColumnIndex(i));
		}
		auto complex_result = ProcessAndConjunction(*and_expr, deferred_filters.column_ids, *deferred_filters.context,
		                                            read_info, column_indexes);
		if (complex_result.can_pushdown) {
			if (complex_result.is_unsatisfiable) {
				// Filters are unsatisfiable
				combined_filter = "1 = 0";
				all_required_ctes.clear();
			} else if (!complex_result.sql_condition.empty()) {
				// Merge complex filter with dynamic filter
				if (!combined_filter.empty()) {
					combined_filter = "(" + combined_filter + ") AND (" + complex_result.sql_condition + ")";
				} else {
					combined_filter = complex_result.sql_condition;
				}
				MergeCTERequirementsMap(all_required_ctes, complex_result.required_ctes);
			}
		}
	}

	if (!combined_filter.empty()) {
		filter = combined_filter;
		cte_section = GenerateCTESectionFromRequirements(all_required_ctes, read_info);
	}

	deferred_filters.evaluated = true;
}

static ComplexFilterPushdownResult ProcessComplexFilter(const Expression &filter, const vector<column_t> &column_ids,
                                                        ClientContext &context, const DuckLakeFunctionInfo &read_info,
                                                        const vector<ColumnIndex> &column_indexes) {
	ComplexFilterPushdownResult result;

	FilterCombiner combiner(context);
	auto filter_result = combiner.AddFilter(filter.Copy());

	if (filter_result == FilterResult::UNSATISFIABLE) {
		result.can_pushdown = true;
		result.is_unsatisfiable = true;
		return result;
	}

	vector<FilterPushdownResult> pushdown_results;
	auto table_filters = combiner.GenerateTableScanFilters(column_indexes, pushdown_results);

	bool has_remaining_filters = combiner.HasFilters();

	if (!has_remaining_filters) {
		auto filter_result = ConvertTableFilterSetToSQL(table_filters, column_ids, read_info);
		if (!filter_result.where_conditions.empty()) {
			result.can_pushdown = true;
			result.sql_condition = filter_result.where_conditions;
			result.required_ctes = std::move(filter_result.required_ctes);
			return result;
		}
	}

	// FilterCombiner couldn't handle the entire expression, fall back to recursive processing
	switch (filter.GetExpressionType()) {
	case ExpressionType::CONJUNCTION_AND: {
		auto &and_expr = filter.Cast<BoundConjunctionExpression>();
		return ProcessAndConjunction(and_expr, column_ids, context, read_info, column_indexes);
	}
	case ExpressionType::CONJUNCTION_OR: {
		auto &or_expr = filter.Cast<BoundConjunctionExpression>();
		return ProcessOrDisjunction(or_expr, column_ids, context, read_info, column_indexes);
	}
	default:
		result.can_pushdown = false;
		return result;
	}
}

static FilterSQLResult ConvertTableFilterSetToSQL(const TableFilterSet &table_filters,
                                                  const vector<column_t> &column_ids,
                                                  const DuckLakeFunctionInfo &read_info) {
	FilterSQLResult result;
	string conditions;

	for (auto &entry : table_filters.filters) {
		auto column_index_val = entry.first;
		idx_t column_idx = column_index_val;

		auto column_id = column_ids[column_idx];

		if (IsVirtualColumn(column_id)) {
			// skip pushing filters on virtual columns
			continue;
		}

		unordered_set<string> referenced_stats;
		auto filter_condition = GenerateFilterPushdown(*entry.second, referenced_stats);
		if (filter_condition.empty()) {
			// failed to generate filter for this column
			continue;
		}

		// FIXME: handle structs
		auto column_index = PhysicalIndex(column_id);
		auto &root_id = read_info.table.GetFieldId(column_index);
		auto field_index = root_id.GetFieldIndex().index;

		// generate the final filter for this column
		string cte_name = StringUtil::Format("col_%d_stats", field_index);

		// if any of the referenced stats are NULL we cannot prune
		string null_checks;
		for (auto &stat : referenced_stats) {
			null_checks += stat + " IS NULL OR ";
		}

		if (!conditions.empty()) {
			conditions += " AND ";
		}
		// finally add the filter
		conditions += StringUtil::Format("data_file_id IN (SELECT data_file_id FROM %s WHERE %s(%s))", cte_name,
		                                 null_checks, filter_condition);
		// Add the CTE requirement for this column
		CTERequirement req(field_index, referenced_stats);
		req.reference_count = 1;
		result.required_ctes.emplace(field_index, std::move(req));
	}

	result.where_conditions = conditions;
	return result;
}

static ComplexFilterPushdownResult ProcessAndConjunction(const BoundConjunctionExpression &and_expr,
                                                         const vector<column_t> &column_ids, ClientContext &context,
                                                         const DuckLakeFunctionInfo &read_info,
                                                         const vector<ColumnIndex> &column_indexes) {
	ComplexFilterPushdownResult result;

	FilterCombiner group_combiner(context);

	for (idx_t i = 0; i < and_expr.children.size(); i++) {
		auto filter_result = group_combiner.AddFilter(and_expr.children[i]->Copy());
		if (filter_result == FilterResult::UNSATISFIABLE) {
			// If any expression makes the AND unsatisfiable then the entire AND is unsatisfiable
			result.can_pushdown = true;
			result.is_unsatisfiable = true;
			return result;
		}
	}

	vector<string> and_conditions;
	vector<unique_ptr<Expression>> remaining_filters;

	vector<FilterPushdownResult> pushdown_results;
	auto grouped_table_filters = group_combiner.GenerateTableScanFilters(column_indexes, pushdown_results);

	if (!grouped_table_filters.filters.empty()) {
		auto filter_result = ConvertTableFilterSetToSQL(grouped_table_filters, column_ids, read_info);
		if (!filter_result.where_conditions.empty()) {
			and_conditions.push_back(filter_result.where_conditions);

			// Create a temporary result to use MergeCTERequirements
			ComplexFilterPushdownResult temp_result;
			temp_result.required_ctes = std::move(filter_result.required_ctes);
			result.MergeCTERequirements(temp_result);
		}
	}

	// Now recursively process only the expressions that FilterCombiner couldn't handle
	group_combiner.GenerateFilters(
	    [&](unique_ptr<Expression> filter) { remaining_filters.push_back(std::move(filter)); });
	for (auto &remaining_filter : remaining_filters) {
		auto child_result = ProcessComplexFilter(*remaining_filter, column_ids, context, read_info, column_indexes);
		if (child_result.can_pushdown) {
			if (child_result.is_unsatisfiable) {
				result.can_pushdown = true;
				result.is_unsatisfiable = true;
				return result;
			} else if (!child_result.sql_condition.empty()) {
				and_conditions.push_back(child_result.sql_condition);
				result.MergeCTERequirements(child_result);
			}
		}
	}

	if (!and_conditions.empty()) {
		string combined_and;
		for (idx_t i = 0; i < and_conditions.size(); i++) {
			if (i > 0) {
				combined_and += " AND ";
			}
			combined_and += "(" + and_conditions[i] + ")";
		}

		result.can_pushdown = true;
		result.sql_condition = combined_and;
	} else {
		result.can_pushdown = false;
	}

	return result;
}

static ComplexFilterPushdownResult ProcessOrDisjunction(const BoundConjunctionExpression &or_expr,
                                                        const vector<column_t> &column_ids, ClientContext &context,
                                                        const DuckLakeFunctionInfo &read_info,
                                                        const vector<ColumnIndex> &column_indexes) {
	ComplexFilterPushdownResult result;

	vector<idx_t> satisfiable_children;

	for (idx_t i = 0; i < or_expr.children.size(); i++) {
		FilterCombiner child_combiner(context);
		auto child_filter_result = child_combiner.AddFilter(or_expr.children[i]->Copy());

		if (child_filter_result == FilterResult::UNSATISFIABLE) {
			// Skip unsatisfiable children - they don't contribute to the OR
			continue;
		}

		satisfiable_children.push_back(i);
	}

	// If no children are satisfiable, the entire OR is unsatisfiable
	if (satisfiable_children.empty()) {
		result.can_pushdown = true;
		result.is_unsatisfiable = true;
		return result;
	}

	// If only one child is satisfiable, process it directly
	if (satisfiable_children.size() == 1) {
		idx_t child_idx = satisfiable_children[0];
		return ProcessComplexFilter(*or_expr.children[child_idx], column_ids, context, read_info, column_indexes);
	}

	vector<string> or_conditions;
	for (auto child_idx : satisfiable_children) {
		auto child_result =
		    ProcessComplexFilter(*or_expr.children[child_idx], column_ids, context, read_info, column_indexes);
		if (child_result.can_pushdown) {
			if (child_result.is_unsatisfiable) {
				// Skip unsatisfiable children - they don't contribute to the OR
				continue;
			} else if (!child_result.sql_condition.empty()) {
				or_conditions.push_back(child_result.sql_condition);
				result.MergeCTERequirements(child_result);
			}
		} else {
			// If any satisfiable child cannot be pushed down, we cannot optimize the entire OR
			// because we might miss data that should match the unpushable condition
			result.can_pushdown = false;
			return result;
		}
	}

	// If all satisfiable children could be pushed down, create the OR condition
	if (!or_conditions.empty()) {
		string combined_or;
		for (idx_t i = 0; i < or_conditions.size(); i++) {
			if (i > 0) {
				combined_or += " OR ";
			}
			combined_or += "(" + or_conditions[i] + ")";
		}

		result.can_pushdown = true;
		result.sql_condition = combined_or;
	} else if (satisfiable_children.size() > 0) {
		// We had satisfiable children but none produced SQL conditions - they must all be unsatisfiable
		result.can_pushdown = true;
		result.is_unsatisfiable = true;
	} else {
		result.can_pushdown = false;
	}

	return result;
}

unique_ptr<MultiFileList> DuckLakeMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                       const MultiFileOptions &options,
                                                                       MultiFilePushdownInfo &info,
                                                                       vector<unique_ptr<Expression>> &filters) {
	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || filters.empty()) {
		// filter pushdown is only supported when scanning full tables
		return nullptr;
	}

	// Check for duplicates while building the new filter list
	vector<unique_ptr<Expression>> new_filters_to_add;

	for (auto &filter : filters) {
		bool is_duplicate = false;
		for (auto &existing_filter : deferred_filters.pending_complex_filters) {
			if (filter->Equals(*existing_filter)) {
				is_duplicate = true;
				break;
			}
		}
		if (!is_duplicate) {
			new_filters_to_add.push_back(filter->Copy());
		}
	}

	auto result = CreateCopyWithDeferredEvaluation(context, info.column_ids);

	if (new_filters_to_add.empty()) {
		return std::move(result);
	}

	for (auto &filter : new_filters_to_add) {
		result->deferred_filters.pending_complex_filters.push_back(filter->Copy());
	}

	FilterCombiner combiner(context);
	for (auto &existing_filter : deferred_filters.pending_complex_filters) {
		combiner.AddFilter(existing_filter->Copy());
	}
	for (auto &filter : new_filters_to_add) {
		combiner.AddFilter(filter->Copy());
	}

	// Generate column indexes for table filter extraction
	vector<FilterPushdownResult> pushdown_results;
	auto simple_table_filters = combiner.GenerateTableScanFilters(info.column_indexes, pushdown_results);

	for (auto &entry : simple_table_filters.filters) {
		result->deferred_filters.processed_table_filters.filters[entry.first] = entry.second->Copy();
	}

	return std::move(result);
}

bool ValueIsFinite(const Value &val) {
	if (val.type().id() != LogicalTypeId::FLOAT && val.type().id() != LogicalTypeId::DOUBLE) {
		return true;
	}
	double constant_val = val.GetValue<double>();
	return Value::IsFinite(constant_val);
}

string CastValueToTarget(const Value &val, const LogicalType &type) {
	if (type.IsNumeric() && ValueIsFinite(val)) {
		// for (finite) numerics we directly emit the number
		return val.ToString();
	}
	// convert to a string
	return DuckLakeUtil::SQLLiteralToString(val.ToString());
}

string CastStatsToTarget(const string &stats, const LogicalType &type) {
	// we only need to cast numerics
	if (type.IsNumeric()) {
		return "TRY_CAST(" + stats + " AS " + type.ToString() + ")";
	}
	return stats;
}

string GenerateConstantFilter(const ConstantFilter &constant_filter, const LogicalType &type,
                              unordered_set<string> &referenced_stats) {
	auto constant_str = CastValueToTarget(constant_filter.constant, type);
	auto min_value = CastStatsToTarget("min_value", type);
	auto max_value = CastStatsToTarget("max_value", type);
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		// x = constant
		// this can only be true if "constant BETWEEN min AND max"
		referenced_stats.insert("min_value");
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s BETWEEN %s AND %s", constant_str, min_value, max_value);
	case ExpressionType::COMPARE_NOTEQUAL:
		// x <> constant
		// this can only be false if "constant = min AND constant = max" (i.e. min = max = constant)
		referenced_stats.insert("min_value");
		referenced_stats.insert("max_value");
		return StringUtil::Format("NOT (%s = %s AND %s = %s)", min_value, constant_str, max_value, constant_str);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// x >= constant
		// this can only be true if "max >= C"
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s >= %s", max_value, constant_str);
	case ExpressionType::COMPARE_GREATERTHAN:
		// x > constant
		// this can only be true if "max > C"
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s > %s", max_value, constant_str);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// x <= constant
		// this can only be true if "min <= C"
		referenced_stats.insert("min_value");
		return StringUtil::Format("%s <= %s", min_value, constant_str);
	case ExpressionType::COMPARE_LESSTHAN:
		// x < constant
		// this can only be true if "min < C"
		referenced_stats.insert("min_value");
		return StringUtil::Format("%s < %s", min_value, constant_str);
	default:
		// unsupported
		return string();
	}
}

string GenerateConstantFilterDouble(const ConstantFilter &constant_filter, const LogicalType &type,
                                    unordered_set<string> &referenced_stats) {
	double constant_val = constant_filter.constant.GetValue<double>();
	bool constant_is_nan = Value::IsNan(constant_val);
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		// x = constant
		if (constant_is_nan) {
			// x = NAN - check for `contains_nan`
			referenced_stats.insert("contains_nan");
			return "contains_nan";
		}
		// else check as if this is a numeric
		return GenerateConstantFilter(constant_filter, type, referenced_stats);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (constant_is_nan) {
			// skip these filters if the constant is nan
			// note that > and >= we can actually handle since nan is the biggest value
			// (>= is equal to =, > is always false)
			return string();
		}
		// generate the numeric filter
		string filter = GenerateConstantFilter(constant_filter, type, referenced_stats);
		if (filter.empty()) {
			return string();
		}
		// since NaN is bigger than anything - we also need to check for contains_nan
		referenced_stats.insert("contains_nan");
		return "(" + filter + ") OR contains_nan";
	}
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
		if (constant_is_nan) {
			// skip these filters if the constant is nan
			return string();
		}
		// these are equivalent to the numeric filter
		return GenerateConstantFilter(constant_filter, type, referenced_stats);
	default:
		// unsupported
		return string();
	}
}

string GenerateFilterPushdown(const TableFilter &filter, unordered_set<string> &referenced_stats) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto &type = constant_filter.constant.type();
		switch (type.id()) {
		case LogicalTypeId::BLOB:
			return string();
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			return GenerateConstantFilterDouble(constant_filter, type, referenced_stats);
		default:
			return GenerateConstantFilter(constant_filter, type, referenced_stats);
		}
	}
	case TableFilterType::IS_NULL:
		// IS NULL can only be true if the file has any NULL values
		referenced_stats.insert("null_count");
		return "null_count > 0";
	case TableFilterType::IS_NOT_NULL:
		// IS NOT NULL can only be true if the file has any valid values
		referenced_stats.insert("value_count");
		return "value_count > 0";
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_or_filter = filter.Cast<ConjunctionOrFilter>();
		string result;
		for (auto &child_filter : conjunction_or_filter.child_filters) {
			if (!result.empty()) {
				result += " OR ";
			}
			string child_str = GenerateFilterPushdown(*child_filter, referenced_stats);
			if (child_str.empty()) {
				return string();
			}
			result += "(" + child_str + ")";
		}
		return result;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = filter.Cast<ConjunctionAndFilter>();
		string result;
		for (auto &child_filter : conjunction_and_filter.child_filters) {
			string child_str = GenerateFilterPushdown(*child_filter, referenced_stats);
			if (child_str.empty()) {
				continue; // skip this child, we can still restrict based on other children
			}
			if (!result.empty()) {
				result += " AND ";
			}
			result += "(" + child_str + ")";
		}
		return result;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return GenerateFilterPushdown(*optional_filter.child_filter, referenced_stats);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		string result;
		for (auto &value : in_filter.values) {
			if (!result.empty()) {
				result += " OR ";
			}
			auto temporary_constant_filter = ConstantFilter(ExpressionType::COMPARE_EQUAL, value);
			auto next_filter = GenerateFilterPushdown(temporary_constant_filter, referenced_stats);
			if (next_filter.empty()) {
				return string();
			}
			result += "(" + next_filter + ")";
		}
		return result;
	}
	default:
		// unsupported filter
		return string();
	}
}

unique_ptr<MultiFileList>
DuckLakeMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                             const vector<string> &names, const vector<LogicalType> &types,
                                             const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || filters.filters.empty()) {
		// filter pushdown is only supported when scanning full tables
		return nullptr;
	}

	auto result = CreateCopyWithDeferredEvaluation(context, column_ids);

	for (auto &entry : filters.filters) {
		auto column_idx = entry.first;
		auto &new_filter = entry.second;

		auto processed_it = result->deferred_filters.processed_table_filters.filters.find(column_idx);
		if (processed_it != result->deferred_filters.processed_table_filters.filters.end()) {
			if (new_filter->Equals(*processed_it->second)) {
				continue;
			}
		}

		result->deferred_filters.pending_dynamic_filters.filters[column_idx] = new_filter->Copy();
	}

	return std::move(result);
}

unique_ptr<DuckLakeMultiFileList>
DuckLakeMultiFileList::CreateCopyWithDeferredEvaluation(ClientContext &context,
                                                        const vector<column_t> &column_ids) const {
	auto result = make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data, filter,
	                                               cte_section);

	// Copy all deferred filter state and override specific fields for deferred evaluation
	result->deferred_filters = deferred_filters.Copy();
	result->deferred_filters.evaluated = false;
	result->deferred_filters.context = &context;
	result->deferred_filters.column_ids = column_ids;

	return result;
}

vector<OpenFileInfo> DuckLakeMultiFileList::GetAllFiles() {
	vector<OpenFileInfo> file_list;
	for (idx_t i = 0; i < GetTotalFileCount(); i++) {
		file_list.push_back(GetFile(i));
	}
	return file_list;
}

FileExpandResult DuckLakeMultiFileList::GetExpandResult() {
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t DuckLakeMultiFileList::GetTotalFileCount() {
	return GetFiles().size();
}

unique_ptr<NodeStatistics> DuckLakeMultiFileList::GetCardinality(ClientContext &context) {
	auto stats = read_info.table.GetTableStats(context);
	if (!stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(stats->record_count);
}

DuckLakeTableEntry &DuckLakeMultiFileList::GetTable() {
	return read_info.table;
}

OpenFileInfo DuckLakeMultiFileList::GetFile(idx_t i) {
	auto &files = GetFiles();
	if (i >= files.size()) {
		return OpenFileInfo();
	}
	auto &file_entry = files[i];
	auto &file = file_entry.file;
	OpenFileInfo result(file.path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	idx_t inlined_data_file_start = files.size() - inlined_data_tables.size();
	if (transaction_local_data) {
		inlined_data_file_start--;
	}
	if (transaction_local_data && i + 1 == files.size()) {
		// scanning transaction local data
		extended_info->options["transaction_local_data"] = Value::BOOLEAN(true);
		extended_info->options["inlined_data"] = Value::BOOLEAN(true);
		if (file_entry.row_id_start.IsValid()) {
			extended_info->options["row_id_start"] = Value::UBIGINT(file_entry.row_id_start.GetIndex());
		}
		extended_info->options["snapshot_id"] = Value(LogicalType::BIGINT);
		if (file_entry.mapping_id.IsValid()) {
			extended_info->options["mapping_id"] = Value::UBIGINT(file_entry.mapping_id.index);
		}
	} else if (i >= inlined_data_file_start) {
		// scanning inlined data
		auto inlined_data_index = i - inlined_data_file_start;
		auto &inlined_data_table = inlined_data_tables[inlined_data_index];
		extended_info->options["table_name"] = inlined_data_table.table_name;
		extended_info->options["inlined_data"] = Value::BOOLEAN(true);
		extended_info->options["schema_version"] =
		    Value::BIGINT(NumericCast<int64_t>(inlined_data_table.schema_version));
	} else {
		extended_info->options["file_size"] = Value::UBIGINT(file.file_size_bytes);
		if (file.footer_size.IsValid()) {
			extended_info->options["footer_size"] = Value::UBIGINT(file.footer_size.GetIndex());
		}
		if (files[i].row_id_start.IsValid()) {
			extended_info->options["row_id_start"] = Value::UBIGINT(files[i].row_id_start.GetIndex());
		}
		Value snapshot_id;
		if (files[i].snapshot_id.IsValid()) {
			snapshot_id = Value::BIGINT(NumericCast<int64_t>(files[i].snapshot_id.GetIndex()));
		} else {
			snapshot_id = Value(LogicalType::BIGINT);
		}
		extended_info->options["snapshot_id"] = std::move(snapshot_id);
		if (!file.encryption_key.empty()) {
			extended_info->options["encryption_key"] = Value::BLOB_RAW(file.encryption_key);
		}
		// files managed by DuckLake are never modified - we can keep them cached
		extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		// etag / last modified time can be set to dummy values
		extended_info->options["etag"] = Value("");
		extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
		if (!file_entry.delete_file.path.empty() || file_entry.max_row_count.IsValid()) {
			extended_info->options["has_deletes"] = Value::BOOLEAN(true);
		}
		if (file_entry.mapping_id.IsValid()) {
			extended_info->options["mapping_id"] = Value::UBIGINT(file_entry.mapping_id.index);
		}
	}
	result.extended_info = std::move(extended_info);
	return result;
}

unique_ptr<MultiFileList> DuckLakeMultiFileList::Copy() {
	auto result = make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data, filter,
	                                               cte_section);
	result->files = GetFiles();
	result->read_file_list = read_file_list;
	result->delete_scans = delete_scans;

	result->deferred_filters = deferred_filters.Copy();

	return std::move(result);
}

const DuckLakeFileListEntry &DuckLakeMultiFileList::GetFileEntry(idx_t file_idx) {
	auto &files = GetFiles();
	return files[file_idx];
}

DuckLakeFileData GetFileData(const DuckLakeDataFile &file) {
	DuckLakeFileData result;
	result.path = file.file_name;
	result.encryption_key = file.encryption_key;
	result.file_size_bytes = file.file_size_bytes;
	result.footer_size = file.footer_size;
	return result;
}

DuckLakeFileData GetDeleteData(const DuckLakeDataFile &file) {
	DuckLakeFileData result;
	if (!file.delete_file) {
		return result;
	}
	auto &delete_file = *file.delete_file;
	result.path = delete_file.file_name;
	result.encryption_key = delete_file.encryption_key;
	result.file_size_bytes = delete_file.file_size_bytes;
	result.footer_size = delete_file.footer_size;
	return result;
}

vector<DuckLakeFileListExtendedEntry> DuckLakeMultiFileList::GetFilesExtended() {
	lock_guard<mutex> l(file_lock);
	EvaluateDeferredFilters();

	vector<DuckLakeFileListExtendedEntry> result;
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!read_info.table_id.IsTransactionLocal()) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		result = metadata_manager.GetExtendedFilesForTable(read_info.table, read_info.snapshot, cte_section, filter);
	}
	if (transaction.HasDroppedFiles()) {
		for (idx_t file_idx = 0; file_idx < result.size(); file_idx++) {
			if (transaction.FileIsDropped(result[file_idx].file.path)) {
				result.erase_at(file_idx);
				file_idx--;
			}
		}
	}
	// if the transaction has any local deletes - apply them to the file list
	if (transaction.HasLocalDeletes(read_info.table_id)) {
		for (auto &file_entry : result) {
			transaction.GetLocalDeleteForFile(read_info.table_id, file_entry.file.path, file_entry.delete_file);
		}
	}
	idx_t transaction_row_start = TRANSACTION_LOCAL_ID_START;
	for (auto &file : transaction_local_files) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = file.row_count;
		file_entry.file = GetFileData(file);
		file_entry.delete_file = GetDeleteData(file);
		file_entry.row_id_start = transaction_row_start;
		transaction_row_start += file.row_count;
		result.push_back(std::move(file_entry));
	}
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = 0;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		result.push_back(std::move(file_entry));
	}
	if (transaction_local_data) {
		// we have transaction local inlined data - create the dummy file entry
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file.path = DUCKLAKE_TRANSACTION_LOCAL_INLINED_FILENAME;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = transaction_local_data->data->Count();
		file_entry.row_id_start = transaction_row_start;
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		result.push_back(std::move(file_entry));
	}
	if (!read_file_list) {
		// we have not read the file list yet - construct it from the extended file list
		for (auto &file : result) {
			DuckLakeFileListEntry file_entry;
			file_entry.file = file.file;
			file_entry.row_id_start = file.row_id_start;
			file_entry.delete_file = file.delete_file;
			files.emplace_back(std::move(file_entry));
		}
		read_file_list = true;
	}
	return result;
}

void DuckLakeMultiFileList::GetFilesForTable() {
	EvaluateDeferredFilters();

	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!read_info.table_id.IsTransactionLocal()) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		files = metadata_manager.GetFilesForTable(read_info.table, read_info.snapshot, cte_section, filter);
	}
	if (transaction.HasDroppedFiles()) {
		for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
			if (transaction.FileIsDropped(files[file_idx].file.path)) {
				files.erase_at(file_idx);
				file_idx--;
			}
		}
	}
	// if the transaction has any local deletes - apply them to the file list
	if (transaction.HasLocalDeletes(read_info.table_id)) {
		for (auto &file_entry : files) {
			transaction.GetLocalDeleteForFile(read_info.table_id, file_entry.file.path, file_entry.delete_file);
		}
	}
	idx_t transaction_row_start = TRANSACTION_LOCAL_ID_START;
	for (auto &file : transaction_local_files) {
		DuckLakeFileListEntry file_entry;
		file_entry.file = GetFileData(file);
		file_entry.row_id_start = transaction_row_start;
		file_entry.delete_file = GetDeleteData(file);
		file_entry.mapping_id = file.mapping_id;
		transaction_row_start += file.row_count;
		files.emplace_back(std::move(file_entry));
	}
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
	if (transaction_local_data) {
		// we have transaction local inlined data - create the dummy file entry
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = DUCKLAKE_TRANSACTION_LOCAL_INLINED_FILENAME;
		file_entry.row_id_start = transaction_row_start;
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

void DuckLakeMultiFileList::GetTableInsertions() {
	if (read_info.table_id.IsTransactionLocal()) {
		throw InternalException("Cannot get changes between snapshots for transaction-local files");
	}
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	auto &metadata_manager = transaction.GetMetadataManager();
	files = metadata_manager.GetTableInsertions(read_info.table, *read_info.start_snapshot, read_info.snapshot);
	// add inlined data tables as sources (if any)
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

void DuckLakeMultiFileList::GetTableDeletions() {
	if (read_info.table_id.IsTransactionLocal()) {
		throw InternalException("Cannot get changes between snapshots for transaction-local files");
	}
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	auto &metadata_manager = transaction.GetMetadataManager();
	delete_scans = metadata_manager.GetTableDeletions(read_info.table, *read_info.start_snapshot, read_info.snapshot);
	for (auto &file : delete_scans) {
		DuckLakeFileListEntry file_entry;
		file_entry.file = file.file;
		file_entry.row_id_start = file.row_id_start;
		file_entry.snapshot_id = file.snapshot_id;
		file_entry.mapping_id = file.mapping_id;
		files.emplace_back(std::move(file_entry));
	}
	// add inlined data tables as sources (if any)
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

bool DuckLakeMultiFileList::IsDeleteScan() const {
	return read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS;
}

const DuckLakeDeleteScanEntry &DuckLakeMultiFileList::GetDeleteScanEntry(idx_t file_idx) {
	return delete_scans[file_idx];
}

const vector<DuckLakeFileListEntry> &DuckLakeMultiFileList::GetFiles() {
	lock_guard<mutex> l(file_lock);
	if (!read_file_list) {
		// we have not read the file list yet - read it
		switch (read_info.scan_type) {
		case DuckLakeScanType::SCAN_TABLE:
			GetFilesForTable();
			break;
		case DuckLakeScanType::SCAN_INSERTIONS:
			GetTableInsertions();
			break;
		case DuckLakeScanType::SCAN_DELETIONS:
			GetTableDeletions();
			break;
		default:
			throw InternalException("Unknown DuckLake scan type");
		}
		read_file_list = true;
	}
	return files;
}

} // namespace duckdb
