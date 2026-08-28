#include "common/ducklake_util.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_metadata_manager.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeDataFile> transaction_local_files_p,
                                             shared_ptr<DuckLakeInlinedData> transaction_local_data_p,
                                             unique_ptr<FilterPushdownInfo> filter_info_p)
    : read_info(read_info), read_file_list(false), transaction_local_files(std::move(transaction_local_files_p)),
      transaction_local_data(std::move(transaction_local_data_p)), filter_info(std::move(filter_info_p)) {
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeFileListEntry> files_to_scan)
    : read_info(read_info), files(std::move(files_to_scan)), read_file_list(true) {
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             const DuckLakeInlinedTableInfo &inlined_table)
    : read_info(read_info), read_file_list(true) {
	DuckLakeFileListEntry file_entry;
	file_entry.file.path = inlined_table.table_name;
	file_entry.row_id_start = 0;
	file_entry.data_type = DuckLakeDataType::INLINED_DATA;
	files.push_back(std::move(file_entry));
	inlined_data_tables.push_back(inlined_table);
}

//! Combine two filters on the same column - both must hold, so AND their conjuncts and drop duplicates
static unique_ptr<Expression> MergeFilterExpressions(unique_ptr<Expression> left, unique_ptr<Expression> right) {
	vector<unique_ptr<Expression>> conjuncts;
	conjuncts.push_back(std::move(left));
	conjuncts.push_back(std::move(right));
	LogicalFilter::SplitPredicates(conjuncts);

	vector<unique_ptr<Expression>> merged;
	for (auto &conjunct : conjuncts) {
		bool is_duplicate = false;
		for (auto &existing : merged) {
			if (existing->Equals(*conjunct)) {
				is_duplicate = true;
				break;
			}
		}
		if (!is_duplicate) {
			merged.push_back(std::move(conjunct));
		}
	}
	if (merged.size() == 1) {
		return std::move(merged[0]);
	}
	auto result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &conjunct : merged) {
		result->GetChildrenMutable().push_back(std::move(conjunct));
	}
	return std::move(result);
}

static bool IsStructExtract(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	return func.Function().GetName() == "struct_extract" && func.GetChildren().size() == 2 &&
	       func.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
}

//! Collect the maximal sub-expressions a filter reads a column through, without descending into them
static void CollectFilterSubjects(const Expression &expr, vector<reference<const Expression>> &subjects) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
	    expr.GetExpressionClass() == ExpressionClass::BOUND_REF || IsStructExtract(expr)) {
		subjects.push_back(expr);
		return;
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { CollectFilterSubjects(child, subjects); });
}

//! Find the single sub-expression a filter constrains, or nullptr if it does not constrain exactly one
static optional_ptr<const Expression> GetFilterSubject(const Expression &expr) {
	vector<reference<const Expression>> subjects;
	CollectFilterSubjects(expr, subjects);
	if (subjects.empty()) {
		return nullptr;
	}
	for (idx_t i = 1; i < subjects.size(); i++) {
		if (!subjects[i].get().Equals(subjects[0].get())) {
			return nullptr;
		}
	}
	return subjects[0].get();
}

//! Rewrite the subject to the column placeholder an ExpressionFilter is evaluated against
static unique_ptr<Expression> ReplaceFilterSubject(const Expression &expr, const Expression &subject,
                                                   const LogicalType &type) {
	if (expr.Equals(subject)) {
		return make_uniq<BoundReferenceExpression>(type, 0U);
	}
	auto result = expr.Copy();
	ExpressionIterator::EnumerateChildren(
	    *result, [&](unique_ptr<Expression> &child) { child = ReplaceFilterSubject(*child, subject, type); });
	return result;
}

optional_ptr<const DuckLakeFieldId> DuckLakeMultiFileList::ResolveFilterField(const Expression &subject,
                                                                              column_t column_id) const {
	if (IsVirtualColumn(column_id)) {
		return nullptr;
	}
	// peel the struct_extract chain, the innermost extract names the outermost field
	vector<string> path;
	reference<const Expression> current = subject;
	while (IsStructExtract(current.get())) {
		auto &func = current.get().Cast<BoundFunctionExpression>();
		auto &key = func.GetChildren()[1]->Cast<BoundConstantExpression>().GetValue();
		if (key.IsNull() || key.type().id() != LogicalTypeId::VARCHAR) {
			return nullptr;
		}
		path.push_back(StringValue::Get(key));
		current = *func.GetChildren()[0];
	}
	if (current.get().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF &&
	    current.get().GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return nullptr;
	}
	optional_ptr<const DuckLakeFieldId> field_id = read_info.table.GetFieldId(PhysicalIndex(column_id));
	for (auto it = path.rbegin(); it != path.rend(); it++) {
		field_id = field_id->GetChildByName(*it);
		if (!field_id) {
			return nullptr;
		}
	}
	return field_id;
}

unique_ptr<DuckLakeFilterNode> DuckLakeMultiFileList::GetColumnFilterNode(column_t column_id, const Expression &expr,
                                                                          const LogicalType &column_type) const {
	auto subject = GetFilterSubject(expr);
	if (subject) {
		if (!IsStructExtract(*subject)) {
			return make_uniq<DuckLakeFilterNode>(
			    ColumnFilterInfo(read_info.table.GetFieldId(PhysicalIndex(column_id)).GetFieldIndex().index,
			                     column_type, make_uniq<ExpressionFilter>(expr.Copy())));
		}
		// stats for a nested field are stored against that field, not against the column that contains it
		auto field_id = ResolveFilterField(*subject, column_id);
		if (!field_id) {
			return nullptr;
		}
		auto rewritten = ReplaceFilterSubject(expr, *subject, field_id->Type());
		return make_uniq<DuckLakeFilterNode>(ColumnFilterInfo(field_id->GetFieldIndex().index, field_id->Type(),
		                                                      make_uniq<ExpressionFilter>(std::move(rewritten))));
	}
	// a conjunction may constrain several nested fields of the same column, each with their own stats
	if (expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
		return nullptr;
	}
	auto result = make_uniq<DuckLakeFilterNode>(DuckLakeFilterNodeType::CONJUNCTION_AND);
	for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
		auto node = GetColumnFilterNode(column_id, *child, column_type);
		if (node) {
			result->children.push_back(std::move(node));
		}
	}
	if (result->children.empty()) {
		return nullptr;
	}
	return std::move(result);
}

unique_ptr<DuckLakeFilterNode> DuckLakeMultiFileList::GetFilterNode(column_t column_id,
                                                                    unique_ptr<TableFilter> filter) const {
	if (IsVirtualColumn(column_id)) {
		return nullptr;
	}
	auto column_index = PhysicalIndex(column_id);
	// Get the column type from the table schema, not from the scan types array
	const auto &column_type = read_info.column_types[column_index.index];
	auto expr_filter = ExpressionFilter::FromTableFilter(*filter, column_type);
	if (!expr_filter->expr) {
		return nullptr;
	}
	return GetColumnFilterNode(column_id, *expr_filter->expr, column_type);
}

unique_ptr<DuckLakeFilterNode> DuckLakeMultiFileList::GetExpressionFilterNode(MultiFilePushdownInfo &info,
                                                                              const Expression &expr) const {
	auto subject = GetFilterSubject(expr);
	if (!subject) {
		return nullptr;
	}
	// the innermost reference identifies the column, the projection index is the same space the filter set uses
	reference<const Expression> root = *subject;
	while (IsStructExtract(root.get())) {
		root = *root.get().Cast<BoundFunctionExpression>().GetChildren()[0];
	}
	if (root.get().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto projection_index = root.get().Cast<BoundColumnRefExpression>().Binding().column_index;
	if (projection_index >= info.column_ids.size()) {
		return nullptr;
	}
	auto column_id = info.column_ids[projection_index];
	auto field_id = ResolveFilterField(*subject, column_id);
	if (!field_id) {
		return nullptr;
	}
	auto rewritten = ReplaceFilterSubject(expr, *subject, field_id->Type());
	return make_uniq<DuckLakeFilterNode>(ColumnFilterInfo(field_id->GetFieldIndex().index, field_id->Type(),
	                                                      make_uniq<ExpressionFilter>(std::move(rewritten))));
}

void DuckLakeMultiFileList::AddFilterToPushdownInfo(FilterPushdownInfo &pushdown_info, column_t column_id,
                                                    unique_ptr<TableFilter> filter) const {
	auto node = GetFilterNode(column_id, std::move(filter));
	if (node) {
		AddFilterNodeToPushdownInfo(pushdown_info, *node);
	}
}

void DuckLakeMultiFileList::AddFilterNodeToPushdownInfo(FilterPushdownInfo &pushdown_info,
                                                        DuckLakeFilterNode &node) const {
	if (node.type == DuckLakeFilterNodeType::CONJUNCTION_AND) {
		// every conjunct has to hold, so each can be filtered on separately
		for (auto &child : node.children) {
			AddFilterNodeToPushdownInfo(pushdown_info, *child);
		}
		return;
	}
	if (node.type != DuckLakeFilterNodeType::COLUMN_FILTER) {
		return;
	}
	auto &column_filter = *node.column_filter;
	auto entry = pushdown_info.column_filters.find(column_filter.column_field_index);
	if (entry == pushdown_info.column_filters.end()) {
		pushdown_info.column_filters.emplace(column_filter.column_field_index, std::move(column_filter));
		return;
	}
	auto &existing_filter = entry->second.table_filter;
	existing_filter = make_uniq<ExpressionFilter>(
	    MergeFilterExpressions(std::move(existing_filter->expr), std::move(column_filter.table_filter->expr)));
}

unique_ptr<MultiFileList>
DuckLakeMultiFileList::DynamicFilterPushdown(MultiFileDynamicPushdownInfo &dynamic_pushdown_info) const {
	auto &options = dynamic_pushdown_info.options;
	auto &names = dynamic_pushdown_info.column_names;
	auto &types = dynamic_pushdown_info.column_types;
	auto &column_ids = dynamic_pushdown_info.column_ids;
	auto &context = dynamic_pushdown_info.context;
	auto &filters = dynamic_pushdown_info.filters;

	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || !filters.HasFilters()) {
		// filter pushdown is only supported when scanning full tables
		return nullptr;
	}

	// the final filter set does not always carry over the filters we pushed down earlier - merge into those
	auto pushdown_info = filter_info ? filter_info->Copy() : make_uniq<FilterPushdownInfo>();

	for (auto &entry : filters) {
		auto column_id = column_ids[entry.GetIndex().GetIndex()];
		AddFilterToPushdownInfo(
		    *pushdown_info, column_id,
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "DuckLakeMultiFileList::DynamicFilterPushdown")
		        .Copy());
	}

	if (pushdown_info->column_filters.empty()) {
		// no pushdown possible
		return nullptr;
	}

	return make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                        std::move(pushdown_info));
}

//! Reduce an expression to per-column filters using the combiner, which also propagates equalities
unique_ptr<DuckLakeFilterNode> DuckLakeMultiFileList::CombineFilterNode(ClientContext &context,
                                                                        MultiFilePushdownInfo &info,
                                                                        const Expression &expr) const {
	// the combiner is fed one conjunct at a time, the optimizer splits them before they reach it
	vector<unique_ptr<Expression>> conjuncts;
	conjuncts.push_back(expr.Copy());
	LogicalFilter::SplitPredicates(conjuncts);

	FilterCombiner combiner(context);
	for (auto &conjunct : conjuncts) {
		if (combiner.AddFilter(std::move(conjunct)) == FilterResult::UNSATISFIABLE) {
			return make_uniq<DuckLakeFilterNode>(DuckLakeFilterNodeType::MATCH_NONE);
		}
	}
	vector<FilterPushdownResult> pushdown_results;
	auto table_filter_set = combiner.GenerateTableScanFilters(info.column_indexes, pushdown_results);
	if (combiner.HasFilters() || !table_filter_set.HasFilters()) {
		return nullptr;
	}
	auto result = make_uniq<DuckLakeFilterNode>(DuckLakeFilterNodeType::CONJUNCTION_AND);
	for (auto &entry : table_filter_set) {
		auto node = GetFilterNode(info.column_ids[entry.GetIndex().GetIndex()], entry.TakeFilter());
		if (node) {
			result->children.push_back(std::move(node));
		}
	}
	if (result->children.empty()) {
		return nullptr;
	}
	return std::move(result);
}

unique_ptr<DuckLakeFilterNode> DuckLakeMultiFileList::BuildFilterTree(ClientContext &context,
                                                                      MultiFilePushdownInfo &info,
                                                                      const Expression &expr,
                                                                      FilterTreeState &state) const {
	if (state.budget == 0) {
		return nullptr;
	}
	state.budget--;

	const bool is_or = expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
	if (!is_or) {
		// the combiner sees all conjuncts at once, so let it try before splitting them up
		auto node = CombineFilterNode(context, info, expr);
		if (node) {
			return node;
		}
	}

	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto result = make_uniq<DuckLakeFilterNode>(is_or ? DuckLakeFilterNodeType::CONJUNCTION_OR
		                                                  : DuckLakeFilterNodeType::CONJUNCTION_AND);
		bool complete = true;
		for (auto &child : conjunction.GetChildren()) {
			auto node = BuildFilterTree(context, info, *child, state);
			if (!node) {
				// a branch we cannot express prunes nothing, so the whole disjunction prunes nothing
				if (is_or) {
					complete = false;
					break;
				}
				continue;
			}
			if (node->type == DuckLakeFilterNodeType::MATCH_NONE) {
				// a branch that matches nothing drops out of a disjunction and decides a conjunction
				if (is_or) {
					state.removed_branch = true;
					continue;
				}
				return node;
			}
			result->children.push_back(std::move(node));
		}
		if (complete && result->children.empty() && is_or) {
			// every branch matched nothing
			return make_uniq<DuckLakeFilterNode>(DuckLakeFilterNodeType::MATCH_NONE);
		}
		if (complete && !result->children.empty()) {
			return std::move(result);
		}
	}

	if (is_or) {
		// the branches did not work out - the combiner may still express the disjunction on a single column
		auto node = CombineFilterNode(context, info, expr);
		if (node) {
			return node;
		}
	}
	// the combiner only expresses a subset of what we can evaluate against column stats
	return GetExpressionFilterNode(info, expr);
}

//! Collect the columns a filter tree references
static void GetFilterTreeColumns(const DuckLakeFilterNode &node, unordered_set<idx_t> &columns) {
	if (node.type == DuckLakeFilterNodeType::COLUMN_FILTER) {
		columns.insert(node.column_filter->column_field_index);
		return;
	}
	for (const auto &child : node.children) {
		GetFilterTreeColumns(*child, columns);
	}
}

unique_ptr<MultiFileList> DuckLakeMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                       const MultiFileOptions &options,
                                                                       MultiFilePushdownInfo &info,
                                                                       vector<unique_ptr<Expression>> &filters) const {
	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || filters.empty()) {
		return nullptr;
	}

	FilterCombiner combiner(context);
	for (auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}
	vector<FilterPushdownResult> pushdown_results;
	auto table_filter_set = combiner.GenerateTableScanFilters(info.column_indexes, pushdown_results);

	auto pushdown_info = filter_info ? filter_info->Copy() : make_uniq<FilterPushdownInfo>();

	for (auto &entry : table_filter_set) {
		auto column_id = info.column_ids[entry.GetIndex().GetIndex()];
		AddFilterToPushdownInfo(*pushdown_info, column_id, entry.TakeFilter());
	}

	// a disjunction cannot be reduced to per-column filters without losing the correlation between them
	for (auto &filter : filters) {
		if (filter->GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
			continue;
		}
		bool already_pushed = false;
		for (auto &tree : pushdown_info->filter_trees) {
			if (tree.source->Equals(*filter)) {
				already_pushed = true;
				break;
			}
		}
		if (already_pushed) {
			continue;
		}
		FilterTreeState state;
		auto root = BuildFilterTree(context, info, *filter, state);
		if (!root) {
			continue;
		}
		unordered_set<idx_t> columns;
		GetFilterTreeColumns(*root, columns);
		if (columns.size() < 2 && !state.removed_branch) {
			// a disjunction over a single column is already covered by the per-column filters
			continue;
		}
		DuckLakeFilterTree tree;
		tree.root = std::move(root);
		tree.source = filter->Copy();
		pushdown_info->filter_trees.push_back(std::move(tree));
	}

	if (pushdown_info->Empty()) {
		return nullptr;
	}

	return make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                        std::move(pushdown_info));
}

vector<OpenFileInfo> DuckLakeMultiFileList::GetAllFiles() const {
	vector<OpenFileInfo> file_list;
	for (idx_t i = 0; i < GetTotalFileCount(); i++) {
		file_list.push_back(GetFile(i));
	}
	return file_list;
}

FileExpandResult DuckLakeMultiFileList::GetExpandResult() const {
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t DuckLakeMultiFileList::GetTotalFileCount() const {
	return GetFiles().size();
}

unique_ptr<NodeStatistics> DuckLakeMultiFileList::GetCardinality(ClientContext &context) const {
	auto stats = read_info.table.GetTableStats(context);
	if (!stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(stats->record_count);
}

DuckLakeTableEntry &DuckLakeMultiFileList::GetTable() {
	return read_info.table;
}

OpenFileInfo DuckLakeMultiFileList::GetFile(idx_t i) const {
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

unique_ptr<MultiFileList> DuckLakeMultiFileList::Copy() const {
	unique_ptr<FilterPushdownInfo> filter_copy;
	if (filter_info) {
		filter_copy = filter_info->Copy();
	}

	auto result = make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                               std::move(filter_copy));
	result->files = GetFiles();
	result->read_file_list = read_file_list;
	result->delete_scans = delete_scans;
	result->inlined_data_tables = inlined_data_tables;
	return std::move(result);
}

const DuckLakeFileListEntry &DuckLakeMultiFileList::GetFileEntry(idx_t file_idx) const {
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
	if (file.delete_files.empty()) {
		return result;
	}
	auto &delete_file = file.delete_files.back();
	result.path = delete_file.file_name;
	result.encryption_key = delete_file.encryption_key;
	result.file_size_bytes = delete_file.file_size_bytes;
	result.footer_size = delete_file.footer_size;
	result.format = delete_file.format;
	return result;
}

vector<DuckLakeFileListExtendedEntry> DuckLakeMultiFileList::GetFilesExtended() const {
	lock_guard<mutex> l(file_lock);
	vector<DuckLakeFileListExtendedEntry> result;
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!IsTransactionLocal(read_info.table_id)) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		result = metadata_manager.GetExtendedFilesForTable(read_info.table, read_info.snapshot, filter_info.get());
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
	idx_t transaction_row_start = DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START;
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
		file_entry.row_id_start = GetTransactionLocalRowIdStart(transaction_row_start);
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		result.push_back(std::move(file_entry));
	}
	return result;
}

void DuckLakeMultiFileList::GetFilesForTable() const {
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!IsTransactionLocal(read_info.table_id)) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		files = metadata_manager.GetFilesForTable(read_info.table, read_info.snapshot, filter_info.get());
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
	// if the transaction has any local inlined file deletes - apply them to the file list
	if (transaction.HasLocalInlinedFileDeletes(read_info.table_id)) {
		for (auto &file_entry : files) {
			if (file_entry.file_id.IsValid()) {
				transaction.GetLocalInlinedFileDeletesForFile(read_info.table_id, file_entry.file_id.index,
				                                              file_entry.inlined_file_deletions);
			}
		}
	}
	idx_t transaction_row_start = DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START;
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
		file_entry.row_id_start = GetTransactionLocalRowIdStart(transaction_row_start);
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

void DuckLakeMultiFileList::GetTableInsertions() const {
	if (IsTransactionLocal(read_info.table_id)) {
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

void DuckLakeMultiFileList::GetTableDeletions() const {
	if (IsTransactionLocal(read_info.table_id)) {
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

bool DuckLakeMultiFileList::CanUseGlobalStats() const {
	return read_info.CanUseGlobalStats();
}

bool DuckLakeMultiFileList::IsDeleteScan() const {
	return read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS;
}

const DuckLakeDeleteScanEntry &DuckLakeMultiFileList::GetDeleteScanEntry(idx_t file_idx) {
	return delete_scans[file_idx];
}

const vector<DuckLakeFileListEntry> &DuckLakeMultiFileList::GetFiles() const {
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

idx_t DuckLakeMultiFileList::GetTransactionLocalRowIdStart(idx_t transaction_row_start) const {
	if (transaction_local_data && transaction_local_data->HasPreservedRowIds()) {
		// preserved row_ids are absolute, so row_id_start must be 0
		return 0;
	}
	return transaction_row_start;
}

} // namespace duckdb
