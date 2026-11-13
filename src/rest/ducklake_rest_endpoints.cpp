#include "rest/ducklake_rest_server.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "rest/ducklake_rest_catalog_changes.hpp"
#include "rest/ducklake_rest_filters.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include <algorithm>
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
static DuckLakeTableEntry &GetTableEntry(DuckLakeRESTRequest &request, DuckLakeSnapshot snapshot, TableIndex table_id) {
	auto entry = request.catalog->GetEntryById(*request.transaction, snapshot, table_id);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException("Table with id %llu does not exist at snapshot %llu", table_id.index,
		                       snapshot.snapshot_id);
	}
	return entry->Cast<DuckLakeTableEntry>();
}

//! The most entries that a page has
static constexpr idx_t MAX_PAGE_SIZE = 10000;

static idx_t GetPageSize(DuckLakeRESTRequest &request, optional_idx page_size) {
	if (!page_size.IsValid()) {
		return request.server->DefaultPageSize();
	}
	if (page_size.GetIndex() == 0) {
		throw InvalidInputException("The page_size has to be larger than 0");
	}
	return MinValue<idx_t>(page_size.GetIndex(), MAX_PAGE_SIZE);
}

//! The position that a page token stands for, 0 for the first page
static idx_t GetPageStart(const string &page_token) {
	if (page_token.empty()) {
		return 0;
	}
	idx_t result;
	if (!TryCast::Operation<string_t, idx_t>(string_t(page_token), result)) {
		throw InvalidInputException("\"%s\" is not a page token", page_token);
	}
	return result;
}

//! Keeps the page of entries that are ordered by their keys: the page token is the key of the last entry of the page
//! before, a next page token is returned if there are entries after the page
template <class T, class KEY>
static void GetKeyPage(DuckLakeRESTRequest &request, vector<T> &entries, const KEY &get_key, optional_idx page_size,
                       const string &page_token, string &next_page_token) {
	std::sort(entries.begin(), entries.end(), [&](const T &a, const T &b) { return get_key(a) < get_key(b); });
	idx_t start = 0;
	if (!page_token.empty()) {
		auto after = GetPageStart(page_token);
		while (start < entries.size() && get_key(entries[start]) <= after) {
			start++;
		}
	}
	auto size = GetPageSize(request, page_size);
	if (start + size >= entries.size()) {
		entries.erase(entries.begin(), entries.begin() + NumericCast<int64_t>(start));
		return;
	}
	entries.erase(entries.begin() + NumericCast<int64_t>(start + size), entries.end());
	entries.erase(entries.begin(), entries.begin() + NumericCast<int64_t>(start));
	next_page_token = to_string(get_key(entries.back()));
}

//! Moves the entries that follow the first page of a scan into a plan, and returns the page tokens of the plan. The
//! plan is shared by the lists of entries of the scan
template <class T>
static void SplitIntoPages(DuckLakeRESTRequest &request, TableIndex table_id, vector<T> &entries,
                           vector<T> DuckLakeRESTPlan::*member, const set<idx_t> &known_mappings,
                           vector<string> &page_tokens) {
	auto page_size = request.server->FilesPerPage();
	if (entries.size() <= page_size) {
		return;
	}
	auto plan = make_shared_ptr<DuckLakeRESTPlan>();
	plan->prefix = request.prefix;
	plan->table_id = table_id.index;
	plan->page_size = page_size;
	plan->known_mappings = known_mappings;
	auto &remainder = plan.get()->*member;
	for (idx_t i = page_size; i < entries.size(); i++) {
		remainder.push_back(std::move(entries[i]));
	}
	entries.erase(entries.begin() + NumericCast<int64_t>(page_size), entries.end());
	auto page_count = (remainder.size() + page_size - 1) / page_size;
	auto id = request.server->Plans().Add(std::move(plan));
	for (idx_t i = 0; i < page_count; i++) {
		page_tokens.push_back(id + "." + to_string(i));
	}
}

//! Adds the column mappings that the files refer to, except for the ones that the client has
template <class T>
static void AddMappingIds(const vector<T> &entries, set<idx_t> &mapping_ids) {
	for (auto &entry : entries) {
		if (entry.mapping_id.IsValid()) {
			mapping_ids.insert(entry.mapping_id.index);
		}
	}
}

static vector<DuckLakeColumnMappingInfo> GetColumnMappings(DuckLakeRESTRequest &request, set<idx_t> mapping_ids,
                                                           const set<idx_t> &known_mappings) {
	for (auto known : known_mappings) {
		mapping_ids.erase(known);
	}
	return request.GetMetadataManager().GetColumnMappings(mapping_ids);
}

//===--------------------------------------------------------------------===//
// Read Endpoints
//===--------------------------------------------------------------------===//
RESTConfigResponse DuckLakeRESTEndpoints::Config(DuckLakeRESTRequest &request, const RESTConfigRequest &parameters) {
	RESTConfigResponse result;
	result.defaults = request.server->ClientDefaults();
	result.overrides = request.server->ClientOverrides();
	// the URLs of the catalog have its prefix
	result.overrides["prefix"] = request.prefix;
	for (auto &route : DuckLakeRESTAPI::GetRoutes()) {
		result.endpoints.push_back(route.method + " " + route.path);
	}
	return result;
}

DuckLakeMetadata DuckLakeRESTEndpoints::GetMetadata(DuckLakeRESTRequest &request,
                                                    const RESTGetMetadataRequest &parameters) {
	auto metadata = request.GetMetadataManager().LoadDuckLake();
	for (auto &tag : metadata.tags) {
		if (tag.key == "data_path") {
			// clients get the resolved data path
			tag.value = request.catalog->DataPath();
		}
	}
	return metadata;
}

//! Adds the catalog and statistics of the snapshot that the request asks for
static void AddSnapshotData(DuckLakeRESTRequest &request, const RESTSnapshotIncludes &includes,
                            RESTSnapshotData &result) {
	auto &metadata_manager = request.GetMetadataManager();
	auto &snapshot = result.snapshot;
	// clients that pass the versions they have loaded get the catalog and statistics if they changed
	bool include_catalog =
	    includes.include_catalog || (includes.cached_metadata_version.IsValid() &&
	                                 includes.cached_metadata_version.GetIndex() != snapshot.schema_version);
	bool include_stats = includes.include_stats || (includes.cached_data_version.IsValid() &&
	                                                includes.cached_data_version.GetIndex() != snapshot.next_file_id);
	if (include_catalog) {
		result.catalog = make_uniq<DuckLakeCatalogInfo>(metadata_manager.GetCatalogForSnapshot(snapshot));
		auto next_column_ids = metadata_manager.GetNextColumnIds();
		for (auto &table : result.catalog->tables) {
			auto entry = next_column_ids.find(table.id.index);
			if (entry != next_column_ids.end()) {
				table.next_column_id = entry->second;
			}
		}
	}
	if (include_stats) {
		result.stats = metadata_manager.GetGlobalTableStats(snapshot);
	}
}

RESTSnapshotData DuckLakeRESTEndpoints::GetLatestSnapshot(DuckLakeRESTRequest &request,
                                                          const RESTGetLatestSnapshotRequest &parameters) {
	RESTSnapshotData result;
	result.snapshot = request.transaction->GetSnapshot();
	AddSnapshotData(request, parameters, result);
	return result;
}

RESTSnapshotData DuckLakeRESTEndpoints::GetSnapshotAt(DuckLakeRESTRequest &request,
                                                      const RESTGetSnapshotAtRequest &parameters) {
	Value value;
	if (parameters.unit == "version") {
		idx_t snapshot_id;
		if (TryCast::Operation<string_t, idx_t>(string_t(parameters.value), snapshot_id)) {
			value = Value::UBIGINT(snapshot_id);
		} else {
			// the name of a snapshot tag
			value = Value(parameters.value);
		}
	} else {
		value = Value(parameters.value).DefaultCastAs(LogicalType::TIMESTAMP_TZ);
	}
	BoundAtClause at_clause(Identifier(parameters.unit), value);
	auto bound = SnapshotBound::UPPER_BOUND;
	if (parameters.bound == "lower") {
		bound = SnapshotBound::LOWER_BOUND;
	} else if (parameters.bound == "time_travel") {
		bound = SnapshotBound::TIME_TRAVEL;
	}
	auto newest_snapshot_id =
	    parameters.newest_snapshot_id.IsValid() ? parameters.newest_snapshot_id.GetIndex() : DConstants::INVALID_INDEX;
	RESTSnapshotData result;
	auto &metadata_manager = request.GetMetadataManager();
	result.snapshot = *metadata_manager.GetSnapshot(at_clause, bound, newest_snapshot_id);
	if (bound == SnapshotBound::TIME_TRAVEL && at_clause.Unit() == "timestamp" &&
	    request.catalog->RecordsExpiredSnapshots()) {
		// time travel by timestamp refuses times before expired snapshots
		metadata_manager.CheckSnapshotAfterExpiredGap(result.snapshot, value, newest_snapshot_id, set<idx_t>());
	}
	AddSnapshotData(request, parameters, result);
	return result;
}

//! The value of a field in rows that were written before the field existed
static string MissingFieldExpression(const DuckLakeFieldId &field) {
	auto initial_default = field.GetInitialDefault();
	return StringUtil::Format("CAST(%s AS %s)", initial_default ? initial_default->ToString() : "NULL",
	                          field.Type().ToString());
}

//! Whether a field was written with the type and the (nested) field ids that it has
static bool SameFields(const DuckLakeFieldId &written, const DuckLakeFieldId &field) {
	auto &written_children = written.Children();
	auto &children = field.Children();
	if (written.GetFieldIndex() != field.GetFieldIndex() || written.Type() != field.Type() ||
	    written_children.size() != children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!SameFields(*written_children[i], *children[i])) {
			return false;
		}
	}
	return true;
}

//! The expression that turns a value of a field as it was written into a value of the field as it is, matching the
//! (nested) fields by their field ids
static string EvolveFieldExpression(const string &expression, const DuckLakeFieldId &written,
                                    const DuckLakeFieldId &field, idx_t depth) {
	if (SameFields(written, field)) {
		return expression;
	}
	auto &written_children = written.Children();
	auto &children = field.Children();
	auto variable = "x" + to_string(depth);
	switch (field.Type().id()) {
	case LogicalTypeId::STRUCT: {
		if (written.Type().id() != LogicalTypeId::STRUCT) {
			break;
		}
		vector<string> entries;
		for (auto &child : children) {
			optional_ptr<const DuckLakeFieldId> written_child;
			for (auto &candidate : written_children) {
				if (candidate->GetFieldIndex() == child->GetFieldIndex()) {
					written_child = candidate.get();
				}
			}
			string value;
			if (written_child) {
				auto extracted = StringUtil::Format("struct_extract(%s, %s)", expression,
				                                    SQLString::ToString(written_child->Name().GetIdentifierName()));
				value = EvolveFieldExpression(extracted, *written_child, *child, depth + 1);
			} else {
				value = MissingFieldExpression(*child);
			}
			entries.push_back(
			    StringUtil::Format("%s: %s", SQLString::ToString(child->Name().GetIdentifierName()), value));
		}
		return StringUtil::Format("CASE WHEN %s IS NULL THEN NULL ELSE {%s} END", expression,
		                          StringUtil::Join(entries, ", "));
	}
	case LogicalTypeId::LIST:
		if (written.Type().id() != LogicalTypeId::LIST || written_children.size() != 1 || children.size() != 1) {
			break;
		}
		return StringUtil::Format("list_transform(%s, lambda %s: %s)", expression, variable,
		                          EvolveFieldExpression(variable, *written_children[0], *children[0], depth + 1));
	case LogicalTypeId::MAP:
		if (written.Type().id() != LogicalTypeId::MAP || written_children.size() != 2 || children.size() != 2) {
			break;
		}
		return StringUtil::Format(
		    "map_from_entries(list_transform(map_entries(%s), lambda %s: {'key': %s, 'value': %s}))", expression,
		    variable,
		    EvolveFieldExpression(StringUtil::Format("struct_extract(%s, 'key')", variable), *written_children[0],
		                          *children[0], depth + 1),
		    EvolveFieldExpression(StringUtil::Format("struct_extract(%s, 'value')", variable), *written_children[1],
		                          *children[1], depth + 1));
	default:
		break;
	}
	return StringUtil::Format("CAST(%s AS %s)", expression, field.Type().ToString());
}

//! Sets the fields of a struct that the client does not read to NULL, in the struct and in the structs it holds
static void RemoveUnreadFields(Vector &column, idx_t count, const DuckLakeFieldId &field,
                               const RESTColumnIdentifier &identifier) {
	if (identifier.children.empty() || column.GetType().id() != LogicalTypeId::STRUCT) {
		return;
	}
	auto &entries = StructVector::GetEntries(column);
	auto &children = field.Children();
	for (idx_t i = 0; i < children.size() && i < entries.size(); i++) {
		auto child = std::find_if(
		    identifier.children.begin(), identifier.children.end(),
		    [&](const RESTColumnIdentifier &child) { return child.field_id == children[i]->GetFieldIndex().index; });
		entries[i].Flatten();
		if (child == identifier.children.end()) {
			entries[i].Reference(Value(entries[i].GetType()), count_t(count));
			entries[i].Flatten();
			continue;
		}
		RemoveUnreadFields(entries[i], count, *children[i], *child);
	}
}

//! Reads the inlined rows of a table in the schema it has at the snapshot, nothing if the table has none
static unique_ptr<RESTInlinedRows>
ReadInlinedRows(DuckLakeRESTRequest &request, DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                const std::function<unique_ptr<QueryResult>(const Identifier &, const vector<string> &)> &read_rows,
                const RESTOptional<vector<RESTColumnIdentifier>> &projection, bool ended_rows) {
	auto inlined_tables = table.GetInlinedDataTables(*request.transaction, snapshot);
	if (inlined_tables.empty()) {
		return nullptr;
	}
	// timestamps of columns that were changed to TIMESTAMPTZ are in UTC (without ICU, all timestamps are)
	auto &connection = request.transaction->GetConnection();
	Value current_time_zone;
	if (connection.context->TryGetCurrentSetting("TimeZone", current_time_zone)) {
		connection.Query("SET TimeZone = 'UTC'")->ThrowIfError();
	}
	unordered_map<idx_t, reference<const RESTColumnIdentifier>> projected_fields;
	for (auto &column : projection.value) {
		projected_fields.emplace(column.field_id, column);
	}
	auto &metadata_manager = request.GetMetadataManager();
	auto col_names = metadata_manager.InlinedColNames();
	vector<string> names {col_names.row_id.GetIdentifierName(), col_names.begin_snapshot.GetIdentifierName(),
	                      col_names.end_snapshot.GetIdentifierName()};
	vector<reference<const DuckLakeFieldId>> fields;
	//! The identifiers of the columns, to leave out the fields of structs that are not read
	vector<optional_ptr<const RESTColumnIdentifier>> identifiers;
	for (auto &column : table.GetColumns().Physical()) {
		auto &field = table.GetFieldId(column.Physical());
		optional_ptr<const RESTColumnIdentifier> identifier;
		if (projection.has_value) {
			auto entry = projected_fields.find(field.GetFieldIndex().index);
			if (entry == projected_fields.end()) {
				continue;
			}
			identifier = &entry->second.get();
		}
		names.push_back(column.Name().GetIdentifierName());
		fields.push_back(field);
		identifiers.push_back(identifier);
	}
	auto result = make_uniq<RESTInlinedRows>();
	unique_ptr<ColumnDataCollection> collection;
	for (auto &inlined_table : inlined_tables) {
		auto table_at_version = request.catalog->GetTableAtSchemaVersion(*request.transaction, table.GetTableId(),
		                                                                 inlined_table.schema_version);
		if (!table_at_version) {
			continue; // LCOV_EXCL_LINE: every schema version of an inlined table has its table
		}
		vector<string> columns;
		for (idx_t i = 0; i < DuckLakeInlinedColNames::COLUMN_COUNT; i++) {
			bool end_column = names[i] == col_names.end_snapshot.GetIdentifierName();
			bool end_derived = ended_rows || !request.catalog->SupportsBranching();
			columns.push_back(end_column && !end_derived ? "CAST(NULL AS BIGINT)" : SQLIdentifier::ToString(names[i]));
		}
		for (auto &field : fields) {
			auto written = table_at_version->GetFieldData().GetByFieldIndex(field.get().GetFieldIndex());
			if (!written) {
				columns.push_back(MissingFieldExpression(field.get()));
				continue;
			}
			auto read = metadata_manager.CastColumnToTarget(SQLIdentifier::ToString(written->Name()), written->Type());
			columns.push_back(EvolveFieldExpression(read, *written, field.get(), 0));
		}
		auto query_result = read_rows(inlined_table.table_name, columns);
		query_result->ThrowIfError("Failed to read inlined data: ");
		if (!collection) {
			for (idx_t col_idx = 0; col_idx < query_result->ColumnCount(); col_idx++) {
				RESTResultColumn column;
				column.name = names[col_idx];
				column.type = query_result->GetTypes()[col_idx];
				result->columns.push_back(std::move(column));
			}
			collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), query_result->GetTypes());
		}
		while (true) {
			auto chunk = query_result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t i = 0; i < fields.size(); i++) {
				if (identifiers[i]) {
					auto &column = chunk->data[DuckLakeInlinedColNames::COLUMN_COUNT + i];
					column.Flatten();
					RemoveUnreadFields(column, chunk->size(), fields[i].get(), *identifiers[i]);
				}
			}
			collection->Append(*chunk);
		}
	}
	if (!collection) {
		return nullptr; // LCOV_EXCL_LINE: the first inlined table creates the collection
	}
	result->rows = RESTRows(std::move(collection));
	return result;
}

//! The plan of a page token and the position of the page, throws if the plan expired or belongs to another catalog or
//! table
static shared_ptr<DuckLakeRESTPlan> GetPlanOfPage(DuckLakeRESTRequest &request, idx_t table_id,
                                                  const string &page_token, idx_t &page_index) {
	auto separator = page_token.rfind('.');
	shared_ptr<DuckLakeRESTPlan> plan;
	if (separator != string::npos &&
	    TryCast::Operation<string_t, idx_t>(string_t(page_token.substr(separator + 1)), page_index)) {
		plan = request.server->Plans().Get(page_token.substr(0, separator));
	}
	if (!plan || plan->prefix != request.prefix || plan->table_id != table_id) {
		throw CatalogException("The page \"%s\" does not exist: it expired or it belongs to another table", page_token);
	}
	return plan;
}

//! The entries of the page of a plan
template <class T>
static void GetPage(const vector<T> &entries, idx_t page_size, idx_t page_index, vector<T> &result) {
	for (idx_t i = page_index * page_size; i < entries.size() && i < (page_index + 1) * page_size; i++) {
		result.push_back(entries[i]);
	}
}

static void RequireOnlyPageToken(bool no_other_members) {
	if (!no_other_members) {
		throw InvalidInputException(
		    "A request with a page_token cannot have other members: they are the ones of the request that returned it");
	}
}

//! The top-level column of a table with the field id, if there is one
static optional_ptr<const DuckLakeFieldId> GetRootField(DuckLakeTableEntry &table, idx_t column_id) {
	for (auto &column : table.GetColumns().Physical()) {
		auto &field = table.GetFieldId(column.Physical());
		if (field.GetFieldIndex().index == column_id) {
			return &field;
		}
	}
	return nullptr;
}

//! Reads the min or max of a column from its statistics, if they are exact: the same values as the optimizer of DuckDB
//! uses for aggregates over the statistics of a table
static bool TryGetMinMax(const DuckLakeColumnStats &column_stats, bool is_min, Value &result) {
	auto &type = column_stats.type;
	if (type != LogicalType::VARCHAR && !type.IsNumeric() && !type.IsTemporal()) {
		return false;
	}
	auto stats = column_stats.ToStats();
	if (!stats) {
		return false;
	}
	if (stats->GetStatsType() == StatisticsType::NUMERIC_STATS) {
		if (!NumericStats::HasMinMax(*stats)) {
			return false;
		}
		result = is_min ? NumericStats::Min(*stats) : NumericStats::Max(*stats);
		return true;
	}
	if (stats->GetStatsType() != StatisticsType::STRING_STATS || !StringStats::HasMinMax(*stats)) {
		return false;
	}
	if (StringStats::GetMinType(*stats) != StringStatsType::EXACT_STATS ||
	    StringStats::GetMaxType(*stats) != StringStatsType::EXACT_STATS) {
		return false;
	}
	result = is_min ? StringStats::Min(*stats) : StringStats::Max(*stats);
	return true;
}

//! Answers aggregates over a table from its statistics where that is exact, and returns whether all were answered
static bool AnswerAggregates(DuckLakeRESTRequest &request, DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                             const vector<RESTAggregate> &aggregates, vector<RESTAggregateResult> &results) {
	auto net_count = request.GetMetadataManager().GetNetRowCount(table, snapshot);
	// the statistics of the table are the ones of the latest snapshot
	bool is_latest = snapshot.snapshot_id == request.transaction->GetSnapshot().snapshot_id;
	shared_ptr<DuckLakeTableStats> table_stats;
	if (is_latest) {
		table_stats = table.GetTableStats(*request.transaction);
	}
	// the bounds of the statistics only widen, they are exact if no row was ever deleted
	bool exact = net_count.IsValid() && table_stats && !table_stats->record_count_unknown &&
	             table_stats->record_count == net_count.GetIndex();
	bool all_answered = true;
	for (auto &aggregate : aggregates) {
		RESTAggregateResult result;
		if (aggregate.function == "count_star") {
			if (net_count.IsValid()) {
				result.answered = true;
				result.value = Value::BIGINT(NumericCast<int64_t>(net_count.GetIndex()));
			}
		} else {
			if (!aggregate.column_id.IsValid()) {
				throw InvalidInputException("The aggregate \"%s\" needs a column_id", aggregate.function);
			}
			auto field = GetRootField(table, aggregate.column_id.GetIndex());
			if (!field) {
				throw InvalidInputException("Column %llu is not a top-level column of the table",
				                            aggregate.column_id.GetIndex());
			}
			optional_ptr<const DuckLakeColumnStats> stats;
			if (exact) {
				auto entry = table_stats->column_stats.find(field->GetFieldIndex());
				if (entry != table_stats->column_stats.end() && entry->second.type == field->Type()) {
					stats = &entry->second;
				}
			}
			if (stats) {
				auto &column_stats = *stats;
				if (aggregate.function == "count") {
					// the table statistics only know whether a column has NULLs, not how many
					if (column_stats.has_null_count && column_stats.null_count == 0) {
						result.answered = true;
						result.value = Value::BIGINT(NumericCast<int64_t>(net_count.GetIndex()));
					}
				} else {
					Value value;
					if (TryGetMinMax(column_stats, aggregate.function == "min", value)) {
						result.answered = true;
						result.value = std::move(value);
					}
				}
			}
		}
		all_answered = all_answered && result.answered;
		results.push_back(std::move(result));
	}
	return all_answered;
}

RESTSelectResponse DuckLakeRESTEndpoints::Select(DuckLakeRESTRequest &request, const RESTSelectRequest &parameters) {
	if (!parameters.page_token.empty()) {
		RequireOnlyPageToken(RESTIsNull(parameters.snapshot) && !parameters.snapshot_id.IsValid() &&
		                     !parameters.extended && !parameters.filters && parameters.aggregates.empty() &&
		                     parameters.known_column_mappings.empty() && !parameters.columns.has_value);
		idx_t page_index = 0;
		auto plan = GetPlanOfPage(request, parameters.table_id, parameters.page_token, page_index);
		RESTSelectResponse page;
		GetPage(plan->files, plan->page_size, page_index, page.files);
		GetPage(plan->extended_files, plan->page_size, page_index, page.extended_files);
		set<idx_t> mapping_ids;
		AddMappingIds(page.files, mapping_ids);
		AddMappingIds(page.extended_files, mapping_ids);
		page.column_mappings = GetColumnMappings(request, std::move(mapping_ids), plan->known_mappings);
		return page;
	}
	auto snapshot = request.GetSnapshot(parameters);
	auto &table = GetTableEntry(request, snapshot, TableIndex(parameters.table_id));
	auto &metadata_manager = request.GetMetadataManager();
	unique_ptr<FilterPushdownInfo> filters;
	if (parameters.filters) {
		filters = DuckLakeRESTFilters::Read(*request.connection.context, *parameters.filters, table);
	}
	RESTSelectResponse result;
	if (!parameters.aggregates.empty()) {
		if (parameters.filters) {
			throw InvalidInputException("Aggregates cannot be answered for a scan with filters");
		}
		if (AnswerAggregates(request, table, snapshot, parameters.aggregates, result.aggregate_results)) {
			return result;
		}
	}
	if (parameters.extended) {
		result.extended_files = metadata_manager.GetExtendedFilesForTable(table, snapshot, filters.get());
	} else {
		result.files = metadata_manager.GetFilesForTable(table, snapshot, filters.get());
		metadata_manager.ReadFilePartitions(table.GetTableId(), snapshot, result.files);
	}
	SplitIntoPages(request, table.GetTableId(), result.files, &DuckLakeRESTPlan::files,
	               parameters.known_column_mappings, result.page_tokens);
	SplitIntoPages(request, table.GetTableId(), result.extended_files, &DuckLakeRESTPlan::extended_files,
	               parameters.known_column_mappings, result.page_tokens);
	set<idx_t> mapping_ids;
	AddMappingIds(result.files, mapping_ids);
	AddMappingIds(result.extended_files, mapping_ids);
	result.column_mappings = GetColumnMappings(request, std::move(mapping_ids), parameters.known_column_mappings);
	result.inlined_data = ReadInlinedRows(
	    request, table, snapshot,
	    [&](const Identifier &inlined_table_name, const vector<string> &columns) {
		    return metadata_manager.ReadInlinedData(snapshot, inlined_table_name, columns);
	    },
	    parameters.columns, true);
	return result;
}

RESTListSnapshotsResponse DuckLakeRESTEndpoints::ListSnapshots(DuckLakeRESTRequest &request,
                                                               const RESTListSnapshotsRequest &parameters) {
	DuckLakeSnapshotFilter filter;
	filter.exclude_latest = parameters.exclude_latest;
	filter.older_than = parameters.older_than;
	filter.has_snapshot_ids = parameters.snapshot_ids.has_value;
	filter.snapshot_ids = parameters.snapshot_ids.value;
	// the listing and its pages hold the lineage of the served catalog, as ducklake_snapshots does
	filter.chain_only = true;
	RESTListSnapshotsResponse result;
	result.snapshots = request.GetMetadataManager().GetAllSnapshots(filter);
	GetKeyPage(
	    request, result.snapshots, [](const DuckLakeSnapshotInfo &snapshot) { return snapshot.id; },
	    parameters.page_size, parameters.page_token, result.next_page_token);
	return result;
}

RESTTableChangesResponse DuckLakeRESTEndpoints::TableChanges(DuckLakeRESTRequest &request,
                                                             const RESTTableChangesRequest &parameters) {
	if (!parameters.page_token.empty()) {
		RequireOnlyPageToken(RESTIsNull(parameters.snapshot) && !parameters.snapshot_id.IsValid() &&
		                     RESTIsNull(parameters.start_snapshot) && !parameters.start_snapshot_id.IsValid() &&
		                     parameters.changes.empty() && parameters.known_column_mappings.empty() &&
		                     !parameters.columns.has_value);
		idx_t page_index = 0;
		auto plan = GetPlanOfPage(request, parameters.table_id, parameters.page_token, page_index);
		RESTTableChangesResponse page;
		GetPage(plan->insertions, plan->page_size, page_index, page.insertions);
		GetPage(plan->deletions, plan->page_size, page_index, page.deletions);
		set<idx_t> mapping_ids;
		AddMappingIds(page.insertions, mapping_ids);
		AddMappingIds(page.deletions, mapping_ids);
		page.column_mappings = GetColumnMappings(request, std::move(mapping_ids), plan->known_mappings);
		return page;
	}
	if (parameters.changes.empty()) {
		throw InvalidInputException("The request for the changes of a table needs the changes to return");
	}
	auto snapshot = request.GetSnapshot(parameters);
	auto start_snapshot = request.GetSnapshot(parameters.start_snapshot, parameters.start_snapshot_id);
	auto &table = GetTableEntry(request, snapshot, TableIndex(parameters.table_id));
	auto &metadata_manager = request.GetMetadataManager();
	RESTTableChangesResponse result;
	bool deletions = parameters.changes == "deletions";
	if (deletions) {
		result.deletions = metadata_manager.GetTableDeletions(table, start_snapshot, snapshot);
		SplitIntoPages(request, table.GetTableId(), result.deletions, &DuckLakeRESTPlan::deletions,
		               parameters.known_column_mappings, result.page_tokens);
	} else {
		result.insertions = metadata_manager.GetTableInsertions(table, start_snapshot, snapshot);
		SplitIntoPages(request, table.GetTableId(), result.insertions, &DuckLakeRESTPlan::insertions,
		               parameters.known_column_mappings, result.page_tokens);
	}
	set<idx_t> mapping_ids;
	AddMappingIds(result.insertions, mapping_ids);
	AddMappingIds(result.deletions, mapping_ids);
	result.column_mappings = GetColumnMappings(request, std::move(mapping_ids), parameters.known_column_mappings);
	result.inlined_data = ReadInlinedRows(
	    request, table, snapshot,
	    [&](const Identifier &inlined_table_name, const vector<string> &columns) {
		    if (deletions) {
			    return metadata_manager.ReadInlinedDataDeletions(start_snapshot, snapshot, inlined_table_name, columns);
		    }
		    return metadata_manager.ReadInlinedDataInsertions(start_snapshot, snapshot, inlined_table_name, columns);
	    },
	    parameters.columns, deletions);
	return result;
}

RESTCompactResponse DuckLakeRESTEndpoints::Compact(DuckLakeRESTRequest &request, const RESTCompactRequest &parameters) {
	// the compaction runs in transactions of its own
	auto catalog_name = request.catalog->GetName().GetIdentifierName();
	auto &db = DatabaseInstance::GetDatabase(*request.connection.context);
	request.connection.Rollback();
	bool flush = parameters.type == "flush_inlined_data";
	const char *function = "ducklake_flush_inlined_data";
	if (parameters.type == "merge_adjacent") {
		function = "ducklake_merge_adjacent_files";
	} else if (parameters.type == "rewrite_delete") {
		function = "ducklake_rewrite_data_files";
	}
	vector<string> arguments {SQLString::ToString(catalog_name)};
	if (flush) {
		if (!parameters.schema_name.empty()) {
			arguments.push_back("schema_name => " + SQLString::ToString(parameters.schema_name));
		}
		if (!parameters.table_name.empty()) {
			arguments.push_back("table_name => " + SQLString::ToString(parameters.table_name));
		}
	} else if (!parameters.table_name.empty()) {
		arguments.push_back(SQLString::ToString(parameters.table_name));
		if (!parameters.schema_name.empty()) {
			arguments.push_back("schema => " + SQLString::ToString(parameters.schema_name));
		}
	} else if (!parameters.schema_name.empty()) {
		throw InvalidInputException("A schema_name needs a table_name");
	}
	auto add_number = [&](const char *name, const optional_idx &value) {
		if (value.IsValid()) {
			arguments.push_back(StringUtil::Format("%s => %llu", name, value.GetIndex()));
		}
	};
	add_number("max_compacted_files", parameters.max_compacted_files);
	add_number("min_file_size", parameters.min_file_size);
	add_number("max_file_size", parameters.max_file_size);
	auto add_timestamp = [&](const char *name, const string &value) {
		if (!value.empty()) {
			arguments.push_back(StringUtil::Format("%s => %s::TIMESTAMPTZ", name, SQLString::ToString(value)));
		}
	};
	add_timestamp("newer_than", parameters.newer_than);
	add_timestamp("older_than", parameters.older_than);
	if (parameters.delete_threshold.has_value) {
		arguments.push_back(StringUtil::Format("delete_threshold => %f", parameters.delete_threshold.value));
	}
	Connection con(db);
	auto columns =
	    flush ? "schema_name, table_name, rows_flushed" : "schema_name, table_name, files_processed, files_created";
	auto result =
	    con.Query(StringUtil::Format("SELECT %s FROM %s(%s)", columns, function, StringUtil::Join(arguments, ", ")));
	result->ThrowIfError();
	RESTCompactResponse response;
	for (auto &row : *result) {
		RESTCompactionResult entry;
		entry.schema_name = row.GetValue<string>(0);
		entry.table_name = row.GetValue<string>(1);
		if (flush) {
			entry.rows_flushed = row.GetValue<idx_t>(2);
		} else {
			entry.files_processed = row.GetValue<idx_t>(2);
			entry.files_created = row.GetValue<idx_t>(3);
		}
		response.results.push_back(std::move(entry));
	}
	return response;
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//
//! Replays the operations of a client transaction in the transaction of the request
//! The name of an entry in a schema
struct RESTEntryName {
	CatalogType type;
	vector<string> schema_path;
	string name;
};

class DuckLakeRESTCommit : public DuckLakeRESTCommitHandler {
public:
	explicit DuckLakeRESTCommit(DuckLakeRESTRequest &request)
	    : request(request), transaction(*request.transaction), catalog(*request.catalog),
	      context(*request.connection.context) {
		// the client wrote the files of the transaction, and cleans them up itself
		transaction.SetFilesOwnedByClient();
	}

	RESTCommitTransactionResponse Run(const RESTCommitTransactionRequest &parameters);

protected:
	void CreateSchema(RESTCreateSchemaRequest &parameters) override;
	void CreateTable(RESTCreateTableRequest &parameters) override;
	void CreateView(RESTCreateViewRequest &parameters) override;
	void CreateMacro(RESTCreateMacroRequest &parameters) override;
	void Alter(RESTAlterRequest &parameters) override;
	void Drop(RESTDropRequest &parameters) override;
	void SetOption(RESTSetOptionRequest &parameters) override;
	void InsertRows(RESTInsertRowsRequest &parameters) override;
	void InsertFiles(RESTInsertFilesRequest &parameters) override;
	void DeleteData(RESTDeleteDataRequest &parameters) override;

private:
	void RemapNameMap(DuckLakeDataFile &file);
	//! Deletes inlined rows, which are in the inlined data of any schema version of the table
	void AddInlinedDeletes(DuckLakeTableEntry &table, const set<idx_t> &row_ids);
	//! Maps the id of a partitioning that the client set to the id of the partitioning of the table
	void RegisterPartition(DuckLakeTableEntry &table, optional_idx client_partition_id);
	//! Files written for a partitioning that the transaction set refer to the partitioning of the client
	void RemapPartition(DuckLakeTableEntry &table, DuckLakeDataFile &file);

	DuckLakeSchemaEntry &GetSchema(const vector<string> &schema_path);
	//! The path of a schema of the request: the id of a committed schema, or the placeholder id of a created one
	vector<string> GetSchemaPath(int64_t schema_id);
	//! The name of an entry of the request, as it is at this point of the transaction
	RESTEntryName &GetEntryName(int64_t entry_id);
	//! Registers the placeholder id of a created entry
	void AddPlaceholder(int64_t placeholder_id, CatalogType type, const vector<string> &schema_path,
	                    const string &name);
	//! The qualified name of an entry in a schema of the catalog
	QualifiedName GetEntryQualifiedName(const vector<string> &schema_path, const string &name);
	//! The qualified name of a schema
	QualifiedName GetSchemaName(const vector<string> &schema_path);
	//! Resolves an id of the request, negative ids refer to entries created by earlier requests
	TableIndex GetTableId(int64_t id);
	DuckLakeTableEntry &GetTable(TableIndex table_id);
	void AddCreatedEntry(optional_ptr<CatalogEntry> entry, int64_t placeholder_id);
	//! Whether the next request creates the dropped macro again, as CREATE OR REPLACE does
	bool NextRequestReplaces(const RESTEntryName &name);

private:
	DuckLakeRESTRequest &request;
	DuckLakeTransaction &transaction;
	DuckLakeCatalog &catalog;
	ClientContext &context;
	map<int64_t, TableIndex> placeholder_ids;
	//! The request after the one that is applied
	optional_ptr<RESTCommitRequest> next_request;
	//! The macros that the requests dropped without creating them again
	identifier_set_t dropped_macros;
	//! The paths of the schemas created by the requests, by their placeholder ids
	map<int64_t, vector<string>> schema_paths;
	//! The names of the entries that the requests refer to, by their ids
	map<int64_t, RESTEntryName> entry_names;
	//! The schemas of the tables created by the transaction
	map<TableIndex, SchemaIndex> local_table_schemas;
	//! The name maps of the client transaction, by their id in the client transaction
	map<idx_t, MappingIndex> name_map_ids;
	//! The partitionings set by the client transaction, by their id in the client transaction
	map<idx_t, idx_t> partition_ids;
	//! The placeholder ids of the created entries, by the oid of the entry
	map<idx_t, int64_t> entry_placeholders;
};

void DuckLakeRESTCommit::AddCreatedEntry(optional_ptr<CatalogEntry> entry, int64_t placeholder_id) {
	if (entry) {
		entry_placeholders[entry->oid] = placeholder_id;
	}
}

DuckLakeSchemaEntry &DuckLakeRESTCommit::GetSchema(const vector<string> &schema_path) {
	vector<Identifier> path;
	for (auto &name : schema_path) {
		path.emplace_back(name);
	}
	D_ASSERT(!path.empty());
	auto schema = catalog.GetSchema(catalog.GetCatalogTransaction(context), path, OnEntryNotFound::THROW_EXCEPTION);
	return schema->Cast<DuckLakeSchemaEntry>();
}

QualifiedName DuckLakeRESTCommit::GetEntryQualifiedName(const vector<string> &schema_path, const string &name) {
	vector<Identifier> path;
	path.push_back(catalog.GetName());
	for (auto &schema_name : schema_path) {
		path.emplace_back(schema_name);
	}
	D_ASSERT(path.size() > 1);
	return QualifiedName(std::move(path), Identifier(name));
}

QualifiedName DuckLakeRESTCommit::GetSchemaName(const vector<string> &schema_path) {
	vector<Identifier> path;
	path.push_back(catalog.GetName());
	for (auto &name : schema_path) {
		path.emplace_back(name);
	}
	return QualifiedName(std::move(path), Identifier());
}

static vector<string> GetPathOfSchema(DuckLakeSchemaEntry &schema) {
	vector<string> path;
	for (optional_ptr<DuckLakeSchemaEntry> entry = &schema; entry; entry = entry->ParentDuckLakeSchema()) {
		path.push_back(entry->name.GetIdentifierName());
	}
	std::reverse(path.begin(), path.end());
	return path;
}

static void CheckPlaceholderId(int64_t placeholder_id) {
	if (placeholder_id >= 0) {
		throw InvalidInputException("A placeholder id is negative, not %lld", placeholder_id);
	}
}

vector<string> DuckLakeRESTCommit::GetSchemaPath(int64_t schema_id) {
	if (schema_id < 0) {
		auto entry = schema_paths.find(schema_id);
		if (entry == schema_paths.end()) {
			throw InvalidInputException("Placeholder id %lld does not refer to a schema created in this transaction",
			                            schema_id);
		}
		return entry->second;
	}
	auto entry =
	    catalog.GetEntryById(transaction, transaction.GetSnapshot(), SchemaIndex(NumericCast<idx_t>(schema_id)));
	if (!entry || entry->type != CatalogType::SCHEMA_ENTRY) {
		throw CatalogException("Schema with id %lld does not exist", schema_id);
	}
	return GetPathOfSchema(entry->Cast<DuckLakeSchemaEntry>());
}

RESTEntryName &DuckLakeRESTCommit::GetEntryName(int64_t entry_id) {
	auto known = entry_names.find(entry_id);
	if (known != entry_names.end()) {
		return known->second;
	}
	if (entry_id < 0) {
		throw InvalidInputException("Placeholder id %lld does not refer to an entry created in this transaction",
		                            entry_id);
	}
	auto snapshot = transaction.GetSnapshot();
	auto table = catalog.GetEntryById(transaction, snapshot, TableIndex(NumericCast<idx_t>(entry_id)));
	optional_ptr<CatalogEntry> entry = table;
	optional_ptr<SchemaCatalogEntry> schema;
	if (entry) {
		schema = entry->ParentSchema();
	} else {
		entry = catalog.GetEntryById(transaction, snapshot, MacroIndex(NumericCast<idx_t>(entry_id)));
		if (entry) {
			schema = entry->Cast<MacroCatalogEntry>().schema;
		}
	}
	if (!entry) {
		throw CatalogException("Entry with id %lld does not exist", entry_id);
	}
	RESTEntryName result {entry->type, GetPathOfSchema(schema->Cast<DuckLakeSchemaEntry>()),
	                      entry->name.GetIdentifierName()};
	return entry_names.emplace(entry_id, std::move(result)).first->second;
}

void DuckLakeRESTCommit::AddPlaceholder(int64_t placeholder_id, CatalogType type, const vector<string> &schema_path,
                                        const string &name) {
	entry_names[placeholder_id] = RESTEntryName {type, schema_path, name};
}

TableIndex DuckLakeRESTCommit::GetTableId(int64_t table_id) {
	if (table_id >= 0) {
		return TableIndex(NumericCast<idx_t>(table_id));
	}
	auto entry = placeholder_ids.find(table_id);
	if (entry == placeholder_ids.end()) {
		throw InvalidInputException("Placeholder id %lld does not refer to an entry created in this transaction",
		                            table_id);
	}
	return entry->second;
}

DuckLakeTableEntry &DuckLakeRESTCommit::GetTable(TableIndex table_id) {
	if (!DuckLakeTransaction::IsTransactionLocal(table_id.index)) {
		auto entry = catalog.GetEntryById(transaction, transaction.GetSnapshot(), table_id);
		if (entry && entry->type == CatalogType::TABLE_ENTRY) {
			return entry->Cast<DuckLakeTableEntry>();
		}
		throw CatalogException("Table with id %llu does not exist", table_id.index);
	}
	// tables created by the transaction are not in the id map of their catalog set
	auto schema_id = local_table_schemas.find(table_id);
	if (schema_id != local_table_schemas.end()) {
		auto local_entries = transaction.GetTransactionLocalEntries(CatalogType::TABLE_ENTRY, schema_id->second);
		if (local_entries) {
			for (auto &local_entry : local_entries->GetEntries()) {
				auto &catalog_entry = *local_entry.second;
				if (catalog_entry.type == CatalogType::TABLE_ENTRY &&
				    catalog_entry.Cast<DuckLakeTableEntry>().GetTableId() == table_id) {
					return catalog_entry.Cast<DuckLakeTableEntry>();
				}
			}
		}
	}
	throw CatalogException("Table with id %llu does not exist", table_id.index);
}

void DuckLakeRESTCommit::CreateSchema(RESTCreateSchemaRequest &parameters) {
	CheckPlaceholderId(parameters.placeholder_id);
	if (parameters.schema_name.empty()) {
		throw InvalidInputException("create_schema requires a schema_name");
	}
	vector<string> path;
	if (parameters.parent_schema_id.has_value) {
		path = GetSchemaPath(parameters.parent_schema_id.value);
	}
	path.push_back(parameters.schema_name);
	auto info = make_uniq<CreateSchemaInfo>();
	info->SetQualifiedName(GetSchemaName(path));
	info->on_conflict =
	    parameters.if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	AddCreatedEntry(catalog.CreateSchemaExtended(catalog.GetCatalogTransaction(context), *info, parameters.schema_uuid,
	                                             parameters.path),
	                parameters.placeholder_id);
	schema_paths[parameters.placeholder_id] = std::move(path);
}

void DuckLakeRESTCommit::CreateTable(RESTCreateTableRequest &parameters) {
	CheckPlaceholderId(parameters.placeholder_id);
	auto schema_path = GetSchemaPath(parameters.schema_id);
	auto &schema = GetSchema(schema_path);
	auto info = make_uniq<CreateTableInfo>(schema, Identifier(parameters.table_name));
	vector<Value> initial_defaults;
	for (auto &column : parameters.columns) {
		auto logical_index = LogicalIndex(info->columns.LogicalColumnCount());
		initial_defaults.push_back(DuckLakeRESTCatalogChanges::ReadInitialDefault(context, column));
		info->columns.AddColumn(DuckLakeRESTCatalogChanges::ReadColumn(context, column));
		if (column.not_null) {
			info->constraints.push_back(make_uniq<NotNullConstraint>(logical_index));
		}
	}
	for (auto &expr : parameters.partitioned_by) {
		info->partition_keys.push_back(Parser::GetBuiltinParser().ParseSingleExpression(expr));
	}
	for (auto &expr : parameters.sorted_by) {
		info->sort_keys.push_back(Parser::GetBuiltinParser().ParseSingleExpression(expr));
	}
	for (auto &option : parameters.options) {
		info->options[option.first] = Parser::GetBuiltinParser().ParseSingleExpression(option.second);
	}
	if (parameters.comment.has_value) {
		info->comment = Value(parameters.comment.value);
	}
	BoundCreateTableInfo bound_info(schema, std::move(info));
	auto uuid = parameters.table_uuid.empty() ? transaction.GenerateUUID() : parameters.table_uuid;
	auto path = parameters.path;
	if (path.empty()) {
		path = schema.GenerateTableDataPath(uuid, bound_info.Base().GetTableName().GetIdentifierName());
	}
	auto entry = schema.CreateTableExtended(catalog.GetCatalogTransaction(context), bound_info, std::move(uuid),
	                                        std::move(path), nullptr, nullptr, {}, std::move(initial_defaults));
	if (!entry) {
		// LCOV_EXCL_START
		// a conflict throws, as the request has no IF NOT EXISTS
		throw CatalogException("Table \"%s\" already exists", parameters.table_name);
		// LCOV_EXCL_STOP
	}
	AddCreatedEntry(entry, parameters.placeholder_id);
	auto &table = entry->Cast<DuckLakeTableEntry>();
	auto table_id = table.GetTableId();
	local_table_schemas[table_id] = schema.GetSchemaId();
	RegisterPartition(table, parameters.partition_id);
	placeholder_ids[parameters.placeholder_id] = table_id;
	AddPlaceholder(parameters.placeholder_id, CatalogType::TABLE_ENTRY, schema_path, parameters.table_name);
}

void DuckLakeRESTCommit::CreateView(RESTCreateViewRequest &parameters) {
	CheckPlaceholderId(parameters.placeholder_id);
	auto schema_path = GetSchemaPath(parameters.schema_id);
	auto &schema = GetSchema(schema_path);
	if (parameters.dialect != "duckdb") {
		throw NotImplementedException("Unsupported view dialect \"%s\"", parameters.dialect);
	}
	auto info = make_uniq<CreateViewInfo>(schema, Identifier(parameters.view_name));
	auto parser = Parser::GetBuiltinParser();
	info->query = CreateViewInfo::ParseSelect(parser, parameters.sql);
	for (auto &alias : parameters.column_aliases) {
		info->aliases.emplace_back(alias);
	}
	if (parameters.comment.has_value) {
		info->comment = Value(parameters.comment.value);
	}
	for (auto &entry : parameters.column_comments) {
		info->column_comments_map[Identifier(entry.first)] = Value(entry.second);
	}
	AddCreatedEntry(schema.CreateViewExtended(catalog.GetCatalogTransaction(context), *info, parameters.view_uuid),
	                parameters.placeholder_id);
	AddPlaceholder(parameters.placeholder_id, CatalogType::VIEW_ENTRY, schema_path, parameters.view_name);
}

void DuckLakeRESTCommit::CreateMacro(RESTCreateMacroRequest &parameters) {
	CheckPlaceholderId(parameters.placeholder_id);
	auto schema_path = GetSchemaPath(parameters.schema_id);
	auto &schema = GetSchema(schema_path);
	auto info = CreateMacroInfoFromDucklake(context, parameters.macro, schema.name);
	auto entry = schema.CreateFunction(catalog.GetCatalogTransaction(context), *info);
	AddCreatedEntry(entry, parameters.placeholder_id);
	if (entry) {
		AddPlaceholder(parameters.placeholder_id, entry->type, schema_path, entry->name.GetIdentifierName());
	}
}

//! The initial default of the column or field that an alteration adds, NULL for other alterations
static Value GetAddedInitialDefault(ClientContext &context, const RESTAlteration &alteration) {
	switch (alteration.type) {
	case RESTAlterationType::ADD_COLUMN:
		return DuckLakeRESTCatalogChanges::ReadInitialDefault(context, alteration.Cast<RESTAddColumn>().column);
	case RESTAlterationType::ADD_FIELD:
		return DuckLakeRESTCatalogChanges::ReadInitialDefault(context, alteration.Cast<RESTAddField>().field);
	default:
		return Value();
	}
}

void DuckLakeRESTCommit::Alter(RESTAlterRequest &parameters) {
	auto &name = GetEntryName(parameters.entry_id);
	if (name.type != CatalogType::TABLE_ENTRY && name.type != CatalogType::VIEW_ENTRY) {
		throw InvalidInputException("Entry %lld is not a table or a view", parameters.entry_id);
	}
	AlterEntryData entry(GetEntryQualifiedName(name.schema_path, name.name), OnEntryNotFound::THROW_EXCEPTION);
	auto info = DuckLakeRESTCatalogChanges::ReadAlter(context, *parameters.alter, entry);
	transaction.SetInitialDefault(GetAddedInitialDefault(context, *parameters.alter));
	catalog.Alter(context, *info);
	transaction.SetInitialDefault(Value());
	if (parameters.alter->type == RESTAlterationType::RENAME_TABLE) {
		name.name = parameters.alter->Cast<RESTRenameTable>().new_name;
	} else if (parameters.alter->type == RESTAlterationType::RENAME_VIEW) {
		name.name = parameters.alter->Cast<RESTRenameView>().new_name;
	}
	if (parameters.partition_id.IsValid()) {
		auto &schema = GetSchema(name.schema_path);
		auto table =
		    schema.GetEntry(catalog.GetCatalogTransaction(context), CatalogType::TABLE_ENTRY, Identifier(name.name));
		if (table) {
			RegisterPartition(table->Cast<DuckLakeTableEntry>(), parameters.partition_id);
		}
	}
}

void DuckLakeRESTCommit::Drop(RESTDropRequest &parameters) {
	DropInfo info;
	if (parameters.entry_type == "schema") {
		info.type = CatalogType::SCHEMA_ENTRY;
		info.SetQualifiedName(GetSchemaName(GetSchemaPath(parameters.entry_id)));
	} else {
		if (parameters.entry_type == "table") {
			info.type = CatalogType::TABLE_ENTRY;
		} else if (parameters.entry_type == "view") {
			info.type = CatalogType::VIEW_ENTRY;
		} else if (parameters.entry_type == "table_macro") {
			info.type = CatalogType::TABLE_MACRO_ENTRY;
		} else {
			info.type = CatalogType::MACRO_ENTRY;
		}
		auto &name = GetEntryName(parameters.entry_id);
		if (name.type != info.type) {
			throw InvalidInputException("Entry %lld is not a %s", parameters.entry_id, parameters.entry_type);
		}
		info.SetQualifiedName(GetEntryQualifiedName(name.schema_path, name.name));
		if (info.type == CatalogType::MACRO_ENTRY) {
			// the tables that call the macro are checked once all requests are applied, a cascade drops them too
			if (!NextRequestReplaces(name)) {
				dropped_macros.insert(Identifier(name.name));
			}
			auto &schema = GetSchema(name.schema_path);
			entry_names.erase(parameters.entry_id);
			schema.DropEntry(context, info, true);
			return;
		}
		entry_names.erase(parameters.entry_id);
	}
	catalog.DropEntry(context, info);
}

bool DuckLakeRESTCommit::NextRequestReplaces(const RESTEntryName &name) {
	if (name.type != CatalogType::MACRO_ENTRY && name.type != CatalogType::TABLE_MACRO_ENTRY) {
		return false;
	}
	if (!next_request || next_request->type != RESTCommitRequestType::CREATE_MACRO) {
		return false;
	}
	auto &create = next_request->Cast<RESTCreateMacroRequest>();
	return create.macro.macro_name == name.name && GetSchemaPath(create.schema_id) == name.schema_path;
}

void DuckLakeRESTCommit::AddInlinedDeletes(DuckLakeTableEntry &table, const set<idx_t> &row_ids) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto col_names = metadata_manager.InlinedColNames();
	for (auto &inlined_table : table.GetInlinedDataTables(transaction, transaction.GetSnapshot(), false)) {
		set<idx_t> deleted_row_ids;
		vector<string> batch;
		auto flush_batch = [&]() {
			if (batch.empty()) {
				return;
			}
			auto result = metadata_manager.Query(StringUtil::Format(
			    "SELECT %s FROM {METADATA_CATALOG}.%s WHERE %s IN (%s)", SQLIdentifier::ToString(col_names.row_id),
			    SQLIdentifier::ToString(inlined_table.table_name), SQLIdentifier::ToString(col_names.row_id),
			    StringUtil::Join(batch, ", ")));
			result->ThrowIfError("Failed to find the inlined rows to delete: ");
			for (auto &row : *result) {
				deleted_row_ids.insert(row.GetValue<idx_t>(0));
			}
			batch.clear();
		};
		for (auto row_id : row_ids) {
			batch.push_back(to_string(row_id));
			if (batch.size() >= 1000) {
				flush_batch();
			}
		}
		flush_batch();
		transaction.AddNewInlinedDeletes(table.GetTableId(), inlined_table.table_name, std::move(deleted_row_ids));
	}
}

void DuckLakeRESTCommit::DeleteData(RESTDeleteDataRequest &parameters) {
	auto &table = GetTable(GetTableId(parameters.table_id));
	auto table_id = table.GetTableId();
	if (!parameters.delete_files.empty()) {
		// a delete file replaces the delete file that its data file has
		set<idx_t> data_file_ids;
		for (auto &file : parameters.delete_files) {
			if (file.data_file_id.IsValid() && !DuckLakeTransaction::IsTransactionLocal(file.data_file_id.index)) {
				data_file_ids.insert(file.data_file_id.index);
			}
		}
		auto existing_delete_files =
		    transaction.GetMetadataManager().GetDeleteFiles(table, transaction.GetSnapshot(), data_file_ids);
		for (auto &file : parameters.delete_files) {
			auto existing = existing_delete_files.find(file.data_file_id.index);
			if (existing != existing_delete_files.end()) {
				file.overwrites_existing_delete = true;
				file.overwritten_delete_file = existing->second;
			}
		}
		transaction.AddDeletes(table_id, std::move(parameters.delete_files));
	}
	if (!parameters.inlined_row_ids.empty()) {
		AddInlinedDeletes(table, parameters.inlined_row_ids);
	}
	for (auto &deletes : parameters.file_row_deletes) {
		transaction.AddNewInlinedFileDeletes(table_id, deletes.data_file_id, std::move(deletes.row_ids));
	}
	for (auto &file : parameters.dropped_files) {
		transaction.DropFile(table_id, DataFileIndex(file.data_file_id), file.path, file.record_count,
		                     file.file_size_bytes);
	}
	if (parameters.delete_attempted) {
		transaction.MarkDeleteAttempted(table_id);
	}
}

void DuckLakeRESTCommit::RegisterPartition(DuckLakeTableEntry &table, optional_idx client_partition_id) {
	auto partition_data = table.GetPartitionData();
	if (!client_partition_id.IsValid() || !partition_data) {
		return;
	}
	partition_ids[client_partition_id.GetIndex()] = partition_data->partition_id;
}

void DuckLakeRESTCommit::RemapPartition(DuckLakeTableEntry &table, DuckLakeDataFile &file) {
	if (!file.partition_id.IsValid() || !DuckLakeTransaction::IsTransactionLocal(file.partition_id.GetIndex())) {
		return;
	}
	auto entry = partition_ids.find(file.partition_id.GetIndex());
	if (entry == partition_ids.end()) {
		throw InvalidInputException("File \"%s\" refers to a partitioning that was not set in this transaction",
		                            file.file_name);
	}
	file.partition_id = entry->second;
}

void DuckLakeRESTCommit::SetOption(RESTSetOptionRequest &parameters) {
	auto &option_in = parameters.option;
	DuckLakeConfigOption option;
	option.option.key = option_in.key;
	option.option.value = option_in.value;
	if (option_in.schema_id.IsValid()) {
		option.schema_id = SchemaIndex(option_in.schema_id.GetIndex());
	}
	if (option_in.table_id.has_value) {
		if (option_in.table_id.value < 0) {
			throw InvalidInputException("set_option cannot refer to the table %lld that the transaction creates, its "
			                            "options are part of create_table",
			                            option_in.table_id.value);
		}
		option.table_id = GetTable(GetTableId(option_in.table_id.value)).GetTableId();
	}
	if (parameters.reset) {
		transaction.ResetConfigOption(option);
	} else {
		transaction.SetConfigOption(option);
	}
}

void DuckLakeRESTCommit::RemapNameMap(DuckLakeDataFile &file) {
	if (!file.mapping_id.IsValid()) {
		return;
	}
	auto entry = name_map_ids.find(file.mapping_id.index);
	if (entry != name_map_ids.end()) {
		file.mapping_id = entry->second;
	} else if (DuckLakeTransaction::IsTransactionLocal(file.mapping_id.index)) {
		throw InvalidInputException("Name map %llu does not refer to a name map created in this transaction",
		                            file.mapping_id.index);
	}
}

void DuckLakeRESTCommit::InsertRows(RESTInsertRowsRequest &parameters) {
	auto &table = GetTable(GetTableId(parameters.table_id));
	auto new_data = make_uniq<DuckLakeInlinedData>();
	// the server reads the rows of a request into a collection of its own
	D_ASSERT(parameters.rows.owned_rows);
	new_data->data = std::move(parameters.rows.owned_rows);
	new_data->row_ids = std::move(parameters.row_ids);
	new_data->column_stats = std::move(parameters.column_stats);
	if (table.GetInlinedDataTables().empty() ||
	    !table.HasCurrentInlinedDataTable(
	        catalog.GetTableSchemaVersions(transaction, table.GetTableId()).last_change)) {
		// the commit creates the inlined data table, which the server-side commit of a metadata server cannot
		transaction.SetRequiresNewInlinedTable(true);
	}
	transaction.AppendInlinedData(table.GetTableId(), std::move(new_data));
}

void DuckLakeRESTCommit::InsertFiles(RESTInsertFilesRequest &parameters) {
	auto &table = GetTable(GetTableId(parameters.table_id));
	for (auto &mapping : parameters.name_maps) {
		auto client_id = mapping.mapping_id.index;
		mapping.table_id = table.GetTableId();
		name_map_ids[client_id] = transaction.AddNameMap(DuckLakeNameMap::FromColumnMapping(std::move(mapping)));
	}
	for (auto &file : parameters.files) {
		RemapNameMap(file);
		RemapPartition(table, file);
	}
	transaction.AppendFiles(table.GetTableId(), std::move(parameters.files));
}

RESTCommitTransactionResponse DuckLakeRESTCommit::Run(const RESTCommitTransactionRequest &parameters) {
	if (!RESTIsNull(parameters.snapshot) || parameters.snapshot_id.IsValid()) {
		// conflicts are detected against the snapshot the client transaction started from
		transaction.SetSnapshot(request.GetSnapshot(parameters));
	}
	if (parameters.retry) {
		auto &retry = *parameters.retry;
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_max_retry_count", Value::UBIGINT(retry.max_retry_count));
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_retry_wait_ms", Value::UBIGINT(retry.retry_wait_ms));
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_retry_backoff", Value::DOUBLE(retry.retry_backoff));
	}
	if (parameters.commit_info) {
		transaction.SetCommitMessage(*parameters.commit_info);
	}
	auto &requests = parameters.requests;
	for (idx_t i = 0; i < requests.size(); i++) {
		next_request = nullptr;
		if (i + 1 < requests.size()) {
			next_request = requests[i + 1].get();
		}
		Apply(*requests[i]);
	}
	if (!dropped_macros.empty()) {
		DuckLakeSchemaEntry::ThrowIfTablesCallMacros(context, catalog, dropped_macros);
	}
	auto transaction_ref = transaction.shared_from_this();
	request.connection.Commit();
	RESTCommitTransactionResponse result;
	auto committed = transaction_ref->GetCommittedSnapshot();
	if (committed.snapshot_id != DConstants::INVALID_INDEX) {
		result.snapshot = committed;
	}
	for (auto &entry : transaction_ref->GetCommittedEntryOids()) {
		auto placeholder = entry_placeholders.find(entry.second);
		if (placeholder == entry_placeholders.end()) {
			continue; // LCOV_EXCL_LINE: the create requests record the entries that get an id
		}
		RESTCreatedEntry created_entry;
		created_entry.placeholder_id = placeholder->second;
		created_entry.type = EnumUtil::ToString(entry.first.first);
		created_entry.id = entry.first.second;
		result.entry_ids.push_back(std::move(created_entry));
	}
	for (auto &option : transaction_ref->GetCommittedTableOptions()) {
		RESTOptionSetting setting;
		setting.key = option.option.key;
		setting.value = option.option.value;
		setting.table_id = option.table_id.index;
		result.table_options.push_back(std::move(setting));
	}
	return result;
}

RESTCommitTransactionResponse DuckLakeRESTEndpoints::CommitTransaction(DuckLakeRESTRequest &request,
                                                                       const RESTCommitTransactionRequest &parameters) {
	DuckLakeRESTCommit commit(request);
	return commit.Run(parameters);
}

} // namespace duckdb
