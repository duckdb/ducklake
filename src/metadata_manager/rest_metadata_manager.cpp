#include "metadata_manager/rest_metadata_manager.hpp"

#include <atomic>
#include <thread>

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "common/ducklake_types.hpp"
#include "rest/ducklake_rest_catalog_changes.hpp"
#include "rest/ducklake_rest_filters.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_transaction_state.hpp"
#include "storage/ducklake_view_entry.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

RESTMetadataManager::RESTMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

RESTMetadataManager::~RESTMetadataManager() {
}

//! The core refers to the inlined data of a table at a schema version by the name that the inlined data table has in a
//! metadata database (users see it as the filename of the rows). The REST API only knows the table and the version.
static const char *INLINED_TABLE_PREFIX = "ducklake_inlined_data_";

static Identifier InlinedTableName(TableIndex table_id, idx_t schema_version) {
	return DuckLakeMetadataManager::InlinedTableNameFor(table_id.index, schema_version);
}

struct RESTInlinedTableHandle {
	TableIndex table_id;
	idx_t schema_version;
};

static RESTInlinedTableHandle ParseInlinedTableName(const string &name) {
	if (!StringUtil::StartsWith(name, INLINED_TABLE_PREFIX)) {
		throw InternalException("\"%s\" is not the name of the inlined data of a REST catalog", name);
	}
	auto parts = StringUtil::Split(name.substr(strlen(INLINED_TABLE_PREFIX)), '_');
	if (parts.size() != 2) {
		throw InternalException("\"%s\" is not the name of the inlined data of a REST catalog", name);
	}
	return {TableIndex(std::stoull(parts[0])), std::stoull(parts[1])};
}

//! Entries created by the transaction are referred to by negative placeholder ids
static int64_t ToRequestId(idx_t id) {
	if (DuckLakeTransaction::IsTransactionLocal(id)) {
		return -NumericCast<int64_t>(id - DuckLakeConstants::TRANSACTION_LOCAL_ID_START + 1);
	}
	return NumericCast<int64_t>(id);
}

DuckLakeRESTClient &RESTMetadataManager::GetClient() {
	return transaction.GetCatalog().GetRESTClient();
}

bool RESTMetadataManager::HasPendingChanges() const {
	return !catalog_requests.empty();
}

//===--------------------------------------------------------------------===//
// Initialization
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
// the initializer does not initialize the metadata of a remote catalog
unique_ptr<QueryResult> RESTMetadataManager::AttachMetadata(const string &attach_query) {
	throw InternalException("REST catalogs have no metadata database to attach");
}

bool RESTMetadataManager::MetadataExists() {
	return true;
}

void RESTMetadataManager::InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) {
	throw InvalidInputException("DuckLake REST catalogs are initialized by the server");
}
// LCOV_EXCL_STOP

//! Sets or resets an option of the metadata, as the server does when the transaction commits
template <class SETTING, class MATCH>
static void ApplyOptionChange(vector<SETTING> &settings, SETTING setting, bool reset, MATCH &&matches) {
	settings.erase(std::remove_if(settings.begin(), settings.end(), matches), settings.end());
	if (!reset) {
		settings.push_back(std::move(setting));
	}
}

static void ApplyOptionChange(DuckLakeMetadata &metadata, const RESTOptionSetting &option, bool reset) {
	DuckLakeTag tag;
	tag.key = option.key;
	tag.value = option.value;
	if (option.table_id.has_value) {
		DuckLakeTableSetting setting;
		setting.table_id = TableIndex(NumericCast<idx_t>(option.table_id.value));
		setting.tag = tag;
		ApplyOptionChange(metadata.table_settings, std::move(setting), reset, [&](const DuckLakeTableSetting &entry) {
			return entry.table_id == TableIndex(NumericCast<idx_t>(option.table_id.value)) && entry.tag.key == tag.key;
		});
	} else if (option.schema_id.IsValid()) {
		DuckLakeSchemaSetting setting;
		setting.schema_id = SchemaIndex(option.schema_id.GetIndex());
		setting.tag = tag;
		ApplyOptionChange(metadata.schema_settings, std::move(setting), reset, [&](const DuckLakeSchemaSetting &entry) {
			return entry.schema_id == SchemaIndex(option.schema_id.GetIndex()) && entry.tag.key == tag.key;
		});
	} else {
		ApplyOptionChange(metadata.tags, tag, reset, [&](const DuckLakeTag &entry) { return entry.key == tag.key; });
	}
}

DuckLakeMetadata RESTMetadataManager::LoadDuckLake() {
	auto result = GetClient().GetMetadata(RESTGetMetadataRequest());
	// the options of the transaction are only sent to the server when it commits
	for (auto &request : catalog_requests) {
		if (request->type == RESTCommitRequestType::SET_OPTION) {
			auto &set_option = request->Cast<RESTSetOptionRequest>();
			ApplyOptionChange(result, set_option.option, set_option.reset);
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// SQL
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
// no metadata query reaches a remote metadata manager
static unique_ptr<QueryResult> ThrowSQLNotSupported(const string &query) {
	throw NotImplementedException("DuckLake REST catalogs do not support metadata queries in SQL: %s", query);
}

unique_ptr<QueryResult> RESTMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::Execute(string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::ExecuteInTransaction(string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::Query(string &query) {
	return ThrowSQLNotSupported(query);
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Reads
//===--------------------------------------------------------------------===//
RESTSnapshotData RESTMetadataManager::GetSnapshotData(DuckLakeSnapshot snapshot, bool include_catalog,
                                                      bool include_stats) {
	// a snapshot is found by its id
	RESTGetSnapshotAtRequest request;
	request.unit = "version";
	request.value = to_string(snapshot.snapshot_id);
	request.include_catalog = include_catalog;
	request.include_stats = include_stats;
	return GetClient().GetSnapshotAt(request);
}

unique_ptr<DuckLakeSnapshot> RESTMetadataManager::GetSnapshot() {
	// the catalog and statistics of a new snapshot come along, which saves a round-trip to load them
	auto &client = GetClient();
	RESTGetLatestSnapshotRequest request;
	idx_t loaded_metadata_version = client.loaded_metadata_version;
	idx_t loaded_data_version = client.loaded_data_version;
	if (loaded_metadata_version == DConstants::INVALID_INDEX) {
		request.include_catalog = true;
	} else {
		request.cached_metadata_version = loaded_metadata_version;
	}
	if (loaded_data_version == DConstants::INVALID_INDEX) {
		request.include_stats = true;
	} else {
		request.cached_data_version = loaded_data_version;
	}
	auto response = client.GetLatestSnapshot(request);
	auto &snapshot = response.snapshot;
	lock_guard<mutex> guard(cache_lock);
	if (response.catalog) {
		prefetched_catalog = std::move(response.catalog);
		prefetched_metadata_version = snapshot.schema_version;
	}
	if (response.stats.has_value) {
		auto &snapshot_stats = table_stats[snapshot.snapshot_id];
		for (auto &table : response.stats.value) {
			snapshot_stats.push_back(std::move(table));
		}
		client.loaded_data_version = snapshot.next_file_id;
	}
	return make_uniq<DuckLakeSnapshot>(snapshot);
}

unique_ptr<DuckLakeSnapshot> RESTMetadataManager::GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound,
                                                              idx_t newest_snapshot_id) {
	auto value = at_clause.GetValue();
	if (at_clause.Unit() == "timestamp") {
		auto context = transaction.context.lock();
		value = value.CastAs(*context, LogicalType::TIMESTAMP_TZ);
	}
	RESTGetSnapshotAtRequest request;
	request.unit = StringUtil::Lower(at_clause.Unit().GetIdentifierName());
	request.value = value.ToString();
	switch (bound) {
	case SnapshotBound::LOWER_BOUND:
		request.bound = "lower";
		break;
	case SnapshotBound::UPPER_BOUND:
		request.bound = "upper";
		break;
	case SnapshotBound::TIME_TRAVEL:
		request.bound = "time_travel";
		break;
	}
	request.newest_snapshot_id = newest_snapshot_id;
	auto response = GetClient().GetSnapshotAt(request);
	return make_uniq<DuckLakeSnapshot>(response.snapshot);
}

DuckLakeCatalogInfo RESTMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto &client = GetClient();
	{
		lock_guard<mutex> guard(cache_lock);
		if (prefetched_catalog && prefetched_metadata_version == snapshot.schema_version) {
			// the catalog only changes with the schema version
			auto result = std::move(*prefetched_catalog);
			prefetched_catalog.reset();
			client.loaded_metadata_version = snapshot.schema_version;
			return result;
		}
	}
	auto response = GetSnapshotData(snapshot, true, false);
	client.loaded_metadata_version = snapshot.schema_version;
	if (!response.catalog) {
		// LCOV_EXCL_START
		throw InvalidInputException("DuckLake REST: the server did not return the catalog of snapshot %llu",
		                            snapshot.snapshot_id);
		// LCOV_EXCL_STOP
	}
	return std::move(*response.catalog);
}

vector<DuckLakeGlobalStatsInfo> RESTMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot,
                                                                         const vector<TableIndex> &table_ids) {
	// table statistics are not versioned, a transaction keeps the statistics it read first
	{
		lock_guard<mutex> guard(cache_lock);
		auto entry = table_stats.find(snapshot.snapshot_id);
		if (entry != table_stats.end()) {
			return entry->second;
		}
	}
	auto response = GetSnapshotData(snapshot, false, true);
	auto &result = response.stats.value;
	lock_guard<mutex> guard(cache_lock);
	table_stats.emplace(snapshot.snapshot_id, result);
	GetClient().loaded_data_version = snapshot.next_file_id;
	return std::move(result);
}

map<TableIndex, idx_t> RESTMetadataManager::GetTableRecordCounts(DuckLakeSnapshot snapshot) {
	map<TableIndex, idx_t> result;
	for (auto &stats : GetGlobalTableStats(snapshot)) {
		if (!stats.record_count_unknown) {
			result.emplace(stats.table_id, stats.record_count);
		}
	}
	return result;
}

//! Fetches the pages of a response in parallel. The column mappings of the pages are added to the catalog, and the
//! files of the pages are added to the response in the order of the pages.
template <class RESPONSE, class FETCH, class APPEND>
static void AddPages(DuckLakeCatalog &catalog, const vector<string> &page_tokens, const FETCH &fetch_page,
                     const APPEND &append_page) {
	if (page_tokens.empty()) {
		return;
	}
	static constexpr idx_t MAX_PARALLEL_PAGES = 8;
	vector<RESPONSE> results(page_tokens.size());
	std::atomic<idx_t> next_page {0};
	std::atomic<bool> failed {false};
	mutex error_lock;
	std::exception_ptr error;
	auto fetch = [&]() {
		while (!failed) {
			auto page = next_page++;
			if (page >= page_tokens.size()) {
				return;
			}
			try {
				results[page] = fetch_page(page_tokens[page]);
			} catch (...) { // LCOV_EXCL_START
				// a page fails, e.g. as the server stopped
				lock_guard<mutex> guard(error_lock);
				if (!error) {
					error = std::current_exception();
				}
				failed = true;
			} // LCOV_EXCL_STOP
		}
	};
	vector<std::thread> threads;
	for (idx_t i = 1; i < MinValue<idx_t>(page_tokens.size(), MAX_PARALLEL_PAGES); i++) {
		threads.emplace_back(fetch);
	}
	fetch();
	for (auto &thread : threads) {
		thread.join();
	}
	if (error) {
		std::rethrow_exception(error); // LCOV_EXCL_LINE
	}
	for (auto &result : results) {
		catalog.AddNameMaps(std::move(result.column_mappings));
		append_page(result);
	}
}

//! Appends the entries of a page to the entries of a response
template <class T>
static void AppendEntries(vector<T> &entries, vector<T> &page) {
	std::move(page.begin(), page.end(), std::back_inserter(entries));
}

static RESTColumnIdentifier ToColumnIdentifier(idx_t field_id, const RESTInlinedField &field) {
	RESTColumnIdentifier result;
	result.field_id = field_id;
	if (!field.all) {
		for (auto &child : field.fields) {
			result.children.push_back(ToColumnIdentifier(child.first, child.second));
		}
	}
	return result;
}

//! Sets the columns of a request that returns inlined rows: none if the client has the rows already
template <class REQUEST, class PLAN>
static void SetProjection(REQUEST &request, const PLAN &plan) {
	if (!plan.fetch) {
		request.columns = RESTOptional<vector<RESTColumnIdentifier>>(vector<RESTColumnIdentifier>());
		return;
	}
	auto &projection = plan.projection;
	if (projection.all) {
		return;
	}
	vector<RESTColumnIdentifier> columns;
	for (auto &column : projection.columns) {
		columns.push_back(ToColumnIdentifier(column.first, column.second));
	}
	request.columns = RESTOptional<vector<RESTColumnIdentifier>>(std::move(columns));
}

RESTMetadataManager::InlinedRowsPlan
RESTMetadataManager::PlanInlinedRows(const RESTInlinedRowsKey &key,
                                     optional_ptr<const vector<DuckLakeProjectedColumn>> projection) {
	auto wanted = RESTInlinedProjection::From(projection);
	if (inlined_rows_cache.Covers(key, wanted)) {
		return {false, wanted};
	}
	if (inlined_rows_cache.HasResponse(key)) {
		// other scans of the table need other columns: get all of them, so that no scan has to ask again
		wanted = RESTInlinedProjection();
	}
	return {true, wanted};
}

vector<DuckLakeFileListEntry>
RESTMetadataManager::GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                                      const FilterPushdownInfo *filter_info, bool include_local_changes,
                                      optional_ptr<const vector<DuckLakeProjectedColumn>> projection) {
	if (IsTransactionLocal(table.GetTableId())) {
		return {};
	}
	RESTSelectRequest request;
	request.snapshot = snapshot;
	request.table_id = table.GetTableId().index;
	// the inlined rows of an earlier scan at this snapshot can have the columns already
	RESTInlinedRowsKey inlined_key {RESTInlinedRowsKind::SCAN, 0, snapshot.snapshot_id, table.GetTableId().index};
	auto plan = PlanInlinedRows(inlined_key, projection);
	SetProjection(request, plan);
	if (filter_info) {
		request.filters = make_uniq<RESTScanFilters>(DuckLakeRESTFilters::Write(*filter_info, table));
	}
	request.known_column_mappings = transaction.GetCatalog().GetLoadedNameMapIds();
	auto response = GetClient().Select(request);
	transaction.GetCatalog().AddNameMaps(std::move(response.column_mappings));
	AddPages<RESTSelectResponse>(
	    transaction.GetCatalog(), response.page_tokens,
	    [&](const string &page_token) {
		    RESTSelectRequest page_request;
		    page_request.table_id = table.GetTableId().index;
		    page_request.page_token = page_token;
		    return GetClient().Select(page_request);
	    },
	    [&](RESTSelectResponse &page) { AppendEntries(response.files, page.files); });
	if (plan.fetch) {
		inlined_rows_cache.Add(inlined_key, plan.projection, std::move(response.inlined_data));
	}
	return std::move(response.files);
}

vector<DuckLakeFileListExtendedEntry>
RESTMetadataManager::GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                                              const FilterPushdownInfo *filter_info,
                                              optional_ptr<const vector<DuckLakeProjectedColumn>> projection) {
	D_ASSERT(!IsTransactionLocal(table.GetTableId()));
	RESTSelectRequest request;
	request.snapshot = snapshot;
	request.table_id = table.GetTableId().index;
	request.extended = true;
	RESTInlinedRowsKey inlined_key {RESTInlinedRowsKind::SCAN, 0, snapshot.snapshot_id, table.GetTableId().index};
	auto plan = PlanInlinedRows(inlined_key, projection);
	SetProjection(request, plan);
	if (filter_info) {
		request.filters = make_uniq<RESTScanFilters>(DuckLakeRESTFilters::Write(*filter_info, table));
	}
	request.known_column_mappings = transaction.GetCatalog().GetLoadedNameMapIds();
	auto response = GetClient().Select(request);
	transaction.GetCatalog().AddNameMaps(std::move(response.column_mappings));
	AddPages<RESTSelectResponse>(
	    transaction.GetCatalog(), response.page_tokens,
	    [&](const string &page_token) {
		    RESTSelectRequest page_request;
		    page_request.table_id = table.GetTableId().index;
		    page_request.page_token = page_token;
		    return GetClient().Select(page_request);
	    },
	    [&](RESTSelectResponse &page) { AppendEntries(response.extended_files, page.extended_files); });
	if (plan.fetch) {
		inlined_rows_cache.Add(inlined_key, plan.projection, std::move(response.inlined_data));
	}
	return std::move(response.extended_files);
}

unique_ptr<QueryResult> RESTMetadataManager::ToQueryResult(RESTRowSet rows) {
	auto context = transaction.context.lock();
	vector<Identifier> names;
	for (auto &column : rows.columns) {
		names.emplace_back(column.name);
	}
	return make_uniq<QueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), std::move(names),
	                              std::move(rows.rows.owned_rows), context->GetClientProperties());
}

shared_ptr<RESTInlinedRowsResponse> RESTMetadataManager::FetchInlinedRows(const RESTInlinedRowsKey &key,
                                                                          DuckLakeSnapshot start_snapshot,
                                                                          DuckLakeSnapshot snapshot) {
	unique_ptr<RESTInlinedRows> inlined_data;
	if (key.kind == RESTInlinedRowsKind::SCAN) {
		RESTSelectRequest request;
		request.snapshot = snapshot;
		request.table_id = key.table_id;
		inlined_data = std::move(GetClient().Select(request).inlined_data);
	} else {
		// LCOV_EXCL_START
		// the change feed fetches the inlined rows that it reads
		RESTTableChangesRequest request;
		request.table_id = key.table_id;
		request.start_snapshot = start_snapshot;
		request.snapshot = snapshot;
		request.changes = key.kind == RESTInlinedRowsKind::INSERTIONS ? "insertions" : "deletions";
		inlined_data = std::move(GetClient().TableChanges(request).inlined_data);
		// LCOV_EXCL_STOP
	}
	return inlined_rows_cache.Add(key, RESTInlinedProjection(), std::move(inlined_data));
}

unique_ptr<QueryResult> RESTMetadataManager::ReadCachedInlinedRows(RESTInlinedRowsKind kind,
                                                                   DuckLakeSnapshot start_snapshot,
                                                                   DuckLakeSnapshot snapshot,
                                                                   const string &inlined_table_name,
                                                                   const vector<string> &columns_to_read) {
	auto handle = ParseInlinedTableName(inlined_table_name);
	idx_t start_snapshot_id = kind == RESTInlinedRowsKind::SCAN ? 0 : start_snapshot.snapshot_id;
	RESTInlinedRowsKey key {kind, start_snapshot_id, snapshot.snapshot_id, handle.table_id.index};
	auto rows_ptr = inlined_rows_cache.Find(key, columns_to_read);
	if (!rows_ptr) { // LCOV_EXCL_START
		// the rows usually came along with the file list of the scan. Otherwise (or if they came with other
		// columns) the server returns all columns, which are read from the response itself: the cache can have
		// changed in the meantime
		auto response = FetchInlinedRows(key, start_snapshot, snapshot);
		rows_ptr = response->rows;
		if (!rows_ptr) {
			throw InvalidInputException("DuckLake REST: the server did not return the inlined rows of \"%s\"",
			                            inlined_table_name);
		}
	} // LCOV_EXCL_STOP
	// scans of the table can read the rows at the same time
	lock_guard<mutex> read_guard(rows_ptr->read_lock);
	auto &rows = rows_ptr->rows;
	// the columns to read are column names, optionally cast to the type to read them as
	auto context = transaction.context.lock();
	vector<idx_t> column_indexes;
	vector<LogicalType> types;
	for (auto &column : columns_to_read) {
		auto expressions = Parser::GetBuiltinParser().ParseExpressionList(column);
		if (expressions.size() != 1) {
			throw InvalidInputException("DuckLake REST: \"%s\" is not a column of the inlined rows", column);
		}
		optional_ptr<const ParsedExpression> column_ref = expressions[0].get();
		optional<LogicalType> cast_type;
		if (column_ref->GetExpressionClass() == ExpressionClass::CAST) {
			auto &cast = column_ref->Cast<CastExpression>();
			cast_type = TransformStringToLogicalType(cast.TargetType().ToString(), *context);
			column_ref = cast.Child();
		}
		if (column_ref->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			throw InvalidInputException("DuckLake REST: \"%s\" is not a column of the inlined rows", column);
		}
		auto &colref = column_ref->Cast<ColumnRefExpression>();
		// the inlined readers qualify their columns with the row alias of the inlined table
		if (colref.IsQualified() && (colref.ColumnNames().size() != 2 || colref.ColumnNames()[0] != "inlined_data")) {
			throw InvalidInputException("DuckLake REST: \"%s\" is not a column of the inlined rows", column);
		}
		auto &name = colref.GetColumnName();
		optional_idx column_index;
		for (idx_t i = 0; i < rows.columns.size(); i++) {
			if (Identifier(rows.columns[i].name) == name) {
				column_index = i;
			}
		}
		if (!column_index.IsValid()) {
			throw InvalidInputException("DuckLake REST: the inlined rows have no column \"%s\"", column);
		}
		column_indexes.push_back(column_index.GetIndex());
		types.push_back(cast_type ? *cast_type : rows.columns[column_index.GetIndex()].type);
	}
	RESTInlinedRows result;
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		RESTResultColumn column;
		column.name = "col" + to_string(i + 1);
		column.type = types[i];
		result.columns.push_back(std::move(column));
	}
	auto projected_rows = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	ColumnDataAppendState append_state;
	projected_rows->InitializeAppend(append_state);
	DataChunk projected;
	projected.Initialize(Allocator::DefaultAllocator(), types);
	for (auto &chunk : rows.rows.rows->Chunks()) {
		projected.Reset();
		for (idx_t i = 0; i < column_indexes.size(); i++) {
			auto &source = chunk.data[column_indexes[i]];
			if (source.GetType() == types[i]) {
				projected.data[i].Reference(source);
			} else {
				// LCOV_EXCL_START
				// the server returns the rows in the types of the snapshot
				VectorOperations::DefaultCast(source, projected.data[i], chunk.size());
				// LCOV_EXCL_STOP
			}
		}
		projected.SetCardinality(chunk.size());
		projected_rows->Append(append_state, projected);
	}
	result.rows = RESTRows(std::move(projected_rows));
	return ToQueryResult(std::move(result));
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedData(DuckLakeSnapshot snapshot,
                                                             const Identifier &inlined_table_name,
                                                             const vector<string> &columns_to_read) {
	return ReadCachedInlinedRows(RESTInlinedRowsKind::SCAN, snapshot, snapshot, inlined_table_name.GetIdentifierName(),
	                             columns_to_read);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot,
                                                                       DuckLakeSnapshot end_snapshot,
                                                                       const Identifier &inlined_table_name,
                                                                       const vector<string> &columns_to_read) {
	return ReadCachedInlinedRows(RESTInlinedRowsKind::INSERTIONS, start_snapshot, end_snapshot,
	                             inlined_table_name.GetIdentifierName(), columns_to_read);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot,
                                                                      DuckLakeSnapshot end_snapshot,
                                                                      const Identifier &inlined_table_name,
                                                                      const vector<string> &columns_to_read) {
	return ReadCachedInlinedRows(RESTInlinedRowsKind::DELETIONS, start_snapshot, end_snapshot,
	                             inlined_table_name.GetIdentifierName(), columns_to_read);
}

optional_idx RESTMetadataManager::GetNetRowCount(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot) {
	D_ASSERT(!IsTransactionLocal(table.GetTableId()));
	// the server counts the rows from its statistics
	RESTSelectRequest request;
	request.table_id = table.GetTableId().index;
	request.snapshot = snapshot;
	RESTAggregate aggregate;
	aggregate.function = "count_star";
	request.aggregates.push_back(std::move(aggregate));
	auto response = GetClient().Select(request);
	if (response.aggregate_results.size() != 1) {
		throw InvalidInputException("DuckLake REST: the server did not return the result of the aggregate");
	}
	auto &result = response.aggregate_results[0];
	if (!result.answered) {
		return optional_idx();
	}
	return NumericCast<idx_t>(result.value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>());
}

vector<DuckLakeInlinedTableInfo> RESTMetadataManager::GetInlinedDataTables(const DuckLakeTableEntry &table,
                                                                           DuckLakeSnapshot snapshot) {
	auto table_id = table.GetTableId();
	if (IsTransactionLocal(table_id)) {
		return {};
	}
	// the server returns the inlined rows of the table as one set of rows, which the core reads as the inlined data
	// of the table at this snapshot
	bool has_inlined_rows;
	if (!inlined_rows_cache.HasInlinedRows(snapshot.snapshot_id, table_id.index, has_inlined_rows)) {
		RESTInlinedRowsKey key {RESTInlinedRowsKind::SCAN, 0, snapshot.snapshot_id, table_id.index};
		has_inlined_rows = FetchInlinedRows(key, snapshot, snapshot)->rows != nullptr;
	}
	if (!has_inlined_rows) {
		return {};
	}
	DuckLakeInlinedTableInfo inlined_table;
	inlined_table.table_name = InlinedTableName(table_id, snapshot.schema_version);
	// the core reads the inlined rows in the schema of the table at the snapshot
	inlined_table.schema_version = DConstants::INVALID_INDEX;
	return {std::move(inlined_table)};
}

vector<DuckLakeSnapshotInfo> RESTMetadataManager::GetAllSnapshots(const DuckLakeSnapshotFilter &filter) {
	// the branch and chain restrictions of the filter stay unsent, the server applies the lineage of its catalog
	RESTListSnapshotsRequest request;
	request.exclude_latest = filter.exclude_latest;
	request.older_than = filter.older_than;
	if (filter.has_snapshot_ids) {
		// LCOV_EXCL_START
		// expiring snapshots and snapshot tags filter by id, a REST catalog refuses both
		if (filter.snapshot_ids.empty()) {
			// the snapshot ids are query parameters, which cannot express an empty list
			return {};
		}
		request.snapshot_ids = filter.snapshot_ids;
		// LCOV_EXCL_STOP
	}
	vector<DuckLakeSnapshotInfo> result;
	while (true) {
		auto page = GetClient().ListSnapshots(request);
		std::move(page.snapshots.begin(), page.snapshots.end(), std::back_inserter(result));
		if (page.next_page_token.empty()) {
			return result;
		}
		request.page_token = std::move(page.next_page_token);
	}
}

//! Expiring snapshots and deleting files is performed on the server
[[noreturn]] static void ThrowMaintenanceOnServer(const char *what) {
	throw NotImplementedException("DuckLake REST catalogs do not support %s: run the maintenance functions on the "
	                              "DuckLake that the server serves",
	                              what);
}

// LCOV_EXCL_START
// the bind of the expiry refuses REST catalogs
void RESTMetadataManager::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	ThrowMaintenanceOnServer("expiring snapshots");
}
// LCOV_EXCL_STOP

vector<DuckLakeSnapshotTagInfo> RESTMetadataManager::GetSnapshotTags(bool with_snapshot_time) {
	throw NotImplementedException("DuckLake REST catalogs do not support snapshot tags");
}

vector<DuckLakeFileForCleanup> RESTMetadataManager::GetOldFilesForCleanup(const string &older_than) {
	ThrowMaintenanceOnServer("cleaning up files");
}

unique_ptr<QueryResult> RESTMetadataManager::ReadMetadataTable(const string &table_name) {
	throw NotImplementedException("DuckLake REST catalogs do not have metadata tables to read");
}

vector<string> RESTMetadataManager::GetKnownFilePaths() {
	ThrowMaintenanceOnServer("deleting orphaned files");
}

static RESTOptionSetting GetOptionSetting(const DuckLakeConfigOption &option) {
	RESTOptionSetting result;
	result.key = option.option.key;
	result.value = option.option.value;
	if (option.schema_id.IsValid()) {
		result.schema_id = option.schema_id.index;
	}
	if (option.table_id.IsValid()) {
		result.table_id = ToRequestId(option.table_id.index);
	}
	return result;
}

idx_t RESTMetadataManager::SetConfigOption(const DuckLakeConfigOption &option) {
	auto &request = AddCommitRequest<RESTSetOptionRequest>();
	request.option = GetOptionSetting(option);
	// the server writes the option when the transaction commits
	return 1;
}

bool RESTMetadataManager::ResetConfigOption(const DuckLakeConfigOption &option) {
	auto &request = AddCommitRequest<RESTSetOptionRequest>();
	request.option = GetOptionSetting(option);
	request.reset = true;
	return true;
}

vector<DuckLakeTableSizeInfo> RESTMetadataManager::GetTableSizes(DuckLakeSnapshot snapshot) {
	// the sizes are the sizes of the files of the tables
	auto data = GetSnapshotData(snapshot, true, false);
	vector<DuckLakeTableSizeInfo> result;
	if (!data.catalog) {
		return result; // LCOV_EXCL_LINE: the server returns the catalog that the request includes
	}
	auto &client = GetClient();
	for (auto &table : data.catalog->tables) {
		DuckLakeTableSizeInfo size;
		size.schema_id = table.schema_id;
		size.table_id = table.id;
		size.table_name = table.name.GetIdentifierName();
		size.table_uuid = table.uuid;
		auto add_files = [&](const vector<DuckLakeFileListExtendedEntry> &files) {
			for (auto &file : files) {
				size.file_count++;
				size.file_size_bytes += file.file.file_size_bytes;
				if (!file.delete_file.path.empty()) {
					size.delete_file_count++;
					size.delete_file_size_bytes += file.delete_file.file_size_bytes;
				}
			}
		};
		RESTSelectRequest request;
		request.snapshot = snapshot;
		request.table_id = table.id.index;
		request.extended = true;
		// no columns of the inlined rows are needed
		request.columns = RESTOptional<vector<RESTColumnIdentifier>>(vector<RESTColumnIdentifier>());
		auto response = client.Select(request);
		add_files(response.extended_files);
		for (auto &page_token : response.page_tokens) {
			RESTSelectRequest page_request;
			page_request.table_id = table.id.index;
			page_request.page_token = page_token;
			add_files(client.Select(page_request).extended_files);
		}
		result.push_back(std::move(size));
	}
	return result;
}

vector<DuckLakeColumnMappingInfo> RESTMetadataManager::GetColumnMappings(optional_idx start_from) {
	// the column mappings come with the files that refer to them
	return {};
}

RESTTableChangesResponse
RESTMetadataManager::GetTableChanges(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
                                     DuckLakeSnapshot snapshot, const string &changes,
                                     optional_ptr<const vector<DuckLakeProjectedColumn>> projection) {
	RESTTableChangesRequest request;
	request.table_id = table.GetTableId().index;
	request.start_snapshot = start_snapshot;
	request.snapshot = snapshot;
	request.changes = changes;
	auto kind = changes == "insertions" ? RESTInlinedRowsKind::INSERTIONS : RESTInlinedRowsKind::DELETIONS;
	RESTInlinedRowsKey inlined_key {kind, start_snapshot.snapshot_id, snapshot.snapshot_id, table.GetTableId().index};
	auto plan = PlanInlinedRows(inlined_key, projection);
	SetProjection(request, plan);
	request.known_column_mappings = transaction.GetCatalog().GetLoadedNameMapIds();
	auto response = GetClient().TableChanges(request);
	transaction.GetCatalog().AddNameMaps(std::move(response.column_mappings));
	AddPages<RESTTableChangesResponse>(
	    transaction.GetCatalog(), response.page_tokens,
	    [&](const string &page_token) {
		    RESTTableChangesRequest page_request;
		    page_request.table_id = table.GetTableId().index;
		    page_request.page_token = page_token;
		    return GetClient().TableChanges(page_request);
	    },
	    [&](RESTTableChangesResponse &page) {
		    AppendEntries(response.insertions, page.insertions);
		    AppendEntries(response.deletions, page.deletions);
	    });
	if (plan.fetch) {
		inlined_rows_cache.Add(inlined_key, plan.projection, std::move(response.inlined_data));
	}
	return response;
}

vector<DuckLakeFileListEntry>
RESTMetadataManager::GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
                                        DuckLakeSnapshot snapshot,
                                        optional_ptr<const vector<DuckLakeProjectedColumn>> projection) {
	D_ASSERT(!IsTransactionLocal(table.GetTableId()));
	return std::move(GetTableChanges(table, start_snapshot, snapshot, "insertions", projection).insertions);
}

vector<DuckLakeDeleteScanEntry>
RESTMetadataManager::GetTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
                                       DuckLakeSnapshot snapshot, bool include_local_changes,
                                       optional_ptr<const vector<DuckLakeProjectedColumn>> projection) {
	D_ASSERT(!IsTransactionLocal(table.GetTableId()));
	return std::move(GetTableChanges(table, start_snapshot, snapshot, "deletions", projection).deletions);
}

// LCOV_EXCL_START
// finding the files to clean up throws first
void RESTMetadataManager::RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files) {
	ThrowMaintenanceOnServer("cleaning up files");
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Catalog Changes
//===--------------------------------------------------------------------===//
void RESTMetadataManager::OnCreateSchema(DuckLakeSchemaEntry &schema) {
	auto &request = AddCommitRequest<RESTCreateSchemaRequest>();
	request.placeholder_id = ToRequestId(schema.GetSchemaId().index);
	created_oids[request.placeholder_id] = schema.oid;
	auto parent = schema.ParentDuckLakeSchema();
	if (parent) {
		request.parent_schema_id = ToRequestId(parent->GetSchemaId().index);
	}
	request.schema_name = schema.name.GetIdentifierName();
	request.schema_uuid = schema.GetSchemaUUID();
	request.path = schema.DataPath();
}

void RESTMetadataManager::OnCreateTable(DuckLakeSchemaEntry &schema, const CreateTableInfo &info,
                                        const DuckLakeTableEntry &table) {
	auto &request = AddCommitRequest<RESTCreateTableRequest>();
	request.placeholder_id = ToRequestId(table.GetTableId().index);
	created_oids[request.placeholder_id] = table.oid;
	request.schema_id = ToRequestId(schema.GetSchemaId().index);
	request.table_name = info.GetTableName().GetIdentifierName();
	request.table_uuid = table.GetTableUUID();
	request.path = table.DataPath();
	auto version = transaction.GetCatalog().GetDuckLakeVersion();
	auto not_null_fields = table.GetNotNullFields();
	for (auto &column : table.GetColumns().Logical()) {
		// fail with the error of a commit to a metadata database, rather than on a type the server cannot parse
		DuckLakeTypes::ToString(column.GetType());
		DuckLakeTypes::CheckSupportedType(column.GetType(), version);
		auto &initial_default = table.GetFieldData().GetByRootIndex(column.Physical()).GetColumnData().initial_default;
		request.columns.push_back(
		    DuckLakeRESTCatalogChanges::WriteColumn(column, not_null_fields.count(column.Name()) > 0, initial_default));
	}
	for (auto &key : info.partition_keys) {
		request.partitioned_by.push_back(key->ToString());
	}
	for (auto &key : info.sort_keys) {
		request.sorted_by.push_back(key->ToString());
	}
	for (auto &option : info.options) {
		request.options[option.first] = option.second->ToString();
	}
	if (!table.comment.IsNull()) {
		request.comment = table.comment.ToString();
	}
}

void RESTMetadataManager::OnCreateView(DuckLakeViewEntry &view) {
	auto &request = AddCommitRequest<RESTCreateViewRequest>();
	request.placeholder_id = ToRequestId(view.GetViewId().index);
	created_oids[request.placeholder_id] = view.oid;
	request.schema_id = ToRequestId(view.ParentSchema().Cast<DuckLakeSchemaEntry>().GetSchemaId().index);
	request.view_name = view.name.GetIdentifierName();
	request.view_uuid = view.GetViewUUID();
	request.dialect = "duckdb";
	request.sql = view.GetQuerySQL();
	for (auto &alias : view.aliases) {
		request.column_aliases.push_back(alias.GetIdentifierName());
	}
	if (!view.comment.IsNull()) {
		request.comment = view.comment.ToString();
	}
	auto view_info = view.GetInfo();
	for (auto &entry : view_info->Cast<CreateViewInfo>().column_comments_map) {
		if (!entry.second.IsNull()) {
			request.column_comments[entry.first.GetIdentifierName()] = entry.second.ToString();
		}
	}
}

void RESTMetadataManager::OnCreateMacro(MacroCatalogEntry &macro) {
	auto &request = AddCommitRequest<RESTCreateMacroRequest>();
	// macros have no id before the commit: the placeholder id is a local id
	auto placeholder_id = ToRequestId(transaction.GetLocalCatalogId());
	local_macros[macro] = placeholder_id;
	request.placeholder_id = placeholder_id;
	created_oids[request.placeholder_id] = macro.oid;
	request.schema_id = ToRequestId(macro.schema.Cast<DuckLakeSchemaEntry>().GetSchemaId().index);
	request.macro = DuckLakeTransactionState::GetMacroInfo(macro);
}

//! The type an ALTER TABLE gives a column, if any
static LogicalType GetAlteredType(const AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		return LogicalType::INVALID;
	}
	auto &alter = info.Cast<AlterTableInfo>();
	switch (alter.alter_table_type) {
	case AlterTableType::ADD_COLUMN:
		return alter.Cast<AddColumnInfo>().new_column.GetType();
	case AlterTableType::ADD_FIELD:
		return alter.Cast<AddFieldInfo>().new_field.GetType();
	case AlterTableType::ALTER_COLUMN_TYPE:
		return alter.Cast<ChangeColumnTypeInfo>().target_type;
	default:
		return LogicalType::INVALID;
	}
}

//! Whether an ADD COLUMN or ADD FIELD with IF NOT EXISTS finds the column or field, so the ALTER changes nothing
static bool IsSkippedAdd(CatalogEntry &entry, const AlterInfo &info) {
	if (entry.type != CatalogType::TABLE_ENTRY || info.type != AlterType::ALTER_TABLE) {
		return false;
	}
	auto &table = entry.Cast<DuckLakeTableEntry>();
	auto &alter = info.Cast<AlterTableInfo>();
	if (alter.alter_table_type == AlterTableType::ADD_COLUMN) {
		auto &add = alter.Cast<AddColumnInfo>();
		return add.if_column_not_exists && table.ColumnExists(add.new_column.Name());
	}
	if (alter.alter_table_type == AlterTableType::ADD_FIELD) {
		auto &add = alter.Cast<AddFieldInfo>();
		// a path lookup does not enter every type of field (e.g. a VARIANT), so find the field in its parent
		auto parent = add.if_field_not_exists ? table.TryGetFieldId(add.column_path) : nullptr;
		return parent && parent->GetChildByName(add.new_field.Name());
	}
	return false;
}

void RESTMetadataManager::OnAlterEntry(CatalogEntry &entry, const AlterInfo &info) {
	if (IsSkippedAdd(entry, info)) {
		return;
	}
	auto altered_type = GetAlteredType(info);
	if (altered_type.id() != LogicalTypeId::INVALID) {
		DuckLakeTypes::ToString(altered_type);
		DuckLakeTypes::CheckSupportedType(altered_type, transaction.GetCatalog().GetDuckLakeVersion());
	}
	auto &request = AddCommitRequest<RESTAlterRequest>();
	if (entry.type == CatalogType::TABLE_ENTRY) {
		request.entry_id = ToRequestId(entry.Cast<DuckLakeTableEntry>().GetTableId().index);
	} else {
		request.entry_id = ToRequestId(entry.Cast<DuckLakeViewEntry>().GetViewId().index);
	}
	auto context = transaction.context.lock();
	request.alter = DuckLakeRESTCatalogChanges::WriteAlter(*context, info);
}

void RESTMetadataManager::OnDropEntry(CatalogEntry &entry) {
	auto &request = AddCommitRequest<RESTDropRequest>();
	switch (entry.type) {
	case CatalogType::SCHEMA_ENTRY:
		request.entry_type = "schema";
		request.entry_id = ToRequestId(entry.Cast<DuckLakeSchemaEntry>().GetSchemaId().index);
		return;
	case CatalogType::TABLE_ENTRY:
		request.entry_type = "table";
		request.entry_id = ToRequestId(entry.Cast<DuckLakeTableEntry>().GetTableId().index);
		return;
	case CatalogType::VIEW_ENTRY:
		request.entry_type = "view";
		request.entry_id = ToRequestId(entry.Cast<DuckLakeViewEntry>().GetViewId().index);
		return;
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY: {
		bool scalar = entry.type == CatalogType::MACRO_ENTRY;
		request.entry_type = scalar ? "scalar_macro" : "table_macro";
		auto local_macro = local_macros.find(entry);
		if (local_macro != local_macros.end()) {
			request.entry_id = local_macro->second;
		} else if (scalar) {
			request.entry_id = ToRequestId(entry.Cast<DuckLakeScalarMacroEntry>().GetIndex().index);
		} else {
			request.entry_id = ToRequestId(entry.Cast<DuckLakeTableMacroEntry>().GetIndex().index);
		}
		return;
	}
	default:
		throw InternalException("Unsupported catalog entry type for DROP");
	}
}

void RESTMetadataManager::OnEntryChanged(CatalogEntry &entry) {
	if (catalog_requests.empty() || entry.type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &table = entry.Cast<DuckLakeTableEntry>();
	auto partition_data = table.GetPartitionData();
	if (!partition_data || !DuckLakeTransaction::IsTransactionLocal(partition_data->partition_id)) {
		return;
	}
	// files written for the new partitioning refer to it by this id
	auto &last_request = *catalog_requests.back();
	if (last_request.type == RESTCreateTableRequest::TYPE) {
		last_request.Cast<RESTCreateTableRequest>().partition_id = partition_data->partition_id;
	} else if (last_request.type == RESTAlterRequest::TYPE) {
		last_request.Cast<RESTAlterRequest>().partition_id = partition_data->partition_id;
	}
}

void RESTMetadataManager::OnDropFile(TableIndex table_id, DataFileIndex data_file_id, const string &path,
                                     idx_t row_count, idx_t file_size_bytes) {
	RESTDroppedFile file;
	file.data_file_id = data_file_id.index;
	file.path = path;
	file.record_count = row_count;
	file.file_size_bytes = file_size_bytes;
	dropped_files[table_id].push_back(std::move(file));
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//
template <class T>
static T &AddRequest(vector<unique_ptr<RESTCommitRequest>> &requests) {
	auto request = make_uniq<T>();
	auto &result = *request;
	requests.push_back(std::move(request));
	return result;
}

void RESTMetadataManager::AddDeletes(const TransactionChangeInformation &transaction_changes,
                                     vector<unique_ptr<RESTCommitRequest>> &requests) {
	auto &dropped_tables = transaction_changes.dropped_tables;
	set<TableIndex> tables_with_deletes;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		if (dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto &changes = entry.second;
		if (changes.new_delete_files.empty() && changes.new_inlined_data_deletes.empty() &&
		    !changes.new_inlined_file_deletes) {
			continue;
		}
		tables_with_deletes.insert(entry.first);
		auto &request = AddRequest<RESTDeleteDataRequest>(requests);
		request.table_id = ToRequestId(entry.first.index);
		for (auto &file_entry : changes.new_delete_files) {
			for (auto &delete_file : file_entry.second) {
				request.delete_files.push_back(delete_file);
			}
		}
		for (auto &inlined_entry : changes.new_inlined_data_deletes) {
			request.inlined_row_ids.insert(inlined_entry.second->rows.begin(), inlined_entry.second->rows.end());
		}
		if (changes.new_inlined_file_deletes) {
			for (auto &file_entry : changes.new_inlined_file_deletes->file_deletes) {
				RESTFileRowDeletes deletes;
				deletes.data_file_id = file_entry.first;
				deletes.row_ids = file_entry.second;
				request.file_row_deletes.push_back(std::move(deletes));
			}
		}
		request.dropped_files = GetDroppedFiles(entry.first);
	}
	for (auto &entry : dropped_files) {
		if (tables_with_deletes.find(entry.first) != tables_with_deletes.end() ||
		    dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto &request = AddRequest<RESTDeleteDataRequest>(requests);
		request.table_id = ToRequestId(entry.first.index);
		request.dropped_files = GetDroppedFiles(entry.first);
	}
	for (auto &table_id : transaction.GetTablesDeleteAttempted()) {
		// deletes from tables created by the transaction cannot conflict with other transactions
		if (dropped_tables.find(table_id) != dropped_tables.end() || IsTransactionLocal(table_id)) {
			continue;
		}
		auto &request = AddRequest<RESTDeleteDataRequest>(requests);
		request.table_id = ToRequestId(table_id.index);
		request.delete_attempted = true;
	}
}

void RESTMetadataManager::AddInserts(const TransactionChangeInformation &transaction_changes,
                                     vector<unique_ptr<RESTCommitRequest>> &requests) {
	auto &dropped_tables = transaction_changes.dropped_tables;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		if (dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto table_id = ToRequestId(entry.first.index);
		auto &changes = entry.second;
		if (!changes.new_data_files.empty()) {
			auto &request = AddRequest<RESTInsertFilesRequest>(requests);
			request.table_id = table_id;
			request.files = changes.new_data_files;
			for (auto &name_map_entry : transaction.GetNewNameMaps().name_maps) {
				auto &name_map = *name_map_entry.second;
				if (name_map.table_id != entry.first) {
					continue;
				}
				DuckLakeColumnMappingInfo mapping;
				mapping.table_id = name_map.table_id;
				mapping.mapping_id = name_map.id;
				mapping.map_type = "map_by_name";
				mapping.map_columns = name_map.FlattenColumns();
				request.name_maps.push_back(std::move(mapping));
			}
		}
		if (changes.new_inlined_data && changes.new_inlined_data->data) {
			auto &inlined_data = *changes.new_inlined_data;
			auto &request = AddRequest<RESTInsertRowsRequest>(requests);
			request.table_id = table_id;
			request.rows = RESTRows(*inlined_data.data);
			if (inlined_data.HasPreservedRowIds()) {
				request.row_ids = inlined_data.row_ids;
			}
			request.column_stats = inlined_data.column_stats;
		}
	}
}

vector<RESTDroppedFile> RESTMetadataManager::GetDroppedFiles(TableIndex table_id) {
	auto entry = dropped_files.find(table_id);
	if (entry == dropped_files.end()) {
		return vector<RESTDroppedFile>();
	}
	return entry->second;
}

void RESTMetadataManager::FlushChangesServerSide(DuckLakeTransaction &flush_transaction,
                                                 DuckLakeSnapshot transaction_snapshot,
                                                 const TransactionChangeInformation &transaction_changes,
                                                 const DuckLakeRetryConfig &retry_config) {
	RESTCommitTransactionRequest request;
	auto &requests = request.requests;
	// deletes go first, as the catalog changes of the transaction can depend on them (e.g. SET NOT NULL)
	AddDeletes(transaction_changes, requests);
	for (auto &catalog_request : catalog_requests) {
		requests.push_back(std::move(catalog_request));
	}
	catalog_requests.clear();
	// inserts go last, they are in the format of the tables at the end of the transaction
	AddInserts(transaction_changes, requests);
	if (transaction_snapshot.snapshot_id != DConstants::INVALID_INDEX) {
		request.snapshot = transaction_snapshot;
	}
	request.commit_info = make_uniq<DuckLakeSnapshotCommit>(flush_transaction.GetCommitInfo());
	request.retry = make_uniq<RESTRetryConfig>();
	request.retry->max_retry_count = retry_config.max_retry_count;
	request.retry->retry_wait_ms = retry_config.retry_wait_ms;
	request.retry->retry_backoff = retry_config.retry_backoff;
	RESTCommitTransactionResponse response;
	try {
		response = GetClient().CommitTransaction(request);
	} catch (std::exception &ex) {
		// the server rejected the commit, but after a failed request or response it might have committed
		if (ErrorData(ex).Type() != ExceptionType::IO) {
			flush_transaction.CleanupFiles();
		}
		throw;
	}
	if (RESTIsNull(response.snapshot)) {
		// nothing was written that creates a snapshot (e.g. only options were set)
		flush_transaction.ApplyServerSideCommit(transaction_snapshot.schema_version);
		return;
	}
	auto &catalog = flush_transaction.GetCatalog();
	// the created entries keep their oid under the ids the commit gave them
	ducklake_entry_oids_t entry_oids;
	for (auto &entry : response.entry_ids) {
		auto type = EnumUtil::FromString<CatalogType>(entry.type);
		auto oid = created_oids.find(entry.placeholder_id);
		if (oid != created_oids.end()) {
			entry_oids.emplace(make_pair(type, entry.id), oid->second);
		}
	}
	catalog.SetEntryOids(entry_oids);
	for (auto &setting : response.table_options) {
		DuckLakeConfigOption option;
		option.option.key = setting.key;
		option.option.value = setting.value;
		option.table_id = TableIndex(setting.table_id.value);
		catalog.SetConfigOption(option);
	}
	catalog.SetCommittedSnapshotId(response.snapshot.snapshot_id);
	flush_transaction.ApplyServerSideCommit(response.snapshot.schema_version, response.snapshot.next_file_id);
}

} // namespace duckdb
