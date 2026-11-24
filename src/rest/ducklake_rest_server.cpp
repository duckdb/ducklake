#include "rest/ducklake_rest_server.hpp"
#include "rest/ducklake_rest_arrow.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "ducklake_extension.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

#include "rest/ducklake_rest_client.hpp"

#include "httplib.hpp"
#include "re2/re2.h"

namespace duckdb {

//! The number of commits that share a snapshot at most
static constexpr idx_t MAX_COMMIT_GROUP = 256;
//! How long a test holds its commits back at most, so that a hold that is never met fails the test instead of
//! hanging it
static constexpr int64_t MAX_COMMIT_HOLD_SECONDS = 60;

bool DuckLakeRESTPendingCommit::CanShareSnapshotWith(const DuckLakeRESTPendingCommit &other) const {
	return shares_snapshot && other.shares_snapshot && server == other.server && prefix == other.prefix &&
	       accepts_arrow == other.accepts_arrow;
}

vector<reference<DuckLakeRESTPendingCommit>> DuckLakeRESTCommitQueue::TakeNext() {
	vector<reference<DuckLakeRESTPendingCommit>> result;
	// the commits are taken off the queue after the only allocation, so that a failure loses none of them
	result.reserve(MinValue<idx_t>(pending.size(), MAX_COMMIT_GROUP));
	result.push_back(pending.front());
	pending.pop_front();
	auto &first = result[0].get();
	// the commits that cannot share the snapshot keep their place in the queue
	for (auto entry = pending.begin(); entry != pending.end() && result.size() < MAX_COMMIT_GROUP;) {
		if (first.CanShareSnapshotWith(entry->get())) {
			result.push_back(*entry);
			entry = pending.erase(entry);
		} else {
			entry++;
		}
	}
	return result;
}

void DuckLakeRESTCommitQueue::Submit(DuckLakeRESTPendingCommit &commit, idx_t hold, const run_commits_t &run_commits) {
	unique_lock<mutex> guard(lock);
	pending.push_back(commit);
	if (hold > 0) {
		// a test holds its commits back until they all wait, so that they form the groups it expects
		released = released || pending.size() >= hold;
		ready.notify_all();
		bool met = ready.wait_for(guard, std::chrono::seconds(MAX_COMMIT_HOLD_SECONDS),
		                          [&] { return released || commit.done; });
		released = released || !met;
		ready.notify_all();
	}
	ready.wait(guard, [&] { return commit.done || !running; });
	if (commit.done) {
		return;
	}
	running = true;
	vector<reference<DuckLakeRESTPendingCommit>> commits;
	try {
		while (!commit.done) {
			commits = TakeNext();
			guard.unlock();
			run_commits(commits);
			guard.lock();
			for (auto &entry : commits) {
				entry.get().done = true;
			}
			commits.clear();
			ready.notify_all();
		}
	} catch (...) {
		// LCOV_EXCL_START: the commits catch their errors, only a failure outside of them gets here
		// the queue must not stay blocked, and it must not keep the commit of a request that leaves
		if (!guard.owns_lock()) {
			guard.lock();
		}
		for (auto &entry : commits) {
			auto &taken = entry.get();
			// a commit that has its response or its error ran to its end
			taken.interrupted = taken.response.empty() && !taken.error.HasError();
			taken.done = true;
		}
		if (!commit.done) {
			for (auto entry = pending.begin(); entry != pending.end(); entry++) {
				if (&entry->get() == &commit) {
					pending.erase(entry);
					break;
				}
			}
			commit.interrupted = true;
			commit.done = true;
		}
	} // LCOV_EXCL_STOP
	running = false;
	released = released && !pending.empty();
	ready.notify_all();
}

//===--------------------------------------------------------------------===//
// Request
//===--------------------------------------------------------------------===//
DuckLakeRESTRequest::DuckLakeRESTRequest(DatabaseInstance &db, const string &catalog_name) : connection(db) {
	connection.BeginTransaction();
	auto &context = *connection.context;
	auto &entry = Catalog::GetCatalog(context, Identifier(catalog_name));
	if (entry.GetCatalogType() != "ducklake") {
		throw InvalidInputException("Catalog \"%s\" is not a DuckLake catalog", catalog_name);
	}
	catalog = &entry.Cast<DuckLakeCatalog>();
	transaction = &DuckLakeTransaction::Get(context, *catalog);
	GetMetadataManager();
}

DuckLakeMetadataManager &DuckLakeRESTRequest::GetMetadataManager() {
	auto &metadata_manager = transaction->GetMetadataManager();
	// the files of REST responses hold their row counts
	metadata_manager.SetReadFileCounts(true);
	return metadata_manager;
}

DuckLakeSnapshot DuckLakeRESTRequest::GetSnapshotById(idx_t snapshot_id) {
	BoundAtClause at_clause(Identifier("version"), Value::UBIGINT(snapshot_id));
	return transaction->GetSnapshot(at_clause);
}

DuckLakeSnapshot DuckLakeRESTRequest::GetSnapshot(const DuckLakeSnapshot &snapshot, optional_idx snapshot_id) {
	if (!RESTIsNull(snapshot)) {
		return snapshot;
	}
	if (!snapshot_id.IsValid()) {
		return transaction->GetSnapshot();
	}
	return GetSnapshotById(snapshot_id.GetIndex());
}

DuckLakeSnapshot DuckLakeRESTRequest::GetSnapshot(const RESTSnapshotReference &reference) {
	return GetSnapshot(reference.snapshot, reference.snapshot_id);
}

//===--------------------------------------------------------------------===//
// Server
//===--------------------------------------------------------------------===//
DuckLakeRESTServer::DuckLakeRESTServer() {
}

void DuckLakeRESTServer::AddCatalog(const string &prefix, shared_ptr<DatabaseInstance> db, string catalog_name) {
	if (prefix.empty() || prefix.find('/') != string::npos) {
		throw InvalidInputException("The prefix of a catalog has to be a name without slashes, not \"%s\"", prefix);
	}
	auto commit_queue = DuckLakeRESTServerRegistry::Get().GetCommitQueue(*db, catalog_name);
	lock_guard<mutex> guard(catalog_lock);
	catalogs[prefix] = ServedCatalog {prefix, std::move(db), std::move(catalog_name), std::move(commit_queue)};
}

//! Plans expire after this many seconds
static constexpr int64_t PLAN_EXPIRATION_SECONDS = 600;
//! The number of plans that are kept
static constexpr idx_t MAX_PLANS = 256;

string DuckLakeRESTPlans::Add(shared_ptr<DuckLakeRESTPlan> plan) {
	plan->created = std::chrono::steady_clock::now();
	auto id = UUID::ToString(UUID::GenerateRandomUUID());
	lock_guard<mutex> guard(lock);
	for (auto entry = plans.begin(); entry != plans.end();) {
		if (plan->created - entry->second->created > std::chrono::seconds(PLAN_EXPIRATION_SECONDS)) {
			entry = plans.erase(entry); // LCOV_EXCL_LINE: plans expire after PLAN_EXPIRATION_SECONDS
		} else {
			entry++;
		}
	}
	while (plans.size() >= MAX_PLANS) {
		// the oldest plan goes
		auto oldest = plans.begin();
		for (auto entry = plans.begin(); entry != plans.end(); entry++) {
			if (entry->second->created < oldest->second->created) {
				oldest = entry;
			}
		}
		plans.erase(oldest);
	}
	plans.emplace(id, std::move(plan));
	return id;
}

shared_ptr<DuckLakeRESTPlan> DuckLakeRESTPlans::Get(const string &id) {
	lock_guard<mutex> guard(lock);
	auto entry = plans.find(id);
	if (entry == plans.end()) {
		return nullptr;
	}
	if (std::chrono::steady_clock::now() - entry->second->created > std::chrono::seconds(PLAN_EXPIRATION_SECONDS)) {
		// LCOV_EXCL_START
		// plans expire after PLAN_EXPIRATION_SECONDS
		plans.erase(entry);
		return nullptr;
		// LCOV_EXCL_STOP
	}
	return entry->second;
}

void DuckLakeRESTServer::SetClientProperties(map<string, string> defaults, map<string, string> overrides) {
	lock_guard<mutex> guard(catalog_lock);
	client_defaults = std::move(defaults);
	client_overrides = std::move(overrides);
}

bool DuckLakeRESTServer::RemoveCatalog(const string &prefix) {
	ServedCatalog removed;
	{
		lock_guard<mutex> guard(catalog_lock);
		auto entry = catalogs.find(prefix);
		if (entry == catalogs.end()) {
			return false;
		}
		removed = std::move(entry->second);
		catalogs.erase(entry);
	}
	return true;
}

DuckLakeRESTServer::ServedCatalog DuckLakeRESTServer::GetCatalog(optional_ptr<const string> prefix) {
	lock_guard<mutex> guard(catalog_lock);
	if (prefix) {
		auto entry = catalogs.find(*prefix);
		if (entry == catalogs.end()) {
			throw CatalogException("The server serves no catalog under the prefix \"%s\"", *prefix);
		}
		return entry->second;
	}
	if (catalogs.size() != 1) {
		throw CatalogException(
		    "The server serves %llu catalogs: the URLs need the prefix of one of them (/v1/<prefix>/...)",
		    catalogs.size());
	}
	return catalogs.begin()->second;
}

DuckLakeRESTServer::~DuckLakeRESTServer() {
	Stop();
}

static int32_t StatusCodeForError(ExceptionType type) {
	switch (type) {
	case ExceptionType::TRANSACTION:
		return 409;
	case ExceptionType::CATALOG:
		return 404;
	case ExceptionType::INVALID_INPUT:
	case ExceptionType::BINDER:
	case ExceptionType::PARSER:
	case ExceptionType::CONVERSION:
	case ExceptionType::CONSTRAINT:
	case ExceptionType::INVALID_CONFIGURATION:
		return 400;
	case ExceptionType::NOT_IMPLEMENTED:
		return 501;
	default:
		return 500;
	}
}

static string ErrorBody(ExceptionType type, const string &message, int32_t status) {
	RESTErrorResponse response;
	response.error.type = Exception::ExceptionTypeToString(type);
	response.error.message = message;
	response.error.code = status;
	return DuckLakeRESTSerializer::ToJSON(response);
}

//! The number of commits that a test holds back until they all wait in the queue
static idx_t CommitHold(DatabaseInstance &db) {
	Value hold;
	db.TryGetCurrentSetting(Identifier("ducklake_debug_rest_commit_hold"), hold);
	return hold.IsNull() ? 0 : hold.GetValue<idx_t>();
}

int32_t DuckLakeRESTServer::HandleRequest(const RESTRoute &route, optional_ptr<const string> prefix,
                                          const vector<string> &path_values, const vector<pair<string, string>> &query,
                                          const string &body, const string &request_content_type, const string &accept,
                                          string &response, string &content_type) {
	content_type = "application/json";
	try {
		// the catalog of a request that finds the prefix is the one that it asks for, if it asks for one
		string requested_catalog;
		if (route.no_prefix) {
			for (auto &entry : query) {
				if (entry.first == "catalog" && !entry.second.empty()) {
					requested_catalog = entry.second;
				}
			}
			prefix = requested_catalog.empty() ? nullptr : optional_ptr<const string>(&requested_catalog);
		}
		auto served = GetCatalog(prefix);
		auto &endpoints = DuckLakeRESTAPI::GetEndpoints();
		auto entry = endpoints.find(route.endpoint);
		if (entry == endpoints.end()) {
			throw InternalException("DuckLake REST: the endpoint \"%s\" has no handler", route.endpoint);
		}
		string json_body = body;
		shared_ptr<const string> request_data;
		vector<RESTMultipartPart> request_parts;
		if (RESTMultipart::IsMultipart(request_content_type)) {
			// the JSON document is a part of the request, the other parts are tables
			request_data = make_shared_ptr<const string>(body);
			request_parts = RESTMultipart::Parse(*request_data, request_content_type);
			auto &json_part = RESTMultipart::Find(request_parts, "request");
			json_body = request_data->substr(json_part.offset, json_part.size);
		}
		auto request_json = route.ServerRequest(path_values, query, json_body);
		// lists are Arrow, unless the request asks for JSON only
		bool accepts_arrow =
		    accept.empty() || accept.find("multipart/mixed") != string::npos || accept.find("*/*") != string::npos;
		bool group_commit = route.endpoint == "group_commit_transaction";
		if (group_commit || route.endpoint == "commit_transaction") {
			// the commit runs after the queued ones, so that its metadata transaction sees what they published
			DuckLakeRESTPendingCommit commit;
			commit.endpoint = route.endpoint;
			commit.shares_snapshot = group_commit && DuckLakeRESTCommitGroup::IsAppendOnly(request_json);
			commit.request_json = std::move(request_json);
			commit.request_data = std::move(request_data);
			commit.request_parts = std::move(request_parts);
			commit.server = this;
			commit.prefix = served.prefix;
			commit.accepts_arrow = accepts_arrow;
			served.commit_queue->Submit(
			    commit, CommitHold(*served.db),
			    [&](vector<reference<DuckLakeRESTPendingCommit>> &commits) { RunCommits(served, commits); });
			if (commit.interrupted) {
				// LCOV_EXCL_START: only the failure of the queue itself interrupts a commit
				// an IO error, as for a lost response: the client must not remove the files of the transaction
				throw IOException("The commit did not run to its end, it is unknown whether it was published");
			} // LCOV_EXCL_STOP
			if (commit.error.HasError()) {
				auto status = StatusCodeForError(commit.error.Type());
				response = ErrorBody(commit.error.Type(), commit.error.RawMessage(), status);
				return status;
			}
			response = std::move(commit.response);
			content_type = commit.content_type;
			return 200;
		}
		DuckLakeRESTRequest request(*served.db, served.catalog_name);
		request.request_data = std::move(request_data);
		request.request_parts = std::move(request_parts);
		request.server = this;
		request.prefix = served.prefix;
		request.accepts_arrow = accepts_arrow;
		try {
			response = entry->second(request, request_json);
			content_type = request.content_type;
			if (request.connection.HasActiveTransaction()) {
				request.connection.Commit();
			}
		} catch (...) {
			if (request.connection.HasActiveTransaction()) {
				request.connection.Rollback();
			}
			throw;
		}
		return 200;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		auto status = StatusCodeForError(error.Type());
		response = ErrorBody(error.Type(), error.RawMessage(), status);
		return status;
	}
}

static void InitializeCommitRequest(DuckLakeRESTRequest &request, const DuckLakeRESTPendingCommit &commit) {
	request.request_data = commit.request_data;
	request.request_parts = commit.request_parts;
	request.server = commit.server;
	request.prefix = commit.prefix;
	request.accepts_arrow = commit.accepts_arrow;
}

//! The error of a commit that failed after it published its changes: an IO error, as for a lost response, so that
//! the client does not remove the files of the transaction
static ErrorData ErrorAfterPublication(const ErrorData &error) {
	return ErrorData(ExceptionType::IO,
	                 "The transaction was committed, but the server failed to respond: " + error.RawMessage());
}

//! The error of a group that can be published: an IO error as well, so that its clients keep their files
static ErrorData ErrorOfUnknownPublication(const ErrorData &error) {
	return ErrorData(ExceptionType::IO, "It is unknown whether the transaction was committed, its commit was sent "
	                                    "and failed without a rejection: " +
	                                        error.RawMessage());
}

void DuckLakeRESTServer::RunCommit(const ServedCatalog &served, DuckLakeRESTPendingCommit &commit) {
	auto outcome = DuckLakeCommitOutcome::NOT_PUBLISHED;
	try {
		auto endpoint = DuckLakeRESTAPI::GetEndpoints().at(commit.endpoint);
		DuckLakeRESTRequest request(*served.db, served.catalog_name);
		InitializeCommitRequest(request, commit);
		try {
			// the endpoint commits the transaction of the request
			commit.response = endpoint(request, commit.request_json);
			commit.content_type = request.content_type;
		} catch (...) {
			outcome = request.commit_outcome;
			if (request.connection.HasActiveTransaction()) {
				request.connection.Rollback();
			}
			throw;
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		// as for a group: a commit that is published, or that can be, fails with an IO error
		if (outcome == DuckLakeCommitOutcome::PUBLISHED) {
			commit.error = ErrorAfterPublication(error);
		} else if (outcome == DuckLakeCommitOutcome::PUBLICATION_UNKNOWN) {
			commit.error = ErrorOfUnknownPublication(error);
		} else {
			commit.error = error;
		}
	}
}

bool DuckLakeRESTServer::RunCommitGroup(const ServedCatalog &served,
                                        vector<reference<DuckLakeRESTPendingCommit>> &group) {
	auto outcome = DuckLakeCommitOutcome::NOT_PUBLISHED;
	string response;
	string content_type;
	ErrorData error;
	try {
		DuckLakeRESTRequest request(*served.db, served.catalog_name);
		InitializeCommitRequest(request, group[0].get());
		try {
			response = DuckLakeRESTCommitGroup::Commit(request, group);
			content_type = request.content_type;
		} catch (...) {
			outcome = request.commit_outcome;
			if (request.connection.HasActiveTransaction()) {
				request.connection.Rollback();
			}
			throw;
		}
	} catch (std::exception &ex) {
		if (outcome == DuckLakeCommitOutcome::NOT_PUBLISHED) {
			return false;
		}
		// the commits of a group that is published, or that can be, never run again
		ErrorData cause(ex);
		bool published = outcome == DuckLakeCommitOutcome::PUBLISHED;
		error = published ? ErrorAfterPublication(cause) : ErrorOfUnknownPublication(cause);
	}
	// a failure here does not reach the fallback either: the queue answers the commits that have no result
	for (auto &entry : group) {
		auto &commit = entry.get();
		if (error.HasError()) {
			commit.error = error;
			continue;
		}
		commit.response = response;
		commit.content_type = content_type;
	}
	return true;
}

void DuckLakeRESTServer::RunCommits(const ServedCatalog &served,
                                    vector<reference<DuckLakeRESTPendingCommit>> &commits) {
	if (commits.size() > 1 && RunCommitGroup(served, commits)) {
		return;
	}
	for (auto &commit : commits) {
		RunCommit(served, commit.get());
	}
}

idx_t DuckLakeRESTServer::Start(const string &host, idx_t port) {
	server = make_uniq<duckdb_httplib::Server>();
	server->set_tcp_nodelay(true);
	for (auto &route : DuckLakeRESTAPI::GetRoutes()) {
		for (bool with_prefix : {false, true}) {
			if (with_prefix && route.no_prefix) {
				continue;
			}
			auto handler = [this, &route, with_prefix](const duckdb_httplib::Request &req,
			                                           duckdb_httplib::Response &res) {
				string response;
				if (!token.empty() && req.get_header_value("Authorization") != "Bearer " + token) {
					res.status = 401;
					res.set_content(ErrorBody(ExceptionType::PERMISSION,
					                          "Missing or invalid token for the DuckLake REST server", 401),
					                "application/json");
					return;
				}
				// the first group is the prefix of the catalog, if the URL has one
				idx_t first_value = with_prefix ? 2 : 1;
				string prefix;
				if (with_prefix) {
					prefix = req.matches.groups[1].str();
				}
				vector<string> path_values;
				for (idx_t i = first_value; i < req.matches.groups.size(); i++) {
					path_values.push_back(req.matches.groups[i].str());
				}
				vector<pair<string, string>> query;
				for (auto &param : req.params) {
					query.emplace_back(param.first, param.second);
				}
				string content_type;
				res.status = HandleRequest(route, with_prefix ? optional_ptr<const string>(&prefix) : nullptr,
				                           path_values, query, req.body, req.get_header_value("Content-Type"),
				                           req.get_header_value("Accept"), response, content_type);
				res.set_content(response, content_type);
			};
			auto pattern = route.PathPattern(with_prefix);
			if (route.method == "GET") {
				server->Get(pattern, handler);
			} else {
				server->Post(pattern, handler);
			}
		}
	}
	server->set_error_handler([](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		if (res.body.empty()) {
			res.set_content(ErrorBody(ExceptionType::INVALID_INPUT, "Unknown DuckLake REST endpoint", res.status),
			                "application/json");
		}
	});
	int32_t bound_port;
	if (port == 0) {
		bound_port = server->bind_to_any_port(host);
	} else {
		bound_port = server->bind_to_port(host, NumericCast<int32_t>(port)) ? NumericCast<int32_t>(port) : -1;
	}
	if (bound_port < 0) {
		throw IOException("Failed to start DuckLake REST server on %s:%llu", host, port);
	}
	listen_uri = StringUtil::Format("http://%s:%d", host, bound_port);
	server_thread = std::thread([this]() { server->listen_after_bind(); });
	return NumericCast<idx_t>(bound_port);
}

void DuckLakeRESTServer::Stop() {
	if (server) {
		server->stop();
	}
	if (server_thread.joinable()) {
		server_thread.join();
	}
}

// LCOV_EXCL_START
// serving until the server stops blocks the caller
void DuckLakeRESTServer::Wait() {
	if (server_thread.joinable()) {
		server_thread.join();
	}
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Registry
//===--------------------------------------------------------------------===//
//! Attaches the DuckLake in a database of its own, so the served catalog is isolated from the caller
static shared_ptr<DatabaseInstance> OpenIsolatedDuckLake(ClientContext &context, const string &path,
                                                         const string &attach_options, const string &catalog_name) {
	// the database of the server uses as many threads as the database that starts it
	DBConfig config;
	config.SetOptionByName("threads",
	                       Value::BIGINT(NumericCast<int64_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())));
	DuckDB db(nullptr, &config);
	db.LoadStaticExtension<DucklakeExtension>();
	Connection con(db);
	auto query = StringUtil::Format("ATTACH %s AS %s", SQLString(path), SQLIdentifier(catalog_name));
	if (!attach_options.empty()) {
		query += " (" + attach_options + ")";
	}
	auto result = con.Query(query);
	result->ThrowIfError("Failed to attach the DuckLake served over REST: ");
	return db.instance;
}

DuckLakeRESTServerRegistry &DuckLakeRESTServerRegistry::Get() {
	// leaked on purpose - the servers have to outlive any static destruction order
	static auto registry = new DuckLakeRESTServerRegistry();
	return *registry;
}

//! The address of a URI of a server or of one of its catalogs: the scheme, host and port
static string ServerAddress(const string &uri) {
	auto scheme_end = uri.find("://");
	auto path_start = uri.find('/', scheme_end == string::npos ? 0 : scheme_end + 3);
	return path_start == string::npos ? uri : uri.substr(0, path_start);
}

//! The prefix of the catalog in a URI, empty if it has none
static string UriPrefix(const string &uri) {
	auto address = ServerAddress(uri);
	auto prefix = uri.substr(address.size());
	while (StringUtil::StartsWith(prefix, "/")) {
		prefix.erase(0, 1);
	}
	StringUtil::RTrim(prefix, "/");
	return prefix;
}

DuckLakeRESTServer &DuckLakeRESTServerRegistry::Start(unique_ptr<DuckLakeRESTServer> server, const string &host,
                                                      idx_t port) {
	server->Start(host, port);
	auto listen_uri = server->ListenURI();
	lock_guard<mutex> guard(lock);
	auto &result = *server;
	servers[listen_uri] = std::move(server);
	return result;
}

bool DuckLakeRESTServerRegistry::Stop(const string &uri) {
	auto prefix = UriPrefix(uri);
	if (!prefix.empty()) {
		// a catalog of the server
		auto server = GetServer(uri);
		return server && server->RemoveCatalog(prefix);
	}
	unique_ptr<DuckLakeRESTServer> server;
	{
		lock_guard<mutex> guard(lock);
		auto entry = servers.find(ServerAddress(uri));
		if (entry == servers.end()) {
			return false;
		}
		server = std::move(entry->second);
		servers.erase(entry);
	}
	server->Stop();
	return true;
}

shared_ptr<DuckLakeRESTCommitQueue> DuckLakeRESTServerRegistry::GetCommitQueue(DatabaseInstance &db,
                                                                               const string &catalog_name) {
	lock_guard<mutex> guard(lock);
	for (auto entry = commit_queues.begin(); entry != commit_queues.end();) {
		if (entry->second.expired()) {
			entry = commit_queues.erase(entry);
		} else {
			entry++;
		}
	}
	auto &entry = commit_queues[make_pair(&db, StringUtil::Lower(catalog_name))];
	auto queue = entry.lock();
	if (!queue) {
		queue = make_shared_ptr<DuckLakeRESTCommitQueue>();
		entry = queue;
	}
	return queue;
}

optional_ptr<DuckLakeRESTServer> DuckLakeRESTServerRegistry::GetServer(const string &uri) {
	lock_guard<mutex> guard(lock);
	auto entry = servers.find(ServerAddress(uri));
	if (entry == servers.end()) {
		return nullptr;
	}
	return entry->second.get();
}

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//
static Value GetNamedParameter(TableFunctionBindInput &input, const string &name) {
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end()) {
		return Value();
	}
	return entry->second;
}

//! The entries of a MAP(VARCHAR, VARCHAR) value
static map<string, string> ToStringMap(const Value &value) {
	map<string, string> result;
	if (value.IsNull()) {
		return result;
	}
	for (auto &entry : MapValue::GetChildren(value)) {
		auto &children = StructValue::GetChildren(entry);
		// a key of a map cannot be NULL
		if (children[1].IsNull()) {
			throw InvalidInputException("ducklake_rest_serve: the properties cannot be NULL");
		}
		result[StringValue::Get(children[0])] = StringValue::Get(children[1]);
	}
	return result;
}

static unique_ptr<FunctionData> DuckLakeRESTServeBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto target = input.inputs[0];
	if (target.IsNull()) {
		throw BinderException("ducklake_rest_serve: the catalog cannot be NULL");
	}
	auto target_str = StringValue::Get(target);
	auto host_val = GetNamedParameter(input, "host");
	auto port_val = GetNamedParameter(input, "port");
	auto options_val = GetNamedParameter(input, "attach_options");
	auto wait_val = GetNamedParameter(input, "wait");
	auto token_val = GetNamedParameter(input, "token");
	string host = host_val.IsNull() ? "localhost" : StringValue::Get(host_val);
	idx_t port = port_val.IsNull() ? 0 : port_val.GetValue<idx_t>();
	string attach_options = options_val.IsNull() ? string() : StringValue::Get(options_val);
	bool wait = !wait_val.IsNull() && BooleanValue::Get(wait_val);

	auto prefix_val = GetNamedParameter(input, "prefix");
	auto defaults_val = GetNamedParameter(input, "defaults");
	auto overrides_val = GetNamedParameter(input, "overrides");
	auto files_per_page_val = GetNamedParameter(input, "files_per_page");
	auto page_size_val = GetNamedParameter(input, "page_size");
	// the arguments are checked before a server starts
	if (!page_size_val.IsNull() && page_size_val.GetValue<idx_t>() == 0) {
		throw BinderException("ducklake_rest_serve: page_size has to be larger than 0");
	}
	if (!files_per_page_val.IsNull() && files_per_page_val.GetValue<idx_t>() == 0) {
		throw BinderException("ducklake_rest_serve: files_per_page has to be larger than 0");
	}

	auto &registry = DuckLakeRESTServerRegistry::Get();
	// a server that already listens on this address serves the catalog as well
	optional_ptr<DuckLakeRESTServer> server;
	if (port != 0) {
		server = registry.GetServer(StringUtil::Format("http://%s:%llu", host, port));
	}
	bool isolated = StringUtil::StartsWith(target_str, "ducklake:");
	const string catalog_name = isolated ? "ducklake_rest" : target_str;
	string prefix = prefix_val.IsNull() ? catalog_name : StringValue::Get(prefix_val);
	if (server) {
		// a catalog that is served under the prefix has to release its DuckLake first
		server->RemoveCatalog(prefix);
	}
	shared_ptr<DatabaseInstance> db;
	if (isolated) {
		db = OpenIsolatedDuckLake(context, target_str, attach_options, catalog_name);
	} else {
		auto &catalog = Catalog::GetCatalog(context, Identifier(target_str));
		if (catalog.GetCatalogType() != "ducklake") {
			throw BinderException("ducklake_rest_serve: catalog \"%s\" is not a DuckLake catalog", target_str);
		}
		db = context.db;
	}
	if (!server) {
		server = registry.Start(make_uniq<DuckLakeRESTServer>(), host, port);
	}
	if (!token_val.IsNull()) {
		server->SetToken(StringValue::Get(token_val));
	}
	if (!page_size_val.IsNull()) {
		server->SetDefaultPageSize(page_size_val.GetValue<idx_t>());
	}
	if (!files_per_page_val.IsNull()) {
		server->SetFilesPerPage(files_per_page_val.GetValue<idx_t>());
	}
	if (!defaults_val.IsNull() || !overrides_val.IsNull()) {
		server->SetClientProperties(ToStringMap(defaults_val), ToStringMap(overrides_val));
	}
	server->AddCatalog(prefix, std::move(db), catalog_name);
	auto listen_uri = server->ListenURI() + "/" + prefix;
	if (wait) {
		server->Wait(); // LCOV_EXCL_LINE
	}

	names.emplace_back("listen_uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back({Value(listen_uri)});
	return std::move(result);
}

static unique_ptr<FunctionData> DuckLakeRESTStopBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto uri = input.inputs[0];
	if (uri.IsNull()) {
		throw BinderException("ducklake_rest_stop: the listen URI cannot be NULL");
	}
	auto stopped = DuckLakeRESTServerRegistry::Get().Stop(StringValue::Get(uri));
	names.emplace_back("stopped");
	return_types.emplace_back(LogicalType::BOOLEAN);
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back({Value::BOOLEAN(stopped)});
	return std::move(result);
}

//! Reads a JSON response as the type that the API defines for it, so tests find responses that do not match the model
static void ValidateResponse(const string &method, const string &path, int32_t status, const string &content_type,
                             const string &response) {
	if (StringUtil::StartsWith(content_type, "multipart/")) {
		return;
	}
	auto route_path = path.substr(0, path.find('?'));
	for (auto &route : DuckLakeRESTAPI::GetRoutes()) {
		if (route.method != method ||
		    (!duckdb_re2::RE2::FullMatch(route_path, route.PathPattern(false)) &&
		     (route.no_prefix || !duckdb_re2::RE2::FullMatch(route_path, route.PathPattern(true))))) {
			continue;
		}
		try {
			if (response.empty()) {
				throw IOException("the response has no body");
			}
			if (status == 200) {
				DuckLakeRESTAPI::ValidateResponse(route.endpoint, response);
			} else {
				RESTJsonReader document(response);
				DuckLakeRESTSerializer::Read<RESTErrorResponse>(document.Root());
			}
		} catch (std::exception &ex) {
			// LCOV_EXCL_START
			// the server conforms to the API, this finds responses that do not
			throw IOException(
			    "ducklake_rest_request: the response of \"%s %s\" does not match the DuckLake REST API: %s", method,
			    route_path, ErrorData(ex).Message());
			// LCOV_EXCL_STOP
		}
		return;
	}
}

static unique_ptr<FunctionData> DuckLakeRESTRequestBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("ducklake_rest_request: the arguments cannot be NULL");
		}
	}
	auto token = GetNamedParameter(input, "token");
	DuckLakeRESTClient client(*context.db, StringValue::Get(input.inputs[0]),
	                          token.IsNull() ? string() : StringValue::Get(token));
	auto accept = GetNamedParameter(input, "accept");
	string response;
	string content_type;
	auto method = StringUtil::Upper(StringValue::Get(input.inputs[1]));
	auto &path = StringValue::Get(input.inputs[2]);
	auto status = client.SendRaw(method, path, StringValue::Get(input.inputs[3]), response, content_type,
	                             accept.IsNull() ? "application/json" : StringValue::Get(accept));
	ValidateResponse(method, path, status, content_type, response);
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("response");
	// multipart responses hold binary data
	bool binary = StringUtil::StartsWith(content_type, "multipart/");
	return_types.emplace_back(binary ? LogicalType::BLOB : LogicalType::JSON());
	names.emplace_back("content_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back(
	    {Value::INTEGER(status), binary ? Value::BLOB_RAW(response) : Value(response), Value(content_type)});
	return std::move(result);
}

static void DuckLakeRESTExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<MetadataBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	DuckLakeBaseMetadataFunction::ScanRows(data.rows, state.offset, output);
}

DuckLakeRESTServeFunction::DuckLakeRESTServeFunction()
    : TableFunction("ducklake_rest_serve", FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTServeBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("host", LogicalType::VARCHAR)
		    .Add("port", LogicalType::UBIGINT)
		    .Add("attach_options", LogicalType::VARCHAR)
		    .Add("wait", LogicalType::BOOLEAN)
		    .Add("token", LogicalType::VARCHAR)
		    .Add("prefix", LogicalType::VARCHAR)
		    .Add("files_per_page", LogicalType::UBIGINT)
		    .Add("page_size", LogicalType::UBIGINT)
		    .Add("defaults", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR))
		    .Add("overrides", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	});
}

DuckLakeRESTRequestFunction::DuckLakeRESTRequestFunction()
    : TableFunction("ducklake_rest_request",
                    FunctionSignature()
                        .AddPositionalOnly("listen_uri", LogicalType::VARCHAR)
                        .AddPositionalOnly("method", LogicalType::VARCHAR)
                        .AddPositionalOnly("path", LogicalType::VARCHAR)
                        .AddPositionalOnly("body", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTRequestBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("token", LogicalType::VARCHAR).Add("accept", LogicalType::VARCHAR);
	});
}

static unique_ptr<FunctionData> DuckLakeRESTRowsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("ducklake_rest_rows: the arguments cannot be NULL");
		}
	}
	auto token = GetNamedParameter(input, "token");
	auto part_name = GetNamedParameter(input, "part");
	DuckLakeRESTClient client(*context.db, StringValue::Get(input.inputs[0]),
	                          token.IsNull() ? string() : StringValue::Get(token));
	string response;
	string content_type;
	auto status =
	    client.SendRaw(StringUtil::Upper(StringValue::Get(input.inputs[1])), StringValue::Get(input.inputs[2]),
	                   StringValue::Get(input.inputs[3]), response, content_type, "multipart/mixed");
	if (!RESTMultipart::IsMultipart(content_type)) {
		throw InvalidInputException("ducklake_rest_rows: the response (status %d) has no tables: %s", status,
		                            response.substr(0, 200));
	}
	auto data = make_shared_ptr<const string>(std::move(response));
	auto parts = RESTMultipart::Parse(*data, content_type);
	auto &part = RESTMultipart::Find(parts, part_name.IsNull() ? "inlined_data" : StringValue::Get(part_name));
	vector<string> column_names;
	auto rows = RESTArrow::Read(context, data, part.offset, part.size, {}, column_names);
	for (idx_t i = 0; i < column_names.size(); i++) {
		names.emplace_back(column_names[i]);
		return_types.push_back(rows->Types()[i]);
	}
	auto result = make_uniq<MetadataBindData>();
	for (auto &chunk : rows->Chunks()) {
		for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
			vector<Value> row;
			for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
				row.push_back(chunk.GetValue(col_idx, row_idx));
			}
			result->rows.push_back(std::move(row));
		}
	}
	return std::move(result);
}

DuckLakeRESTRowsFunction::DuckLakeRESTRowsFunction()
    : TableFunction("ducklake_rest_rows",
                    FunctionSignature()
                        .AddPositionalOnly("listen_uri", LogicalType::VARCHAR)
                        .AddPositionalOnly("method", LogicalType::VARCHAR)
                        .AddPositionalOnly("path", LogicalType::VARCHAR)
                        .AddPositionalOnly("body", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTRowsBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("token", LogicalType::VARCHAR).Add("part", LogicalType::VARCHAR);
	});
}

DuckLakeRESTStopFunction::DuckLakeRESTStopFunction()
    : TableFunction("ducklake_rest_stop", FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTStopBind, DuckLakeRunOnceState::Init) {
}

} // namespace duckdb
