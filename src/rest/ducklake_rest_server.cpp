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

namespace duckdb {

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
}

DuckLakeMetadataManager &DuckLakeRESTRequest::GetMetadataManager() {
	return transaction->GetMetadataManager();
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
	lock_guard<mutex> guard(catalog_lock);
	catalogs[prefix] = ServedCatalog {prefix, std::move(db), std::move(catalog_name)};
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
			entry = plans.erase(entry);
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
		plans.erase(entry);
		return nullptr;
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
		DuckLakeRESTRequest request(*served.db, served.catalog_name);
		string json_body = body;
		if (RESTMultipart::IsMultipart(request_content_type)) {
			// the JSON document is a part of the request, the other parts are tables
			request.request_data = make_shared_ptr<const string>(body);
			request.request_parts = RESTMultipart::Parse(*request.request_data, request_content_type);
			auto &json_part = RESTMultipart::Find(request.request_parts, "request");
			json_body = request.request_data->substr(json_part.offset, json_part.size);
		}
		auto request_json = route.ServerRequest(path_values, query, json_body);
		request.server = this;
		request.prefix = served.prefix;
		// lists are Arrow, unless the request asks for JSON only
		request.accepts_arrow =
		    accept.empty() || accept.find("multipart/mixed") != string::npos || accept.find("*/*") != string::npos;
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

idx_t DuckLakeRESTServer::Start(const string &host, idx_t port, idx_t thread_count) {
	server = make_uniq<duckdb_httplib::Server>();
	server->set_tcp_nodelay(true);
	if (thread_count > 0) {
		server->new_task_queue = [thread_count]() {
			return new duckdb_httplib::ThreadPool(thread_count);
		};
	}
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

void DuckLakeRESTServer::Wait() {
	if (server_thread.joinable()) {
		server_thread.join();
	}
}

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
		if (children[0].IsNull() || children[1].IsNull()) {
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
		auto page_size = page_size_val.GetValue<idx_t>();
		if (page_size == 0) {
			throw BinderException("ducklake_rest_serve: page_size has to be larger than 0");
		}
		server->SetDefaultPageSize(page_size);
	}
	if (!files_per_page_val.IsNull()) {
		auto files_per_page = files_per_page_val.GetValue<idx_t>();
		if (files_per_page == 0) {
			throw BinderException("ducklake_rest_serve: files_per_page has to be larger than 0");
		}
		server->SetFilesPerPage(files_per_page);
	}
	if (!defaults_val.IsNull() || !overrides_val.IsNull()) {
		server->SetClientProperties(ToStringMap(defaults_val), ToStringMap(overrides_val));
	}
	server->AddCatalog(prefix, std::move(db), catalog_name);
	auto listen_uri = server->ListenURI() + "/" + prefix;
	if (wait) {
		server->Wait();
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
	auto status = client.SendRaw(StringUtil::Upper(StringValue::Get(input.inputs[1])),
	                             StringValue::Get(input.inputs[2]), StringValue::Get(input.inputs[3]), response,
	                             content_type, accept.IsNull() ? "application/json" : StringValue::Get(accept));
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
