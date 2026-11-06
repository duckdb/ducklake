#include "rest/ducklake_rest_client.hpp"
#include "rest/ducklake_rest_multipart.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/exception.hpp"

#include "httplib.hpp"

namespace duckdb {

DuckLakeRESTClient::DuckLakeRESTClient(DatabaseInstance &db_p, string base_url_p, string token_p)
    : base_url(std::move(base_url_p)), token(std::move(token_p)), db(db_p.shared_from_this()) {
	StringUtil::RTrim(base_url, "/");
	// the path of the URL is the prefix of the catalog on the server
	auto scheme_end = base_url.find("://");
	auto path_start = base_url.find('/', scheme_end == string::npos ? 0 : scheme_end + 3);
	if (path_start != string::npos) {
		prefix = base_url.substr(path_start + 1);
		server_url = base_url.substr(0, path_start);
	} else {
		server_url = base_url;
	}
}

DuckLakeRESTClient::~DuckLakeRESTClient() {
}

shared_ptr<ClientContext> DuckLakeRESTClient::GetContext() {
	auto database = db.lock();
	if (!database) {
		throw InvalidInputException("The database of the DuckLake REST client is closed");
	}
	// the context stays valid when the connection is gone
	Connection connection(*database);
	return connection.context;
}

//! Servers close connections that are idle for a while (5 seconds for the reference server) - a connection that has
//! been idle for most of that time is not used anymore
static constexpr int64_t MAX_IDLE_SECONDS = 3;

unique_ptr<duckdb_httplib::Client> DuckLakeRESTClient::GetConnection(bool &reused) {
	{
		lock_guard<mutex> guard(lock);
		auto now = std::chrono::steady_clock::now();
		while (!idle_connections.empty()) {
			auto idle = std::move(idle_connections.back());
			idle_connections.pop_back();
			if (now - idle.idle_since > std::chrono::seconds(MAX_IDLE_SECONDS)) {
				// the older connections have been idle for longer
				idle_connections.clear();
				break;
			}
			reused = true;
			return std::move(idle.connection);
		}
	}
	reused = false;
	auto connection = make_uniq<duckdb_httplib::Client>(server_url);
	connection->set_keep_alive(true);
	connection->set_tcp_nodelay(true);
	connection->set_read_timeout(300, 0);
	connection->set_write_timeout(300, 0);
	return connection;
}

void DuckLakeRESTClient::ReturnConnection(unique_ptr<duckdb_httplib::Client> connection) {
	lock_guard<mutex> guard(lock);
	idle_connections.push_back({std::move(connection), std::chrono::steady_clock::now()});
}

int32_t DuckLakeRESTClient::Send(const string &method, const string &target, const string &body, bool idempotent,
                                 string &response, string &content_type, const string &accept,
                                 const string &request_content_type) {
	duckdb_httplib::Headers headers;
	if (!accept.empty()) {
		headers.emplace("Accept", accept);
	}
	if (!token.empty()) {
		headers.emplace("Authorization", "Bearer " + token);
	}
	for (idx_t attempt = 0;; attempt++) {
		bool reused;
		auto connection = GetConnection(reused);
		auto result = method == "GET" ? connection->Get(target, headers)
		                              : connection->Post(target, headers, body, request_content_type);
		if (result) {
			ReturnConnection(std::move(connection));
			response = std::move(result->body);
			content_type = result->get_header_value("Content-Type");
			return result->status;
		}
		auto error = result.error();
		bool closed = error == duckdb_httplib::Error::Read || error == duckdb_httplib::Error::Write ||
		              error == duckdb_httplib::Error::Connection;
		if (attempt == 0 && reused && closed && idempotent) {
			// the server closed the connection while it was idle: the request is sent again on a new connection. Other
			// requests are not, as the server might have applied them
			continue;
		}
		throw IOException("Failed to send DuckLake REST request \"%s %s\" to %s: %s", method, target, server_url,
		                  duckdb_httplib::to_string(error));
	}
}

int32_t DuckLakeRESTClient::SendRaw(const string &method, const string &target, const string &body, string &response,
                                    string &content_type, const string &accept) {
	return Send(method, target, body, false, response, content_type, accept, "application/json");
}

void DuckLakeRESTClient::SetPathMapping(string client_path, string server_path) {
	path_mapper = make_uniq<RESTPathMapper>(std::move(client_path), std::move(server_path));
}

void DuckLakeRESTClient::Discover(const string &catalog_name) {
	RESTConfigRequest request;
	// the name in the URL is the name of the catalog if there is none
	request.catalog_name = catalog_name.empty() ? prefix : catalog_name;
	auto response = Config(request);
	defaults = std::move(response.defaults);
	overrides = std::move(response.overrides);
	endpoints.clear();
	for (auto &route : response.endpoints) {
		endpoints.insert(route);
	}
	prefix = GetProperty("prefix", prefix);
}

string DuckLakeRESTClient::GetProperty(const string &name, const string &client_value) const {
	auto override_entry = overrides.find(name);
	if (override_entry != overrides.end()) {
		return override_entry->second;
	}
	if (!client_value.empty()) {
		return client_value;
	}
	auto default_entry = defaults.find(name);
	return default_entry != defaults.end() ? default_entry->second : string();
}

RESTHttpResponse DuckLakeRESTClient::Call(const string &endpoint, const string &json, bool idempotent,
                                          const vector<RESTBodyPart> &parts) {
	auto &route = DuckLakeRESTAPI::GetRoute(endpoint);
	if (!endpoints.empty() && endpoints.find(route.method + " " + route.path) == endpoints.end()) {
		throw NotImplementedException("The DuckLake REST server does not support \"%s %s\"", route.method, route.path);
	}
	string body;
	auto target = route.ClientTarget(json, prefix, body);
	RESTHttpResponse result;
	auto &response = result.body;
	string request_content_type = "application/json";
	if (!parts.empty()) {
		// the tables of the request are parts besides its JSON document
		RESTMultipartWriter multipart;
		multipart.AddPart("request", "application/json", body);
		for (auto &part : parts) {
			multipart.AddPart(part.name, REST_ARROW_STREAM, part.content);
		}
		body = multipart.Finish(request_content_type);
	}
	auto status = Send(route.method, target, body, idempotent, response, result.content_type,
	                   "multipart/mixed, application/json;q=0.9", request_content_type);
	if (status != 200) {
		RESTErrorResponse error;
		try {
			RESTJsonReader document(response);
			error = DuckLakeRESTSerializer::Read<RESTErrorResponse>(document.Root());
		} catch (std::exception &) {
			throw IOException("DuckLake REST request \"%s %s\" failed with status %d", route.method, target, status);
		}
		auto type = Exception::StringToExceptionType(error.error.type);
		ErrorData(type, error.error.message).Throw();
	}
	return result;
}

} // namespace duckdb
