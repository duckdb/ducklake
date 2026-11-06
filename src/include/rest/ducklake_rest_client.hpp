//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_client.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/connection.hpp"
#include "rest/ducklake_rest_api.hpp"

#include <chrono>

namespace duckdb_httplib {
class Client;
} // namespace duckdb_httplib

namespace duckdb {

//! Sends requests to a DuckLake REST server, the connections to the server are pooled
class DuckLakeRESTClient : public DuckLakeRESTClientAPI {
public:
	//! Requests carry the token as a bearer token, if it is set
	DuckLakeRESTClient(DatabaseInstance &db, string base_url, string token);
	~DuckLakeRESTClient() override;

	RESTHttpResponse Call(const string &endpoint, const string &json, bool idempotent,
	                      const vector<RESTBodyPart> &parts) override;
	shared_ptr<ClientContext> GetContext() override;

	//! Asks the server how to connect to a catalog: the properties of the server are merged with the properties of the
	//! client, which finds the prefix of the catalog. The catalog is the one the URL names, if the name is empty
	void Discover(const string &catalog_name);
	//! The value of a property: the value of the server if it overrides it, the client value if it has one, else the
	//! default
	string GetProperty(const string &name, const string &client_value = string()) const;
	const string &Prefix() const {
		return prefix;
	}

	//! The client has another view of the data path than the server: paths are mapped between the two
	void SetPathMapping(string client_path, string server_path);
	optional_ptr<const RESTPathMapper> GetPathMapper() const override {
		return path_mapper.get();
	}
	//! Sends an HTTP request, returns the status code, and the body and the content type of the response. The request
	//! has the accept header, if it is set
	int32_t SendRaw(const string &method, const string &target, const string &body, string &response,
	                string &content_type, const string &accept = string());

	const string &BaseURL() const {
		return base_url;
	}

	//! The schema version and data version (next file id) of the latest catalog and statistics the client loaded
	atomic<idx_t> loaded_metadata_version {DConstants::INVALID_INDEX};
	atomic<idx_t> loaded_data_version {DConstants::INVALID_INDEX};

private:
	//! A connection that was used before and is kept for later requests
	struct IdleConnection {
		unique_ptr<duckdb_httplib::Client> connection;
		std::chrono::steady_clock::time_point idle_since;
	};

	//! Returns an idle connection, if there is one that the server has not closed yet, or a new connection
	unique_ptr<duckdb_httplib::Client> GetConnection(bool &reused);
	void ReturnConnection(unique_ptr<duckdb_httplib::Client> connection);
	//! Sends a request, returns the status code and the body of the response
	int32_t Send(const string &method, const string &target, const string &body, bool idempotent, string &response,
	             string &content_type, const string &accept, const string &request_content_type);

private:
	unique_ptr<RESTPathMapper> path_mapper;
	//! The requests that the server supports, as the method and the path, unknown before Discover
	unordered_set<string> endpoints;
	map<string, string> defaults;
	map<string, string> overrides;
	//! The URL of the server and the prefix of the catalog on the server, which is the path of the URL
	string base_url;
	string server_url;
	string prefix;
	string token;
	//! The database for the contexts of the conversions of Arrow. A context holds its database, so the client, which
	//! the database owns, cannot hold a context
	weak_ptr<DatabaseInstance> db;
	mutex lock;
	vector<IdleConnection> idle_connections;
};

} // namespace duckdb
