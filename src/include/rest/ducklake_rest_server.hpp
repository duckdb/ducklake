//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_server.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "rest/ducklake_rest_api.hpp"
#include "rest/ducklake_rest_multipart.hpp"

#include <chrono>
#include <thread>

namespace duckdb_httplib {
class Server;
} // namespace duckdb_httplib

namespace duckdb {
class DuckLakeCatalog;
class DuckLakeTransaction;
class DuckLakeMetadataManager;
class DuckLakeRESTServer;
struct DuckLakeSnapshot;

//! The state of a single REST request, which runs in its own transaction on the served catalog
class DuckLakeRESTRequest {
public:
	DuckLakeRESTRequest(DatabaseInstance &db, const string &catalog_name);

	DuckLakeMetadataManager &GetMetadataManager();
	//! The snapshot of a request: the snapshot, the snapshot with the id, or the latest snapshot if both are absent
	DuckLakeSnapshot GetSnapshot(const DuckLakeSnapshot &snapshot, optional_idx snapshot_id);
	DuckLakeSnapshot GetSnapshot(const RESTSnapshotReference &reference);
	DuckLakeSnapshot GetSnapshotById(idx_t snapshot_id);

	Connection connection;
	DuckLakeCatalog *catalog;
	DuckLakeTransaction *transaction;
	//! The server and the prefix of the catalog that the request is for
	optional_ptr<DuckLakeRESTServer> server;
	string prefix;
	//! Whether the lists of the response are Arrow IPC streams in a multipart response, rather than JSON
	bool accepts_arrow = false;
	//! The parts of the request that are tables, if it is a multipart request, and the data they are regions of
	vector<RESTMultipartPart> request_parts;
	shared_ptr<const string> request_data;
	//! The content type of the response, set by the endpoint if it is not JSON
	string content_type = "application/json";
};

//! The pages of the scans that were split into pages, which the server keeps for a while
struct DuckLakeRESTPlan {
	//! The catalog and the table of the scan
	string prefix;
	idx_t table_id = 0;
	//! The number of entries of a page
	idx_t page_size = 1;
	//! The column mappings that the client of the scan has
	set<idx_t> known_mappings;
	//! The entries that are not in the response of the scan, whichever the scan returns
	vector<DuckLakeFileListEntry> files;
	vector<DuckLakeFileListExtendedEntry> extended_files;
	vector<DuckLakeFileListEntry> insertions;
	vector<DuckLakeDeleteScanEntry> deletions;
	std::chrono::steady_clock::time_point created;
};

class DuckLakeRESTPlans {
public:
	//! Keeps a plan, returns its id
	string Add(shared_ptr<DuckLakeRESTPlan> plan);
	//! The plan with the id, nullptr if it expired
	shared_ptr<DuckLakeRESTPlan> Get(const string &id);

private:
	mutex lock;
	map<string, shared_ptr<DuckLakeRESTPlan>> plans;
};

//! An HTTP server that exposes DuckLake catalogs through the DuckLake REST API. The URLs of a catalog have its prefix
//! after the version (`/v1/<prefix>/...`). The URLs without a prefix refer to the catalog that the server serves, if it
//! serves exactly one.
class DuckLakeRESTServer {
public:
	DuckLakeRESTServer();
	~DuckLakeRESTServer();

	//! Serves a catalog under the prefix, replacing the catalog that was served under it
	void AddCatalog(const string &prefix, shared_ptr<DatabaseInstance> db, string catalog_name);
	//! Stops serving the catalog under the prefix, returns whether there was one
	bool RemoveCatalog(const string &prefix);
	//! The number of files of the first response of a scan, and of every other page of the scan
	idx_t FilesPerPage() const {
		return files_per_page;
	}
	void SetFilesPerPage(idx_t files_per_page_p) {
		files_per_page = files_per_page_p;
	}
	//! The number of entries of a page if the request does not ask for a number
	idx_t DefaultPageSize() const {
		return default_page_size;
	}
	void SetDefaultPageSize(idx_t page_size) {
		default_page_size = page_size;
	}
	DuckLakeRESTPlans &Plans() {
		return plans;
	}

	//! The properties that clients get from the server, besides the prefix of their catalog
	void SetClientProperties(map<string, string> defaults, map<string, string> overrides);
	const map<string, string> &ClientDefaults() const {
		return client_defaults;
	}
	const map<string, string> &ClientOverrides() const {
		return client_overrides;
	}

	//! Binds the socket and starts serving in the background, returns the bound port
	idx_t Start(const string &host, idx_t port, idx_t thread_count = 0);
	//! Requests have to carry the token as a bearer token, if it is set
	void SetToken(string token_p) {
		token = std::move(token_p);
	}
	void Stop();
	//! Blocks until the server is stopped
	void Wait();

	const string &ListenURI() const {
		return listen_uri;
	}

	//! Handles a request for an endpoint of a catalog, returns the HTTP status code. The prefix is only set for URLs
	//! with a prefix
	int32_t HandleRequest(const RESTRoute &route, optional_ptr<const string> prefix, const vector<string> &path_values,
	                      const vector<pair<string, string>> &query, const string &body,
	                      const string &request_content_type, const string &accept, string &response,
	                      string &content_type);

private:
	struct ServedCatalog {
		string prefix;
		shared_ptr<DatabaseInstance> db;
		string catalog_name;
	};
	//! The catalog of a request, throws if there is none
	ServedCatalog GetCatalog(optional_ptr<const string> prefix);

private:
	mutex catalog_lock;
	map<string, ServedCatalog> catalogs;
	idx_t files_per_page = 10000;
	idx_t default_page_size = 1000;
	DuckLakeRESTPlans plans;
	map<string, string> client_defaults;
	map<string, string> client_overrides;
	string token;
	unique_ptr<duckdb_httplib::Server> server;
	std::thread server_thread;
	string listen_uri;
};

//! The REST servers running in this process, by listen URI
class DuckLakeRESTServerRegistry {
public:
	static DuckLakeRESTServerRegistry &Get();

	//! Starts a server, which serves no catalogs yet
	DuckLakeRESTServer &Start(unique_ptr<DuckLakeRESTServer> server, const string &host, idx_t port);
	//! Stops the server, or stops serving a catalog if the URI has a prefix
	bool Stop(const string &listen_uri);
	//! The server that listens on the address of a URI, which can have a prefix
	optional_ptr<DuckLakeRESTServer> GetServer(const string &listen_uri);

private:
	mutex lock;
	unordered_map<string, unique_ptr<DuckLakeRESTServer>> servers;
};

struct DuckLakeRESTServeFunction : public TableFunction {
	DuckLakeRESTServeFunction();
};

//! Sends a request to a DuckLake REST server and returns the rows of a table in its response
struct DuckLakeRESTRowsFunction : public TableFunction {
	DuckLakeRESTRowsFunction();
};

struct DuckLakeRESTStopFunction : public TableFunction {
	DuckLakeRESTStopFunction();
};

//! Sends a request to a DuckLake REST server, e.g. to explore the API
struct DuckLakeRESTRequestFunction : public TableFunction {
	DuckLakeRESTRequestFunction();
};

} // namespace duckdb
