# DuckLake REST API

The DuckLake REST API exposes a DuckLake catalog over HTTP. Clients read the catalog and commit transactions through
a small set of structured endpoints, without sending SQL to the server.

This document describes the API as implemented by the reference server in this repository
(`ducklake_rest_serve`) and the client (`RESTMetadataManager`, used by `ATTACH 'ducklake:rest:<url>'`).

## Usage

### Starting a server

```sql
-- serve an attached DuckLake catalog
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data/');
SELECT listen_uri FROM ducklake_rest_serve('lake', host := 'localhost', port := 8181);

-- or attach the DuckLake in a database of its own, isolated from the caller
SELECT listen_uri FROM ducklake_rest_serve('ducklake:metadata.ducklake', port := 8181,
                                           attach_options := 'DATA_PATH ''data/''');
```

| Parameter        | Description                                                                              |
|------------------|------------------------------------------------------------------------------------------|
| `catalog`        | The name of an attached DuckLake catalog, or a `ducklake:` path to attach in isolation.  |
| `host`           | The host to listen on (default `localhost`).                                             |
| `port`           | The port to listen on, `0` (the default) picks a free port.                             |
| `attach_options` | Options for the isolated `ATTACH`, e.g. `DATA_PATH '...'`.                               |
| `wait`           | Block until the server is stopped, e.g. to run a server from the CLI.                    |
| `token`          | Require requests to carry this token (`Authorization: Bearer <token>`).                  |
| `prefix`         | The prefix of the catalog in the URLs of the server (default: the name of the catalog).  |
| `files_per_page` | The number of files in a response of a scan and in each of its pages (default 10000).   |
| `page_size`      | The number of entries of a page of a list if the request has no `page_size` (default 1000). |
| `defaults`       | A `MAP` of the properties that clients use unless they have a value of their own.        |
| `overrides`      | A `MAP` of the properties that clients use instead of their own values.                  |

The function returns the `listen_uri` of the catalog: the address of the server and the prefix of the catalog
(`http://localhost:8181/lake`). A server can serve several catalogs: serving a catalog on the port of a server of the same
process adds it to that server, under its own prefix. Serving a catalog under a prefix that is served already replaces the
catalog. `ducklake_rest_stop` stops serving the catalog of a `listen_uri` with a prefix, and stops the server for a URI
without one (`http://localhost:8181`).

`ducklake_rest_request(listen_uri, method, path, body)` sends an HTTP request to a server and returns the status code,
the response and its content type, which is useful to explore the API. The request asks for JSON; the `accept` option sets
another `Accept` header, e.g. `multipart/mixed` for the [Arrow](#arrow) responses, which are a `BLOB`:

```sql
SELECT response FROM ducklake_rest_request('http://localhost:8181', 'GET', '/v1/snapshots?exclude_latest=true', '');
SELECT response FROM ducklake_rest_request('http://localhost:8181', 'GET', '/v1/snapshots/latest', '');
```

### Connecting to a server

```sql
ATTACH 'ducklake:rest:http://localhost:8181/lake' AS lake;
```

The client starts with a request to `GET /v1/config`, which is never prefixed, and which names the catalog it wants:
the `catalog` of the request is the `META_CATALOG` option of the `ATTACH` (`ATTACH 'ducklake:rest:http://localhost:8181'
AS lake (META_CATALOG 'sales')`), or the path of the URL, which is the name that the catalog is served under. A URL
without a path and without `META_CATALOG` connects to the catalog of a server that serves exactly one catalog.

The response of the server has the `prefix` of the catalog in its `overrides`, which the client puts in the URLs of all
other requests (`/v1/<prefix>/...`). The other `defaults` and `overrides` are properties for the client: the client uses a
value of its own over a default, and an override over its own value. The server sets them with the `defaults` and
`overrides` of `ducklake_rest_serve`. The `endpoints` of the response are the requests that the server supports: the
client reports requests that the server does not support, instead of sending them.

`ducklake:rest:` only connects to a server that is already running (see [Starting a server](#starting-a-server)): it never
starts one.

A server that requires a token is attached with `ATTACH 'ducklake:rest:http://localhost:8181' AS lake (META_TOKEN '...')`.

The client reads the data path and the options of the DuckLake from the server. A `DATA_PATH` in the `ATTACH` of the
client is the view of the client of the data path, e.g. when it has mounted the data path somewhere else: the client
maps the paths between its data path and the data path of the server (`DataPath` fields below), so the server only ever
sees its own data path. Paths outside of the data path are not mapped. Data files are read and written by the
client directly, so the data path has to be accessible from both the client and the server.

## Conventions

* All endpoints live under `/v1/`, followed by the prefix of the catalog if there is one: `/v1/<prefix>/tables/{table_id}/scan`
  is the scan of a table of the catalog with that prefix. The paths below are the ones without a prefix, which a server
  that serves one catalog accepts as well, except for `config`, which never has a prefix. A server that serves several catalogs answers requests without a prefix, or
  with a prefix of no catalog, with a `404`. The resources a request refers to are in the path (`/v1/tables/{table_id}/scan`),
  reads without further input are `GET` requests whose filters are query parameters, and the other requests are `POST`
  requests with a JSON object as body (reads that take a snapshot, filters or projections, and `POST
  /v1/transactions/commit`). Responses are JSON objects. The parameters of the URL are not part of the body.
* Each request runs in a transaction of its own on the server.
* Lists that grow with the data are returned in pages: `snapshots` takes a `page_size` and a `page_token`, and returns a
  `next_page_token` if there are more entries. The first page has no token, a page token is only valid for the request that returned it, and the server
  picks the number of entries (and can return fewer than the `page_size`, and at most 10000). A response without
  `next_page_token` is the last page.
* The files of `scan` and `changes` are split into pages if there are many. The response has the first page of files and
  `page_tokens`, the token of every other page. A request with a `page_token` returns that page, and has no other
  members besides the table: the request is either the request for the files, or a page token. A client can request the
  pages in parallel and in any order. The server keeps the pages of a scan for a few minutes: after that (or on another
  server) the request fails with a `404`, and a client plans the scan again.

### Types

Types are structured JSON objects. `id` is the name of the type in the
[DuckLake specification](https://ducklake.select/docs/stable/specification/data_types) (e.g. `int32`, `varchar`,
`timestamptz`, `json`); nested and parameterized types have additional fields:

```json
{"id": "int32"}
{"id": "decimal", "width": 18, "scale": 3}
{"id": "list", "child": {"id": "varchar"}}
{"id": "map", "key": {"id": "varchar"}, "value": {"id": "int64"}}
{"id": "struct", "children": [{"name": "a", "type": {"id": "int32"}}]}
{"id": "geometry", "crs": "EPSG:4326"}
{"id": "json"}
```

### Arrow

Rows are never JSON: the rows of tables (the inlined rows of a scan or of the changes of a table, and the rows to insert
in a commit) and the lists of files are the bulk of the data that the API sends, and they are Arrow. A message with
tables is a `multipart/mixed` message, in a response or in a request:

- the first part is the JSON document of the message. In a response it is named `response`, and its lists of files
  (`files`, `extended_files`, `insertions` and `deletions`) are empty. In a request it is named `request`.
- the other parts are the tables, each an [Arrow IPC stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
  (`application/vnd.apache.arrow.stream`). The parts of a response are named after the member of the response: `files`,
  `extended_files`, `insertions`, `deletions`, and `inlined_data` for the inlined rows. The rows of an `insert_rows`
  request of a commit are the part `rows.<n>`, where `<n>` is the position of the request in `requests`.

Responses without tables (e.g. only `aggregate_results` or `page_tokens`) are JSON. A request that only accepts JSON
(`Accept: application/json`) gets the lists of files in JSON, and no inlined rows, which is meant for exploring the API.
`ducklake_rest_rows(listen_uri, method, path, body, part := 'inlined_data')` sends a request and returns the rows of one
part of the response:

```sql
SELECT * FROM ducklake_rest_rows('http://localhost:8181', 'POST', '/v1/tables/1/scan', '{}');
SELECT * FROM ducklake_rest_rows('http://localhost:8181', 'POST', '/v1/tables/1/scan', '{}', part := 'files');
```

The tables of files have one row per entry of the list, and a column for each member of the entry, with the name of the
member. Members that have their default value are `NULL`, and the members of a file (`file`, `delete_file`,
`previous_delete_file`) are structs with the members `path`, `encryption_key`, `file_size_bytes`, `footer_size` and
`format`. `column_min_max` is a list of structs with the members `column_id`, `min`, `max` and `contains_nan`, and
`inlined_file_deletions` of a deletion is a list of structs with the members `row_id` and `snapshot_id`. Lists that are
empty are `NULL`.

The table of `inlined_data` has the row id, the begin and the end snapshot of the rows, and the columns of the table,
with the Arrow types of their DuckLake types. Types that Arrow does not have are
[extension types](https://arrow.apache.org/docs/format/Columnar.html#extension-types) of the Arrow implementation of
DuckDB (e.g. `uuid`, `int128`, `timetz`, `geometry`, `variant`). The table is part of the response if the table has
inlined data, also if it has no rows. The table of an `insert_rows` request has the columns of the table.

## Specification

The API is specified as a [Smithy](https://smithy.io) model in `src/rest/api/*.smithy`: the `DuckLake` service, an
operation per endpoint (with `@http`, `@httpLabel` and `@httpQuery` for the URL) and the shapes they exchange. The reference below, the request and
response structures of the server and the client, and their JSON conversions are generated from it with
`python3 scripts/generate_rest_api.py`, which reads the model with the
[Smithy CLI](https://smithy.io/2.0/guides/smithy-cli/cli_installation.html) (`smithy` on the `PATH`, or `SMITHY`).
`--check` verifies that the generated files are up to date. To change the API, change the model and run the script.

Operations that are `@readonly` read the catalog, the members of the `CommitRequest` union are the requests of a commit
(`commit_transaction`). Maintenance (expiring snapshots and deleting files) is not part of the API: it is performed on the
server, by running the maintenance functions on the DuckLake that the server serves. The traits of `src/rest/api/codegen.smithy`
bind shapes to the C++ classes of DuckLake; they do not change the JSON of the API. Members are encoded as follows:

| Type           | Smithy shape     | JSON                                                                         |
|----------------|------------------|------------------------------------------------------------------------------|
| `string`       | `String`         | a string                                                                     |
| `bool`         | `Boolean`        | a boolean                                                                    |
| `uint64`       | `UInt64`         | a non-negative integer                                                       |
| `int64`        | `Long`           | an integer                                                                   |
| `double`       | `Double`         | a number                                                                     |
| `path`         | `DataPath`       | a path of a file or directory, which clients map between their data path and the data path of the server |
| `timestamp`    | `Timestamp`      | a timestamp with time zone as a string, e.g. `"2025-01-01 00:00:00+00"`      |
| `text`         | `Text`           | a value as a string, in the [text format of DuckLake](https://ducklake.select/docs/stable/specification/data_types#type-encoding-for-statistics); the member is absent for `NULL` |
| `logical_type` | `LogicalType`    | a [type](#types)                                                             |
| `json`         | `Document`       | any JSON value, specified by the description                                 |
| `list of T`    | `list`           | an array                                                                     |
| `object of T`  | `map`            | an object with arbitrary keys                                                |

Members that are not `@required` and have no default are optional: they can be `null` or absent. Unions, like
`CommitRequest`, are objects with exactly one member, e.g. `{"create_table": {...}}`.

<!-- BEGIN GENERATED REFERENCE: edit src/rest/api/*.smithy and run scripts/generate_rest_api.py -->

## Read Endpoints

These endpoints read the catalog. Each request runs in a transaction of its own on the server.

### `config`

Returns how a client connects to a catalog of the server. This is the first request of a client.

The client merges the properties of the response with its own: the `defaults` are used unless the client has a
value of its own, and the `overrides` are used whatever the client has. The `prefix` override is the prefix of the
catalog in the URLs of all other requests. The `endpoints` are the requests that the server supports, as the method and
the path without prefix (e.g. `GET /v1/snapshots`).

`GET /v1/config`

Parameters in the URL (the other fields of the request are the JSON body):

| Parameter | In | Type | Description |
|-----------|----|------|-------------|
| `catalog` | query | `string` | The name that the catalog is served under. The catalog of the server if it serves exactly one. |

Request: no body.

Response:

| Field | Type | Description |
|-------|------|-------------|
| `defaults` | object of `string` (default `{}`) | Properties that the client uses if it has no value for them. |
| `overrides` | object of `string` (default `{}`) | Properties that the client uses instead of its own values, e.g. `prefix`. |
| `endpoints` | list of `string` (default `[]`) | The requests that the server supports. |

### `get_latest_snapshot`

Returns the latest snapshot together with (optionally) the catalog and the table statistics of that snapshot.

Clients pass `cached_metadata_version` and `cached_data_version` when they start a transaction, so they get the
latest snapshot and, if it changed since, its catalog and statistics in a single round-trip.

`GET /v1/snapshots/latest`

Parameters in the URL (the other fields of the request are the JSON body):

| Parameter | In | Type | Description |
|-----------|----|------|-------------|
| `include_catalog` | query | `bool` | Return the catalog of the snapshot. |
| `include_stats` | query | `bool` | Return the table statistics of the snapshot. |
| `cached_metadata_version` | query | `uint64` | The schema version of the latest catalog the client has loaded. If the snapshot has a different schema version, its catalog is returned. |
| `cached_data_version` | query | `uint64` | The next file id of the snapshot of the latest statistics the client has loaded. If the snapshot has a different next file id, its statistics are returned. |

Request: no body.

Response: [`SnapshotData`](#snapshotdata).

### `get_metadata`

Returns the global metadata of the DuckLake (the contents of `ducklake_metadata`).

The `data_path` is the resolved data path of the served DuckLake.

`GET /v1/metadata`

Request: no body.

Response: [`Metadata`](#metadata).

### `get_snapshot_at`

Returns the snapshot of an `AT` clause together with (optionally) the catalog and the table statistics of that
snapshot. The request fails if there is no such snapshot, e.g. because it expired.

`GET /v1/snapshots/at`

Parameters in the URL (the other fields of the request are the JSON body):

| Parameter | In | Type | Description |
|-----------|----|------|-------------|
| `include_catalog` | query | `bool` | Return the catalog of the snapshot. |
| `include_stats` | query | `bool` | Return the table statistics of the snapshot. |
| `cached_metadata_version` | query | `uint64` | The schema version of the latest catalog the client has loaded. If the snapshot has a different schema version, its catalog is returned. |
| `cached_data_version` | query | `uint64` | The next file id of the snapshot of the latest statistics the client has loaded. If the snapshot has a different next file id, its statistics are returned. |
| `unit` | query | [`AtUnit`](#atunit) | The unit of the value of the `AT` clause. |
| `value` | query | `string` | The snapshot id or the name of a snapshot tag (for `version`), or the timestamp (for `timestamp`). |
| `bound` | query | [`SnapshotBound`](#snapshotbound) | Whether a timestamp finds the last snapshot before or the first snapshot after it. |
| `newest_snapshot_id` | query | `uint64` | The newest snapshot that the clause can find, snapshots committed after a client's transaction started are not visible to it. |

Request: no body.

Response: [`SnapshotData`](#snapshotdata).

### `list_snapshots`

Returns the snapshots, optionally filtered.

`GET /v1/snapshots`

Parameters in the URL (the other fields of the request are the JSON body):

| Parameter | In | Type | Description |
|-----------|----|------|-------------|
| `exclude_latest` | query | `bool` | Exclude the latest snapshot. |
| `older_than` | query | `timestamp` | Only snapshots created before this timestamp. |
| `snapshot_id` (repeated) | query | list of `uint64` | Only these snapshots. |
| `page_size` | query | `uint64` | The maximum number of entries to return. The server picks a number if absent, and can return fewer entries. |
| `page_token` | query | `string` | The `next_page_token` of the previous page. The first page if absent. |

Request: no body.

Response:

| Field | Type | Description |
|-------|------|-------------|
| `snapshots` | list of [`SnapshotInfo`](#snapshotinfo) (default `[]`) | The snapshots, ordered by their ids. |
| `next_page_token` | `string` (optional) | The token of the next page, absent on the last page. |

### `select`

Plans a scan of a table.

Returns the data files of the table that are visible in a snapshot, together with their delete files and
inlined deletions, and the rows of the table that are inlined in the catalog. `inlined_data` holds the visible
inlined rows, with the row id, the begin and end snapshot
and the columns of the table at the snapshot, whatever the schema of the table was when the rows were written. Values of a
column that was changed from `TIMESTAMP` to `TIMESTAMPTZ` are the timestamps in UTC. It is absent if the table has
no inlined rows.

The files of a partitioned table have the `partition_id` of the partitioning they were written with and the values of
their partition keys (`partition_values`), so clients can group the files by partition.

The `aggregates` of a request without `filters` are answered from the statistics of the table where the server can,
instead of listing the files: `aggregate_results` has an entry for each aggregate, and tells whether the server
answered it. If it answered all of them, the response has no files and no inlined rows: the results are the answer.
Otherwise the response is the one of a scan, with the results of the aggregates that were answered.

A scan of many files is returned in pages. The response has the first page of files, and `page_tokens` has the token
of every other page. A request with a `page_token` and no other member (except for the table) returns the page of
the token: the other members are those of the request that returned the token, and are not repeated. The pages of a
response can be requested in parallel and in any order. They belong to the catalog and the table of the request, and
expire after a while: the request fails with a `404`, and a client plans the scan again.

`POST /v1/tables/{table_id}/scan`

Parameters in the URL (the other fields of the request are the JSON body):

| Parameter | In | Type | Description |
|-----------|----|------|-------------|
| `table_id` | path | `uint64` | The table to scan. |

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshots/latest` and `snapshots/at`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `page_token` | `string` (optional) | A page token of an earlier response: returns that page of files. The request has no other members then. |
| `aggregates` | list of [`Aggregate`](#aggregate) (default `[]`) | Aggregates over the whole table that the server answers from statistics if it can. Not with `filters`. |
| `extended` | `bool` (default `false`) | Return the extended file list used to plan deletes and updates in `extended_files`, instead of `files`. |
| `filters` | [`ScanFilters`](#scanfilters) (optional) | Filters to prune files with. |
| `known_column_mappings` | list of `uint64` (default `[]`) | The ids of the column mappings that the client has: the response only has the other column mappings that its files refer to. |
| `columns` | list of [`ColumnIdentifier`](#columnidentifier) (optional) | The columns that the client reads. The inlined rows only have these columns, besides the row id and the begin and end snapshot. All columns of the table if absent. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `files` | list of [`FileListEntry`](#filelistentry) (default `[]`) | The files of the scan. |
| `extended_files` | list of [`FileListExtendedEntry`](#filelistextendedentry) (optional) | The files of the scan, for `extended` scans. |
| `inlined_data` | Arrow table (optional) | Sent as the Arrow IPC stream of the part `inlined_data`, see [Arrow](#arrow). The inlined rows of the table, absent if it has no inlined data. |
| `page_tokens` | list of `string` (default `[]`) | The tokens of the other pages of the files of the scan, if there are many. The files of the scan are the files of the response and the files of all its pages. |
| `aggregate_results` | list of [`AggregateResult`](#aggregateresult) (default `[]`) | The results of the `aggregates` of the request, in their order. |
| `column_mappings` | list of [`NameMap`](#namemap) (default `[]`) | The column mappings that the files of the response refer to (by their `mapping_id`), and that the client does not have. They map the columns of files that were added with `ducklake_add_data_files` by name. |

### `table_changes`

Returns the files that hold the changes of a table between two snapshots, for change data feeds.

`inlined_data` holds the inlined rows that were inserted (`insertions`) or deleted
(`deletions`) in the range, with the row id, the begin and end snapshot and the columns of the table at the last
snapshot of the range. It is absent if the table has no inlined rows.

Many files are returned in pages, see `scan`: a request is either the request for the changes, or has a `page_token`
and no other member (except for the table).

`POST /v1/tables/{table_id}/changes`

Parameters in the URL (the other fields of the request are the JSON body):

| Parameter | In | Type | Description |
|-----------|----|------|-------------|
| `table_id` | path | `uint64` | The table. The snapshot is the last snapshot of the range. |

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshots/latest` and `snapshots/at`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `start_snapshot` | [`Snapshot`](#snapshot) (optional) | The first snapshot of the range. |
| `start_snapshot_id` | `uint64` (optional) | The id of the first snapshot of the range. |
| `page_token` | `string` (optional) | A page token of an earlier response: returns that page of files. The request has no other members then. |
| `changes` | [`TableChangeType`](#tablechangetype) (optional) | The changes to return. Required, unless the request has a `page_token`. |
| `known_column_mappings` | list of `uint64` (default `[]`) | The ids of the column mappings that the client has: the response only has the other column mappings that its files refer to. |
| `columns` | list of [`ColumnIdentifier`](#columnidentifier) (optional) | The columns that the client reads. The inlined rows only have these columns, besides the row id and the begin and end snapshot. All columns of the table if absent. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `insertions` | list of [`FileListEntry`](#filelistentry) (default `[]`) | For `insertions`: the files with inserted rows. |
| `deletions` | list of [`DeleteScanEntry`](#deletescanentry) (default `[]`) | For `deletions`: the files with deleted rows. |
| `inlined_data` | Arrow table (optional) | Sent as the Arrow IPC stream of the part `inlined_data`, see [Arrow](#arrow). The inlined rows that changed, absent if the table has no inlined data. |
| `page_tokens` | list of `string` (default `[]`) | The tokens of the other pages of the files, see `scan`. |
| `column_mappings` | list of [`NameMap`](#namemap) (default `[]`) | The column mappings that the files of the response refer to (by their `mapping_id`), and that the client does not have. They map the columns of files that were added with `ducklake_add_data_files` by name. |

## Server Commands

These endpoints make the server perform work on the DuckLake outside of a client transaction, and commit it in a
transaction of its own.

### `compact`

Compacts the DuckLake on the server.

The server merges or rewrites data files, or writes inlined data to files, and commits the result in a transaction
of its own, so the compaction does not see the uncommitted changes of a client. The files are read and written by
the server; the parameters are the options of `ducklake_merge_adjacent_files`, `ducklake_rewrite_data_files` and
`ducklake_flush_inlined_data`.

`POST /v1/compactions`

Request:

| Field | Type | Description |
|-------|------|-------------|
| `type` | [`CompactionType`](#compactiontype) | The type of the compaction. |
| `schema_name` | `string` (optional) | The schema of `table_name`, the default schema if absent. For `flush_inlined_data`, all tables of the schema are flushed if there is no `table_name`. |
| `table_name` | `string` (optional) | Only compact this table, all tables if absent. |
| `max_compacted_files` | `uint64` (optional) | The maximum number of files to compact. |
| `min_file_size` | `uint64` (optional) | For `merge_adjacent`: only merge files of at least this size. |
| `max_file_size` | `uint64` (optional) | For `merge_adjacent`: only merge files of at most this size. |
| `newer_than` | `timestamp` (optional) | For `merge_adjacent`: only merge files written after this timestamp. |
| `older_than` | `timestamp` (optional) | For `merge_adjacent`: only merge files written before this timestamp. |
| `delete_threshold` | `double` (optional) | For `rewrite_delete`: the fraction of deleted rows above which files are rewritten. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `results` | list of [`CompactionResult`](#compactionresult) (default `[]`) | The compactions that were performed, per table. |

## Committing Transactions

A client records the changes of a transaction and sends them to `commit_transaction` when it commits. The changes
are a list of requests that are applied in order; later requests can refer to entries created by earlier requests
through negative placeholder ids: `-1` is the first entry (schema, table, view, ...) created in the transaction,
`-2` the second one, etc. A request that creates an entry sets its `placeholder_id`.

Clients send the deletes of committed data first, then the catalog changes in the order in which they were made,
and then the inserted data in the format of the tables at the end of the transaction. This order matters: e.g. a
`SET NOT NULL` that follows a delete of the `NULL` rows only succeeds after the delete.

Requests refer to existing entries by their ids, so they act on the entry that the client saw, whatever other
transactions did to its name since.

### `commit_transaction`

Commits a set of changes atomically.

If the transaction conflicts with a transaction that committed after its snapshot, the server retries the
commit when that is safe; otherwise it fails with a `409`. The snapshot is the snapshot the changes are based
on: the server detects conflicts with every snapshot committed after it. Without a snapshot, the changes are
based on the latest snapshot.

`POST /v1/transactions/commit`

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshots/latest` and `snapshots/at`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `commit_info` | [`CommitInfo`](#commitinfo) (optional) | The commit information of the snapshot. |
| `retry` | [`RetryConfig`](#retryconfig) (optional) | How the server retries the commit on conflicts. |
| `requests` | list of `commit_request` (default `[]`) | The changes, applied in order, e.g. `[{"create_table": {...}}, {"insert_rows": {...}}]`. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The committed snapshot, absent if the requests did not create a snapshot (e.g. if they only set options). If other transactions created snapshots since the snapshot of the request, its id is more than one above the id of the snapshot of the request. |
| `entry_ids` | list of [`CreatedEntry`](#createdentry) (default `[]`) | The entries the transaction created, with the ids the commit gave them. |
| `table_options` | list of [`OptionSetting`](#optionsetting) (default `[]`) | The options of the tables the transaction created or altered, with the ids the commit gave the tables. |

### `create_schema`

Creates a schema.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `placeholder_id` | `int64` | The placeholder id of the schema: a negative number, e.g. `-1` for the first entry created in the transaction. Later requests of the transaction refer to the schema with it, and `entry_ids` returns the id that the commit gave the schema with it. |
| `parent_schema_id` | `int64` (optional) | The schema that holds the new schema, absent for a schema at the top level. A negative id is the placeholder id of a schema created in the transaction. |
| `schema_name` | `string` | The name of the schema. |
| `schema_uuid` | `string` (optional) | The uuid of the schema. |
| `path` | `path` (optional) | The data path of the schema. |
| `if_not_exists` | `bool` (default `false`) | Do nothing if the schema exists. |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_table`

Creates a table.

Columns get the field ids `1..n` in the order of the columns (nested fields are numbered depth-first), as in `CREATE
TABLE`.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `placeholder_id` | `int64` | The placeholder id of the table, which `insert_rows`, `insert_files` and `delete_data` refer to the table with. See `create_schema`. |
| `schema_id` | `int64` | The schema of the table. A negative id is the placeholder id of a schema created in the transaction. |
| `table_name` | `string` | The name of the table. |
| `table_uuid` | `string` (optional) | The uuid of the table. |
| `path` | `path` (optional) | The data path of the table. |
| `columns` | list of [`ColumnDefinition`](#columndefinition) | The columns of the table. |
| `partitioned_by` | list of `string` (default `[]`) | The partition keys as SQL expressions, e.g. `["year(ts)"]`. |
| `sorted_by` | list of `string` (default `[]`) | The sort keys as SQL expressions. |
| `options` | object of `string` (default `{}`) | Table options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`. |
| `comment` | `string` (optional) | The comment of the table. |
| `partition_id` | `uint64` (optional) | An id of the client for the partitioning of the table. Files that are inserted later in the transaction refer to the partitioning with it. |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_view`

Creates a view.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `placeholder_id` | `int64` | See `create_schema`. |
| `schema_id` | `int64` | The schema of the view, see `create_table`. |
| `view_name` | `string` | The name of the view. |
| `view_uuid` | `string` (optional) | The uuid of the view. |
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the view, only `duckdb` for now. |
| `sql` | `string` | The query of the view. |
| `column_aliases` | list of `string` (default `[]`) | The names of the columns of the view. |
| `comment` | `string` (optional) | The comment of the view. |
| `column_comments` | object of `string` (default `{}`) | The comments of the columns of the view, by column name. |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_macro`

Creates a macro.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `placeholder_id` | `int64` | See `create_schema`. |
| `schema_id` | `int64` | The schema of the macro, see `create_table`. |
| `macro` | [`MacroInfo`](#macroinfo) | The macro (its ids are ignored). |

Response: as for [`commit_transaction`](#commit_transaction).

### `alter`

Alters a table or a view.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `entry_id` | `int64` | The table or view. A negative id is the placeholder id of an entry created in the transaction. |
| `alter` | [`Alteration`](#alteration) | The change. |
| `partition_id` | `uint64` (optional) | For `set_partitioned_by`: an id of the client for the new partitioning (as for `create_table`). |

Response: as for [`commit_transaction`](#commit_transaction).

### `drop`

Drops a schema, a table, a view or a macro. A schema has to be empty.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `entry_type` | [`DropEntryType`](#dropentrytype) | The type of the entry. |
| `entry_id` | `int64` | The entry. A negative id is the placeholder id of an entry created in the transaction. |

Response: as for [`commit_transaction`](#commit_transaction).

### `set_option`

Sets or resets a DuckLake option (`ducklake_metadata`).

Request:

| Field | Type | Description |
|-------|------|-------------|
| `option` | [`OptionSetting`](#optionsetting) | The option. |
| `reset` | `bool` (default `false`) | Reset the option instead of setting it. |

Response: as for [`commit_transaction`](#commit_transaction).

### `insert_rows`

Inserts rows that are inlined in the catalog.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `rows` | Arrow table | Sent as the Arrow IPC stream of the part `rows.<n>`, see [Arrow](#arrow). The rows to insert: a table with the columns of the table. `<n>` is the position of the request in `requests`. |
| `row_ids` | list of `int64` (default `[]`) | The row ids of the rows (for updates that keep the row ids of the updated rows). |
| `column_stats` | list of [`ColumnStats`](#columnstats) (default `[]`) | Each entry has an additional field `column_id`. The statistics of the rows per column. |

Response: as for [`commit_transaction`](#commit_transaction).

### `insert_files`

Registers Parquet files that were written by the client.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `files` | list of [`DataFile`](#datafile) | The files. |
| `name_maps` | list of [`NameMap`](#namemap) (default `[]`) | The name maps of the files, which map the columns of the files by name. A file refers to its name map with its `mapping_id`, an id of the client. |

Response: as for [`commit_transaction`](#commit_transaction).

### `delete_data`

Deletes rows from a table.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `delete_files` | list of [`DeleteFile`](#deletefile) (default `[]`) | Delete files written by the client. |
| `inlined_row_ids` | list of `uint64` (default `[]`) | The row ids of the inlined rows that are deleted. |
| `file_row_deletes` | list of [`FileRowDeletes`](#filerowdeletes) (default `[]`) | Deletes of rows of data files that are inlined in the catalog. |
| `dropped_files` | list of [`DroppedFile`](#droppedfile) (default `[]`) | Data files that are deleted entirely. |
| `delete_attempted` | `bool` (default `false`) | Whether a delete was evaluated against the table, even if it did not delete any rows. This makes the commit conflict with concurrent inserts. |

Response: as for [`commit_transaction`](#commit_transaction).

## Types

The structures that the endpoints share. Fields that are optional can be `null` or absent; the server and the
client leave them out when they have no value.

### `AddColumn`

| Field | Type | Description |
|-------|------|-------------|
| `column` | [`ColumnDefinition`](#columndefinition) | The column. |
| `if_not_exists` | `bool` (default `false`) | Do nothing if the column exists. |

### `AddField`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the parent column. |
| `field` | [`ColumnDefinition`](#columndefinition) | The field. |
| `if_not_exists` | `bool` (default `false`) | Do nothing if the field exists. |

### `Aggregate`

An aggregate over the rows of a table.

| Field | Type | Description |
|-------|------|-------------|
| `function` | [`AggregateFunction`](#aggregatefunction) | The function. |
| `column_id` | `uint64` (optional) | The column, as its field id. For aggregates other than `count_star`: a top-level column. |

### `AggregateFunction`

The function of an aggregate.

| Value | Description |
|-------|-------------|
| `count_star` | `count(*)`. |
| `count` | `count(column)`: the number of rows where the column is not NULL. |
| `min` | `min(column)`. |
| `max` | `max(column)`. |

### `AggregateResult`

The result of an aggregate of a scan.

| Field | Type | Description |
|-------|------|-------------|
| `answered` | `bool` | Whether the server answered the aggregate. If not, the files of the scan have to be read to answer it. |
| `value` | `text` (optional) | The result, `null` if the aggregate is NULL (e.g. the `min` of a column without values) or not answered. |

### `Alteration`

The change of an `alter` request: an object with exactly one member, the name of the change.
Columns are referred to by name; nested fields by their path from the top-level column, e.g. `["s", "a"]`.
Expressions are SQL expressions.

```json
{"rename_column": {"old_name": "i", "new_name": "j"}}
```

An object with exactly one member, the name of the variant:

| Variant | Type |
|---------|------|
| `rename_table` | [`RenameTable`](#renametable) |
| `rename_view` | [`RenameView`](#renameview) |
| `rename_column` | [`RenameColumn`](#renamecolumn) |
| `rename_field` | [`RenameField`](#renamefield) |
| `add_column` | [`AddColumn`](#addcolumn) |
| `add_field` | [`AddField`](#addfield) |
| `remove_column` | [`RemoveColumn`](#removecolumn) |
| `remove_field` | [`RemoveField`](#removefield) |
| `change_column_type` | [`ChangeColumnType`](#changecolumntype) |
| `set_default` | [`SetDefault`](#setdefault) |
| `set_not_null` | [`SetNotNull`](#setnotnull) |
| `drop_not_null` | [`DropNotNull`](#dropnotnull) |
| `set_partitioned_by` | [`SetPartitionedBy`](#setpartitionedby) |
| `set_sorted_by` | [`SetSortedBy`](#setsortedby) |
| `set_options` | [`SetOptions`](#setoptions) |
| `reset_options` | [`ResetOptions`](#resetoptions) |
| `set_comment` | [`SetComment`](#setcomment) |
| `set_column_comment` | [`SetColumnComment`](#setcolumncomment) |

### `AtUnit`

The unit of an `AT` clause.

| Value | Description |
|-------|-------------|
| `version` | A snapshot id. |
| `timestamp` | A timestamp. |

### `CatalogEntryType`

The type of a catalog entry.

| Value | Description |
|-------|-------------|
| `TABLE_ENTRY` |  |
| `VIEW_ENTRY` |  |

### `CatalogInfo`

The catalog at a snapshot.

| Field | Type | Description |
|-------|------|-------------|
| `schemas` | list of [`SchemaInfo`](#schemainfo) (default `[]`) | The schemas. |
| `tables` | list of [`TableInfo`](#tableinfo) (default `[]`) | The tables. |
| `views` | list of [`ViewInfo`](#viewinfo) (default `[]`) | The views. |
| `macros` | list of [`MacroInfo`](#macroinfo) (default `[]`) | The macros. |
| `partitions` | list of [`PartitionInfo`](#partitioninfo) (default `[]`) | The partitionings of the tables. |
| `sorts` | list of [`SortInfo`](#sortinfo) (default `[]`) | The sort orders of the tables. |

### `ChangeColumnType`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |
| `column_type` | `logical_type` | The new type. |
| `expression` | `string` (optional) | The `USING` expression that converts the old values. |

### `ColumnDefinition`

The definition of a column in a catalog change.

| Field | Type | Description |
|-------|------|-------------|
| `column_name` | `string` | The name of the column. |
| `column_type` | `logical_type` | The type of the column. |
| `default` | `string` (optional) | The default of the column, as a SQL expression. |
| `initial_default` | `string` (optional) | The text of the value that rows from before the column was added get, already cast to the column type. A client evaluates a literal default in the time zone of its session and sends the result, with an explicit offset for `TIMESTAMPTZ` and `TIMETZ`, and may omit a NULL. A request with a literal default that depends on the time zone and no `initial_default` is rejected, as the server does not know the time zone of the client. |
| `comment` | `string` (optional) | The comment of the column. |
| `not_null` | `bool` (default `false`) | Whether the column is `NOT NULL`. |

### `ColumnIdentifier`

A column that a request reads, as the field id of a top-level column, or of a field of a struct.

| Field | Type | Description |
|-------|------|-------------|
| `field_id` | `uint64` | The field id of the column or field. |
| `children` | list of [`ColumnIdentifier`](#columnidentifier) (default `[]`) | The fields of a struct that the client reads, as column identifiers. The other fields are `NULL` in the response. The whole column if empty. |

### `ColumnInfo`

A column of a table, as stored in `ducklake_column`.

| Field | Type | Description |
|-------|------|-------------|
| `column_id` | `uint64` (optional) | The field id of the column. |
| `column_name` | `string` | The name of the column. |
| `column_type` | `string` | The type of the column, as a type name of the DuckLake specification (e.g. `int32`, `varchar`). |
| `initial_default` | `text` (optional) | The value of the column for rows that were written before the column was added. |
| `default_value` | `text` (optional) | The default value of the column. |
| `default_value_type` | `string` (default `""`) | Whether the default is a `literal` or an `expression`. |
| `nulls_allowed` | `bool` (default `true`) | Whether the column can hold `NULL`. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the column. |
| `children` | list of [`ColumnInfo`](#columninfo) (default `[]`) | The fields of nested columns. |

### `ColumnStats`

The statistics of a column of a file or of inserted rows.

```json
{"type": {"id": "int32"}, "min_value": "1", "max_value": "2", "null_count": 0, "value_count": 2,
 "column_size_bytes": 10, "any_valid": true, "bounds_unknown": false, "min_is_exact": true, "max_is_exact": true}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | `logical_type` | The type of the column. |
| `min_value` | `string` (optional) | The minimum value. |
| `max_value` | `string` (optional) | The maximum value. |
| `null_count` | `uint64` (optional) | The number of `NULL` values. |
| `value_count` | `uint64` (optional) | The number of values. |
| `column_size_bytes` | `uint64` (default `0`) | The size of the column. |
| `contains_nan` | `bool` (optional) | Whether the column contains `NaN` values. |
| `any_valid` | `bool` (default `true`) | Whether the column contains any value that is not `NULL`. |
| `bounds_unknown` | `bool` (default `false`) | Whether the minimum and maximum are unknown. |
| `min_is_exact` | `bool` (default `false`) | Whether the minimum is exact. |
| `max_is_exact` | `bool` (default `false`) | Whether the maximum is exact. |
| `extra_stats` | `string` (optional) | Type-specific statistics. |

### `CommitInfo`

The commit information of a transaction.

| Field | Type | Description |
|-------|------|-------------|
| `author` | `text` (optional) | The author of the commit. |
| `commit_message` | `text` (optional) | The commit message. |
| `commit_extra_info` | `text` (optional) | Extra information of the commit. |
| `is_commit_info_set` | `bool` (default `false`) | Whether the commit information was set. |

### `CompactionResult`

The compaction of a table.

| Field | Type | Description |
|-------|------|-------------|
| `schema_name` | `string` | The schema of the table. |
| `table_name` | `string` | The name of the table. |
| `files_processed` | `uint64` (optional) | For `merge_adjacent` and `rewrite_delete`: the number of files that were compacted. |
| `files_created` | `uint64` (optional) | For `merge_adjacent` and `rewrite_delete`: the number of files that were written. |
| `rows_flushed` | `uint64` (optional) | For `flush_inlined_data`: the number of rows that were written to files. |

### `CompactionType`

The type of a compaction.

| Value | Description |
|-------|-------------|
| `merge_adjacent` | Merges small adjacent files. |
| `rewrite_delete` | Rewrites files with many deleted rows. |
| `flush_inlined_data` | Writes the data and deletions that are inlined in the catalog to files. |

### `CreatedEntry`

An entry that a commit created.

| Field | Type | Description |
|-------|------|-------------|
| `placeholder_id` | `int64` | The placeholder id of the request that created the entry. |
| `type` | `string` | The type of the entry, e.g. `TABLE_ENTRY`. |
| `id` | `uint64` | The id the commit gave the entry. |

### `DataFile`

A Parquet file written by a client. The file includes the statistics of its columns, so the server does not need to
read it.

| Field | Type | Description |
|-------|------|-------------|
| `path` | `path` | The path of the file. |
| `record_count` | `uint64` | The number of rows. |
| `file_size_bytes` | `uint64` | The size of the file. |
| `footer_size` | `uint64` (optional) | The size of the footer. |
| `row_group_count` | `uint64` (optional) | The number of row groups. |
| `partition_id` | `uint64` (optional) | The partitioning the file was written for. Partitionings set in the transaction are referred to by the `partition_id` of their request. |
| `encryption_key` | `base64` (default `""`) | The key the file is encrypted with, if any. |
| `mapping_id` | `uint64` (optional) | The name map of the file. Name maps created in the transaction are referred to by the `mapping_id` of their request. |
| `column_stats` | list of [`ColumnStats`](#columnstats) (default `[]`) | Each entry has an additional field `column_id`. The statistics of the columns of the file. |
| `partition_values` | list of [`FilePartitionValue`](#filepartitionvalue) (default `[]`) | The values of the partition keys. |
| `delete_files` | list of [`DeleteFile`](#deletefile) (default `[]`) | Delete files for the rows of the file. |

### `DeleteFile`

A delete file written by a client, which replaces the previous delete file of its data file.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` (optional) | The data file whose rows are deleted. If it has a delete file at the snapshot of the transaction, the new delete file replaces that file: it has the deletes of the old delete file as well. |
| `data_file_path` | `path` (default `""`) | The path of the data file. |
| `path` | `path` | The path of the delete file. |
| `format` | [`FileFormat`](#fileformat) (default `"parquet"`) | The format of the delete file. |
| `delete_count` | `uint64` | The number of deleted rows. |
| `file_size_bytes` | `uint64` | The size of the delete file. |
| `footer_size` | `uint64` (default `0`) | The size of the footer of the delete file. |
| `row_group_count` | `uint64` (optional) | The number of row groups of the delete file. |
| `encryption_key` | `base64` (default `""`) | The key the file is encrypted with, if any. |
| `begin_snapshot` | `uint64` (optional) | The snapshot of the deletes, if it is not the snapshot of the commit. |
| `max_snapshot` | `uint64` (optional) | The last snapshot whose deletes the file holds, for partial delete files. |

### `DeleteScanEntry`

A data file whose rows were deleted between two snapshots.

| Field | Type | Description |
|-------|------|-------------|
| `file` | [`FileData`](#filedata) | The data file. |
| `delete_file` | [`FileData`](#filedata) (optional) | The delete file at the end of the range. |
| `previous_delete_file` | [`FileData`](#filedata) (optional) | The delete file at the start of the range. |
| `record_count` | `uint64` | The number of rows of the data file. |
| `row_id_start` | `uint64` (optional) | The row id of the first row. |
| `mapping_id` | `uint64` (optional) | The name map of the file. |
| `snapshot_id` | `uint64` (optional) | The snapshot of the deletes. |
| `insert_snapshot` | `uint64` (optional) | The snapshot that inserted every row of the data file, unset when its rows carry their own. |
| `file_deleted` | `bool` (default `false`) | Whether the data file was deleted entirely. |
| `start_snapshot` | `uint64` (optional) | The first snapshot of the range. |
| `end_snapshot` | `uint64` (optional) | The last snapshot of the range. |
| `file_id` | `uint64` (optional) | The id of the data file. |
| `inlined_file_deletions` | list of `uint64` (default `[]`) | Entries of `row_id` and `snapshot_id`. The deleted rows of the file that are inlined in the catalog, with the snapshot that deleted them. |

### `DropEntryType`

The type of an entry that `drop` drops.

| Value | Description |
|-------|-------------|
| `schema` | A schema. |
| `table` | A table. |
| `view` | A view. |
| `scalar_macro` | A scalar macro. |
| `table_macro` | A table macro. |

### `DropNotNull`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |

### `DroppedFile`

A data file that is deleted entirely.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` | The data file. |
| `path` | `path` | The path of the file. |
| `record_count` | `uint64` | The number of rows of the file. |
| `file_size_bytes` | `uint64` | The size of the file. |

### `ErrorInfo`

The details of an error.

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` (default `"Error"`) | The type of the DuckDB exception, e.g. `Catalog`. |
| `message` | `string` (default `""`) | The message of the error. |
| `code` | `uint64` (default `500`) | The HTTP status code of the response. |

### `ErrorResponse`

The body of a response with a status code other than 200: `409` for transaction conflicts, `404` for catalog errors
(e.g. entries that do not exist or unknown endpoints), `400` for invalid input, `401` for a missing or invalid token,
`501` for operations that are not supported and `500` otherwise. The status code is repeated in the `code` of the error.

```json
{"error": {"type": "Catalog", "message": "Table with id 1000 does not exist at snapshot 3"}}
```

| Field | Type | Description |
|-------|------|-------------|
| `error` | [`ErrorInfo`](#errorinfo) | The error. |

### `FileColumnStats`

The min/max statistics of a column of a file.

| Field | Type | Description |
|-------|------|-------------|
| `min_value` | `string` (optional) | The minimum value. |
| `max_value` | `string` (optional) | The maximum value. |
| `contains_nan` | `bool` (optional) | Whether the column contains `NaN` values. |

### `FileData`

A file that a client reads.

| Field | Type | Description |
|-------|------|-------------|
| `path` | `path` | The absolute path of the file. |
| `encryption_key` | `base64` (default `""`) | The key the file is encrypted with, if any. |
| `file_size_bytes` | `uint64` (default `0`) | The size of the file. |
| `footer_size` | `uint64` (optional) | The size of the footer of the file. |
| `format` | [`FileFormat`](#fileformat) (default `"parquet"`) | The format of the file. |
| `row_count` | `uint64` (optional) | The number of rows of the file. For a delete file: the number of deleted rows. |
| `row_group_count` | `uint64` (optional) | The number of row groups of the file. |

### `FileFormat`

The format of a file.

| Value | Description |
|-------|-------------|
| `parquet` | A Parquet file. |
| `puffin` | A deletion vector in a Puffin file (Iceberg V3). |

### `FileListEntry`

A data file of a table that is visible in a snapshot, together with its deletes.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` (optional) | The id of the data file. |
| `file_id` | `uint64` (optional) | The id of the file. |
| `file` | [`FileData`](#filedata) | The data file. |
| `delete_file` | [`FileData`](#filedata) (optional) | The delete file of the data file. |
| `row_id_start` | `uint64` (optional) | The row id of the first row of the file. |
| `snapshot_id` | `uint64` (optional) | The snapshot that added the file. |
| `snapshot_filter_max` | `uint64` (optional) | Only read rows that were inserted up to this snapshot. |
| `snapshot_filter_min` | `uint64` (optional) | Only read rows that were inserted from this snapshot. |
| `snapshot_filter_excluded` | list of `uint64` (default `[]`) | Skip the rows that these snapshots wrote, which rewrote rows that other files hold as well. |
| `mapping_id` | `uint64` (optional) | The name map of the file. |
| `partition_id` | `uint64` (optional) | The partitioning that the file was written with, absent if the table was not partitioned then. The partition keys of the values are those of this partitioning, which is not necessarily the current one. |
| `partition_values` | list of [`FilePartitionValue`](#filepartitionvalue) (default `[]`) | The values of the partition keys of the file, as text. |
| `inlined_file_deletions` | list of `uint64` (default `[]`) | The positions of the rows of the file that are deleted by deletions inlined in the catalog. |
| `column_min_max` | list of [`FileColumnStats`](#filecolumnstats) (default `[]`) | Each entry has an additional field `column_id`. The statistics of the columns of the scan's `runtime_filter_columns`. |

### `FileListExtendedEntry`

A data file of a table with the ids of its files, used to plan deletes and updates.

| Field | Type | Description |
|-------|------|-------------|
| `file_id` | `uint64` (optional) | The id of the data file. |
| `delete_file_id` | `uint64` (optional) | The id of the delete file. |
| `file` | [`FileData`](#filedata) | The data file. |
| `delete_file` | [`FileData`](#filedata) (optional) | The delete file. |
| `row_id_start` | `uint64` (optional) | The row id of the first row of the file. |
| `snapshot_id` | `uint64` (optional) | The snapshot that added the file. |
| `delete_file_begin_snapshot` | `uint64` (optional) | The snapshot that added the delete file. |
| `record_count` | `uint64` | The number of rows of the file. |
| `mapping_id` | `uint64` (optional) | The name map of the file. |

### `FileOrder`

An order of the files of a scan: by the `min` or `max` of a column in the file.

| Field | Type | Description |
|-------|------|-------------|
| `function` | [`AggregateFunction`](#aggregatefunction) | The function: `min` or `max`. |
| `column_id` | `uint64` | The column, as its field id. |
| `sort_order` | [`SortOrder`](#sortorder) (default `"ASC"`) | The direction. |

### `FilePartitionValue`

The value of a partition key for the rows of a file.

| Field | Type | Description |
|-------|------|-------------|
| `partition_key_index` | `uint64` | The partition key. |
| `partition_value` | `text` (optional) | The value. |

### `FileRowDeletes`

Deletes of rows of a data file that are inlined in the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` | The data file. |
| `row_ids` | list of `uint64` | The positions of the deleted rows in the file. |

### `GlobalColumnStats`

The statistics of a column of a table.

| Field | Type | Description |
|-------|------|-------------|
| `column_id` | `uint64` (optional) | The column. |
| `contains_null` | `bool` (optional) | Whether the column contains `NULL` values. |
| `contains_nan` | `bool` (optional) | Whether the column contains `NaN` values. |
| `min_value` | `string` (optional) | The minimum value. |
| `max_value` | `string` (optional) | The maximum value. |
| `extra_stats` | `string` (optional) | Type-specific statistics. |
| `min_is_exact` | `bool` (default `false`) | Whether the minimum is exact (rather than a bound). |
| `max_is_exact` | `bool` (default `false`) | Whether the maximum is exact (rather than a bound). |

### `GlobalTableStats`

The statistics of a table.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The table. |
| `initialized` | `bool` (default `true`) | Whether the statistics have been initialized. |
| `record_count` | `uint64` (optional) | The number of rows of the table, absent if unknown. |
| `next_row_id` | `uint64` (default `0`) | The next free row id of the table. |
| `file_size_bytes` | `uint64` (optional) | The size of the files of the table, absent if unknown. |
| `column_stats` | list of [`GlobalColumnStats`](#globalcolumnstats) (default `[]`) | The statistics of the columns. |

### `MacroImplementation`

An implementation (overload) of a macro.

| Field | Type | Description |
|-------|------|-------------|
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the implementation. |
| `sql` | `string` | The body of the macro. |
| `type` | [`MacroType`](#macrotype) | The type of the macro. |
| `parameters` | list of [`MacroParameter`](#macroparameter) (default `[]`) | The parameters of the macro. |

### `MacroInfo`

A macro of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `schema_id` | `uint64` (optional) | The schema of the macro. |
| `macro_id` | `uint64` (optional) | The id of the macro. |
| `macro_name` | `string` | The name of the macro. |
| `implementations` | list of [`MacroImplementation`](#macroimplementation) (default `[]`) | The implementations of the macro. |

### `MacroParameter`

A parameter of a macro.

| Field | Type | Description |
|-------|------|-------------|
| `parameter_name` | `string` | The name of the parameter. |
| `parameter_type` | `string` (default `""`) | The type of the parameter, `unknown` if it has none. |
| `default_value` | `text` (optional) | The default value of the parameter. |
| `default_value_type` | `string` (default `""`) | Whether the default is a `literal` or an `expression`. |

### `MacroType`

The type of a macro.

| Value | Description |
|-------|-------------|
| `scalar` | A scalar macro. |
| `table` | A table macro. |

### `Metadata`

The global metadata of the DuckLake (the contents of `ducklake_metadata`).

```json
{
  "tags": [{"key": "version", "value": "1.1-dev1"}, {"key": "data_path", "value": "/data/"}],
  "table_settings": [{"table_id": 1, "key": "data_inlining_row_limit", "value": "10"}]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tags` | list of [`Tag`](#tag) (default `[]`) | The global options, including the `version` and the `data_path` of the DuckLake. |
| `schema_settings` | list of [`SchemaSetting`](#schemasetting) (default `[]`) | The options set for schemas. |
| `table_settings` | list of [`TableSetting`](#tablesetting) (default `[]`) | The options set for tables. |

### `NameMap`

A name map (`ducklake_column_mapping` / `ducklake_name_mapping`) of files that were added with
`ducklake_add_data_files`.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The table. |
| `mapping_id` | `uint64` (optional) | The id of the name map. |
| `type` | `string` (default `"map_by_name"`) | The type of the name map. |
| `columns` | list of [`NameMapColumn`](#namemapcolumn) (default `[]`) | The columns of the name map. |

### `NameMapColumn`

A column of a name map: maps a column of a file to a field of the table.

| Field | Type | Description |
|-------|------|-------------|
| `column_id` | `uint64` | The id of the entry in the name map. |
| `source_name` | `string` | The name of the column in the file. |
| `target_field_id` | `uint64` (optional) | The field of the table. |
| `hive_partition` | `bool` (default `false`) | Whether the column is a hive partition of the file path. |
| `parent_column` | `uint64` (optional) | The entry of the parent column, for nested columns. |

### `NullOrder`

Where a sort key orders `NULL` values.

| Value | Description |
|-------|-------------|
| `NULLS_FIRST` |  |
| `NULLS_LAST` |  |
| `ORDER_DEFAULT` |  |

### `OptionSetting`

An option of the DuckLake.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `string` | The option, e.g. `data_inlining_row_limit`. |
| `value` | `string` (default `""`) | The value of the option. |
| `schema_id` | `uint64` (optional) | The schema, for options of a schema. |
| `table_id` | `int64` (optional) | The table, for options of a table. A table created in the transaction takes its options from `create_table`. |

### `PartitionField`

A partition key of a table.

| Field | Type | Description |
|-------|------|-------------|
| `partition_key_index` | `uint64` | The position of the key in the partitioning. |
| `column_id` | `uint64` (optional) | The column the key is computed from. |
| `transform` | `string` | The transform of the column, e.g. `identity` or `year`. |

### `PartitionInfo`

The partitioning of a table.

| Field | Type | Description |
|-------|------|-------------|
| `partition_id` | `uint64` (optional) | The id of the partitioning. |
| `table_id` | `uint64` (optional) | The table. |
| `fields` | list of [`PartitionField`](#partitionfield) (default `[]`) | The partition keys. |

### `RemoveColumn`

| Field | Type | Description |
|-------|------|-------------|
| `column_name` | `string` | The name of the column. |
| `if_exists` | `bool` (default `false`) | Do nothing if the column does not exist. |
| `cascade` | `bool` (default `false`) | Also drop what depends on the column. |

### `RemoveField`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the field. |
| `if_exists` | `bool` (default `false`) | Do nothing if the field does not exist. |
| `cascade` | `bool` (default `false`) | Also drop what depends on the field. |

### `RenameColumn`

| Field | Type | Description |
|-------|------|-------------|
| `old_name` | `string` | The current name of the column. |
| `new_name` | `string` | The new name of the column. |

### `RenameField`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the field. |
| `new_name` | `string` | The new name of the field. |

### `RenameTable`

| Field | Type | Description |
|-------|------|-------------|
| `new_name` | `string` | The new name of the table. |

### `RenameView`

| Field | Type | Description |
|-------|------|-------------|
| `new_name` | `string` | The new name of the view. |

### `ResetOptions`

| Field | Type | Description |
|-------|------|-------------|
| `options` | list of `string` (default `[]`) | The options to reset. |

### `RetryConfig`

How a commit is retried on conflicts.

| Field | Type | Description |
|-------|------|-------------|
| `max_retry_count` | `uint64` | The number of retries. |
| `retry_wait_ms` | `uint64` | The wait before the first retry. |
| `retry_backoff` | `double` | The factor the wait grows with on every retry. |

### `ScanFilters`

Filters of a scan. Filters are optional: a server may ignore them, and a client applies them to the scanned rows
regardless.

`expressions` are SQL expressions that all hold for the rows of the scan. They refer to the columns of the table
by name, and to the fields of structs by their path (`st.a`). The server prunes files on the statistics of their
columns with the parts of the expressions it understands: comparisons of a column with a constant, `IN` lists of
constants, `IS [NOT] NULL`, and `AND` / `OR` combinations of those. The server ignores the other parts, and leaves
out an `OR` entirely if it does not understand one of its parts.

`order_by` is the order of the files: by the `min` or `max` of a column in a file, in the format of the `aggregates`.
The files without statistics come last. A top-N uses it to read the files with the best values first, so that its
bound prunes the other files early. For `runtime_filter_columns` the server returns the min/max statistics of the
files in `column_min_max`, so the client can prune files at runtime.

```json
{"expressions": ["i >= 150", "s = 'a' OR st.a IN (1, 2)"],
 "order_by": [{"function": "max", "column_id": 1, "sort_order": "DESC"}],
 "runtime_filter_columns": [1]}
```

| Field | Type | Description |
|-------|------|-------------|
| `expressions` | list of `string` (default `[]`) | SQL expressions that hold for all rows of the scan. |
| `order_by` | list of [`FileOrder`](#fileorder) (default `[]`) | The order of the files. The terms that are not a `min` or `max` of a column are ignored. |
| `runtime_filter_columns` | list of `uint64` (default `[]`) | The columns to return the min/max statistics of. |

### `SchemaInfo`

A schema of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `schema_id` | `uint64` (optional) | The id of the schema. |
| `parent_schema_id` | `uint64` (optional) | The parent schema, for nested schemas. |
| `schema_uuid` | `string` (default `""`) | The uuid of the schema. |
| `schema_name` | `string` | The name of the schema. |
| `path` | `path` (default `""`) | The data path of the schema. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the schema. |

### `SchemaSetting`

An option of the DuckLake that is set for a schema.

| Field | Type | Description |
|-------|------|-------------|
| `schema_id` | `uint64` (optional) | The schema. |
| `key` | `string` | The option. |
| `value` | `string` (default `""`) | The value of the option. |

### `SetColumnComment`

| Field | Type | Description |
|-------|------|-------------|
| `entry_type` | [`CatalogEntryType`](#catalogentrytype) | Whether the column is of a table or a view. |
| `column_name` | `string` | The name of the column. |
| `comment` | `text` (optional) | The comment, absent to drop the comment. |

### `SetComment`

| Field | Type | Description |
|-------|------|-------------|
| `entry_type` | [`CatalogEntryType`](#catalogentrytype) | Whether the comment is of a table or a view. |
| `comment` | `text` (optional) | The comment, absent to drop the comment. |

### `SetDefault`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |
| `expression` | `string` (optional) | The default, absent to drop the default. |

### `SetNotNull`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |

### `SetOptions`

| Field | Type | Description |
|-------|------|-------------|
| `options` | object of `string` (default `{}`) | The options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`. |

### `SetPartitionedBy`

| Field | Type | Description |
|-------|------|-------------|
| `partitioned_by` | list of `string` (default `[]`) | The partition keys as SQL expressions, empty to reset the partitioning. |

### `SetSortedBy`

| Field | Type | Description |
|-------|------|-------------|
| `sorted_by` | list of `string` (default `[]`) | The sort keys, e.g. `["j DESC NULLS LAST"]`, empty to reset the sort order. |

### `Snapshot`

A snapshot of the DuckLake, which identifies the state of the catalog to read.

| Field | Type | Description |
|-------|------|-------------|
| `snapshot_id` | `uint64` | The id of the snapshot. |
| `schema_version` | `uint64` | The schema version of the snapshot, which changes with every change of the catalog. |
| `next_catalog_id` | `uint64` | The next free id of a catalog entry. |
| `next_file_id` | `uint64` | The next free id of a file. |

### `SnapshotBound`

How a timestamp finds a snapshot.

| Value | Description |
|-------|-------------|
| `upper` | The last snapshot before the timestamp. |
| `lower` | The first snapshot after the timestamp. |
| `time_travel` | The last snapshot before the timestamp, for time travel: refuses timestamps before the newest snapshot that an expiry removed. |

### `SnapshotData`

A snapshot with its catalog and statistics.

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) | The snapshot. |
| `catalog` | [`CatalogInfo`](#cataloginfo) (optional) | The catalog of the snapshot. |
| `stats` | list of [`GlobalTableStats`](#globaltablestats) (optional) | The statistics of the tables of the snapshot. |

### `SnapshotIncludes`

What a request for a snapshot returns besides the snapshot.

| Field | Type | Description |
|-------|------|-------------|
| `include_catalog` | `bool` (default `false`) | Return the catalog of the snapshot. |
| `include_stats` | `bool` (default `false`) | Return the table statistics of the snapshot. |
| `cached_metadata_version` | `uint64` (optional) | The schema version of the latest catalog the client has loaded. If the snapshot has a different schema version, its catalog is returned. |
| `cached_data_version` | `uint64` (optional) | The next file id of the snapshot of the latest statistics the client has loaded. If the snapshot has a different next file id, its statistics are returned. |

### `SnapshotInfo`

A snapshot, with its commit information.

| Field | Type | Description |
|-------|------|-------------|
| `snapshot_id` | `uint64` | The id of the snapshot. |
| `snapshot_time` | `timestamp` | When the snapshot was committed. |
| `schema_version` | `uint64` | The schema version of the snapshot. |
| `next_file_id` | `uint64` | The next free file id of the snapshot. |
| `changes_made` | `string` (default `""`) | The changes of the snapshot, as in `ducklake_snapshot_changes`. |
| `author` | `text` (optional) | The author of the commit. |
| `commit_message` | `text` (optional) | The commit message. |
| `commit_extra_info` | `text` (optional) | Extra information of the commit. |

### `SnapshotReference`

The snapshot that a request reads. A `snapshot` is used as-is, which also allows reading at snapshots that
have been expired since the client loaded them. Without `snapshot` and `snapshot_id`, the latest snapshot is read.

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshots/latest` and `snapshots/at`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |

### `SortDirection`

The direction of a sort key.

| Value | Description |
|-------|-------------|
| `ASCENDING` |  |
| `DESCENDING` |  |
| `ORDER_DEFAULT` |  |

### `SortField`

A sort key of a table.

| Field | Type | Description |
|-------|------|-------------|
| `sort_key_index` | `uint64` | The position of the key in the sort order. |
| `expression` | `string` | The expression to sort by. |
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the expression. |
| `sort_direction` | [`SortDirection`](#sortdirection) | The direction. |
| `null_order` | [`NullOrder`](#nullorder) | The order of `NULL` values. |

### `SortInfo`

The sort order of a table.

| Field | Type | Description |
|-------|------|-------------|
| `sort_id` | `uint64` (optional) | The id of the sort order. |
| `table_id` | `uint64` (optional) | The table. |
| `fields` | list of [`SortField`](#sortfield) (default `[]`) | The sort keys. |

### `SortOrder`

The direction of an order.

| Value | Description |
|-------|-------------|
| `ASC` | Smallest first. |
| `DESC` | Largest first. |

### `TableChangeType`

The changes that `table_changes` returns.

| Value | Description |
|-------|-------------|
| `insertions` | The files with inserted rows. |
| `deletions` | The files with deleted rows. |

### `TableInfo`

A table of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The id of the table. |
| `schema_id` | `uint64` (optional) | The schema of the table. |
| `table_uuid` | `string` (default `""`) | The uuid of the table. |
| `table_name` | `string` | The name of the table. |
| `path` | `path` (default `""`) | The data path of the table. |
| `columns` | list of [`ColumnInfo`](#columninfo) (default `[]`) | The columns of the table. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the table. |
| `next_column_id` | `uint64` (optional) | The next free column (field) id of the table, which is larger than the id of every column the table ever had, including dropped columns. Clients that add columns use it for the ids of the new columns. |

### `TableSetting`

An option of the DuckLake that is set for a table.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The table. |
| `key` | `string` | The option. |
| `value` | `string` (default `""`) | The value of the option. |

### `Tag`

A key / value pair, e.g. a tag of a catalog entry or an option.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `string` | The key. |
| `value` | `string` (default `""`) | The value. |

### `ViewColumnTag`

A tag of a column of a view.

| Field | Type | Description |
|-------|------|-------------|
| `column_name` | `string` | The column. |
| `key` | `string` | The key of the tag. |
| `value` | `text` (optional) | The value of the tag. |

### `ViewInfo`

A view of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `view_id` | `uint64` (optional) | The id of the view. |
| `schema_id` | `uint64` (optional) | The schema of the view. |
| `view_uuid` | `string` (default `""`) | The uuid of the view. |
| `view_name` | `string` | The name of the view. |
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the view. |
| `column_aliases` | list of `string` (default `[]`) | The names of the columns of the view. |
| `sql` | `string` | The query of the view. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the view. |
| `column_tags` | list of [`ViewColumnTag`](#viewcolumntag) (default `[]`) | The tags of the columns of the view. |

<!-- END GENERATED REFERENCE -->

## Reference Implementation

The server (`src/rest/ducklake_rest_server.cpp`, `src/rest/ducklake_rest_endpoints.cpp`) serves a DuckLake catalog
that is attached to DuckDB, with any metadata backend. Every request runs in a DuckLake transaction of its own:

* Read endpoints call the SQL metadata manager of the served catalog.
* `commit_transaction` pins its transaction to the snapshot of the client, replays the requests through the DuckLake
  catalog (catalog changes) and transaction (data changes), and commits it. Commits therefore get the same conflict
  detection, retries, id assignment and statistics maintenance as regular DuckLake commits.

The client (`RESTMetadataManager`, `src/metadata_manager/rest_metadata_manager.cpp`) records the catalog changes of a
transaction as requests while they are made, and sends them together with the data changes of the transaction when it
commits. Data files are written and read by the client.

Both use the code that is generated from the [specification](#specification) (`src/include/rest/ducklake_rest_api.hpp`):
the server implements the handlers of `DuckLakeRESTEndpoints` and `DuckLakeRESTCommitHandler`, the client calls the
endpoints through `DuckLakeRESTClientAPI`. The JSON conversions that are not generated are in
`src/rest/ducklake_rest_serialization.cpp` (column statistics, rows) and `src/rest/ducklake_rest_catalog_changes.cpp`
(alterations).

### Testing

* `test/sql/rest/` tests the API, with a server that `test/template/rest_setup.test_template` starts in a database of
  its own and `test/template/rest_teardown.test_template` stops.
* `test/configs/rest.json` runs the DuckDB tests on a DuckLake that is served over REST (like
  `test/configs/attach_ducklake.json`):
  `build/release/test/unittest --test-config test/configs/rest.json --test-dir duckdb`
* `test/configs/rest_ducklake.json` runs the DuckLake tests with a REST server that its `on_init` starts for every test, and
  `DUCKLAKE_CONNECTION` pointing to the server. Tests that read the metadata catalog directly are skipped, as it is
  attached by the server:
  `build/release/test/unittest --test-config test/configs/rest_ducklake.json "test/sql/*"`
