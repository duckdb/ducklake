$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#arrowOnly
use ducklake.rest.codegen#cppType
use ducklake.rest.codegen#noPrefix

/// Returns how a client connects to a catalog of the server. This is the first request of a client.
///
/// The client merges the properties of the response with its own: the `defaults` are used unless the client has a
/// value of its own, and the `overrides` are used whatever the client has. The `prefix` override is the prefix of the
/// catalog in the URLs of all other requests. The `endpoints` are the requests that the server supports, as the method and
/// the path without prefix (e.g. `GET /v1/snapshots`).
@http(method: "GET", uri: "/v1/config")
@readonly
@noPrefix
operation Config {
    input := {
        /// The name that the catalog is served under. The catalog of the server if it serves exactly one.
        @httpQuery("catalog")
        catalog_name: String
    }

    output := {
        /// Properties that the client uses if it has no value for them.
        defaults: StringMap = {}

        /// Properties that the client uses instead of its own values, e.g. `prefix`.
        overrides: StringMap = {}

        /// The requests that the server supports.
        endpoints: StringList = []
    }
}

/// Returns the global metadata of the DuckLake (the contents of `ducklake_metadata`).
///
/// The `data_path` is the resolved data path of the served DuckLake.
@http(method: "GET", uri: "/v1/metadata")
@readonly
operation GetMetadata {
    output: Metadata
}

/// Returns the latest snapshot together with (optionally) the catalog and the table statistics of that snapshot.
///
/// Clients pass `cached_metadata_version` and `cached_data_version` when they start a transaction, so they get the
/// latest snapshot and, if it changed since, its catalog and statistics in a single round-trip.
@http(method: "GET", uri: "/v1/snapshots/latest")
@readonly
operation GetLatestSnapshot {
    input := with [SnapshotIncludes] {}
    output: SnapshotData
}

/// Returns the snapshot of an `AT` clause together with (optionally) the catalog and the table statistics of that
/// snapshot. The request fails if there is no such snapshot, e.g. because it expired.
@http(method: "GET", uri: "/v1/snapshots/at")
@readonly
operation GetSnapshotAt {
    input := with [SnapshotIncludes] {
        /// The unit of the value of the `AT` clause.
        @required
        @httpQuery("unit")
        unit: AtUnit

        /// The snapshot id or the name of a snapshot tag (for `version`), or the timestamp (for `timestamp`).
        @required
        @httpQuery("value")
        value: String

        /// Whether a timestamp finds the last snapshot before or the first snapshot after it.
        @httpQuery("bound")
        bound: SnapshotBound = "upper"

        /// The newest snapshot that the clause can find, snapshots committed after a client's transaction started are
        /// not visible to it.
        @httpQuery("newest_snapshot_id")
        newest_snapshot_id: UInt64
    }

    output: SnapshotData
}

/// What a request for a snapshot returns besides the snapshot.
@mixin
structure SnapshotIncludes {
    /// Return the catalog of the snapshot.
    @httpQuery("include_catalog")
    include_catalog: Boolean = false

    /// Return the table statistics of the snapshot.
    @httpQuery("include_stats")
    include_stats: Boolean = false

    /// The schema version of the latest catalog the client has loaded. If the snapshot has a different schema
    /// version, its catalog is returned.
    @httpQuery("cached_metadata_version")
    cached_metadata_version: UInt64

    /// The next file id of the snapshot of the latest statistics the client has loaded. If the snapshot has a
    /// different next file id, its statistics are returned.
    @httpQuery("cached_data_version")
    cached_data_version: UInt64
}

/// A snapshot with its catalog and statistics.
structure SnapshotData {
    /// The snapshot.
    @required
    snapshot: Snapshot

    /// The catalog of the snapshot.
    catalog: CatalogInfo

    /// The statistics of the tables of the snapshot.
    @cppType("RESTOptional<vector<DuckLakeGlobalStatsInfo>>")
    stats: GlobalTableStatsList
}

/// Plans a scan of a table.
///
/// Returns the data files of the table that are visible in a snapshot, together with their delete files and
/// inlined deletions, and the rows of the table that are inlined in the catalog. `inlined_data` holds the visible
/// inlined rows, with the row id, the begin and end snapshot
/// and the columns of the table at the snapshot, whatever the schema of the table was when the rows were written. Values of a
/// column that was changed from `TIMESTAMP` to `TIMESTAMPTZ` are the timestamps in UTC. It is absent if the table has
/// no inlined rows.
///
/// The files of a partitioned table have the `partition_id` of the partitioning they were written with and the values of
/// their partition keys (`partition_values`), so clients can group the files by partition.
///
/// The `aggregates` of a request without `filters` are answered from the statistics of the table where the server can,
/// instead of listing the files: `aggregate_results` has an entry for each aggregate, and tells whether the server
/// answered it. If it answered all of them, the response has no files and no inlined rows: the results are the answer.
/// Otherwise the response is the one of a scan, with the results of the aggregates that were answered.
///
/// A scan of many files is returned in pages. The response has the first page of files, and `page_tokens` has the token
/// of every other page. A request with a `page_token` and no other member (except for the table) returns the page of
/// the token: the other members are those of the request that returned the token, and are not repeated. The pages of a
/// response can be requested in parallel and in any order. They belong to the catalog and the table of the request, and
/// expire after a while: the request fails with a `404`, and a client plans the scan again.
@http(method: "POST", uri: "/v1/tables/{table_id}/scan")
@readonly
operation Select {
    input := with [SnapshotReference] {
        /// The table to scan.
        @required
        @httpLabel
        table_id: UInt64

        /// A page token of an earlier response: returns that page of files. The request has no other members then.
        page_token: String

        /// Aggregates over the whole table that the server answers from statistics if it can. Not with `filters`.
        aggregates: AggregateList = []

        /// Return the extended file list used to plan deletes and updates in `extended_files`, instead of `files`.
        extended: Boolean = false

        /// Filters to prune files with.
        filters: ScanFilters

        /// The ids of the column mappings that the client has: the response only has the other column mappings that its
        /// files refer to.
        @cppType("set<idx_t>")
        known_column_mappings: UInt64List = []

        /// The columns that the client reads. The inlined rows only have these columns, besides the row id and the
        /// begin and end snapshot. All columns of the table if absent.
        @cppType("RESTOptional<vector<RESTColumnIdentifier>>")
        columns: ColumnIdentifierList
    }

    output := {
        /// The files of the scan.
        files: FileListEntryList = []

        /// The files of the scan, for `extended` scans.
        extended_files: FileListExtendedEntryList

        /// The inlined rows of the table, absent if it has no inlined data.
        @arrowOnly("inlined_data")
        inlined_data: InlinedRows

        /// The tokens of the other pages of the files of the scan, if there are many. The files of the scan are the files
        /// of the response and the files of all its pages.
        page_tokens: StringList = []

        /// The results of the `aggregates` of the request, in their order.
        aggregate_results: AggregateResultList = []

        /// The column mappings that the files of the response refer to (by their `mapping_id`), and that the client does
        /// not have. They map the columns of files that were added with `ducklake_add_data_files` by name.
        column_mappings: NameMapList = []
    }
}

/// Returns the snapshots, optionally filtered.
@http(method: "GET", uri: "/v1/snapshots")
@readonly
operation ListSnapshots {
    input := {
        /// Exclude the latest snapshot.
        @httpQuery("exclude_latest")
        exclude_latest: Boolean = false

        /// Only snapshots created before this timestamp.
        @cppType("string")
        @httpQuery("older_than")
        older_than: Timestamp

        /// Only these snapshots.
        @cppType("RESTOptional<vector<idx_t>>")
        @httpQuery("snapshot_id")
        snapshot_ids: UInt64List

        /// The maximum number of entries to return. The server picks a number if absent, and can return fewer entries.
        @httpQuery("page_size")
        page_size: UInt64

        /// The `next_page_token` of the previous page. The first page if absent.
        @httpQuery("page_token")
        page_token: String
    }

    output := {
        /// The snapshots, ordered by their ids.
        snapshots: SnapshotInfoList = []

        /// The token of the next page, absent on the last page.
        next_page_token: String
    }
}

/// Returns the files that hold the changes of a table between two snapshots, for change data feeds.
///
/// `inlined_data` holds the inlined rows that were inserted (`insertions`) or deleted
/// (`deletions`) in the range, with the row id, the begin and end snapshot and the columns of the table at the last
/// snapshot of the range. It is absent if the table has no inlined rows.
///
/// Many files are returned in pages, see `scan`: a request is either the request for the changes, or has a `page_token`
/// and no other member (except for the table).
@http(method: "POST", uri: "/v1/tables/{table_id}/changes")
@readonly
operation TableChanges {
    input := with [SnapshotReference] {
        /// The table. The snapshot is the last snapshot of the range.
        @required
        @httpLabel
        table_id: UInt64

        /// The first snapshot of the range.
        start_snapshot: Snapshot

        /// The id of the first snapshot of the range.
        start_snapshot_id: UInt64

        /// A page token of an earlier response: returns that page of files. The request has no other members then.
        page_token: String

        /// The changes to return. Required, unless the request has a `page_token`.
        changes: TableChangeType

        /// The ids of the column mappings that the client has: the response only has the other column mappings that its
        /// files refer to.
        @cppType("set<idx_t>")
        known_column_mappings: UInt64List = []

        /// The columns that the client reads. The inlined rows only have these columns, besides the row id and the
        /// begin and end snapshot. All columns of the table if absent.
        @cppType("RESTOptional<vector<RESTColumnIdentifier>>")
        columns: ColumnIdentifierList
    }

    output := {
        /// For `insertions`: the files with inserted rows.
        insertions: FileListEntryList = []

        /// For `deletions`: the files with deleted rows.
        deletions: DeleteScanEntryList = []

        /// The inlined rows that changed, absent if the table has no inlined data.
        @arrowOnly("inlined_data")
        inlined_data: InlinedRows

        /// The tokens of the other pages of the files, see `scan`.
        page_tokens: StringList = []

        /// The column mappings that the files of the response refer to (by their `mapping_id`), and that the client does
        /// not have. They map the columns of files that were added with `ducklake_add_data_files` by name.
        column_mappings: NameMapList = []
    }
}

/// The unit of an `AT` clause.
enum AtUnit {
    /// A snapshot id.
    VERSION = "version"

    /// A timestamp.
    TIMESTAMP = "timestamp"
}

/// How a timestamp finds a snapshot.
enum SnapshotBound {
    /// The last snapshot before the timestamp.
    UPPER = "upper"

    /// The first snapshot after the timestamp.
    LOWER = "lower"

    /// The last snapshot before the timestamp, for time travel: refuses timestamps before the newest snapshot that an
    /// expiry removed.
    TIME_TRAVEL = "time_travel"
}

/// Filters of a scan. Filters are optional: a server may ignore them, and a client applies them to the scanned rows
/// regardless.
///
/// `expressions` are SQL expressions that all hold for the rows of the scan. They refer to the columns of the table
/// by name, and to the fields of structs by their path (`st.a`). The server prunes files on the statistics of their
/// columns with the parts of the expressions it understands: comparisons of a column with a constant, `IN` lists of
/// constants, `IS [NOT] NULL`, and `AND` / `OR` combinations of those. The server ignores the other parts, and leaves
/// out an `OR` entirely if it does not understand one of its parts.
///
/// `order_by` is the order of the files: by the `min` or `max` of a column in a file, in the format of the `aggregates`.
/// The files without statistics come last. A top-N uses it to read the files with the best values first, so that its
/// bound prunes the other files early. For `runtime_filter_columns` the server returns the min/max statistics of the
/// files in `column_min_max`, so the client can prune files at runtime.
///
/// ```json
/// {"expressions": ["i >= 150", "s = 'a' OR st.a IN (1, 2)"],
///  "order_by": [{"function": "max", "column_id": 1, "sort_order": "DESC"}],
///  "runtime_filter_columns": [1]}
/// ```
structure ScanFilters {
    /// SQL expressions that hold for all rows of the scan.
    expressions: StringList = []

    /// The order of the files. The terms that are not a `min` or `max` of a column are ignored.
    order_by: FileOrderList = []

    /// The columns to return the min/max statistics of.
    runtime_filter_columns: UInt64List = []
}

/// The function of an aggregate.
enum AggregateFunction {
    /// `count(*)`.
    COUNT_STAR = "count_star"

    /// `count(column)`: the number of rows where the column is not NULL.
    COUNT = "count"

    /// `min(column)`.
    MIN = "min"

    /// `max(column)`.
    MAX = "max"
}

/// An aggregate over the rows of a table.
structure Aggregate {
    /// The function.
    @required
    function: AggregateFunction

    /// The column, as its field id. For aggregates other than `count_star`: a top-level column.
    column_id: UInt64
}

/// The direction of an order.
enum SortOrder {
    /// Smallest first.
    ASC = "ASC"

    /// Largest first.
    DESC = "DESC"
}

/// An order of the files of a scan: by the `min` or `max` of a column in the file.
structure FileOrder {
    /// The function: `min` or `max`.
    @required
    function: AggregateFunction

    /// The column, as its field id.
    @required
    column_id: UInt64

    /// The direction.
    sort_order: SortOrder = "ASC"
}

list FileOrderList {
    member: FileOrder
}

/// The result of an aggregate of a scan.
structure AggregateResult {
    /// Whether the server answered the aggregate. If not, the files of the scan have to be read to answer it.
    @required
    answered: Boolean

    /// The result, `null` if the aggregate is NULL (e.g. the `min` of a column without values) or not answered.
    value: Text
}

list AggregateList {
    member: Aggregate
}

list AggregateResultList {
    member: AggregateResult
}

/// The changes that `table_changes` returns.
enum TableChangeType {
    /// The files with inserted rows.
    INSERTIONS = "insertions"

    /// The files with deleted rows.
    DELETIONS = "deletions"
}
