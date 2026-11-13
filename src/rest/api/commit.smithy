$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#arrowOnly
use ducklake.rest.codegen#cppMap
use ducklake.rest.codegen#cppType

/// Commits a set of changes atomically.
///
/// If the transaction conflicts with a transaction that committed after its snapshot, the server retries the
/// commit when that is safe; otherwise it fails with a `409`. The snapshot is the snapshot the changes are based
/// on: the server detects conflicts with every snapshot committed after it. Without a snapshot, the changes are
/// based on the latest snapshot.
@http(method: "POST", uri: "/v1/transactions/commit")
operation CommitTransaction {
    input := with [SnapshotReference] {
        /// The commit information of the snapshot.
        commit_info: CommitInfo

        /// How the server retries the commit on conflicts.
        retry: RetryConfig

        /// The changes, applied in order, e.g. `[{"create_table": {...}}, {"insert_rows": {...}}]`.
        requests: CommitRequestList = []
    }

    output := {
        /// The committed snapshot, absent if the requests did not create a snapshot (e.g. if they only set options).
        /// If other transactions created snapshots since the snapshot of the request, its id is more than one above the
        /// id of the snapshot of the request.
        snapshot: Snapshot

        /// The entries the transaction created, with the ids the commit gave them.
        entry_ids: CreatedEntryList = []

        /// The options of the tables the transaction created or altered, with the ids the commit gave the tables.
        table_options: OptionSettingList = []
    }
}

/// Creates a schema.
structure CreateSchemaRequest {
    /// The placeholder id of the schema: a negative number, e.g. `-1` for the first entry created in the transaction.
    /// Later requests of the transaction refer to the schema with it, and `entry_ids` returns the id that the commit
    /// gave the schema with it.
    @required
    placeholder_id: Long

    /// The schema that holds the new schema, absent for a schema at the top level. A negative id is the placeholder id
    /// of a schema created in the transaction.
    parent_schema_id: Long

    /// The name of the schema.
    @required
    schema_name: String

    /// The uuid of the schema.
    schema_uuid: String

    /// The data path of the schema.
    path: DataPath

    /// Do nothing if the schema exists.
    if_not_exists: Boolean = false
}

/// Creates a table.
///
/// Columns get the field ids `1..n` in the order of the columns (nested fields are numbered depth-first), as in `CREATE
/// TABLE`.
structure CreateTableRequest {
    /// The placeholder id of the table, which `insert_rows`, `insert_files` and `delete_data` refer to the table with.
    /// See `create_schema`.
    @required
    placeholder_id: Long

    /// The schema of the table. A negative id is the placeholder id of a schema created in the transaction.
    @required
    schema_id: Long

    /// The name of the table.
    @required
    table_name: String

    /// The uuid of the table.
    table_uuid: String

    /// The data path of the table.
    path: DataPath

    /// The columns of the table.
    @required
    columns: ColumnDefinitionList

    /// The partition keys as SQL expressions, e.g. `["year(ts)"]`.
    partitioned_by: StringList = []

    /// The sort keys as SQL expressions.
    sorted_by: StringList = []

    /// Table options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`.
    options: StringMap = {}

    /// The comment of the table.
    @cppType("RESTOptional<string>")
    comment: String

    /// An id of the client for the partitioning of the table. Files that are inserted later in the transaction refer to
    /// the partitioning with it.
    partition_id: UInt64
}

/// Creates a view.
structure CreateViewRequest {
    /// See `create_schema`.
    @required
    placeholder_id: Long

    /// The schema of the view, see `create_table`.
    @required
    schema_id: Long

    /// The name of the view.
    @required
    view_name: String

    /// The uuid of the view.
    view_uuid: String

    /// The SQL dialect of the view, only `duckdb` for now.
    dialect: String = "duckdb"

    /// The query of the view.
    @required
    sql: String

    /// The names of the columns of the view.
    column_aliases: StringList = []

    /// The comment of the view.
    @cppType("RESTOptional<string>")
    comment: String

    /// The comments of the columns of the view, by column name.
    column_comments: StringMap = {}
}

/// Creates a macro.
structure CreateMacroRequest {
    /// See `create_schema`.
    @required
    placeholder_id: Long

    /// The schema of the macro, see `create_table`.
    @required
    schema_id: Long

    /// The macro (its ids are ignored).
    @required
    macro: MacroInfo
}

/// Alters a table or a view.
structure AlterRequest {
    /// The table or view. A negative id is the placeholder id of an entry created in the transaction.
    @required
    entry_id: Long

    /// The change.
    @required
    alter: Alteration

    /// For `set_partitioned_by`: an id of the client for the new partitioning (as for `create_table`).
    partition_id: UInt64
}

/// Drops a schema, a table, a view or a macro. A schema has to be empty.
structure DropRequest {
    /// The type of the entry.
    @required
    entry_type: DropEntryType

    /// The entry. A negative id is the placeholder id of an entry created in the transaction.
    @required
    entry_id: Long
}

/// The type of an entry that `drop` drops.
enum DropEntryType {
    /// A schema.
    SCHEMA = "schema"

    /// A table.
    TABLE = "table"

    /// A view.
    VIEW = "view"

    /// A scalar macro.
    SCALAR_MACRO = "scalar_macro"

    /// A table macro.
    TABLE_MACRO = "table_macro"
}

/// Sets or resets a DuckLake option (`ducklake_metadata`).
structure SetOptionRequest {
    /// The option.
    @required
    option: OptionSetting

    /// Reset the option instead of setting it.
    reset: Boolean = false
}

/// Inserts rows that are inlined in the catalog.
structure InsertRowsRequest {
    /// The table.
    @required
    table_id: Long

    /// The rows to insert: a table with the columns of the table. `<n>` is the position of the request in `requests`.
    @required
    @arrowOnly("rows.<n>")
    rows: Rows

    /// The row ids of the rows (for updates that keep the row ids of the updated rows).
    row_ids: LongList = []

    /// The statistics of the rows per column.
    @cppType("map<FieldIndex, DuckLakeColumnStats>")
    @cppMap(keyField: "column_id", value: ColumnStats)
    column_stats: ColumnStatsEntryList = []
}

/// Registers Parquet files that were written by the client.
structure InsertFilesRequest {
    /// The table.
    @required
    table_id: Long

    /// The files.
    @required
    files: DataFileList

    /// The name maps of the files, which map the columns of the files by name. A file refers to its name map with its
    /// `mapping_id`, an id of the client.
    name_maps: NameMapList = []
}

/// Deletes rows from a table.
structure DeleteDataRequest {
    /// The table.
    @required
    table_id: Long

    /// Delete files written by the client.
    delete_files: DeleteFileList = []

    /// The row ids of the inlined rows that are deleted.
    @cppType("set<idx_t>")
    inlined_row_ids: UInt64List = []

    /// Deletes of rows of data files that are inlined in the catalog.
    file_row_deletes: FileRowDeletesList = []

    /// Data files that are deleted entirely.
    dropped_files: DroppedFileList = []

    /// Whether a delete was evaluated against the table, even if it did not delete any rows. This makes the commit
    /// conflict with concurrent inserts.
    delete_attempted: Boolean = false
}

/// How a commit is retried on conflicts.
structure RetryConfig {
    /// The number of retries.
    @required
    max_retry_count: UInt64

    /// The wait before the first retry.
    @required
    retry_wait_ms: UInt64

    /// The factor the wait grows with on every retry.
    @required
    retry_backoff: Double
}

/// An entry that a commit created.
structure CreatedEntry {
    /// The placeholder id of the request that created the entry.
    @required
    placeholder_id: Long

    /// The type of the entry, e.g. `TABLE_ENTRY`.
    @required
    type: String

    /// The id the commit gave the entry.
    @required
    id: UInt64
}

/// An option of the DuckLake.
structure OptionSetting {
    /// The option, e.g. `data_inlining_row_limit`.
    @required
    key: String

    /// The value of the option.
    value: String = ""

    /// The schema, for options of a schema.
    schema_id: UInt64

    /// The table, for options of a table. A table created in the transaction takes its options from `create_table`.
    table_id: Long
}

/// Deletes of rows of a data file that are inlined in the catalog.
structure FileRowDeletes {
    /// The data file.
    @required
    data_file_id: UInt64

    /// The positions of the deleted rows in the file.
    @required
    @cppType("set<idx_t>")
    row_ids: UInt64List
}

/// A data file that is deleted entirely.
structure DroppedFile {
    /// The data file.
    @required
    data_file_id: UInt64

    /// The path of the file.
    @required
    path: DataPath

    /// The number of rows of the file.
    @required
    record_count: UInt64

    /// The size of the file.
    @required
    file_size_bytes: UInt64
}
