$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#arrowOnly
use ducklake.rest.codegen#cppClass
use ducklake.rest.codegen#cppMap
use ducklake.rest.codegen#cppMember
use ducklake.rest.codegen#cppType
use ducklake.rest.codegen#customSerialization
use ducklake.rest.codegen#hasMember
use ducklake.rest.codegen#nullIf
use ducklake.rest.codegen#nullMember

/// A snapshot of the DuckLake, which identifies the state of the catalog to read.
@cppClass("DuckLakeSnapshot")
@nullIf("value.snapshot_id == DConstants::INVALID_INDEX")
structure Snapshot {
    /// The id of the snapshot.
    @required
    snapshot_id: UInt64

    /// The schema version of the snapshot, which changes with every change of the catalog.
    @required
    schema_version: UInt64

    /// The next free id of a catalog entry.
    @required
    next_catalog_id: UInt64

    /// The next free id of a file.
    @required
    next_file_id: UInt64
}

/// The snapshot that a request reads. A `snapshot` is used as-is, which also allows reading at snapshots that
/// have been expired since the client loaded them. Without `snapshot` and `snapshot_id`, the latest snapshot is read.
@mixin
structure SnapshotReference {
    /// The snapshot, as returned by `snapshots/latest` and `snapshots/at`.
    snapshot: Snapshot

    /// The id of the snapshot.
    snapshot_id: UInt64
}

/// A key / value pair, e.g. a tag of a catalog entry or an option.
@cppClass("DuckLakeTag")
structure Tag {
    /// The key.
    @required
    key: String

    /// The value.
    value: String = ""
}

/// An option of the DuckLake that is set for a schema.
@cppClass("DuckLakeSchemaSetting")
structure SchemaSetting {
    /// The schema.
    schema_id: UInt64

    /// The option.
    @required
    @cppMember("tag.key")
    key: String

    /// The value of the option.
    @cppMember("tag.value")
    value: String = ""
}

/// An option of the DuckLake that is set for a table.
@cppClass("DuckLakeTableSetting")
structure TableSetting {
    /// The table.
    table_id: UInt64

    /// The option.
    @required
    @cppMember("tag.key")
    key: String

    /// The value of the option.
    @cppMember("tag.value")
    value: String = ""
}

/// The global metadata of the DuckLake (the contents of `ducklake_metadata`).
///
/// ```json
/// {
///   "tags": [{"key": "version", "value": "1.1-dev1"}, {"key": "data_path", "value": "/data/"}],
///   "table_settings": [{"table_id": 1, "key": "data_inlining_row_limit", "value": "10"}]
/// }
/// ```
@cppClass("DuckLakeMetadata")
structure Metadata {
    /// The global options, including the `version` and the `data_path` of the DuckLake.
    tags: TagList = []

    /// The options set for schemas.
    schema_settings: SchemaSettingList = []

    /// The options set for tables.
    table_settings: TableSettingList = []
}

/// A schema of the catalog.
@cppClass("DuckLakeSchemaInfo")
structure SchemaInfo {
    /// The id of the schema.
    @cppMember("id")
    schema_id: UInt64

    /// The parent schema, for nested schemas.
    @cppMember("parent_id")
    parent_schema_id: UInt64

    /// The uuid of the schema.
    @cppMember("uuid")
    schema_uuid: String = ""

    /// The name of the schema.
    @required
    @cppMember("name")
    schema_name: String

    /// The data path of the schema.
    path: DataPath = ""

    /// The tags of the schema.
    tags: TagList = []
}

/// A column of a table, as stored in `ducklake_column`.
@cppClass("DuckLakeColumnInfo")
structure ColumnInfo {
    /// The field id of the column.
    @cppMember("id")
    column_id: UInt64

    /// The name of the column.
    @required
    @cppMember("name")
    column_name: String

    /// The type of the column, as a type name of the DuckLake specification (e.g. `int32`, `varchar`).
    @required
    @cppMember("type")
    column_type: String

    /// The value of the column for rows that were written before the column was added.
    initial_default: Text

    /// The default value of the column.
    default_value: Text

    /// Whether the default is a `literal` or an `expression`.
    default_value_type: String = ""

    /// Whether the column can hold `NULL`.
    nulls_allowed: Boolean = true

    /// The tags of the column.
    tags: TagList = []

    /// The fields of nested columns.
    children: ColumnInfoList = []
}

/// A table of the catalog.
@cppClass("DuckLakeTableInfo")
structure TableInfo {
    /// The id of the table.
    @cppMember("id")
    table_id: UInt64

    /// The schema of the table.
    schema_id: UInt64

    /// The uuid of the table.
    @cppMember("uuid")
    table_uuid: String = ""

    /// The name of the table.
    @required
    @cppMember("name")
    table_name: String

    /// The data path of the table.
    path: DataPath = ""

    /// The columns of the table.
    columns: ColumnInfoList = []

    /// The tags of the table.
    tags: TagList = []

    /// The next free column (field) id of the table, which is larger than the id of every column the table ever had,
    /// including dropped columns. Clients that add columns use it for the ids of the new columns.
    next_column_id: UInt64
}

/// A tag of a column of a view.
@cppClass("DuckLakeViewColumnTag")
structure ViewColumnTag {
    /// The column.
    @required
    column_name: String

    /// The key of the tag.
    @required
    key: String

    /// The value of the tag.
    value: Text
}

/// A view of the catalog.
@cppClass("DuckLakeViewInfo")
structure ViewInfo {
    /// The id of the view.
    @cppMember("id")
    view_id: UInt64

    /// The schema of the view.
    schema_id: UInt64

    /// The uuid of the view.
    @cppMember("uuid")
    view_uuid: String = ""

    /// The name of the view.
    @required
    @cppMember("name")
    view_name: String

    /// The SQL dialect of the view.
    dialect: String = "duckdb"

    /// The names of the columns of the view.
    column_aliases: StringList = []

    /// The query of the view.
    @required
    sql: String

    /// The tags of the view.
    tags: TagList = []

    /// The tags of the columns of the view.
    column_tags: ViewColumnTagList = []
}

/// The type of a macro.
enum MacroType {
    /// A scalar macro.
    SCALAR = "scalar"

    /// A table macro.
    TABLE = "table"
}

/// A parameter of a macro.
@cppClass("DuckLakeMacroParameters")
structure MacroParameter {
    /// The name of the parameter.
    @required
    parameter_name: String

    /// The type of the parameter, `unknown` if it has none.
    parameter_type: String = ""

    /// The default value of the parameter.
    default_value: Text

    /// Whether the default is a `literal` or an `expression`.
    default_value_type: String = ""
}

/// An implementation (overload) of a macro.
@cppClass("DuckLakeMacroImplementation")
structure MacroImplementation {
    /// The SQL dialect of the implementation.
    dialect: String = "duckdb"

    /// The body of the macro.
    @required
    sql: String

    /// The type of the macro.
    @required
    type: MacroType

    /// The parameters of the macro.
    parameters: MacroParameterList = []
}

/// A macro of the catalog.
@cppClass("DuckLakeMacroInfo")
structure MacroInfo {
    /// The schema of the macro.
    schema_id: UInt64

    /// The id of the macro.
    macro_id: UInt64

    /// The name of the macro.
    @required
    macro_name: String

    /// The implementations of the macro.
    implementations: MacroImplementationList = []
}

/// A partition key of a table.
@cppClass("DuckLakePartitionFieldInfo")
structure PartitionField {
    /// The position of the key in the partitioning.
    @required
    partition_key_index: UInt64

    /// The column the key is computed from.
    @cppMember("field_id")
    column_id: UInt64

    /// The transform of the column, e.g. `identity` or `year`.
    @required
    transform: String
}

/// The partitioning of a table.
@cppClass("DuckLakePartitionInfo")
structure PartitionInfo {
    /// The id of the partitioning.
    @cppMember("id")
    partition_id: UInt64

    /// The table.
    table_id: UInt64

    /// The partition keys.
    fields: PartitionFieldList = []
}

/// The direction of a sort key.
@cppClass("OrderType")
enum SortDirection {
    ASCENDING = "ASCENDING"
    DESCENDING = "DESCENDING"
    ORDER_DEFAULT = "ORDER_DEFAULT"
}

/// Where a sort key orders `NULL` values.
@cppClass("OrderByNullType")
enum NullOrder {
    NULLS_FIRST = "NULLS_FIRST"
    NULLS_LAST = "NULLS_LAST"
    ORDER_DEFAULT = "ORDER_DEFAULT"
}

/// A sort key of a table.
@cppClass("DuckLakeSortFieldInfo")
structure SortField {
    /// The position of the key in the sort order.
    @required
    sort_key_index: UInt64

    /// The expression to sort by.
    @required
    expression: String

    /// The SQL dialect of the expression.
    dialect: String = "duckdb"

    /// The direction.
    @required
    sort_direction: SortDirection

    /// The order of `NULL` values.
    @required
    null_order: NullOrder
}

/// The sort order of a table.
@cppClass("DuckLakeSortInfo")
structure SortInfo {
    /// The id of the sort order.
    @cppMember("id")
    sort_id: UInt64

    /// The table.
    table_id: UInt64

    /// The sort keys.
    fields: SortFieldList = []
}

/// The catalog at a snapshot.
@cppClass("DuckLakeCatalogInfo")
structure CatalogInfo {
    /// The schemas.
    schemas: SchemaInfoList = []

    /// The tables.
    tables: TableInfoList = []

    /// The views.
    views: ViewInfoList = []

    /// The macros.
    macros: MacroInfoList = []

    /// The partitionings of the tables.
    partitions: PartitionInfoList = []

    /// The sort orders of the tables.
    sorts: SortInfoList = []
}

/// The statistics of a column of a table.
@cppClass("DuckLakeGlobalColumnStatsInfo")
structure GlobalColumnStats {
    /// The column.
    column_id: UInt64

    /// Whether the column contains `NULL` values.
    @hasMember("has_contains_null")
    contains_null: Boolean

    /// Whether the column contains `NaN` values.
    @hasMember("has_contains_nan")
    contains_nan: Boolean

    /// The minimum value.
    @cppMember("min_val")
    @hasMember("has_min")
    min_value: String

    /// The maximum value.
    @cppMember("max_val")
    @hasMember("has_max")
    max_value: String

    /// Type-specific statistics.
    @hasMember("has_extra_stats")
    extra_stats: String

    /// Whether the minimum is exact (rather than a bound).
    min_is_exact: Boolean = false

    /// Whether the maximum is exact (rather than a bound).
    max_is_exact: Boolean = false
}

/// The statistics of a table.
@cppClass("DuckLakeGlobalStatsInfo")
structure GlobalTableStats {
    /// The table.
    table_id: UInt64

    /// Whether the statistics have been initialized.
    initialized: Boolean = true

    /// The number of rows of the table, absent if unknown.
    @nullMember("record_count_unknown")
    record_count: UInt64

    /// The next free row id of the table.
    next_row_id: UInt64 = 0

    /// The size of the files of the table, absent if unknown.
    @cppMember("table_size_bytes")
    @nullMember("missing_table_size")
    file_size_bytes: UInt64

    /// The statistics of the columns.
    column_stats: GlobalColumnStatsList = []
}

/// The statistics of a column of a file or of inserted rows.
///
/// ```json
/// {"type": {"id": "int32"}, "min_value": "1", "max_value": "2", "null_count": 0, "value_count": 2,
///  "column_size_bytes": 10, "any_valid": true, "bounds_unknown": false, "min_is_exact": true, "max_is_exact": true}
/// ```
@mixin(
    localTraits: [cppClass, customSerialization]
)
@cppClass("DuckLakeColumnStats")
@customSerialization
structure ColumnStats {
    /// The type of the column.
    @required
    type: LogicalType

    /// The minimum value.
    min_value: String

    /// The maximum value.
    max_value: String

    /// The number of `NULL` values.
    null_count: UInt64

    /// The number of values.
    value_count: UInt64

    /// The size of the column.
    column_size_bytes: UInt64 = 0

    /// Whether the column contains `NaN` values.
    contains_nan: Boolean

    /// Whether the column contains any value that is not `NULL`.
    any_valid: Boolean = true

    /// Whether the minimum and maximum are unknown.
    bounds_unknown: Boolean = false

    /// Whether the minimum is exact.
    min_is_exact: Boolean = false

    /// Whether the maximum is exact.
    max_is_exact: Boolean = false

    /// Type-specific statistics.
    extra_stats: String
}

/// The format of a file.
@cppClass("DeleteFileFormat")
enum FileFormat {
    /// A Parquet file.
    PARQUET = "parquet"

    /// A deletion vector in a Puffin file (Iceberg V3).
    PUFFIN = "puffin"
}

/// A file that a client reads.
@cppClass("DuckLakeFileData")
@nullIf("value.path.empty()")
structure FileData {
    /// The absolute path of the file.
    @required
    path: DataPath

    /// The key the file is encrypted with, if any.
    encryption_key: Base64Binary = ""

    /// The size of the file.
    file_size_bytes: UInt64 = 0

    /// The size of the footer of the file.
    footer_size: UInt64

    /// The format of the file.
    format: FileFormat = "parquet"

    /// The number of rows of the file. For a delete file: the number of deleted rows.
    row_count: UInt64

    /// The number of row groups of the file.
    row_group_count: UInt64
}

/// The min/max statistics of a column of a file.
@mixin(
    localTraits: [cppClass]
)
@cppClass("DuckLakeFileColumnStats")
structure FileColumnStats {
    /// The minimum value.
    @cppMember("min")
    @hasMember("has_min")
    min_value: String

    /// The maximum value.
    @cppMember("max")
    @hasMember("has_max")
    max_value: String

    /// Whether the column contains `NaN` values.
    @hasMember("has_contains_nan")
    contains_nan: Boolean
}

/// A data file of a table that is visible in a snapshot, together with its deletes.
@cppClass("DuckLakeFileListEntry")
structure FileListEntry {
    /// The id of the data file.
    data_file_id: UInt64

    /// The id of the file.
    file_id: UInt64

    /// The data file.
    @required
    file: FileData

    /// The delete file of the data file.
    delete_file: FileData

    /// The row id of the first row of the file.
    row_id_start: UInt64

    /// The snapshot that added the file.
    snapshot_id: UInt64

    /// Only read rows that were inserted up to this snapshot.
    snapshot_filter_max: UInt64

    /// Only read rows that were inserted from this snapshot.
    snapshot_filter_min: UInt64

    /// Skip the rows that these snapshots wrote, which rewrote rows that other files hold as well.
    snapshot_filter_excluded: UInt64List = []

    /// The name map of the file.
    mapping_id: UInt64

    /// The partitioning that the file was written with, absent if the table was not partitioned then. The partition
    /// keys of the values are those of this partitioning, which is not necessarily the current one.
    partition_id: UInt64

    /// The values of the partition keys of the file, as text.
    partition_values: FilePartitionValueList = []

    /// The positions of the rows of the file that are deleted by deletions inlined in the catalog.
    inlined_file_deletions: UInt64List = []

    /// The statistics of the columns of the scan's `runtime_filter_columns`.
    @cppMap(keyField: "column_id", value: FileColumnStats)
    column_min_max: FileColumnStatsEntryList = []
}

/// A data file of a table with the ids of its files, used to plan deletes and updates.
@cppClass("DuckLakeFileListExtendedEntry")
structure FileListExtendedEntry {
    /// The id of the data file.
    file_id: UInt64

    /// The id of the delete file.
    delete_file_id: UInt64

    /// The data file.
    @required
    file: FileData

    /// The delete file.
    delete_file: FileData

    /// The row id of the first row of the file.
    row_id_start: UInt64

    /// The snapshot that added the file.
    snapshot_id: UInt64

    /// The snapshot that added the delete file.
    delete_file_begin_snapshot: UInt64

    /// The number of rows of the file.
    @required
    @cppMember("row_count")
    record_count: UInt64

    /// The name map of the file.
    mapping_id: UInt64
}

/// A delete file written by a client, which replaces the previous delete file of its data file.
@cppClass("DuckLakeDeleteFile")
structure DeleteFile {
    /// The data file whose rows are deleted. If it has a delete file at the snapshot of the transaction, the new delete
    /// file replaces that file: it has the deletes of the old delete file as well.
    data_file_id: UInt64

    /// The path of the data file.
    data_file_path: DataPath = ""

    /// The path of the delete file.
    @required
    @cppMember("file_name")
    path: DataPath

    /// The format of the delete file.
    format: FileFormat = "parquet"

    /// The number of deleted rows.
    @required
    delete_count: UInt64

    /// The size of the delete file.
    @required
    file_size_bytes: UInt64

    /// The size of the footer of the delete file.
    footer_size: UInt64 = 0

    /// The number of row groups of the delete file.
    row_group_count: UInt64

    /// The key the file is encrypted with, if any.
    encryption_key: Base64Binary = ""

    /// The snapshot of the deletes, if it is not the snapshot of the commit.
    begin_snapshot: UInt64

    /// The last snapshot whose deletes the file holds, for partial delete files.
    max_snapshot: UInt64
}

/// The value of a partition key for the rows of a file.
@cppClass("DuckLakeFilePartition")
structure FilePartitionValue {
    /// The partition key.
    @required
    @cppMember("partition_column_idx")
    partition_key_index: UInt64

    /// The value.
    partition_value: Text
}

/// A Parquet file written by a client. The file includes the statistics of its columns, so the server does not need to
/// read it.
@cppClass("DuckLakeDataFile")
structure DataFile {
    /// The path of the file.
    @required
    @cppMember("file_name")
    path: DataPath

    /// The number of rows.
    @required
    @cppMember("row_count")
    record_count: UInt64

    /// The size of the file.
    @required
    file_size_bytes: UInt64

    /// The size of the footer.
    footer_size: UInt64

    /// The number of row groups.
    row_group_count: UInt64

    /// The partitioning the file was written for. Partitionings set in the transaction are referred to by the
    /// `partition_id` of their request.
    partition_id: UInt64

    /// The key the file is encrypted with, if any.
    encryption_key: Base64Binary = ""

    /// The name map of the file. Name maps created in the transaction are referred to by the `mapping_id` of their
    /// request.
    mapping_id: UInt64

    /// The statistics of the columns of the file.
    @cppMap(keyField: "column_id", value: ColumnStats)
    column_stats: ColumnStatsEntryList = []

    /// The values of the partition keys.
    partition_values: FilePartitionValueList = []

    /// Delete files for the rows of the file.
    delete_files: DeleteFileList = []
}

/// The type of a compaction.
enum CompactionType {
    /// Merges small adjacent files.
    MERGE_ADJACENT_TABLES = "merge_adjacent"

    /// Rewrites files with many deleted rows.
    REWRITE_DELETES = "rewrite_delete"

    /// Writes the data and deletions that are inlined in the catalog to files.
    FLUSH_INLINED_DATA = "flush_inlined_data"
}

/// A data file whose rows were deleted between two snapshots.
@cppClass("DuckLakeDeleteScanEntry")
structure DeleteScanEntry {
    /// The data file.
    @required
    file: FileData

    /// The delete file at the end of the range.
    delete_file: FileData

    /// The delete file at the start of the range.
    previous_delete_file: FileData

    /// The number of rows of the data file.
    @required
    @cppMember("row_count")
    record_count: UInt64

    /// The row id of the first row.
    row_id_start: UInt64

    /// The name map of the file.
    mapping_id: UInt64

    /// The snapshot of the deletes.
    snapshot_id: UInt64

    /// The snapshot that inserted every row of the data file, unset when its rows carry their own.
    insert_snapshot: UInt64

    /// Whether the data file was deleted entirely.
    file_deleted: Boolean = false

    /// The first snapshot of the range.
    start_snapshot: UInt64

    /// The last snapshot of the range.
    end_snapshot: UInt64

    /// The id of the data file.
    file_id: UInt64

    /// The deleted rows of the file that are inlined in the catalog, with the snapshot that deleted them.
    @cppMap(keyField: "row_id", valueField: "snapshot_id")
    inlined_file_deletions: InlinedFileDeletionsEntryList = []
}

/// A column of a name map: maps a column of a file to a field of the table.
@cppClass("DuckLakeNameMapColumnInfo")
structure NameMapColumn {
    /// The id of the entry in the name map.
    @required
    column_id: UInt64

    /// The name of the column in the file.
    @required
    source_name: String

    /// The field of the table.
    target_field_id: UInt64

    /// Whether the column is a hive partition of the file path.
    hive_partition: Boolean = false

    /// The entry of the parent column, for nested columns.
    parent_column: UInt64
}

/// A name map (`ducklake_column_mapping` / `ducklake_name_mapping`) of files that were added with
/// `ducklake_add_data_files`.
@cppClass("DuckLakeColumnMappingInfo")
structure NameMap {
    /// The table.
    table_id: UInt64

    /// The id of the name map.
    mapping_id: UInt64

    /// The type of the name map.
    @cppMember("map_type")
    type: String = "map_by_name"

    /// The columns of the name map.
    @cppMember("map_columns")
    columns: NameMapColumnList = []
}

/// A snapshot, with its commit information.
@cppClass("DuckLakeSnapshotInfo")
structure SnapshotInfo {
    /// The id of the snapshot.
    @required
    @cppMember("id")
    snapshot_id: UInt64

    /// When the snapshot was committed.
    @required
    @cppMember("time")
    snapshot_time: Timestamp

    /// The schema version of the snapshot.
    @required
    schema_version: UInt64

    /// The next free file id of the snapshot.
    @required
    next_file_id: UInt64

    /// The changes of the snapshot, as in `ducklake_snapshot_changes`.
    @cppMember("change_info.changes_made")
    changes_made: String = ""

    /// The author of the commit.
    author: Text

    /// The commit message.
    commit_message: Text

    /// Extra information of the commit.
    commit_extra_info: Text
}

/// The commit information of a transaction.
@cppClass("DuckLakeSnapshotCommit")
structure CommitInfo {
    /// The author of the commit.
    author: Text

    /// The commit message.
    commit_message: Text

    /// Extra information of the commit.
    commit_extra_info: Text

    /// Whether the commit information was set.
    is_commit_info_set: Boolean = false
}

/// A column of a set of rows.
structure ResultColumn {
    /// The name of the column.
    @required
    name: String

    /// The type of the column.
    @required
    type: LogicalType
}

/// The columns and rows of a set of rows: the table of an Arrow IPC stream.
@mixin
structure RowSet {
    /// The columns of the rows, with the names and the types of the columns of the stream.
    @required
    @arrowOnly("columns")
    columns: ResultColumnList

    /// The rows, which are the batches of the stream.
    @required
    @arrowOnly("rows")
    rows: Rows
}

/// A set of rows, e.g. the rows of an inlined data table, as the table of an Arrow IPC stream. The columns of the table
/// are the row id, the begin and the end snapshot of the rows, and the columns of the table.
structure InlinedRows with [RowSet] {}

/// A column that a request reads, as the field id of a top-level column, or of a field of a struct.
structure ColumnIdentifier {
    /// The field id of the column or field.
    @required
    field_id: UInt64

    /// The fields of a struct that the client reads, as column identifiers. The other fields are `NULL` in the
    /// response. The whole column if empty.
    children: ColumnIdentifierList = []
}

/// The definition of a column in a catalog change.
structure ColumnDefinition {
    /// The name of the column.
    @required
    column_name: String

    /// The type of the column.
    @required
    column_type: LogicalType

    /// The default of the column, as a SQL expression.
    @cppMember("default_value")
    default: String

    /// The text of the value that rows from before the column was added get, already cast to the column type.
    /// A client evaluates a literal default in the time zone of its session and sends the result, with an explicit
    /// offset for `TIMESTAMPTZ` and `TIMETZ`, and may omit a NULL. A request with a literal default that depends on the
    /// time zone and no `initial_default` is rejected, as the server does not know the time zone of the client.
    @cppType("RESTOptional<string>")
    initial_default: String

    /// The comment of the column.
    @cppType("RESTOptional<string>")
    comment: String

    /// Whether the column is `NOT NULL`.
    not_null: Boolean = false
}

/// A `ColumnStats` with its `column_id`.
structure ColumnStatsEntry with [ColumnStats] {
    @required
    column_id: UInt64
}

/// A `FileColumnStats` with its `column_id`.
structure FileColumnStatsEntry with [FileColumnStats] {
    @required
    column_id: UInt64
}

structure InlinedFileDeletionsEntry {
    @required
    row_id: UInt64

    @required
    snapshot_id: UInt64
}

list ColumnIdentifierList {
    member: ColumnIdentifier
}

list ColumnDefinitionList {
    member: ColumnDefinition
}

list ColumnInfoList {
    member: ColumnInfo
}

list ColumnStatsEntryList {
    member: ColumnStatsEntry
}

list CommitRequestList {
    member: CommitRequest
}

list CreatedEntryList {
    member: CreatedEntry
}

list DataFileList {
    member: DataFile
}

list DeleteFileList {
    member: DeleteFile
}

list DeleteScanEntryList {
    member: DeleteScanEntry
}

list DroppedFileList {
    member: DroppedFile
}

list FileColumnStatsEntryList {
    member: FileColumnStatsEntry
}

list FileListEntryList {
    member: FileListEntry
}

list FileListExtendedEntryList {
    member: FileListExtendedEntry
}

list FilePartitionValueList {
    member: FilePartitionValue
}

list FileRowDeletesList {
    member: FileRowDeletes
}

list GlobalColumnStatsList {
    member: GlobalColumnStats
}

list GlobalTableStatsList {
    member: GlobalTableStats
}

list InlinedFileDeletionsEntryList {
    member: InlinedFileDeletionsEntry
}

list LogicalTypeList {
    member: LogicalType
}

list LongList {
    member: Long
}

list MacroImplementationList {
    member: MacroImplementation
}

list MacroInfoList {
    member: MacroInfo
}

list MacroParameterList {
    member: MacroParameter
}

list NameMapColumnList {
    member: NameMapColumn
}

list NameMapList {
    member: NameMap
}

list OptionSettingList {
    member: OptionSetting
}

list PartitionFieldList {
    member: PartitionField
}

list PartitionInfoList {
    member: PartitionInfo
}

list ResultColumnList {
    member: ResultColumn
}

list SchemaInfoList {
    member: SchemaInfo
}

list SchemaSettingList {
    member: SchemaSetting
}

list SnapshotInfoList {
    member: SnapshotInfo
}

list SortFieldList {
    member: SortField
}

list SortInfoList {
    member: SortInfo
}

list StringList {
    member: String
}

list TableInfoList {
    member: TableInfo
}

list TableSettingList {
    member: TableSetting
}

list TagList {
    member: Tag
}

list UInt64List {
    member: UInt64
}

list ValueList {
    member: Text
}

list ViewColumnTagList {
    member: ViewColumnTag
}

list ViewInfoList {
    member: ViewInfo
}

map StringMap {
    key: String
    value: String
}

/// The details of an error.
structure ErrorInfo {
    /// The type of the DuckDB exception, e.g. `Catalog`.
    type: String = "Error"

    /// The message of the error.
    message: String = ""

    /// The HTTP status code of the response.
    code: UInt64 = 500
}

/// The body of a response with a status code other than 200: `409` for transaction conflicts, `404` for catalog errors
/// (e.g. entries that do not exist or unknown endpoints), `400` for invalid input, `401` for a missing or invalid token,
/// `501` for operations that are not supported and `500` otherwise. The status code is repeated in the `code` of the error.
///
/// ```json
/// {"error": {"type": "Catalog", "message": "Table with id 1000 does not exist at snapshot 3"}}
/// ```
structure ErrorResponse {
    /// The error.
    @required
    error: ErrorInfo
}
