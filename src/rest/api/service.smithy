$version: "2"

metadata suppressions = [
    {
        id: "HttpMethodSemantics.UnnecessaryReadonlyTrait"
        namespace: "ducklake.rest"
        reason: "Reads that take a snapshot, filters or projections in a JSON body are POST requests"
    }
]

namespace ducklake.rest

use ducklake.rest.codegen#cppIncludes
use ducklake.rest.codegen#docSections
use ducklake.rest.codegen#wireType

/// The DuckLake REST API.
@cppIncludes([
    "common/ducklake_snapshot.hpp"
    "common/ducklake_data_file.hpp"
    "storage/ducklake_metadata_info.hpp"
    "storage/ducklake_stats.hpp"
    "duckdb/common/enums/catalog_type.hpp"
])
@docSections([
    {
        title: "Read Endpoints"
        kind: "read"
        documentation: """
            These endpoints read the catalog. Each request runs in a transaction of its own on the server.
            """
    }
    {
        title: "Server Commands"
        kind: "command"
        documentation: """
            These endpoints make the server perform work on the DuckLake outside of a client transaction, and commit it in a
            transaction of its own.
            """
    }
    {
        title: "Committing Transactions"
        kind: "commit"
        documentation: """
            A client records the changes of a transaction and sends them to `commit_transaction` when it commits. The changes
            are a list of requests that are applied in order; later requests can refer to entries created by earlier requests
            through negative placeholder ids: `-1` is the first entry (schema, table, view, ...) created in the transaction,
            `-2` the second one, etc. A request that creates an entry sets its `placeholder_id`.

            Clients send the deletes of committed data first, then the catalog changes in the order in which they were made,
            and then the inserted data in the format of the tables at the end of the transaction. This order matters: e.g. a
            `SET NOT NULL` that follows a delete of the `NULL` rows only succeeds after the delete.

            Requests refer to existing entries by their ids, so they act on the entry that the client saw, whatever other
            transactions did to its name since.
            """
    }
    {
        title: "Types"
        kind: "types"
        documentation: """
            The structures that the endpoints share. Fields that are optional can be `null` or absent; the server and the
            client leave them out when they have no value.
            """
    }
])
service DuckLake {
    version: "1"
    operations: [
        Config
        GetMetadata
        GetLatestSnapshot
        GetSnapshotAt
        Select
        ListSnapshots
        Compact
        TableChanges
        CommitTransaction
    ]
}

/// A request of a commit.
union CommitRequest {
    create_schema: CreateSchemaRequest
    create_table: CreateTableRequest
    create_view: CreateViewRequest
    create_macro: CreateMacroRequest
    alter: AlterRequest
    drop: DropRequest
    set_option: SetOptionRequest
    insert_rows: InsertRowsRequest
    insert_files: InsertFilesRequest
    delete_data: DeleteDataRequest
}

/// The path of a file or directory. Clients can have another view of the data path of the DuckLake than the server (e.g.
/// another mount point): they map the paths between their data path and the data path of the server.
@wireType("path")
string DataPath

/// An unsigned integer.
@wireType("uint64")
long UInt64

/// A timestamp with time zone, e.g. `"2025-01-01 00:00:00+00"`.
@wireType("timestamp")
string Timestamp

/// A value as text, in the [text format of DuckLake](https://ducklake.select/docs/stable/specification/data_types#type-encoding-for-statistics).
/// It is absent if the value is `NULL`.
@wireType("text")
string Text

/// A DuckLake type, e.g. `{"id": "decimal", "width": 18, "scale": 3}`.
@wireType("logical_type")
document LogicalType

/// Rows, as arrays of values of the types of their columns.
@wireType("rows")
document Rows
