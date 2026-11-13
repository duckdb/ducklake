$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#cppType

/// Compacts the DuckLake on the server.
///
/// The server merges or rewrites data files, or writes inlined data to files, and commits the result in a transaction
/// of its own, so the compaction does not see the uncommitted changes of a client. The files are read and written by
/// the server; the parameters are the options of `ducklake_merge_adjacent_files`, `ducklake_rewrite_data_files` and
/// `ducklake_flush_inlined_data`.
@http(method: "POST", uri: "/v1/compactions")
operation Compact {
    input := {
        /// The type of the compaction.
        @required
        type: CompactionType

        /// The schema of `table_name`, the default schema if absent. For `flush_inlined_data`, all tables of the schema
        /// are flushed if there is no `table_name`.
        schema_name: String

        /// Only compact this table, all tables if absent.
        table_name: String

        /// The maximum number of files to compact.
        max_compacted_files: UInt64

        /// For `merge_adjacent`: only merge files of at least this size.
        min_file_size: UInt64

        /// For `merge_adjacent`: only merge files of at most this size.
        max_file_size: UInt64

        /// For `merge_adjacent`: only merge files written after this timestamp.
        @cppType("string")
        newer_than: Timestamp

        /// For `merge_adjacent`: only merge files written before this timestamp.
        @cppType("string")
        older_than: Timestamp

        /// For `rewrite_delete`: the fraction of deleted rows above which files are rewritten.
        delete_threshold: Double
    }

    output := {
        /// The compactions that were performed, per table.
        results: CompactionResultList = []
    }
}

/// The compaction of a table.
structure CompactionResult {
    /// The schema of the table.
    @required
    schema_name: String

    /// The name of the table.
    @required
    table_name: String

    /// For `merge_adjacent` and `rewrite_delete`: the number of files that were compacted.
    files_processed: UInt64

    /// For `merge_adjacent` and `rewrite_delete`: the number of files that were written.
    files_created: UInt64

    /// For `flush_inlined_data`: the number of rows that were written to files.
    rows_flushed: UInt64
}

list CompactionResultList {
    member: CompactionResult
}
