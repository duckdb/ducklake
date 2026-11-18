$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#cppClass

/// The change of an `alter` request: an object with exactly one member, the name of the change.
/// Columns are referred to by name; nested fields by their path from the top-level column, e.g. `["s", "a"]`.
/// Expressions are SQL expressions.
///
/// ```json
/// {"rename_column": {"old_name": "i", "new_name": "j"}}
/// ```
union Alteration {
    /// Renames the table.
    rename_table: RenameTable

    /// Renames the view.
    rename_view: RenameView

    /// Renames a column.
    rename_column: RenameColumn

    /// Renames a field of a nested column.
    rename_field: RenameField

    /// Adds a column.
    add_column: AddColumn

    /// Adds a field to a nested column.
    add_field: AddField

    /// Removes a column.
    remove_column: RemoveColumn

    /// Removes a field of a nested column.
    remove_field: RemoveField

    /// Changes the type of a column.
    change_column_type: ChangeColumnType

    /// Sets or drops the default of a column.
    set_default: SetDefault

    /// Makes a column `NOT NULL`.
    set_not_null: SetNotNull

    /// Makes a column nullable.
    drop_not_null: DropNotNull

    /// Sets the partitioning of the table.
    set_partitioned_by: SetPartitionedBy

    /// Sets the sort order of the table.
    set_sorted_by: SetSortedBy

    /// Sets options of the table.
    set_options: SetOptions

    /// Resets options of the table.
    reset_options: ResetOptions

    /// Sets the comment of a table or view.
    set_comment: SetComment

    /// Sets the comment of a column of a table or view.
    set_column_comment: SetColumnComment
}

/// The type of a catalog entry.
@cppClass("CatalogType")
enum CatalogEntryType {
    TABLE_ENTRY = "TABLE_ENTRY"
    VIEW_ENTRY = "VIEW_ENTRY"
}

structure RenameTable {
    /// The new name of the table.
    @required
    new_name: String
}

structure RenameView {
    /// The new name of the view.
    @required
    new_name: String
}

structure RenameColumn {
    /// The current name of the column.
    @required
    old_name: String

    /// The new name of the column.
    @required
    new_name: String
}

structure RenameField {
    /// The path of the field.
    @required
    column_path: StringList

    /// The new name of the field.
    @required
    new_name: String
}

structure AddColumn {
    /// The column.
    @required
    column: ColumnDefinition

    /// Do nothing if the column exists.
    if_not_exists: Boolean = false
}

structure AddField {
    /// The path of the parent column.
    @required
    column_path: StringList

    /// The field.
    @required
    field: ColumnDefinition

    /// Do nothing if the field exists.
    if_not_exists: Boolean = false
}

structure RemoveColumn {
    /// The name of the column.
    @required
    column_name: String

    /// Do nothing if the column does not exist.
    if_exists: Boolean = false

    /// Also drop what depends on the column.
    cascade: Boolean = false
}

structure RemoveField {
    /// The path of the field.
    @required
    column_path: StringList

    /// Do nothing if the field does not exist.
    if_exists: Boolean = false

    /// Also drop what depends on the field.
    cascade: Boolean = false
}

structure ChangeColumnType {
    /// The path of the column.
    @required
    column_path: StringList

    /// The new type.
    @required
    column_type: LogicalType

    /// The `USING` expression that converts the old values.
    expression: String
}

structure SetDefault {
    /// The path of the column.
    @required
    column_path: StringList

    /// The default, absent to drop the default.
    expression: String
}

structure SetNotNull {
    /// The path of the column.
    @required
    column_path: StringList
}

structure DropNotNull {
    /// The path of the column.
    @required
    column_path: StringList
}

structure SetPartitionedBy {
    /// The partition keys as SQL expressions, empty to reset the partitioning.
    partitioned_by: StringList = []
}

structure SetSortedBy {
    /// The sort keys, e.g. `["j DESC NULLS LAST"]`, empty to reset the sort order.
    sorted_by: StringList = []
}

structure SetOptions {
    /// The options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`.
    options: StringMap = {}
}

structure ResetOptions {
    /// The options to reset.
    options: StringList = []
}

structure SetComment {
    /// Whether the comment is of a table or a view.
    @required
    entry_type: CatalogEntryType

    /// The comment, absent to drop the comment.
    comment: Text
}

structure SetColumnComment {
    /// Whether the column is of a table or a view.
    @required
    entry_type: CatalogEntryType

    /// The name of the column.
    @required
    column_name: String

    /// The comment, absent to drop the comment.
    comment: Text
}
