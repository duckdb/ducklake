# DuckLake Data File Backends

## Goal

DuckLake data files are selectable by format. Parquet remains the default for
compatibility, and Vortex can be selected for new data files at DuckLake, schema,
or table scope.

## User Surface

DuckLake adds a `data_file_format` option with two accepted values:

- `parquet`
- `vortex`

The option follows the existing DuckLake option precedence:

1. table option
2. schema option
3. global DuckLake option
4. default `parquet`

Examples:

```sql
CALL ducklake_set_option('ducklake', 'data_file_format', 'vortex');
CALL ducklake_set_option('ducklake', 'data_file_format', 'parquet', table_name => 'orders');
ATTACH 'ducklake:metadata.db' AS ducklake (DATA_PATH 'files', DATA_FILE_FORMAT 'vortex');
```

The selected format applies to newly written data files only. Existing files
keep their original file format in metadata and remain readable. An attach-time
`DATA_FILE_FORMAT` initializes the global DuckLake option when creating a new
DuckLake; existing DuckLakes should use `ducklake_set_option` to change the
persisted global default.

## Metadata

`ducklake_data_file.file_format` is the source of truth for each data file.
Existing catalogs without a value, or with `parquet`, are interpreted as Parquet.
New Vortex files store `vortex`.

Delete files are unchanged. Positional delete files continue to be Parquet, and
deletion-vector files continue to be Puffin.

## Write Path

Insert, CTAS, flush-inlined-data, merge, and compaction resolve the target data
file format using `data_file_format`.

For Parquet, all current writer behavior and Parquet-specific options remain
unchanged.

For Vortex:

- DuckDB's `vortex` copy function is used.
- Files use the `.vortex` extension.
- Parquet-specific options are not passed.
- Vortex writes are rejected for encrypted DuckLakes until the Vortex writer
  exposes an equivalent encryption API.

## Read Path

DuckLake scan planning continues to use DuckLake's multi-file reader so file
deletes, row ids, snapshot filtering, inlined data, and mixed-format tables are
handled in one path.

Parquet files use the existing Parquet file reader. Vortex files use a
DuckLake-owned Vortex file reader backed by DuckDB's `read_vortex` table
function.

## Compatibility Notes

Parquet files keep field-id metadata. Vortex files currently do not expose
DuckLake field ids through the DuckDB Vortex extension, so DuckLake-created
Vortex files use the existing field-id-missing fallback mapping path.

This supports straightforward append/read workloads and column renames, but
complex schema evolution across Vortex files should be treated as experimental
until field-id metadata is available in Vortex.
