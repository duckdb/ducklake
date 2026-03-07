# Merge Adjacent Files

The `ducklake_merge_adjacent_files` function combines small Parquet files created by multiple inserts into larger files without affecting snapshots. This is useful for optimizing read performance and reducing file count.

## Usage

### Basic Syntax

```sql
-- Merge adjacent files for all partitions
SELECT table_name, files_processed, files_created
FROM ducklake_merge_adjacent_files('schema_name', 'table_name');

-- Merge with optional parameters
SELECT table_name, files_processed, files_created
FROM ducklake_merge_adjacent_files(
    'schema_name',
    'table_name',
    min_file_size => 100000,      -- Minimum file size in bytes
    max_file_size => 10485760,     -- Maximum file size in bytes (100 MB)
    max_compacted_files => 10        -- Maximum number of files to merge per operation
    partition_filter => 'date_col = 2025-01-01'  -- Filter by partition values
);
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|----------|-------------|
| `schema_name` | VARCHAR | - | Schema name (optional if using 2-argument form) |
| `table_name` | VARCHAR | - | Table name |
| `min_file_size` | UBIGINT | - | Minimum file size in bytes to consider for merging |
| `max_file_size` | UBIGINT | 104857600 (100 MB) | Maximum file size to aim for when merging |
| `max_compacted_files` | UBIGINT | - | Maximum number of files to merge in a single operation |
| `partition_filter` | VARCHAR | - | Partition filter to apply (see below) |

## Partition Filtering

The `partition_filter` parameter allows you to selectively merge files based on partition values. This is particularly useful for large tables where you only want to merge specific partitions.

### Special Keyword

**`NOT_PARTITIONED`** - Filter to merge only files that are not partitioned.

```sql
SELECT table_name, files_processed, files_created
FROM ducklake_merge_adjacent_files('schema', 'table',
    partition_filter => 'NOT_PARTITIONED');
```

### SQL WHERE Clauses

You can use any valid SQL WHERE clause syntax to filter by partition values:

#### Basic Column Comparison

```sql
-- Filter by exact value
partition_filter => "date_col = '2025-01-01'"

-- Filter with comparison operators
partition_filter => "date_col >= '2025-01-01'"
partition_filter => "amount > 1000"
partition_filter => "status = 'active'"
```

#### Multiple Columns and Operators

```sql
-- Filter by multiple columns
partition_filter => "date_col = '2025-01-01' AND amount > 1000"

-- Use IN operator
partition_filter => "date_col IN ('2025-01-01', '2025-01-02', '2025-01-03')"

-- Use BETWEEN operator
partition_filter => "amount BETWEEN 100 AND 1000"

-- Use complex boolean logic
partition_filter => "status = 'active' AND (priority = 'high' OR urgent = true)"
```

### Type Casting

Partition values are automatically cast to the correct SQL type based on the column's data type:

| Column Type | Cast Example |
|-------------|-------------|
| INTEGER | `'100'::INTEGER` |
| BIGINT | `'1000000000000'::BIGINT` |
| DATE | `'2025-01-01'::DATE` |
| TIMESTAMP | `'2025-01-01 12:00:00'::TIMESTAMP` |
| BOOLEAN | `'true'::BOOLEAN` |
| VARCHAR | `'active'::VARCHAR` |
| DOUBLE | `'3.14'::DOUBLE` |
| DECIMAL | `'100.50'::DECIMAL` |

### Supported Data Types

- INTEGER
- BIGINT
- TINYINT, SMALLINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT
- DATE
- TIMESTAMP, TIMESTAMP_NS, TIMESTAMP_MS, TIMESTAMP_SEC
- BOOLEAN
- FLOAT, DOUBLE
- DECIMAL
- VARCHAR

### Partition Transformations

If your table uses partition transformations like `DAY()`, `MONTH()`, or `YEAR()`, you can reference the **original column name** in your filter:

```sql
-- Table partitioned by DAY(date_col)
partition_filter => "DAY(date_col) = 1"

-- Table partitioned by MONTH(date_col) or YEAR(date_col)
partition_filter => "MONTH(date_col) = 1 AND YEAR(date_col) = 2025"
```

The system automatically uses the transformed partition values (e.g., "01" for DAY, "2025" for YEAR) stored in the metadata, so your comparison value will match correctly.

### Column Validation

The function validates that all columns referenced in your WHERE clause are actual partition columns. If you reference a non-partition column, you'll get an error:

```
Error: Invalid partition filter: column 'other_col' is not a partition column.
Partition columns are: date_col, amount
```

This helps catch typos and ensures you're only filtering by valid partition columns.

### Security and Validation

- **SQL Syntax Validation**: Uses DuckDB's parser to validate SQL syntax
- **Single Statement Check**: Ensures only one statement is allowed (prevents multi-statement injection)
- **Column Reference Detection**: Walks expression tree to find actual column references
- **Type Safety**: All values are properly cast to prevent type errors

### Return Values

The function returns the following columns:

| Column | Type | Description |
|---------|------|-------------|
| `schema_name` | VARCHAR | Schema name (when using 2-argument form) |
| `table_name` | VARCHAR | Table name |
| `files_processed` | BIGINT | Number of files processed in the merge operation |
| `files_created` | BIGINT | Number of files created by the merge operation |

**Note**: When merging multiple partition groups, you may see multiple rows returned (one for each partition group that matches the filter and has files to merge).

### Examples

#### Example 1: Merge by Date Range

```sql
-- Create a table with daily partitions
CREATE TABLE sales (date_col DATE, amount BIGINT, value VARCHAR);
ALTER TABLE ducklake.sales SET PARTITIONED BY (date_col);

-- Insert data for several days
INSERT INTO sales VALUES ('2025-01-01'::DATE, 1000, 'a');
INSERT INTO sales VALUES ('2025-01-01'::DATE, 1500, 'b');
INSERT INTO sales VALUES ('2025-01-02'::DATE, 2000, 'c');
INSERT INTO sales VALUES ('2025-01-03'::DATE, 3000, 'd');

-- Merge only files from January 1st
SELECT table_name, files_processed, files_created
FROM ducklake_merge_adjacent_files('ducklake', 'sales',
    partition_filter => "date_col = '2025-01-01'");
```

#### Example 2: Merge by Status

```sql
-- Create a partitioned table
CREATE TABLE orders (status VARCHAR, priority VARCHAR, amount BIGINT);
ALTER TABLE ducklake.orders SET PARTITIONED BY (status, priority);

-- Insert data
INSERT INTO orders VALUES ('active', 'high', 1000, 'a');
INSERT INTO orders VALUES ('active', 'high', 1500, 'b');
INSERT INTO orders VALUES ('active', 'medium', 2000, 'c');
INSERT INTO orders VALUES ('active', 'low', 500, 'd');
INSERT INTO orders VALUES ('inactive', 'low', 600, 'e');

-- Merge only active orders
SELECT table_name, files_processed, files_created
FROM ducklake_merge_adjacent_files('ducklake', 'orders',
    partition_filter => "status = 'active' AND priority = 'high'");
```

#### Example 3: Merge Non-Partitioned Files

```sql
-- Create a table without partitions
CREATE TABLE logs (message TEXT, level INTEGER);
ALTER TABLE ducklake.logs SET PARTITIONED BY (level);

-- Insert data without explicit partitioning
INSERT INTO logs VALUES ('info', 1);
INSERT INTO logs VALUES ('info', 2);
INSERT INTO logs VALUES ('info', 3);
INSERT INTO logs VALUES ('debug', 4);

-- Merge non-partitioned files only
SELECT table_name, files_processed, files_created
FROM ducklake_merge_adjacent_files('ducklake', 'logs',
    partition_filter => 'NOT_PARTITIONED');
```

#### Example 4: Using Transformations

```sql
-- Create a table with DAY transformation
CREATE TABLE daily_stats (date_col DATE, metric_name VARCHAR, value BIGINT);
ALTER TABLE ducklake.daily_stats SET PARTITIONED BY (DAY(date_col));

-- Insert data across multiple days
INSERT INTO daily_stats VALUES ('2025-01-01'::DATE, 'page_views', 1000);
INSERT INTO daily_stats VALUES ('2025-01-01'::DATE, 'page_views', 1500);
INSERT INTO daily_stats VALUES ('2025-01-02'::DATE, 'page_views', 2000);
INSERT INTO daily_stats VALUES ('2025-01-03'::DATE, 'page_views', 2500);
INSERT INTO daily_stats VALUES ('2025-01-04'::DATE, 'page_views', 3000);

-- Merge files from day 1 (the DAY function extracts "01" from the date)
SELECT table_name, files_processed, files_created
FROM ducklake_merge_adjacent_files('ducklake', 'daily_stats',
    partition_filter => "DAY(date_col) = 1");
```

### Notes

- Only files within the same partition group are merged together
- The `NOT_PARTITIONED` keyword allows merging unpartitioned files separately
- Partition filtering is applied before file size filtering
- The function validates SQL syntax and column references
- For best performance, merge files that are close to your target file size

### Related Functions

- `ducklake_merge_adjacent_files` - Merge adjacent files (supports partition filtering)
- `ducklake_rewrite_data_files` - Rewrite files with high delete counts
- `ducklake_set_option` - Set global or table-specific options

For more information, see the DuckDB documentation on [SQL statements](https://duckdb.org/docs/stable/sql/statements/attach.html).
