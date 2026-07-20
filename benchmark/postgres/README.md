# PostgreSQL metadata benchmarks

`run.py` creates deterministic metadata-only fixtures and measures metadata operations through DuckDB. Fixture setup is excluded from the measured interval. Every trial uses a fresh PostgreSQL schema so mutation benchmarks are independent.

The supported scales are `small` (100), `medium` (10,000), `large` (100,000), and `xlarge` (1,000,000). Depending on the operation, the scale controls snapshots, data files, or cleanup candidates.

The runner expects a PostgreSQL-enabled DuckLake build at `/work/ducklake`, the PostgreSQL scanner extension beside that build, and standard `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and `PGSSLMODE` environment variables. For example:

```sh
python3 benchmark/postgres/run.py \
  --adapter postgres_fast \
  --scale xlarge \
  --operation expire_snapshots \
  --selectivity 0.5 \
  --trials 3 \
  --output /work/results/fast.jsonl
```

The Kubernetes manifest is intentionally credential-free. It expects a `metadata-postgres` Secret containing the PostgreSQL environment variables.

Reported timings are medians of three trials from the same protected in-cluster pod. Each timed trial launches a fresh DuckDB CLI, loads the PostgreSQL extension, attaches the catalog, runs the operation, and exits, so these are cold-process end-to-end measurements rather than PostgreSQL server execution time.

`max_rss_kib` is the Python process's cumulative `RUSAGE_CHILDREN` high-water mark. It can include fixture children and earlier trials, so it is retained for diagnostics but is not used for memory-improvement claims. The curated JSONL files contain the raw measurements used by [RESULTS.md](RESULTS.md); do not summarize the whole results directory because exploratory runs may contain duplicate trial identities.
