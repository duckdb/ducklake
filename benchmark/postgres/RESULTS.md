# PostgreSQL metadata benchmark results

These results compare DuckLake's generic PostgreSQL metadata adapter with `postgres_fast` from the same release build. They were collected on 2026-07-20 against PostgreSQL 18.3 (`cnpg` shard `shard-002`) from the protected `ducklake-bench-runner` pod. Each value is the median of three cold-process end-to-end trials.

## Xlarge results

| Operation | Fixture | Generic | `postgres_fast` | Latency reduction | Speedup |
|---|---:|---:|---:|---:|---:|
| Expire snapshots | 1M snapshots, 500K expired | 37.715 s | 4.343 s | **88.49%** | **8.68x** |
| Find snapshots to expire (`dry_run`) | 1M snapshots, 500K selected | 2.590 s | 2.498 s | **3.53%** | **1.04x** |
| Aggregate table information | 1M data files, 100K delete files, 1K tables | 0.601 s | 0.381 s | **36.55%** | **1.58x** |
| Read stats needed by one-table commit | 1M synthetic column-stat rows | 0.567 s | 0.230 s | **59.42%** | **2.46x** |

The stats result is an isolated metadata-query microbenchmark, not a full commit benchmark. The optimization reduces rows materialized for a one-table write from 1,000,000 to 100 (**99.99% fewer rows**). It is implemented in the shared commit path, so PostgreSQL benefits without requiring `postgres_fast`.

Snapshot listing, current-snapshot lookup, cleanup-candidate listing, and file discovery deliberately retain the existing PostgreSQL implementation: native alternatives were benchmarked but were neutral or slower. Their implementation-path improvement is therefore **0%** rather than shipping a regression.

## Interpretation

The expiration fixture is intentionally snapshot-heavy and has no large secondary table/file graph. It measures the principal failure mode—pulling a million candidate snapshots into DuckDB and issuing client-generated cleanup—while correctness for dependent metadata is covered separately by SQL tests and a bidirectional metadata-state comparison.

The timing includes DuckDB process startup, extension loading, catalog attachment, network traffic, result materialization, and process exit. It is not PostgreSQL server-only execution time. The raw authoritative trials are in [`results/release.jsonl`](results/release.jsonl); the isolated stats-query trials are in [`results/commit_stats.jsonl`](results/commit_stats.jsonl).

The JSONL `max_rss_kib` field is not used for claims because `RUSAGE_CHILDREN` is cumulative across fixture and trial child processes.
