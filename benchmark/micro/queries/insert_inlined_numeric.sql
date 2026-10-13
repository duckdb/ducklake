BEGIN;
INSERT INTO lake.num SELECT i, i::INTEGER, i / 7, i / 3, DATE '2000-01-01' + (i % 10000)::INTEGER, TIMESTAMP '2000-01-01' + to_seconds(i) FROM range(100000) r(i);
SELECT CASE WHEN (SELECT count(*) FROM lake.num) <> 100000 OR (SELECT count(*) FROM ducklake_list_files('lake', 'num')) <> 0 THEN error('the rows were not inlined') END;
ROLLBACK;
