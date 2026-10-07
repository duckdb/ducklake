BEGIN;
INSERT INTO lake.m SELECT i, 'v' || i, i * 0.5 FROM range(1000000, 1001000) r(i);
DELETE FROM lake.m WHERE id >= 1000000;
INSERT INTO lake.t SELECT i, 'v' || i, i * 0.5 FROM range(200000, 201000) r(i);
SELECT q, c, s FROM (
SELECT 'v0100' q, count(*) c, sum(x) s FROM lake.m AT (VERSION => 100)
UNION ALL SELECT 'v0150', count(*), sum(x) FROM lake.m AT (VERSION => 150)
UNION ALL SELECT 'v0200', count(*), sum(x) FROM lake.m AT (VERSION => 200)
UNION ALL SELECT 'v0250', count(*), sum(x) FROM lake.m AT (VERSION => 250)
UNION ALL SELECT 'v0300', count(*), sum(x) FROM lake.m AT (VERSION => 300)
UNION ALL SELECT 'v0350', count(*), sum(x) FROM lake.m AT (VERSION => 350)
UNION ALL SELECT 'v0400', count(*), sum(x) FROM lake.m AT (VERSION => 400)
UNION ALL SELECT 'v0450', count(*), sum(x) FROM lake.m AT (VERSION => 450)
UNION ALL SELECT 'v0500', count(*), sum(x) FROM lake.m AT (VERSION => 500)
UNION ALL SELECT 'v0550', count(*), sum(x) FROM lake.m AT (VERSION => 550)
UNION ALL SELECT 'v0600', count(*), sum(x) FROM lake.m AT (VERSION => 600)
UNION ALL SELECT 'v0650', count(*), sum(x) FROM lake.m AT (VERSION => 650)
UNION ALL SELECT 'v0700', count(*), sum(x) FROM lake.m AT (VERSION => 700)
UNION ALL SELECT 'v0750', count(*), sum(x) FROM lake.m AT (VERSION => 750)
UNION ALL SELECT 'v0800', count(*), sum(x) FROM lake.m AT (VERSION => 800)
UNION ALL SELECT 'v0850', count(*), sum(x) FROM lake.m AT (VERSION => 850)
UNION ALL SELECT 'v0900', count(*), sum(x) FROM lake.m AT (VERSION => 900)
UNION ALL SELECT 'v0950', count(*), sum(x) FROM lake.m AT (VERSION => 950)
UNION ALL SELECT 'v1000', count(*), sum(x) FROM lake.m AT (VERSION => 1000)
UNION ALL SELECT 'cur', count(*), sum(x) FROM lake.t
) ORDER BY q;
