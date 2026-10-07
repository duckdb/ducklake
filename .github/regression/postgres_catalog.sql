-- Session-local helpers that seal and copy the PostgreSQL metadata schemas of the regression benchmarks.
-- The renderer collapses this file into one line, so comments may only occupy full lines.

-- Lists the objects of a schema that a table-by-table copy would lose, or returns NULL
CREATE OR REPLACE FUNCTION pg_temp.ducklake_regression_unsupported(schema_name TEXT) RETURNS TEXT LANGUAGE sql STABLE
AS $unsupported$
SELECT string_agg(item, ', ' ORDER BY item) FROM (
	SELECT format('relation %I of kind %s', relname, relkind) AS item FROM pg_class
	WHERE relnamespace = schema_name::regnamespace AND relkind NOT IN ('r', 'i')
	UNION ALL
	SELECT format('table %I with non-heap or non-logged storage, rules, triggers, row security or inheritance', c.relname)
	FROM pg_class c JOIN pg_am am ON am.oid = c.relam
	WHERE c.relnamespace = schema_name::regnamespace AND c.relkind = 'r'
	AND (c.relpersistence <> 'p' OR c.relhasrules OR c.relhastriggers OR c.relrowsecurity OR c.relispartition
	OR c.relhassubclass OR am.amname <> 'heap' OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = c.oid))
	UNION ALL
	SELECT format('identity or generated column %I.%I', c.relname, a.attname)
	FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
	WHERE c.relnamespace = schema_name::regnamespace AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
	AND (a.attidentity <> '' OR a.attgenerated <> '')
	UNION ALL
	SELECT format('function %I', proname) FROM pg_proc WHERE pronamespace = schema_name::regnamespace
	UNION ALL
	SELECT format('type %I', t.typname) FROM pg_type t
	WHERE t.typnamespace = schema_name::regnamespace AND t.typrelid = 0
	AND NOT EXISTS (SELECT 1 FROM pg_type e WHERE e.oid = t.typelem AND e.typrelid <> 0)
) items
$unsupported$;

-- Describes every table of a schema by what a copy must reproduce, leaving out names that depend on the schema
CREATE OR REPLACE FUNCTION pg_temp.ducklake_regression_tables(schema_name TEXT)
RETURNS TABLE (relname NAME, definition TEXT) LANGUAGE sql STABLE AS $tables$
SELECT c.relname, ROW(c.relpersistence, am.amname, c.relhastriggers, c.relhasrules, c.relrowsecurity, c.reloptions,
	t.reloptions, columns.definition, indexes.definition, constraints.definition)::TEXT
FROM pg_class c JOIN pg_am am ON am.oid = c.relam LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
LEFT JOIN (
	SELECT a.attrelid, string_agg(ROW(a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.attstorage,
	a.attcollation, pg_get_expr(d.adbin, d.adrelid))::TEXT, ', ' ORDER BY a.attnum) AS definition
	FROM pg_attribute a JOIN pg_class r ON r.oid = a.attrelid
	LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
	WHERE r.relnamespace = schema_name::regnamespace AND a.attnum > 0 AND NOT a.attisdropped
	GROUP BY a.attrelid
) columns ON columns.attrelid = c.oid
LEFT JOIN (
	SELECT indrelid, string_agg(definition, ', ' ORDER BY definition) AS definition FROM (
		SELECT i.indrelid, ROW(i.indisunique, i.indisprimary, i.indkey, i.indclass, i.indoption,
		pg_get_expr(i.indexprs, i.indrelid), pg_get_expr(i.indpred, i.indrelid))::TEXT AS definition
		FROM pg_index i JOIN pg_class r ON r.oid = i.indrelid WHERE r.relnamespace = schema_name::regnamespace
	) index_definitions GROUP BY indrelid
) indexes ON indexes.indrelid = c.oid
LEFT JOIN (
	SELECT conrelid, string_agg(definition, ', ' ORDER BY definition) AS definition FROM (
		SELECT conrelid, ROW(contype, pg_get_constraintdef(oid))::TEXT AS definition
		FROM pg_constraint WHERE connamespace = schema_name::regnamespace AND conrelid <> 0
	) constraint_definitions GROUP BY conrelid
) constraints ON constraints.conrelid = c.oid
WHERE c.relnamespace = schema_name::regnamespace AND c.relkind = 'r'
$tables$;

CREATE OR REPLACE FUNCTION pg_temp.ducklake_regression_seal(seed TEXT) RETURNS VOID LANGUAGE plpgsql AS $seal$
DECLARE
	tbl TEXT;
BEGIN
	FOR tbl IN SELECT relname FROM pg_class WHERE relnamespace = seed::regnamespace AND relkind = 'r' ORDER BY relname LOOP
		EXECUTE format('ALTER TABLE %I.%I SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)',
		seed, tbl);
		EXECUTE format('ANALYZE %I.%I', seed, tbl);
	END LOOP;
END
$seal$;

-- Truncates and refills work tables defined like the seed's, which gives them fresh storage, and recreates the others
CREATE OR REPLACE FUNCTION pg_temp.ducklake_regression_copy(seed TEXT, work TEXT) RETURNS VOID LANGUAGE plpgsql
AS $copy$
DECLARE
	unsupported TEXT;
	reused TEXT;
	mismatched TEXT;
	tbl RECORD;
BEGIN
	IF seed = work THEN
		RAISE EXCEPTION 'cannot copy schema % onto itself', seed;
	END IF;
	unsupported := pg_temp.ducklake_regression_unsupported(seed);
	IF unsupported IS NOT NULL THEN
		RAISE EXCEPTION 'cannot copy schema % to %: unsupported %', seed, work, unsupported;
	END IF;
	IF to_regnamespace(work) IS NOT NULL AND pg_temp.ducklake_regression_unsupported(work) IS NOT NULL THEN
		EXECUTE format('DROP SCHEMA %I CASCADE', work);
	END IF;
	EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', work);
	FOR tbl IN SELECT w.relname FROM pg_temp.ducklake_regression_tables(work) w
	LEFT JOIN pg_temp.ducklake_regression_tables(seed) s USING (relname)
	WHERE s.definition IS DISTINCT FROM w.definition LOOP
		EXECUTE format('DROP TABLE %I.%I', work, tbl.relname);
	END LOOP;
	SELECT string_agg(format('%I.%I', work, relname), ', ') INTO reused FROM pg_temp.ducklake_regression_tables(work);
	IF reused IS NOT NULL THEN
		EXECUTE 'TRUNCATE ' || reused;
	END IF;
	FOR tbl IN SELECT c.relname, c.reloptions, t.reloptions AS toast_options
	FROM pg_class c LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
	WHERE c.relnamespace = seed::regnamespace AND c.relkind = 'r' ORDER BY c.relname LOOP
		IF to_regclass(format('%I.%I', work, tbl.relname)) IS NULL THEN
			EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL)', work, tbl.relname, seed, tbl.relname);
			IF tbl.reloptions IS NOT NULL THEN
				EXECUTE format('ALTER TABLE %I.%I SET (%s)', work, tbl.relname, array_to_string(tbl.reloptions, ', '));
			END IF;
			IF tbl.toast_options IS NOT NULL THEN
				EXECUTE format('ALTER TABLE %I.%I SET (%s)', work, tbl.relname,
				(SELECT string_agg('toast.' || option, ', ') FROM unnest(tbl.toast_options) option));
			END IF;
		END IF;
		EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I', work, tbl.relname, seed, tbl.relname);
		EXECUTE format('ANALYZE %I.%I', work, tbl.relname);
	END LOOP;
	SELECT string_agg(DISTINCT relname, ', ' ORDER BY relname) INTO mismatched FROM (
		(SELECT * FROM pg_temp.ducklake_regression_tables(seed) EXCEPT SELECT * FROM pg_temp.ducklake_regression_tables(work))
		UNION ALL
		(SELECT * FROM pg_temp.ducklake_regression_tables(work) EXCEPT SELECT * FROM pg_temp.ducklake_regression_tables(seed))
	) differing;
	IF mismatched IS NOT NULL THEN
		RAISE EXCEPTION 'copying schema % to % did not reproduce %', seed, work, mismatched;
	END IF;
END
$copy$;
