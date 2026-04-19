-- Verify that TTL expression overflow is detected and blocked instead of silently
-- deleting data. Previously, Date + toIntervalDay(N) used UInt16 arithmetic that
-- silently wrapped on overflow, corrupting part min/max TTL metadata and causing
-- entire parts to be dropped during merge.
--
-- The fix evaluates the TTL expression twice inside the TTL path only:
-- once with the original types and once with widened `Date32`/`DateTime64` inputs.
-- If the timestamps differ, TTL arithmetic overflow is detected and the mutation
-- is marked as failed (UNFINISHED) before any rows are deleted.

-- Case 1: `MODIFY TTL` with overflow should fail the background mutation.
-- Non-replicated MergeTree waits for mutation by default (alter_sync=1),
-- so the error is returned synchronously as UNFINISHED.
DROP TABLE IF EXISTS t_ttl_overflow;
CREATE TABLE t_ttl_overflow (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_overflow VALUES ('2024-01-01', 1), ('2024-06-15', 2), ('2025-12-31', 3);
ALTER TABLE t_ttl_overflow MODIFY TTL day + toIntervalDay(46000); -- { serverError UNFINISHED }
-- Data must remain intact.
SELECT count() FROM t_ttl_overflow;
DROP TABLE t_ttl_overflow;

-- Case 2: materialize_ttl_after_modify = 0, then explicit MATERIALIZE TTL.
DROP TABLE IF EXISTS t_ttl_overflow2;
CREATE TABLE t_ttl_overflow2 (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_overflow2 VALUES ('2024-01-01', 10), ('2024-06-15', 20);
SET materialize_ttl_after_modify = 0;
ALTER TABLE t_ttl_overflow2 MODIFY TTL day + toIntervalDay(46000);
-- Metadata changed but no materialization yet; data is intact.
SELECT count() FROM t_ttl_overflow2;
-- Explicit `MATERIALIZE TTL` should fail with overflow error.
SET mutations_sync = 1;
ALTER TABLE t_ttl_overflow2 MATERIALIZE TTL; -- { serverError UNFINISHED }
SELECT count() FROM t_ttl_overflow2;
SET materialize_ttl_after_modify = 1;
SET mutations_sync = 0;
DROP TABLE t_ttl_overflow2;

-- Case 3: Overflow detection covers all add* functions for Date columns.
-- Date is UInt16 (days since 1970-01-01, max 65535 ≈ year 2149).
-- Adding a large interval pushes the day number past that limit.
DROP TABLE IF EXISTS t_ttl_date_fns;
CREATE TABLE t_ttl_date_fns (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_date_fns VALUES ('2024-01-01', 1), ('2024-06-15', 2), ('2025-12-31', 3);

-- INTERVAL syntax
ALTER TABLE t_ttl_date_fns MODIFY TTL day + INTERVAL 200 YEAR;   -- { serverError UNFINISHED }
-- addYears / addMonths / addWeeks / addDays: all push past UInt16 max
ALTER TABLE t_ttl_date_fns MODIFY TTL addYears(day, 150);        -- { serverError UNFINISHED }
ALTER TABLE t_ttl_date_fns MODIFY TTL addMonths(day, 2000);      -- { serverError UNFINISHED }
ALTER TABLE t_ttl_date_fns MODIFY TTL addWeeks(day, 6600);       -- { serverError UNFINISHED }
ALTER TABLE t_ttl_date_fns MODIFY TTL addDays(day, 46000);       -- { serverError UNFINISHED }

-- All rows must still be present.
SELECT count() FROM t_ttl_date_fns;

-- Non-overflowing TTLs still work (TTL ≈ 2034–2035, well in the future).
ALTER TABLE t_ttl_date_fns MODIFY TTL addYears(day, 10);
SELECT count() FROM t_ttl_date_fns;
DROP TABLE t_ttl_date_fns;

-- Case 4: Overflow detection covers all add* functions for DateTime columns.
-- DateTime is UInt32 (unix seconds, max 4294967295 ≈ year 2106).
-- Adding ≈100 years to a 2034 timestamp pushes past that limit.
DROP TABLE IF EXISTS t_ttl_datetime_fns;
CREATE TABLE t_ttl_datetime_fns (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY tuple();
-- Use a future timestamp so non-overflowing TTLs don't expire immediately.
INSERT INTO t_ttl_datetime_fns VALUES ('2034-01-01 00:00:00', 1), ('2034-06-15 12:00:00', 2), ('2035-12-31 23:59:59', 3);

-- INTERVAL syntax
ALTER TABLE t_ttl_datetime_fns MODIFY TTL ts + INTERVAL 100 YEAR;          -- { serverError UNFINISHED }
-- add* functions at every granularity that can overflow for a ~2034 timestamp
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addYears(ts, 100);               -- { serverError UNFINISHED }
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addMonths(ts, 1200);             -- { serverError UNFINISHED }
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addWeeks(ts, 5200);              -- { serverError UNFINISHED }
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addDays(ts, 36500);              -- { serverError UNFINISHED }
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addHours(ts, 876000);            -- { serverError UNFINISHED }
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addMinutes(ts, 52560000);        -- { serverError UNFINISHED }
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addSeconds(ts, 3153600000);      -- { serverError UNFINISHED }

-- All rows must still be present.
SELECT count() FROM t_ttl_datetime_fns;

-- Non-overflowing TTL (TTL ≈ 2035, well in the future from test time).
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addYears(ts, 1);
SELECT count() FROM t_ttl_datetime_fns;
DROP TABLE t_ttl_datetime_fns;

-- Case 5: Column TTL (TTLColumnAlgorithm) — overflowing TTL must not silently clear values.
-- Use the same synchronous MODIFY path that is proven to fire the overflow check.
DROP TABLE IF EXISTS t_ttl_col_algo;
CREATE TABLE t_ttl_col_algo (day Date, val String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_col_algo VALUES ('2024-01-01', 'hello'), ('2024-06-15', 'world');
ALTER TABLE t_ttl_col_algo MODIFY COLUMN val String TTL day + toIntervalDay(46000); -- { serverError UNFINISHED }
-- Values must be intact after the failed ALTER.
SELECT val FROM t_ttl_col_algo ORDER BY val;
DROP TABLE t_ttl_col_algo;

-- Case 6: GROUP BY TTL (TTLAggregationAlgorithm) — overflowing TTL must not silently aggregate rows.
-- Two rows share key=1 so that a silently executed GROUP BY TTL would reduce the count
-- from 3 to 2, making the count() assertion diagnostic if overflow goes undetected.
DROP TABLE IF EXISTS t_ttl_groupby_algo;
CREATE TABLE t_ttl_groupby_algo (day Date, key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_ttl_groupby_algo VALUES ('2024-01-01', 1, 10), ('2024-01-01', 1, 20), ('2024-06-15', 2, 30);
ALTER TABLE t_ttl_groupby_algo MODIFY TTL day + toIntervalDay(46000) GROUP BY key SET val = max(val); -- { serverError UNFINISHED }
-- Row count must remain 3, not 2, confirming the aggregation did not run.
SELECT count() FROM t_ttl_groupby_algo;
DROP TABLE t_ttl_groupby_algo;

-- Case 7: RECOMPRESS TTL (TTLUpdateInfoAlgorithm) — overflowing TTL must not corrupt part metadata.
DROP TABLE IF EXISTS t_ttl_recompress_algo;
CREATE TABLE t_ttl_recompress_algo (day Date, val UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_recompress_algo VALUES ('2024-01-01', 1), ('2024-06-15', 2);
ALTER TABLE t_ttl_recompress_algo MODIFY TTL day + toIntervalDay(46000) RECOMPRESS CODEC(ZSTD(1)); -- { serverError UNFINISHED }
-- Data must be intact after the failed ALTER.
SELECT count() FROM t_ttl_recompress_algo;
DROP TABLE t_ttl_recompress_algo;

-- Case 8: Normal (non-overflowing) TTL still works correctly.
DROP TABLE IF EXISTS t_ttl_normal;
CREATE TABLE t_ttl_normal (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_normal VALUES ('2000-01-01', 100);
ALTER TABLE t_ttl_normal MODIFY TTL day + INTERVAL 1 DAY;
OPTIMIZE TABLE t_ttl_normal FINAL;
SELECT count() FROM t_ttl_normal;
DROP TABLE t_ttl_normal;