-- Live check for the server-side batch allocation (Oct 2026 fix).
--
-- The offline suite proves the SQL _allocate_batch sends has the
-- right shape; only a real database can prove it picks the same
-- numbers. This script compares, on live data, the pool the OLD
-- code computed (temp table + NOT IN) with the pool the NEW code
-- computes (seq_ table / z847e_MD_dir + NOT EXISTS).
--
-- READ-ONLY for shared data: plain SELECTs (non-locking reads) and
-- one session-private temporary table built from the sequence
-- engine. Nothing is claimed; MD_batch_* is never written.
-- Run in the faxcomet_MD_list database, as any user.

-- ------------------------------------------------------------
-- 0. Copy the parameters of the schedule you want to check
-- ------------------------------------------------------------
SELECT control_uno, go_flag, cpso_start, cpso_stop, batch_size,
       use_random, interval_days, skip_gaps, updated
FROM MD_scrape_control ORDER BY control_uno;

-- interval_days NULL = the agent default (20). @sweep_start = that
-- row's `updated` (only matters when interval_days < 1).
SET @min = 10000, @max = 250000, @batch = 100,
    @interval = 20, @sweep_start = NULL, @skip_gaps = 0;
-- If cpso_start/cpso_stop differ from 10000/250000, also rename
-- seq_10000_to_249999 below to seq_<cpso_start>_to_<cpso_stop - 1>.

-- ------------------------------------------------------------
-- 1. Sequential mode: old vs new, same snapshot
--    Expect the 'old' and 'new' rows to be identical.
-- ------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS chk_src;
-- exactly what Python's range(@min, @max) used to send
CREATE TEMPORARY TABLE chk_src (cpso_no INT PRIMARY KEY)
    SELECT seq AS cpso_no FROM seq_10000_to_249999;

START TRANSACTION WITH CONSISTENT SNAPSHOT;

-- whole free pool: count, sum and an order-independent fingerprint
SELECT 'old' AS way, COUNT(*) AS free_numbers,
       SUM(t.cpso_no) AS total, BIT_XOR(CRC32(t.cpso_no)) AS fingerprint
FROM chk_src t
WHERE t.cpso_no NOT IN (
        SELECT d.CPSO_no FROM MD_batch_details d
        JOIN MD_batch_header h ON d.batch_uno = h.batch_uno
        WHERE d.CPSO_no BETWEEN @min AND @max
          AND NOT d.isCompleted AND h.end_date IS NULL)
  AND t.cpso_no NOT IN (
        SELECT DISTINCT d.CPSO_no FROM MD_batch_details d
        WHERE d.CPSO_no BETWEEN @min AND @max
          AND ((d.isCompleted AND CASE
                  WHEN @interval >= 1
                       THEN d.updated_date_time > NOW() - INTERVAL @interval DAY
                  WHEN @sweep_start IS NOT NULL
                       THEN d.updated_date_time >= @sweep_start
                  ELSE 0 END)
               OR (@skip_gaps AND d.PermExcluded)))
UNION ALL
SELECT 'new', COUNT(*), SUM(c.seq), BIT_XOR(CRC32(c.seq))
FROM seq_10000_to_249999 c
WHERE NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        WHERE d.CPSO_no = c.seq AND d.CPSO_no BETWEEN @min AND @max
          AND ((d.isCompleted AND CASE
                  WHEN @interval >= 1
                       THEN d.updated_date_time > NOW() - INTERVAL @interval DAY
                  WHEN @sweep_start IS NOT NULL
                       THEN d.updated_date_time >= @sweep_start
                  ELSE 0 END)
               OR (@skip_gaps AND d.PermExcluded)))
  AND NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        JOIN MD_batch_header h ON d.batch_uno = h.batch_uno
        WHERE d.CPSO_no = c.seq AND d.CPSO_no BETWEEN @min AND @max
          AND NOT d.isCompleted AND h.end_date IS NULL);

COMMIT;
DROP TEMPORARY TABLE chk_src;

-- ------------------------------------------------------------
-- 2. Random mode: old vs new over the known doctors
--    (the old temp table held exactly these numbers, so only the
--    NOT IN -> NOT EXISTS rewrite differs). Expect identical rows.
-- ------------------------------------------------------------
-- the old temp table had a PRIMARY KEY: numbers must be unique
SELECT COUNT(*) AS dir_rows, COUNT(DISTINCT CPSO_no) AS distinct_nos
FROM z847e_MD_dir WHERE CPSO_no BETWEEN @min AND @max;

START TRANSACTION WITH CONSISTENT SNAPSHOT;

SELECT 'old' AS way, COUNT(*) AS free_numbers,
       SUM(c.CPSO_no) AS total, BIT_XOR(CRC32(c.CPSO_no)) AS fingerprint
FROM z847e_MD_dir c
WHERE c.CPSO_no BETWEEN @min AND @max
  AND c.CPSO_no NOT IN (
        SELECT d.CPSO_no FROM MD_batch_details d
        JOIN MD_batch_header h ON d.batch_uno = h.batch_uno
        WHERE d.CPSO_no BETWEEN @min AND @max
          AND NOT d.isCompleted AND h.end_date IS NULL)
  AND c.CPSO_no NOT IN (
        SELECT DISTINCT d.CPSO_no FROM MD_batch_details d
        WHERE d.CPSO_no BETWEEN @min AND @max
          AND ((d.isCompleted AND CASE
                  WHEN @interval >= 1
                       THEN d.updated_date_time > NOW() - INTERVAL @interval DAY
                  WHEN @sweep_start IS NOT NULL
                       THEN d.updated_date_time >= @sweep_start
                  ELSE 0 END)
               OR (@skip_gaps AND d.PermExcluded)))
UNION ALL
SELECT 'new', COUNT(*), SUM(c.CPSO_no), BIT_XOR(CRC32(c.CPSO_no))
FROM z847e_MD_dir c
WHERE c.CPSO_no BETWEEN @min AND @max
  AND NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        WHERE d.CPSO_no = c.CPSO_no AND d.CPSO_no BETWEEN @min AND @max
          AND ((d.isCompleted AND CASE
                  WHEN @interval >= 1
                       THEN d.updated_date_time > NOW() - INTERVAL @interval DAY
                  WHEN @sweep_start IS NOT NULL
                       THEN d.updated_date_time >= @sweep_start
                  ELSE 0 END)
               OR (@skip_gaps AND d.PermExcluded)))
  AND NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        JOIN MD_batch_header h ON d.batch_uno = h.batch_uno
        WHERE d.CPSO_no = c.CPSO_no AND d.CPSO_no BETWEEN @min AND @max
          AND NOT d.isCompleted AND h.end_date IS NULL);

COMMIT;

-- ------------------------------------------------------------
-- 3. Memory and plan of the NEW query, in a FRESH connection
--    (open a new connection / session first, so its peak counter
--    starts from zero; re-run the SET @... line above in it).
--    Before the fix every sweeping agent connection peaked at
--    51.4 MiB. The agent sends this query prefixed with
--    SET STATEMENT optimizer_switch='exists_to_in=off' FOR ...;
--    the session setting below does the same for both statements.
--    (Oct 29 on Xenon: rewrite on = 11.1M rows read, ~5 s;
--    off = per-number index lookups, ~1.8 s, same numbers.)
-- ------------------------------------------------------------
SET SESSION optimizer_switch = 'exists_to_in=off';

SELECT c.seq
FROM seq_10000_to_249999 c
WHERE NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        WHERE d.CPSO_no = c.seq AND d.CPSO_no BETWEEN @min AND @max
          AND ((d.isCompleted AND CASE
                  WHEN @interval >= 1
                       THEN d.updated_date_time > NOW() - INTERVAL @interval DAY
                  WHEN @sweep_start IS NOT NULL
                       THEN d.updated_date_time >= @sweep_start
                  ELSE 0 END)
               OR (@skip_gaps AND d.PermExcluded)))
  AND NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        JOIN MD_batch_header h ON d.batch_uno = h.batch_uno
        WHERE d.CPSO_no = c.seq AND d.CPSO_no BETWEEN @min AND @max
          AND NOT d.isCompleted AND h.end_date IS NULL)
ORDER BY c.seq
LIMIT 100;                    -- note how long this took

SELECT ROUND(memory_used / 1048576, 1)     AS cur_mib,
       ROUND(max_memory_used / 1048576, 1) AS peak_mib
FROM information_schema.processlist
WHERE id = CONNECTION_ID();

-- and the plan, for the record
EXPLAIN
SELECT c.seq
FROM seq_10000_to_249999 c
WHERE NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        WHERE d.CPSO_no = c.seq AND d.CPSO_no BETWEEN @min AND @max
          AND ((d.isCompleted AND CASE
                  WHEN @interval >= 1
                       THEN d.updated_date_time > NOW() - INTERVAL @interval DAY
                  WHEN @sweep_start IS NOT NULL
                       THEN d.updated_date_time >= @sweep_start
                  ELSE 0 END)
               OR (@skip_gaps AND d.PermExcluded)))
  AND NOT EXISTS (
        SELECT 1 FROM MD_batch_details d
        JOIN MD_batch_header h ON d.batch_uno = h.batch_uno
        WHERE d.CPSO_no = c.seq AND d.CPSO_no BETWEEN @min AND @max
          AND NOT d.isCompleted AND h.end_date IS NULL)
ORDER BY c.seq
LIMIT 100;
