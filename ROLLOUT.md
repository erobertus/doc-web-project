# Clinic fleet rollout — CPSO scrape agents

Goal: every clinic machine runs one unattended agent that polls
`MD_scrape_control` and scrapes only when told to. You control the
whole fleet from one row in the database.

---

## 0. One-time database prep (run once, from anywhere)

Skip any ALTER you have already applied.

```sql
-- control-table columns added over time
ALTER TABLE MD_scrape_control
  ADD COLUMN abort_check   INT  DEFAULT 0  AFTER use_random,
  ADD COLUMN run_from      TIME NULL       AFTER abort_check,
  ADD COLUMN run_until     TIME NULL       AFTER run_from,
  ADD COLUMN interval_days INT  DEFAULT 20 AFTER run_until,
  ADD COLUMN log_verbose   BIT NOT NULL DEFAULT b'0'
      AFTER interval_days;

-- make sure there is exactly one control row and it says STOP
SELECT * FROM MD_scrape_control;
-- if empty:
INSERT INTO MD_scrape_control (go_flag) VALUES (0);
UPDATE MD_scrape_control SET go_flag = 0;

-- central fleet log (agents fall back to console-only if absent)
CREATE TABLE IF NOT EXISTS MD_scrape_log (
  log_uno   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  log_time  TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  host      VARCHAR(128) NOT NULL,
  level     VARCHAR(8) NOT NULL DEFAULT 'INFO',
  batch_uno INT NULL DEFAULT NULL,
  cpso_no   INT NULL DEFAULT NULL,
  message   TEXT NOT NULL,
  PRIMARY KEY (log_uno),
  KEY idx_time  (log_time),
  KEY idx_level (level, log_time),
  KEY idx_host  (host, log_time),
  KEY idx_cpso  (cpso_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ONE-TIME charset conversion: the new register serves proper
-- Unicode (e.g. Riga Stradins with macrons), which the legacy
-- latin1 tables can neither compare against nor store. Run once
-- while no scrapers are active; latin1 -> utf8mb4 is lossless.
ALTER TABLE z847e_MD_universities     CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE z847e_MD_hospitals        CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE z847e_MD_reg_jurisdiction CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE z847e_MD_languages        CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE z847e_MD_reg_classes      CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE z847e_MD_reg_statuses     CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE z847e_MD_spec_list        CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE z847e_MD_spec_types       CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
-- ~1M rows, several minutes, locks the table:
ALTER TABLE MD_addresses              CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- clear any leftover abort flag / stale open batches
DELETE FROM MD_batch_header
WHERE host = '!!!ABORT_ALL' AND batch_size < 0;

UPDATE MD_batch_details d
JOIN MD_batch_header h ON d.batch_uno = h.batch_uno
SET d.updated_date_time = NOW() - INTERVAL 30 DAY
WHERE NOT d.isCompleted AND h.end_date IS NULL;

UPDATE MD_batch_header
SET InProgress = 0, end_date = NOW(), status_date = NOW()
WHERE end_date IS NULL;
```

Recommended starting parameters (nightly quick refresh):

```sql
UPDATE MD_scrape_control SET
  quick_mode  = 1,
  cpso_start  = 10000,
  cpso_stop   = 200000,
  batch_size  = 50,
  delay_sec   = 1.0,
  use_random  = 1,
  abort_check = 10,
  run_from    = '19:00:00',
  run_until   = '06:30:00',
  interval_days = 20,
  go_flag     = 0;          -- keep OFF until the pilot passes
```

---

## 1. Per-machine install (each clinic PC)

Do the whole installation from the **admin account** — the
scheduled task runs as SYSTEM, so it keeps working after the
admin logs out (or never logs in again).

1. **Python 3.10+ (3.13/3.14 both fine)** — use the CLASSIC
   full installer (`python-3.1x.x-amd64.exe` from python.org):
   Customize installation → tick **"Install for all users"** and
   **"Add python.exe to PATH"** → installs to
   `C:\Program Files\Python31x`. Verify:
   `where python` must show a `C:\Program Files\...` path.
   AVOID the "Python install manager" variant — it installs
   per-user under `C:\Users\<name>\AppData\Local\Python`, whose
   PATH entry the SYSTEM task does not see (agent dies with
   `'python' is not recognized`).
2. **Get the code** (note the branch — the repo default is stale):
   ```
   cd C:\
   git clone -b geocode_on_the_fly https://github.com/erobertus/doc-web-project.git cpso
   ```
   Keep it at a machine-wide path like `C:\cpso` — not under a
   user profile. No git on the machine? Copy the project folder
   from a USB stick / network share instead.
3. **Dependencies** — from an **elevated** prompt (right-click
   cmd → "Run as administrator"; an admin account with a normal
   prompt is NOT enough):
   ```
   cd C:\cpso
   pip install -r requirements.txt
   python -c "import sys, mariadb; print(sys.executable); print(mariadb.__file__)"
   ```
   The install output must NOT say "Defaulting to user
   installation" and the two printed paths must start with
   `C:\Program Files\` — that is the proof the SYSTEM task will
   find everything. If `mariadb` fails to install, install the
   "Microsoft Visual C++ Redistributable (x64)" and retry.
4. **Permissions** (recommended): the Windows default ACL under
   `C:\` usually lets Authenticated Users MODIFY subfolders —
   i.e. the non-admin clinic account could edit scripts that run
   as SYSTEM and contain the DB credentials. Tighten:
   ```
   icacls C:\cpso /inheritance:d
   icacls C:\cpso /remove:g "Authenticated Users" /t
   ```
   Result: Administrators + SYSTEM keep full control, regular
   users can still READ (view agent.log) but not modify the
   code; `git pull` from the admin account still works. Check
   with `icacls C:\cpso` — if there is no
   `Authenticated Users:...(M)` line to begin with, skip this.
5. **Connectivity check** (go_flag is still 0, so nothing is
   scraped — you should see it polling and staying idle):
   ```
   python main.py --agent --poll-interval 15
   ```
   Wait ~20 s, confirm `Agent mode: polling MD_scrape_control...`
   and no database errors, then Ctrl-C.
   - Database error here = the machine cannot reach
     `faxcomet.com:3306` (clinic firewall) — fix before
     continuing.
6. **Schedule it** (admin cmd window):
   ```
   schtasks /create /tn "CPSO scrape agent" /sc onstart ^
     /tr "C:\cpso\run_agent.bat" /ru SYSTEM
   schtasks /run /tn "CPSO scrape agent"
   ```
   The agent now runs headless (no visible window), survives
   reboots and logouts, restarts itself after crashes, and logs
   everything to `C:\cpso\agent.log`.
7. **Verify, then log out**: `C:\cpso\agent.log` should show
   `Agent mode: polling MD_scrape_control...` within a minute,
   and the machine appears centrally:
   ```sql
   SELECT host, MAX(log_time) FROM MD_scrape_log GROUP BY host;
   ```
   Log the admin out, wait a few minutes, re-run the query —
   the timestamp keeps advancing.

Repeat on the next machine. Machines are identical — no
per-machine configuration. Future code updates: `git pull` from
the admin account (regular users have read-only access).

---

## 2. Pilot (one machine, ~15 minutes)

With ONE machine's agent running and the rest not installed yet:

```sql
-- small test range, immediate window, full-detail mode
UPDATE MD_scrape_control SET
  quick_mode = 0, cpso_start = 94200, cpso_stop = 94400,
  batch_size = 20, run_from = NULL, run_until = NULL,
  go_flag = 1;
```

Within `poll_interval` (2 min default) the agent starts. Check:

```sql
-- is it working?
SELECT batch_uno, host, start_date, end_date
FROM MD_batch_header
ORDER BY batch_uno DESC LIMIT 5;

-- results arriving?
SELECT COUNT(*) FROM MD_batch_details
WHERE updated_date_time >= NOW() - INTERVAL 10 MINUTE
  AND isCompleted;
```

Also glance at `C:\cpso\agent.log` on the machine.

Then test the brakes:

```sql
UPDATE MD_scrape_control SET go_flag = 0;
```

The agent should stop within ~`abort_check × delay_sec` seconds
(log: "Stop requested - releasing ..."). If all good:

```sql
-- restore production parameters (section 0) and leave go_flag=0
```

Roll out to the remaining machines, then flip `go_flag = 1` in
the evening and let the window take over.

---

## 3. Day-to-day operations cheat sheet

```sql
-- START the fleet (within the daily window)
UPDATE MD_scrape_control SET go_flag = 1;

-- STOP the fleet (acts within abort_check doctors per agent)
UPDATE MD_scrape_control SET go_flag = 0;

-- switch nightly refresh <-> full detail sweep
UPDATE MD_scrape_control SET quick_mode = 1;   -- or 0

-- discovery pass for NEW doctors (sequential over the top range)
UPDATE MD_scrape_control SET use_random = 0,
  cpso_start = 154000, cpso_stop = 200000;

-- who is working right now (last 24 h, per machine)
SELECT host, COUNT(*) batches, MAX(start_date) last_start,
       SUM(end_date IS NULL) open_batches
FROM MD_batch_header
WHERE start_date >= NOW() - INTERVAL 1 DAY
  AND host <> '!!!ABORT_ALL'
GROUP BY host;

-- progress today
SELECT COUNT(*) done_today FROM MD_batch_details
WHERE updated_date_time >= CURDATE() AND isCompleted;

-- fleet errors/warnings, last 24 h (central log)
SELECT log_time, host, level, cpso_no, LEFT(message, 200) msg
FROM MD_scrape_log
WHERE level IN ('ERROR', 'WARN')
  AND log_time >= NOW() - INTERVAL 1 DAY
ORDER BY log_time DESC;

-- last sign of life per machine
SELECT host, MAX(log_time) last_seen
FROM MD_scrape_log GROUP BY host ORDER BY last_seen;

-- per-doctor detail in the central log (DEBUG rows; a full
-- sweep adds one row per doctor - turn off when not needed)
UPDATE MD_scrape_control SET log_verbose = 1;   -- or 0

-- log housekeeping (run occasionally; DEBUG rows first)
DELETE FROM MD_scrape_log
WHERE level = 'DEBUG' AND log_time < NOW() - INTERVAL 7 DAY;
DELETE FROM MD_scrape_log
WHERE log_time < NOW() - INTERVAL 60 DAY;

-- emergency stop of everything (old mechanism, still works)
--   on any machine:  python main.py -a
-- everything already dead, just clean the bookkeeping:
--   python main.py --force-abort
```

Notes:

- Changes to parameters take effect at each agent's **next
  sweep**; `go_flag` (and the run window) act **mid-sweep** at
  the `abort_check` cadence.
- Leaving `go_flag = 1` permanently is the intended standing
  mode: agents sweep nightly inside the window and only touch
  doctors older than `interval_days`.
- `agent.log` grows slowly; delete it any time, the agent
  recreates it.

## 4. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `pip install mariadb` fails | Install MS Visual C++ Redistributable x64, retry |
| Agent log: `cannot connect` loop | Clinic firewall blocks 3306 to faxcomet.com |
| Agents idle though go_flag=1 | Outside run window? Check `SELECT CURTIME();` vs run_from/run_until (DB clock rules) |
| Every agent stops immediately | Leftover abort row — `DELETE FROM MD_batch_header WHERE host='!!!ABORT_ALL' AND batch_size<0;` |
| `--abort` waits forever | Dead clients' batches are reaped automatically once they pass `--stale-minutes` (30 min default); use `--force-abort` to clean immediately |
| Crashed client stranded its numbers | Self-healing: the next client to request a batch reaps open batches with no completions for 30+ min and releases their numbers — no manual action needed |
| Repeated `fetch failed (HTTP 403/429)` in logs | Cloudflare pushback: raise `delay_sec`, or narrow the window |
