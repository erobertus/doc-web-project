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
  version   VARCHAR(40) NULL,       -- deployed git commit (short)
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
-- already have MD_scrape_log without version? add the column:
ALTER TABLE MD_scrape_log ADD COLUMN version VARCHAR(40) NULL AFTER host;

-- per-agent liveness: one row per host, refreshed every poll
-- (so idle agents still show as alive). Optional - agents skip
-- it gracefully if absent.
CREATE TABLE IF NOT EXISTS MD_scrape_agents (
  host      VARCHAR(128) NOT NULL PRIMARY KEY,
  version   VARCHAR(40) NULL,
  state     VARCHAR(20) NULL,   -- idle / waiting-window / sweeping / no-control
  detail    VARCHAR(255) NULL,
  last_seen TIMESTAMP NOT NULL DEFAULT current_timestamp()
            ON UPDATE current_timestamp(),
  KEY idx_last_seen (last_seen)
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

-- fleet machines' identities (user()@reverse-dns) can exceed the
-- legacy 50-char host column
ALTER TABLE MD_batch_header MODIFY host VARCHAR(128) DEFAULT NULL;

-- add the periodic-update column to the control table (optional)
ALTER TABLE MD_scrape_control
  ADD COLUMN auto_update_hrs INT DEFAULT 0 AFTER log_verbose;

-- random +/- jitter (seconds) on the courtesy delay so the fleet
-- does not fall into lock-step. 0 = fixed delay.
ALTER TABLE MD_scrape_control
  ADD COLUMN delay_jitter FLOAT DEFAULT 0 AFTER skip_gaps;

-- skip known gaps (not-found numbers below the DB max) for fast
-- close-in-time re-sweeps. DEFAULT 0 = re-check gaps (thorough,
-- catches newly-assigned mid-range numbers)
ALTER TABLE MD_scrape_control
  ADD COLUMN skip_gaps BIT NOT NULL DEFAULT b'0'
      AFTER auto_update_hrs;

-- central fleet commands: push code updates and self-destruct to
-- machines by host pattern (agents fall back to disabled if this
-- table is absent)
CREATE TABLE IF NOT EXISTS MD_scrape_command (
  cmd_uno      INT AUTO_INCREMENT PRIMARY KEY,
  host_pattern VARCHAR(128) NOT NULL,   -- SQL LIKE: 'Clinic-%','%',exact
  command      VARCHAR(16)  NOT NULL,   -- 'update' or 'destruct'
  created      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  note         VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

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

Everything from the **admin account**, in an **ELEVATED** cmd
window (right-click cmd → "Run as administrator" — an admin
account with a normal prompt is NOT enough). The scheduled task
runs as SYSTEM, so it keeps working after the admin logs out.

1. **Get `bootstrap_agent.bat` onto the machine and run it** —
   that single file is the only thing you need. Fetch and launch
   it in one paste (`curl` ships with Windows 10/11):
   ```
   curl -L -o %TEMP%\bootstrap_agent.bat https://raw.githubusercontent.com/erobertus/doc-web-project/geocode_on_the_fly/bootstrap_agent.bat && %TEMP%\bootstrap_agent.bat
   ```
   Or as two steps, optionally passing the clinic name (and
   knock sequence) up front instead of at the prompts:
   ```
   curl -L -o %TEMP%\bootstrap_agent.bat https://raw.githubusercontent.com/erobertus/doc-web-project/geocode_on_the_fly/bootstrap_agent.bat
   %TEMP%\bootstrap_agent.bat "Clinic-Newmarket" "tcp:7001,8002,9003"
   ```
   (no git needed beforehand — the bootstrap installs it; USB
   stick / RDP paste also work if the machine has no internet to
   GitHub for the .bat itself.)
2. **The bootstrap does everything**: installs git if missing
   (latest release, silent), clones the repo to `C:\cpso` (or
   fast-forwards an existing clone — re-running it doubles as
   the update mechanism), then hands over to
   `deploy_agent.bat`, which verifies each step: elevation;
   Python — uses a runtime bundled at `C:\cpso\python\` if
   present, else an existing all-users install, else
   **downloads the latest Python from python.org and silently
   installs it for all users**; dependencies into a
   SYSTEM-visible site-packages (catches the per-user shadowing
   trap); read-only permissions for clinic users; agent naming
   and knock sequence (prompted if not given); scheduled-task
   creation; and a smoke check that the agent process is up and
   polling. It
   stops with a specific remedy message on any failure and is
   safe to re-run after fixing.

   Once the classic installer is retired (Python 3.15+), the
   bundled-runtime route replaces the download:
   `py install 3.14 --target C:\cpso\python` before running
   deploy_agent.bat — both scripts prefer it automatically.
3. **Verify from your desk, then log out**:
   ```sql
   SELECT host, MAX(log_time) FROM MD_scrape_log GROUP BY host;
   ```
   The clinic name appears; log the admin out, wait a few
   minutes, re-run — the timestamp keeps advancing.

Notes:
- After deployment, writes to `C:\cpso` (e.g. `git pull` for
  updates) need an ELEVATED prompt — UAC gives a normal admin
  prompt a filtered, effectively read-only token there.
- A database error during the smoke check means the machine
  cannot reach `faxcomet.com:3306` (clinic firewall).
- No git on the machine? Copy the project folder from a USB
  stick to `C:\cpso` and run the same deploy_agent.bat.

---

## 1b. Port knocking (clinics behind a dynamic-IP firewall)

If the database server keeps 3306 closed and only opens it to
known IPs, the agents cannot be allow-listed statically (clinic
IPs are dynamic). Instead the DB server runs a knock daemon and
each agent knocks to authorize its CURRENT IP whenever a
connection fails.

**Server side (faxcomet, once)** — e.g. `knockd`:

```
# /etc/knockd.conf
[options]
    UseSyslog

[openMariaDB]
    sequence      = 7001,8002,9003
    seq_timeout   = 15
    tcpflags      = syn
    command       = /sbin/iptables -I INPUT -s %IP% -p tcp --dport 3306 -j ACCEPT
    cmd_timeout   = 30
    stop_command  = /sbin/iptables -D INPUT -s %IP% -p tcp --dport 3306 -j ACCEPT
```

The firewall must also keep ESTABLISHED connections open
(`-A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT`), so a
connection made during the 30 s window keeps working after the
rule is withdrawn — the long-lived agent only re-knocks when its
socket actually drops (e.g. its IP changed).

**Agent side** — `deploy_agent.bat` handles it. It sources the
sequence (a shared fleet-wide secret) in this order:

1. second argument, **quoted** (it contains commas):
   `deploy_agent.bat "Clinic-Newmarket" "tcp:7001,8002,9003"`
2. the `CPSO_KNOCK` env var, if `set` in the deploy window
3. the value already stored on the machine (kept as-is on a
   re-deploy / update — no re-prompt)
4. otherwise it **prompts interactively** (Enter = no knocking)

Whatever the source, it is persisted machine-wide and the agent
knocks automatically on any failed DB connection. Ports may be
separated by `,` or `;` (`tcp:7001;8002;9003` is equivalent).
Default protocol is tcp; prefix `udp:` for UDP knocks. For a
manual (non-agent) run: `python main.py --knock
"tcp:7001,8002,9003"`. Leaving it unset disables knocking
entirely.

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

-- jitter the delay: 1 s base +/- 0.5 s -> each pause 0.5-1.5 s
UPDATE MD_scrape_control SET delay_sec = 1.0, delay_jitter = 0.5;

-- discovery pass for NEW doctors (sequential over the top range)
UPDATE MD_scrape_control SET use_random = 0,
  cpso_start = 154000, cpso_stop = 200000;

-- fast close-in-time re-sweep: skip known gaps (empty spaces).
UPDATE MD_scrape_control SET skip_gaps = 1;
-- back to the thorough default (re-check gaps, catch new mid-
-- range assignments) - e.g. for the periodic full refresh:
UPDATE MD_scrape_control SET skip_gaps = 0;

-- FORCE a re-scrape now, ignoring how recently numbers were done
-- (testing, quick->deep). interval_days = 0 re-scrapes the range
-- once per number. The campaign epoch is the control row's
-- `updated` timestamp (set by this UPDATE), SHARED by the whole
-- fleet and STABLE across restarts, so: no two agents redo the
-- same number, and stopping then re-running RESUMES (already-done
-- numbers stay skipped) rather than restarting from the bottom.
UPDATE MD_scrape_control
SET cpso_start = 90000, cpso_stop = 120000,
    interval_days = 0, go_flag = 1;
-- to RESTART the interval=0 pass from scratch (redo everything),
-- just touch the row again so `updated` moves forward:
UPDATE MD_scrape_control SET interval_days = 0;
-- restore the normal refresh cadence afterwards:
UPDATE MD_scrape_control SET interval_days = 20;

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

-- ACTIVE agents right now (heartbeat within the last 5 min).
-- This is the reliable "who is alive" list - idle agents still
-- heartbeat every poll, unlike the event log.
SELECT host, version, state, last_seen
FROM MD_scrape_agents
WHERE last_seen >= NOW() - INTERVAL 5 MINUTE
ORDER BY last_seen DESC;

-- agents that have gone silent (were seen, but not lately)
SELECT host, version, state, last_seen
FROM MD_scrape_agents
WHERE last_seen < NOW() - INTERVAL 5 MINUTE
ORDER BY last_seen DESC;

-- last sign of life + deployed version per machine (event log)
SELECT host, version, MAX(log_time) last_seen
FROM MD_scrape_log GROUP BY host, version ORDER BY last_seen;

-- which agent versions are live across the fleet (spot laggards
-- after pushing an 'update' command)
SELECT version, COUNT(DISTINCT host) machines, MAX(log_time) newest
FROM MD_scrape_log
WHERE log_time >= NOW() - INTERVAL 1 DAY
GROUP BY version ORDER BY newest DESC;

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

-- per-machine lifecycle (elevated cmd on that machine):
--   C:\cpso\deploy_agent.bat disable   stop agent, keep install
--   C:\cpso\deploy_agent.bat enable    re-enable + start
--   C:\cpso\deploy_agent.bat remove    task + name + C:\cpso gone
--                                      - Python and git stay

-- CENTRAL fleet commands (host_pattern is a SQL LIKE, matched
-- against the agent name shown in MD_scrape_log.host):
--   push a code update to all clinics
INSERT INTO MD_scrape_command (host_pattern, command)
VALUES ('%', 'update');
--   update just the Newmarket machine
INSERT INTO MD_scrape_command (host_pattern, command)
VALUES ('Clinic-Newmarket', 'update');
--   self-destruct every clinic machine (uninstalls the agent;
--   Python + git stay)
INSERT INTO MD_scrape_command (host_pattern, command)
VALUES ('Clinic-%', 'destruct');
-- Agents act within one poll interval; each command runs once
-- per machine. 'update' = git pull + restart (only if the code
-- changed); 'destruct' runs deploy_agent.bat remove. A destruct
-- older than 180 min (DESTRUCT_TTL_MIN) is ignored, so a freshly
-- deployed machine never obeys a stale destruct; re-issue it if
-- some machines were offline longer than that.
--
-- NOTE: an agent only obeys commands if its running code already
-- has command support (the 'Central fleet control' version or
-- newer). Machines on an older build must be updated once
-- manually (re-run the bootstrap one-liner) to GET that code;
-- after that they self-update from commands.

-- periodic hands-off updates: pull every N hours (0 = off; on-
-- demand commands still work). Use with care - a bad commit
-- reaches the whole fleet automatically.
UPDATE MD_scrape_control SET auto_update_hrs = 24;
```

Notes:

- Changes to parameters take effect at each agent's **next
  sweep**; `go_flag` (and the run window) act **mid-sweep** at
  the `abort_check` cadence.
- Leaving `go_flag = 1` permanently is the intended standing
  mode: agents sweep nightly inside the window and only touch
  doctors older than `interval_days`.
- `agent.log` self-rotates: run_agent.bat keeps it under
  `CPSO_LOG_MAX_MB` (default 20) across `CPSO_LOG_KEEP` (default
  3) generations — `agent.log`, `agent.log.1`, ... — so local
  disk use is bounded (~cap x keep). Tune per machine:
  `setx CPSO_LOG_MAX_MB 50 /M` and/or `setx CPSO_LOG_KEEP 5 /M`,
  then restart the task. (The central `MD_scrape_log` is the
  fleet-wide record; agent.log is just a local tail.)

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
