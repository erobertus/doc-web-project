# CPSO Physician Register Scraper

Scrapes the CPSO (College of Physicians and Surgeons of Ontario)
public register for all active and inactive doctors and stores the
parsed data in a MariaDB database (`faxcomet_MD_list`). Runs in
batches; several processes can run simultaneously — each process
allocates a working pool from `MD_batch_header`/`MD_batch_details`
and completes the batches from that pool. Addresses are normalized
against `MD_geo_pos`, probing the Google Geocoding API for
addresses the database does not know yet.

## Files

| File | Purpose |
|------|---------|
| `main.py` | Batch orchestration, DB writes, record assembly |
| `cpso_site.py` | HTTP fetching + HTML parsing of the **new** register (no DB dependencies) |
| `constants.py` | Table/column names, reference-table maps, final SQL |
| `GeoCoding.py` | Google geocode lookup/caching (`MD_geo_pos`), standalone re-link utility |

## The new CPSO site (2026 rewrite)

The old site (`doctors.cpso.on.ca`, ASP.NET form post) was replaced
by a Microsoft Power Pages portal at `register.cpso.on.ca`. The
scraper no longer submits a search form; it goes straight to the
server-rendered detail page:

- **Detail page**: `https://register.cpso.on.ca/physician-info/?cpsonum=<CPSO#>`
  All data is parsed from elements with stable `scrp-*` CSS classes
  (banner, general info, practice locations, specialties,
  jurisdictions, hospital privileges).
- **Search API** (diagnostics only, `cpso_site.search_by_number`):
  `POST https://register.cpso.on.ca/Get-Search-Results/` with
  `cpsoNumber`, `cbx-includeinactive=on`,
  `cbx-includeinactive-20years=on` → JSON.
- A CPSO number that is not on the register returns the normal page
  shell without a `scrp-banner` element; a page missing the
  `/physician-info/` marker entirely is treated as a transient
  failure (WAF/outage) and retried, **not** excluded.

### Site → DB field mapping

| New site element | DB target |
|---|---|
| banner `scrp-contactname-value` ("Last, First Middle") | `last_name`, `first_name`, `middle_name` |
| banner status pill + `scrp-statusdate-value` | `reg_stat_code` (+ `reg_eff_date` from "as of") |
| banner `scrp-expirydate-value` | `reg_exp_date` |
| banner `scrp-registrationclass-value` | `reg_class_code`, `reg_certif_date` |
| `scrp-formername` | `former_name` |
| `scrp-gender-value` | `gender` (code) |
| `scrp-laguage-value` (sic) | `z847e_MD_doc_x_lang` |
| `scrp-education-value` ("School, YYYY") | `univ_code`, `grad_year` |
| Primary Business Location + additional location cards | `MD_addresses` (primary = `order_no` 1, `isDefault` 1; raw text kept in `address_raw`) |
| `scrp-specialties` table | `z847e_MD_doc_x_spec` (certifying body → `spec_type_code`) |
| `scrp-jurisdiction-value` | `z847e_MD_doc_x_jurisdiction` |
| `scrp-hospitalprivileges` table | `z847e_MD_doc_x_hosp` as "Hospital Name (Location)" |

Address lines are parsed with the locality line
("City Province Postal") recognized by a Canadian-postal or US-ZIP
suffix; full province names are converted to the two-letter codes
the database already uses ("Ontario" → "ON"). Unrecognizable
locality lines (foreign addresses) are kept verbatim as street
lines so nothing is lost; the geocode probe deals with them.

### Behaviour changes vs. the old scraper

1. **Deceased doctors** stay on the register (e.g. CPSO 79082)
   with the banner status `Deceased as of <date>`. That maps to
   the existing `Expired: Member deceased` status via
   `REG_STAT_ALIASES`, and the "as of" date is stored as
   `date_of_death` (the new site has no separate Date of Death
   field). Some historical records are still purged entirely
   (e.g. CPSO 10310): those rows are **kept** in `z847e_MD_dir`
   and flagged with the `Not on Register` status (auto-created on
   first use); nothing else is touched.
2. **Smarter permanent exclusion.** CPSO numbers are
   ever-increasing and gaps are never re-issued, so a number that
   is not found and lies *below* the highest CPSO number already
   in the database is permanently excluded (as before). Numbers
   *above* the database maximum are only marked completed for the
   current run and re-checked next run — they may belong to
   doctors registered after this run. `--perm-exclude` forces the
   old exclude-everything behaviour.
3. **Renamed reference values.** The new site says `Active` where
   the old one said `Active Member`, `Deceased` instead of
   `Expired: Member deceased`, and `Man`/`Woman` instead of
   `Male`/`Female`. `REG_STAT_ALIASES` / `GENDER_ALIASES` in
   `constants.py` reuse the existing reference codes when the old
   name is already present in the table, so existing
   `reg_stat_code` values keep their meaning. Reference-name
   lookups are also case-insensitive now. Note: former names now
   arrive with a suffix like `(Used Until: 26 Aug 2008)`.
4. **Specialty types** are now the certifying body
   ("Royal College of Physicians and Surgeons of Canada", ...) —
   the old site's specialty-type wording no longer exists. New
   names are auto-added to `z847e_MD_spec_types`.
5. `County`/`Electoral District` are not published on the new site;
   the corresponding `MD_addresses` columns stay empty.
6. One shared HTTP session is used for the entire run (the old code
   started a new browser per doctor), and a politeness `--delay`
   (default 1 s) is applied between doctors.

## Running

```
python main.py -h                     # full option list
python main.py -z 100 -r             # random order, batch of 100
python main.py -s 158100 -e 200000   # only the new number range
python main.py -q -r                 # quick refresh via JSON API
python main.py -a                    # abort all running scrapes
```

### Quick mode (`-q` / `--quick`)

Uses only the JSON search API (~1 KB per doctor instead of a
~300 KB page). Per doctor it refreshes the name, former name,
registration status and the DEFAULT address / phone / fax (and
re-runs the fax/postal cleanup SQL); additional locations,
specialties, education, languages and hospital privileges keep
their previously collected values. It automatically falls back to
the full page scrape when (a) the doctor is not in the database
yet, or (b) the status changed away from active — the API only
says "Inactive" without the detailed reason. Good for mid-cycle
refreshes of the fax/postal data; run the full scrape (no `-q`)
for the twice-a-year update.

Defaults: CPSO range 10000–200000, batch 50, 1 s delay, DB
`faxcomet_MD_list` on `faxcomet.com` (see `-h` for the credentials
options).

### Fleet / agent mode (`--agent`)

Unattended mode for running the scrape from many machines (e.g.
one per clinic), all coordinated through the database. Each agent
polls the `MD_scrape_control` table (every `--poll-interval`
seconds, default 120) and, while `go_flag` is set, works the
shared number pool with the parameters stored in the table —
command-line range/batch/delay/quick options are ignored:

```sql
CREATE TABLE MD_scrape_control (
  control_uno INT AUTO_INCREMENT PRIMARY KEY,
  go_flag     BIT NOT NULL DEFAULT 0,
  quick_mode  BIT NOT NULL DEFAULT 0,
  cpso_start  INT DEFAULT 10000,
  cpso_stop   INT DEFAULT 200000,
  batch_size  INT DEFAULT 50,
  delay_sec   FLOAT DEFAULT 1.0,
  use_random  BIT DEFAULT 1,
  abort_check INT DEFAULT 0,
  run_from    TIME NULL,
  run_until   TIME NULL,
  interval_days INT DEFAULT 20,
  log_verbose BIT NOT NULL DEFAULT b'0',
  updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
              ON UPDATE CURRENT_TIMESTAMP
);
INSERT INTO MD_scrape_control (go_flag) VALUES (0);
```

(for a table created before these columns existed:
`ALTER TABLE MD_scrape_control ADD COLUMN abort_check INT
DEFAULT 0 AFTER use_random, ADD COLUMN run_from TIME NULL
AFTER abort_check, ADD COLUMN run_until TIME NULL AFTER
run_from, ADD COLUMN interval_days INT DEFAULT 20 AFTER
run_until;` — agents degrade gracefully until the columns exist)

`interval_days` is the freshness window: a doctor is offered for
re-scraping only when the last update is older than this many
days (also available as `-i/--interval` outside agent mode).
Together with the run window this sets the rhythm of the
standing fleet: e.g. `interval_days = 20` + a nightly window =
every doctor re-checked roughly every three weeks, spread
naturally across nights.

`abort_check` > 0 makes every agent re-check the go flag / abort
request after that many doctors WITHIN a batch (default 0 =
between batches only), so `go_flag = 0` stops the whole fleet
within `abort_check × delay_sec` seconds; the interrupted
batches' unprocessed numbers are released back to the pool
immediately.

`run_from` / `run_until` restrict scraping to a daily time
window, e.g. after hours:

```sql
UPDATE MD_scrape_control
SET run_from = '19:00:00', run_until = '06:30:00';  -- overnight
```

Both NULL = run any time. `run_from > run_until` wraps midnight.
The window is judged by the DATABASE server clock (`CURTIME()`),
so all machines agree regardless of their local timezone
settings. Agents idle outside the window, start sweeping when it
opens, stop within the `abort_check` cadence when it closes
(releasing unfinished numbers), and resume automatically the
next day until the pool is exhausted. `go_flag` still rules
overall: window scheduling only applies while it is set.

Operation:

- **Start a sweep**: `UPDATE MD_scrape_control SET go_flag = 1;`
  — all idle agents pick it up within one poll interval.
- **Stop**: `UPDATE MD_scrape_control SET go_flag = 0;` — each
  agent stops after finishing its current batch (≈ batch_size ×
  delay seconds). `python main.py -a` still works as the
  emergency abort as well.
- **Retune centrally**: change `delay_sec`, `batch_size`, range
  or `quick_mode` in the table; agents apply the new values on
  their next sweep (the `updated` column changing is also what
  tells idle agents to sweep again).
- When the pool is exhausted and `go_flag` stays on, agents idle
  and re-attempt a sweep every 6 h (`AGENT_RESWEEP_SECS`) —
  combined with `request_workload`'s 20-day freshness interval
  this keeps the data continuously up to date at negligible cost.
- Agents survive database outages (reconnect with retry) and
  record their identity in `MD_batch_header.host`, so progress
  per machine is visible there.

Windows setup per machine (`run_agent.bat` wraps the agent with
auto-restart):

```
pip install -r requirements.txt
schtasks /create /tn "CPSO scrape agent" /sc onstart ^
  /tr "C:\path\to\doc-web-project\run_agent.bat" /ru SYSTEM
```

(or point Task Scheduler at `run_agent.bat` with "Run whether
user is logged on or not" + "Restart on failure".)

With ~20 machines at the default 1 s delay the full sweep takes
roughly 2–3 hours, at ~1 request/second per clinic IP.

## Dependencies

See `requirements.txt` (`mariadb`, `requests`, `beautifulsoup4`,
`googlemaps`). `mechanicalsoup` is no longer needed.
