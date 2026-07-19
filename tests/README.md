# Offline regression harness

Runs with **no network and no database**. Real physician-info pages
captured from the register are replayed through the parser and
through `process_record()` against an in-memory fake connection, so
the whole scrape → record-assembly path can be checked on any
machine in a few seconds.

```
python tests/test_offline.py            # run everything
python tests/test_offline.py -v TestScheduling      # one class
python tests/test_offline.py --bless    # re-record the snapshot

powershell -NoProfile -ExecutionPolicy Bypass -File tests\test_task.ps1
```

Needs only `bs4` + `requests` (from `requirements.txt`). The
`mariadb` and `googlemaps` imports are stubbed when absent, so a
bare dev machine can still run the suite.

## What it covers

| Group | Checks |
|---|---|
| `TestParserGolden` | Snapshot of `parse_physician_page()` over every fixture |
| `TestParserInvariants` | Things that must never silently change: missing number → `None`, status detail beats the pill, deceased carries a date, no placeholder/invisible characters leak |
| `TestHelpers` | `_norm`, `_split_as_of`, locality/address/phone parsing |
| `TestSearchResult` | Quick-mode JSON → the same location shape as the detail parser |
| `TestProcessRecord` | Reference-code lookup and aliasing, `NOT NULL` fallbacks, dedupe, address ordering, gap vs. outage |
| `TestProcessRecordQuick` | The two cases quick mode must escalate to a full scrape |
| `TestUpdateRecord` | Full-replace write order, child-row keys, autocommit restore |
| `TestScheduling` | Schedule → `ctl` dict, idle, legacy fallback, fleet-global auto-update, `in_time_window` |
| `TestTaskSelfRepair` | `ensure_task_settings()`: invokes `-Repair` (never `-Register`), survives every subprocess failure, bounded output, and that `run_agent` actually calls it |

`tests/test_task.ps1` covers `agent_task.ps1`'s decision logic. It
runs **unelevated and touches nothing** — it dot-sources the script
with `-LoadOnly` and drives `Repair-Settings` / `Test-HasRepeat`
with real objects from `New-ScheduledTaskSettingsSet`, the same
types `Get-ScheduledTask` returns.

### Deliberately NOT covered

The scheduling `WHERE` clause in `_read_schedule()` is evaluated by
**MariaDB** — `RLIKE`, `CURTIME()`, `DATE_FORMAT(NOW(),'%a')` and
the three window branches. Offline we can only check the Python
side (row → `ctl`, parameter passing) and assert the SQL *shape*.
Whether the predicate actually selects the right row on a given
weekday still needs a live database; the "who ranks where" query in
`ROLLOUT.md` is the way to eyeball that.

Likewise `Get-ScheduledTask` / `Set-ScheduledTask` /
`Register-ScheduledTask` against the live task store need admin, so
`agent_task.ps1`'s decision logic is tested but its writes are not.
**Verify `-Repair` on one machine before pushing it to the fleet** —
it runs as SYSTEM and edits the mechanism keeping the agent alive.

Also untested offline: batch allocation / `GET_LOCK` concurrency,
geocoding, git self-update, and the `.bat` deploy scripts.

## The golden snapshot

`fixtures/golden.json` is the recorded output of
`parse_physician_page()` for every fixture. Any parser change shows
up as a diff, which is the point.

When a change is **intentional**:

```
python tests/test_offline.py --bless
git diff tests/fixtures/golden.json     # <- read this carefully
```

That diff is the regression report. Never bless without reading it.

## Fixtures

Real pages, gzipped (~220 KB each raw — the portal ships desktop,
mobile and print copies of every section — ~30 KB compressed).

| CPSO | Label | Why it is here |
|---|---|---|
| 10310 | `not_on_register` | Purged number: parse must return `None` |
| 55000 | `expired_resigned` | `Expired: Resigned from membership` |
| 72500 | `active_3_hospitals` | Hospital-privilege rows |
| 79082 | `deceased_former` | Deceased **and** has a former name |
| 83500 | `active_2_locations` | Additional business location card |
| 88000 | `expired_terms` | `Expired: Terms and Conditions` |
| 91500 | `active_jurisdiction` | Licence in another jurisdiction |
| 94138 | `deceased` | The record verified against production |
| 110500 | `active_fax` | Primary location carries a fax |
| 119500 | `expired_nonascii` | Non-ASCII payload (utf8mb4) |

Plus `search_72500.json` / `search_79082.json` for quick mode.

Shapes the captured pages happen not to contain — a doctor with no
gender, repeated hospital rows, a locality-only address, more than
four street lines — are covered with `synthetic()` records and the
small `MINI_PAGE` in the harness rather than by hunting for a real
doctor who has them.

### Refreshing / replacing fixtures

Doctors do get purged from the register. When a fixture 404s:

```
python tests/capture_fixtures.py                  # refresh the set
python tests/capture_fixtures.py --scan --numbers "31000 68000 ..."
```

`--scan` fetches candidates and prints what each one exercises
(locations, hospitals, specialties, jurisdictions, former name,
fax, non-ASCII), so a replacement with the same coverage can be
picked. Put the choice in `DETAIL_FIXTURES`, re-run without
`--scan`, then `--bless` and read the diff.

Both modes sleep 1.5 s between requests — keep it that way.

## Is the harness actually any good?

It was validated by mutation testing: 34 deliberate one-line breaks
(dedupe removed, alias map dropped, `NOT NULL` fallback removed,
`ORDER BY priority` dropped, legacy fallback sentinel changed,
overnight window branch deleted, quick-mode escalation disabled,
…) were each introduced in turn, and every one turned the suite red
via a specific, relevant test. Five tests were vacuous on the first
pass — they only started biting once the synthetic cases above were
added — which is exactly what that exercise is for.

If you add a test, break the thing it covers on purpose once and
confirm it fails. A test that has never failed has never been
shown to work.
