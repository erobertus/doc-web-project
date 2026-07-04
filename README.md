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

1. **Doctors purged from the register.** The new register removes
   deceased doctors and some historical records entirely (the old
   site kept listing them as inactive). When a previously known
   CPSO number is no longer found, the row in `z847e_MD_dir` is
   **kept** and its `reg_stat_code` is set to the
   `Not on Register` status (auto-created in
   `z847e_MD_reg_statuses` on first use); nothing else is touched.
2. **Not-found numbers are no longer permanently excluded by
   default** — they are marked completed for the current run and
   re-checked on the next run, because a "missing" number may be
   issued to a new doctor later. Pass `--perm-exclude` to restore
   the old permanent exclusion.
3. **Renamed reference values.** The new site says `Active` where
   the old one said `Active Member`, and `Man`/`Woman` instead of
   `Male`/`Female`. `REG_STAT_ALIASES` / `GENDER_ALIASES` in
   `constants.py` reuse the existing reference codes when the old
   name is already present in the table, so existing
   `reg_stat_code` values keep their meaning. Reference-name
   lookups are also case-insensitive now.
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
python main.py -a                    # abort all running scrapes
```

Defaults: CPSO range 10000–200000, batch 50, 1 s delay, DB
`faxcomet_MD_list` on `faxcomet.com` (see `-h` for the credentials
options).

## Dependencies

See `requirements.txt` (`mariadb`, `requests`, `beautifulsoup4`,
`googlemaps`). `mechanicalsoup` is no longer needed.
