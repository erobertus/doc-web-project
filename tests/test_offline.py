# Offline regression harness for the CPSO scraper.
#
# Runs with NO network and NO database: real physician-info pages
# captured from the register (tests/fixtures/*.html.gz) are replayed
# through the parser and through process_record() against an
# in-memory fake connection.
#
#   python tests/test_offline.py            # run everything
#   python tests/test_offline.py --bless    # re-record golden.json
#
# --bless rewrites the parser snapshot. Only ever do that when a
# parser change is INTENTIONAL, and eyeball `git diff` on
# golden.json afterwards - that diff is the regression report.
#
# What this harness deliberately does NOT cover: the scheduling
# WHERE clause in _read_schedule is evaluated by MariaDB (RLIKE,
# CURTIME(), the overnight-window branches), so only its Python
# side and its SQL shape are checked here. See tests/README.md.

import argparse
import contextlib
import gzip
import io
import json
import os
import sys
import types
import unittest
from datetime import timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FIX_DIR = os.path.join(HERE, 'fixtures')
GOLDEN = os.path.join(FIX_DIR, 'golden.json')
sys.path.insert(0, ROOT)

BLESS = False


# --- optional native deps -------------------------------------
# The scraper imports mariadb and googlemaps; neither is needed
# offline, and a bare dev machine may not have them. Prefer the
# real modules (real exception classes) and stub only if absent.
def _stub(name, **attrs):
    try:
        return __import__(name)
    except ImportError:
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod
        return mod


class _StubError(Exception):
    def __init__(self, msg='', errno=None):
        super().__init__(msg)
        self.errno = errno


_stub('mariadb', Error=_StubError, OperationalError=_StubError,
      InterfaceError=_StubError, connect=lambda **kw: None)
_stub('googlemaps', Client=object)

import mariadb                                       # noqa: E402
import constants as K                                # noqa: E402
import cpso_site                                     # noqa: E402
import main                                          # noqa: E402


def db_error(msg='', errno=None):
    """A driver error of whichever mariadb module got loaded."""
    try:
        return mariadb.Error(msg, errno)
    except TypeError:                # real connector's signature
        e = mariadb.Error(msg)
        e.errno = errno
        return e


def load_fixture(name):
    with gzip.open(os.path.join(FIX_DIR, name), 'rt',
                   encoding='utf-8') as fh:
        return fh.read()


def fixture_names():
    return sorted(f for f in os.listdir(FIX_DIR)
                  if f.endswith('.html.gz'))


def fixture_for(cpso_no):
    hit = [f for f in fixture_names()
           if f.startswith(f'detail_{cpso_no}_')]
    if not hit:
        raise AssertionError(f'no fixture for CPSO {cpso_no}')
    return load_fixture(hit[0])


def parse_fixture(cpso_no):
    return cpso_site.parse_physician_page(fixture_for(cpso_no))


# ==============================================================
#  fake database
# ==============================================================

# every column the control table has ever had, so the fake can
# simulate an older schema by declaring a subset
ALL_CTL_COLS = ('control_uno', 'go_flag', 'quick_mode',
                'cpso_start', 'cpso_stop', 'batch_size',
                'delay_sec', 'use_random', 'updated',
                'abort_check', 'run_from', 'run_until',
                'interval_days', 'log_verbose', 'auto_update_hrs',
                'skip_gaps', 'delay_jitter',
                'agent_pattern', 'dow_pattern', 'priority')


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self._rows = []
        self.lastrowid = None

    def execute(self, stmt, params=()):
        self.db.statements.append((' '.join(stmt.split()), params))
        self._rows = self.db.dispatch(stmt, params, self)

    def __iter__(self):
        return iter(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class FakeDb:
    """Emulates just enough MariaDB for the reference-table and
    record-write paths, and records every statement issued."""

    def __init__(self, refs=None, dir_rows=None):
        # table -> {name: code}
        self.refs = {t: dict(v) for t, v in (refs or {}).items()}
        # CPSO number -> reg_stat_code already on file
        self.dir_rows = dict(dir_rows or {})
        self.statements = []
        self.inserted = []          # (table, {col: val})
        self.deleted = []           # (table, params)
        self.autocommit = True
        self._next = 1000

    # -- connection API --
    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    # -- helpers --
    def _new_code(self):
        self._next += 1
        return self._next

    def _table_of(self, stmt, keyword):
        toks = stmt.split()
        for i, t in enumerate(toks):
            if t.upper() == keyword and i + 1 < len(toks):
                return toks[i + 1].strip('(),')
        return ''

    def dispatch(self, stmt, params, cur):
        s = ' '.join(stmt.split())
        up = s.upper()

        if up.startswith(('START TRANSACTION', 'COMMIT',
                          'ROLLBACK', 'SET ')):
            return []

        if up.startswith('SELECT'):
            return self._select(s, up, params, cur)

        if up.startswith('INSERT'):
            return self._insert(s, params, cur)

        if up.startswith('DELETE'):
            self.deleted.append((self._table_of(s, 'FROM'), params))
            return []

        return []

    def _select(self, s, up, params, cur):
        table = self._table_of(s, 'FROM')
        ref = self.refs.get(table)

        # refresh_ref_from_db: SELECT name, code FROM table
        if ref is not None and 'WHERE' not in up \
                and 'COUNT(' not in up:
            return [(n, c) for n, c in ref.items()]

        # update_reference: SELECT COUNT(*) ... WHERE name = ?
        if 'COUNT(' in up:
            if ref is None:
                return [(0,)]
            want = params[0] if params else None
            return [(1 if want in ref else 0,)]

        # update_reference: SELECT code ... WHERE name = ?
        if ref is not None and params:
            want = params[0]
            return [(ref[want],)] if want in ref else []

        # quick mode: SELECT reg_stat_code FROM dir WHERE CPSO_no = ?
        if table == K.MD_DIR_TABLE and params:
            code = self.dir_rows.get(params[0])
            return [] if code is None else [(code,)]

        # update_x_table: SELECT val FROM t WHERE key=? AND val IN(..)
        return []

    def _insert(self, s, params, cur):
        table = self._table_of(s, 'INTO')
        head = s[s.index('(') + 1:s.index(')')]
        cols = [c.strip() for c in head.split(',')]
        row = dict(zip(cols, params))
        self.inserted.append((table, row))

        ref = self.refs.get(table)
        if ref is not None and params:
            code = self._new_code()
            ref[params[0]] = code
            cur.lastrowid = code
        else:
            cur.lastrowid = self._new_code()
        return []


class FakeControlDb:
    """Serves MD_scrape_control rows, and can pretend to be an
    older schema: any statement mentioning a column outside
    `columns` fails with 1054, exactly like a pre-upgrade DB."""

    def __init__(self, rows, columns=ALL_CTL_COLS, now='20:00:00'):
        self.rows = rows            # list of {col: value}
        self.columns = set(columns)
        self.now = now
        self.statements = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def dispatch(self, stmt, params, cur):
        s = ' '.join(stmt.split())
        missing = [c for c in ALL_CTL_COLS
                   if c in s and c not in self.columns]
        if missing:
            raise db_error(f"Unknown column '{missing[0]}'",
                           errno=1054)
        if not self.rows:
            return []
        select = s[len('SELECT '):s.upper().index(' FROM')]
        exprs = [e.strip() for e in select.split(',')]
        if len(exprs) == 1 and exprs[0].upper().startswith('MAX('):
            col = exprs[0][4:].rstrip(')')
            vals = [r.get(col) for r in self.rows
                    if r.get('go_flag') and r.get(col) is not None]
            return [(max(vals),)] if vals else [(None,)]
        return [tuple(self._value(e, self.rows[0]) for e in exprs)]

    def _value(self, expr, row):
        e = expr.strip()
        if e.upper() == 'CURTIME()':
            return self.now
        if e.endswith('+0'):
            v = row.get(e[:-2].strip())
            return None if v is None else int(v)
        return row.get(e)


# a control row with every column set to a known value
CTL_ROW = {
    'control_uno': 7, 'go_flag': 1, 'quick_mode': 0,
    'cpso_start': 11000, 'cpso_stop': 120000, 'batch_size': 40,
    'delay_sec': 2.5, 'use_random': 1, 'updated': '2026-07-19 03:00:00',
    'abort_check': 25, 'run_from': '19:00:00',
    'run_until': '06:00:00', 'interval_days': 0, 'log_verbose': 1,
    'auto_update_hrs': 6, 'skip_gaps': 1, 'delay_jitter': 0.4,
    'agent_pattern': '.*', 'dow_pattern': '.*', 'priority': 100,
}

# reference tables seeded with the production codes that must
# keep their meaning (reg_stat_code 1 = active, 3 = deceased)
REFS = {
    K.REG_STAT_TABLE: {'Active Member': 1, 'Inactive Member': 2,
                       'Expired: Member deceased': 3,
                       'Expired: Committee terms and conditions': 8},
    K.REG_CLASS_TABLE: {'Independent Practice': 1},
    K.GENDER_TABLE: {'Male': 1, 'Female': 2, 'Unknown': 9},
    K.LANGUAGE_TABLE: {'English': 1, 'French': 2},
    K.UNIV_TABLE: {},
    K.REG_JUR_TABLE: {},
    K.SPEC_TABLE: {},
    K.STYPE_TABLE: {},
    K.HOSP_TABLE: {K.WEB_NO_HOSP: 1},
}


@contextlib.contextmanager
def quiet():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        yield buf


# A minimal page, for shapes the captured fixtures happen not to
# contain. Kept tiny and explicit so what is being tested is
# obvious; the real pages cover everything else.
MINI_PAGE = """
<div class="scrp-banner">
  <span class="scrp-contactname-value">Testerton, Ada Mary</span>
  <span class="scrp-cpsonumber-value">99999</span>
  <span class="scrp-registrationstatus-value">Active</span>
  <span class="scrp-statusdate-value">as of 02 Jul 2010</span>
</div>
<div class="scrp-generalinfo">
  <div id="general-information">
    <div class="scrp-gender"><span>Gender:</span></div>
  </div>
  <span class="scrp-jurisdiction-value">Alberta</span>
  <span class="scrp-jurisdiction-value">Alberta</span>
  <span class="scrp-jurisdiction-value">Manitoba</span>
</div>
"""


def synthetic(**over):
    """A complete parsed record, so a single odd field can be
    exercised without hand-building a whole page."""
    rec = {
        'name': 'Testerton, Ada Mary', 'cpso_no': '99999',
        'status': 'Active', 'status_date': '02 Jul 2010',
        'expiry_date': '', 'reg_class': '', 'reg_class_date': '',
        'former_name': '', 'gender': 'Man', 'languages': ['English'],
        'medical_school': '', 'grad_year': '', 'date_of_death': '',
        'jurisdictions': [], 'locations': [], 'specialties': [],
        'hospitals': [],
    }
    rec.update(over)
    return rec


def location(lines=('1 Main St',), city='Toronto', prov='ON',
             postal='M5G 2C4', country='Canada', phone='', ext='',
             fax=''):
    return {'address': {'lines': list(lines), 'city': city,
                        'prov': prov, 'postal': postal,
                        'country': country,
                        'raw': '\n'.join(lines)},
            'phone': phone, 'ext': ext, 'fax': fax}


@contextlib.contextmanager
def served_parsed(rec, geo=(None, [999])):
    """Feed process_record a chosen parsed record."""
    old = (main.fetch_physician_page, main.parse_physician_page,
           main.get_geocode_db_uno)
    main.fetch_physician_page = lambda *a, **kw: '<html></html>'
    main.parse_physician_page = lambda *a, **kw: rec
    main.get_geocode_db_uno = lambda *a, **kw: geo
    try:
        yield
    finally:
        (main.fetch_physician_page, main.parse_physician_page,
         main.get_geocode_db_uno) = old


@contextlib.contextmanager
def served(cpso_no, geo=(None, [999])):
    """Run main's scrape path against a fixture instead of the
    live site, with the geocoder stubbed out."""
    html = fixture_for(cpso_no)
    old_fetch = main.fetch_physician_page
    old_geo = main.get_geocode_db_uno
    main.fetch_physician_page = lambda *a, **kw: html
    main.get_geocode_db_uno = lambda *a, **kw: geo
    try:
        yield
    finally:
        main.fetch_physician_page = old_fetch
        main.get_geocode_db_uno = old_geo


# ==============================================================
#  1. parser snapshot - catches ANY drift in parse_physician_page
# ==============================================================

class TestParserGolden(unittest.TestCase):

    def test_golden(self):
        current = {name: cpso_site.parse_physician_page(
            load_fixture(name)) for name in fixture_names()}

        if BLESS:
            with open(GOLDEN, 'w', encoding='utf-8') as fh:
                json.dump(current, fh, ensure_ascii=False,
                          indent=1, sort_keys=True)
            self.skipTest(f'blessed {len(current)} fixtures')

        self.assertTrue(os.path.exists(GOLDEN),
                        'golden.json missing - run with --bless')
        with open(GOLDEN, encoding='utf-8') as fh:
            golden = json.load(fh)

        self.assertEqual(sorted(golden), sorted(current),
                         'fixture set changed - re-bless')
        for name in sorted(current):
            with self.subTest(fixture=name):
                self.assertEqual(golden[name], current[name])


# ==============================================================
#  2. parser invariants that must never silently change
# ==============================================================

class TestParserInvariants(unittest.TestCase):

    def test_missing_number_is_none(self):
        # a purged/never-issued number must parse to None so the
        # caller flags a gap rather than writing a blank doctor
        self.assertIsNone(parse_fixture(10310))

    def test_page_marker_present_but_no_banner(self):
        html = fixture_for(10310)
        self.assertIn(cpso_site.PAGE_MARKER, html)

    def test_active_record_shape(self):
        rec = parse_fixture(72500)
        self.assertEqual(rec['cpso_no'], '72500')
        self.assertEqual(rec['status'], 'Active')
        self.assertTrue(rec['name'])
        self.assertIn(',', rec['name'])          # 'Last, First'
        self.assertEqual(len(rec['hospitals']), 3)
        self.assertTrue(all(h['name'] for h in rec['hospitals']))

    def test_status_detail_beats_pill(self):
        # pill says 'Inactive'; the detail line carries the real
        # wording and the date, and must win
        rec = parse_fixture(55000)
        self.assertTrue(rec['status'].startswith('Expired:'))
        self.assertTrue(rec['status_date'])
        self.assertNotIn(' as of ', rec['status'])

    def test_deceased_has_status_date(self):
        # the new register has no Date of Death field; the
        # 'Deceased as of ...' date is what process_record turns
        # into date_of_death, so it must be parsed
        for no in (79082, 94138):
            with self.subTest(cpso=no):
                rec = parse_fixture(no)
                self.assertEqual(rec['status'], K.DECEASED_STAT)
                self.assertTrue(rec['status_date'])

    def test_former_name(self):
        self.assertTrue(parse_fixture(79082)['former_name'])
        self.assertEqual(parse_fixture(94138)['former_name'], '')

    def test_two_locations_primary_first(self):
        rec = parse_fixture(83500)
        self.assertEqual(len(rec['locations']), 2)
        self.assertIsNotNone(rec['locations'][0]['address'])

    def test_fax_captured(self):
        rec = parse_fixture(110500)
        self.assertTrue(any(l['fax'] for l in rec['locations']))

    def test_jurisdiction_present(self):
        rec = parse_fixture(91500)
        self.assertTrue(rec['jurisdictions'])
        self.assertEqual(len(rec['jurisdictions']),
                         len(set(rec['jurisdictions'])))

    def test_jurisdiction_deduped(self):
        # the page ships desktop + mobile + print copies of the
        # section, so the same licence appears several times in
        # the DOM and must be collapsed
        rec = cpso_site.parse_physician_page(MINI_PAGE)
        self.assertEqual(rec['jurisdictions'],
                         ['Alberta', 'Manitoba'])

    def test_mini_page_banner(self):
        rec = cpso_site.parse_physician_page(MINI_PAGE)
        self.assertEqual(rec['name'], 'Testerton, Ada Mary')
        self.assertEqual(rec['cpso_no'], '99999')
        # pill 'Active' + detail 'as of <date>' -> status + date
        self.assertEqual(rec['status'], 'Active')
        self.assertEqual(rec['status_date'], '02 Jul 2010')
        self.assertEqual(rec['gender'], '')

    def test_non_ascii_survives(self):
        rec = parse_fixture(119500)
        self.assertFalse(json.dumps(rec, ensure_ascii=False)
                         .isascii())

    def test_no_invisible_chars_anywhere(self):
        for name in fixture_names():
            rec = cpso_site.parse_physician_page(load_fixture(name))
            if rec is None:
                continue
            blob = json.dumps(rec, ensure_ascii=False)
            with self.subTest(fixture=name):
                self.assertIsNone(cpso_site.RE_INVISIBLE
                                  .search(blob))
                self.assertNotIn('‑', blob)
                self.assertNotIn('\xa0', blob)

    def test_no_placeholder_text_leaks(self):
        for name in fixture_names():
            rec = cpso_site.parse_physician_page(load_fixture(name))
            if rec is None:
                continue
            with self.subTest(fixture=name):
                self.assertNotEqual(rec['former_name'],
                                    cpso_site.NO_FRMR_NAME)
                self.assertNotEqual(rec['gender'],
                                    cpso_site.NOT_AVAILABLE)
                for loc in rec['locations']:
                    if loc['address'] is not None:
                        self.assertNotIn(
                            cpso_site.NO_ADDRESS,
                            loc['address']['lines'])


# ==============================================================
#  3. address / string helpers
# ==============================================================

class TestHelpers(unittest.TestCase):

    def test_norm_strips_invisibles(self):
        self.assertEqual(
            cpso_site._norm('​St‮  Mary﻿\xa0St'),
            'St Mary St')

    def test_norm_nb_hyphen(self):
        self.assertEqual(cpso_site._norm('Sault‑Ste'),
                         'Sault-Ste')

    def test_split_as_of(self):
        self.assertEqual(
            cpso_site._split_as_of('Independent Practice '
                                   'as of 02 Jul 2010'),
            ('Independent Practice', '02 Jul 2010'))
        self.assertEqual(cpso_site._split_as_of('as of 02 Jul 2010'),
                         ('', '02 Jul 2010'))
        self.assertEqual(cpso_site._split_as_of('Active'),
                         ('Active', ''))

    def test_locality_canadian(self):
        self.assertEqual(
            cpso_site.parse_locality_line('Toronto Ontario M5G 2C4'),
            {'postal': 'M5G 2C4', 'country': 'Canada',
             'prov': 'ON', 'city': 'Toronto'})

    def test_locality_two_letter_province(self):
        got = cpso_site.parse_locality_line('Kingston ON  K7L 2V7')
        self.assertEqual(got['prov'], 'ON')
        self.assertEqual(got['city'], 'Kingston')
        self.assertEqual(got['postal'], 'K7L 2V7')

    def test_locality_postal_without_space(self):
        got = cpso_site.parse_locality_line('Ottawa Ontario K1H8L6')
        self.assertEqual(got['postal'], 'K1H 8L6')

    def test_locality_us(self):
        got = cpso_site.parse_locality_line('Buffalo New York 14203')
        self.assertEqual(got, {'postal': '14203', 'prov': 'NY',
                               'country': 'United States',
                               'city': 'Buffalo'})

    def test_locality_five_digits_without_state_rejected(self):
        # some other country's postal code must not be mistaken
        # for a US zip
        self.assertEqual(
            cpso_site.parse_locality_line('Ramat Gan 52621'), {})

    def test_locality_not_a_locality(self):
        self.assertEqual(
            cpso_site.parse_locality_line('123 Main Street'), {})

    def test_address_text_layers(self):
        got = cpso_site.parse_address_text(
            ['Sunnybrook Hospital', '2075 Bayview Ave', 'Suite A1',
             'Toronto Ontario M4N 3M5', 'Canada'])
        self.assertEqual(got['lines'], ['Sunnybrook Hospital',
                                        '2075 Bayview Ave',
                                        'Suite A1'])
        self.assertEqual(got['city'], 'Toronto')
        self.assertEqual(got['prov'], 'ON')
        self.assertEqual(got['postal'], 'M4N 3M5')
        self.assertEqual(got['country'], 'Canada')

    def test_address_uses_last_locality_line(self):
        # a street line that happens to look like a locality must
        # not win over the real one further down
        got = cpso_site.parse_address_text(
            ['London ON N6A 3K7', 'Toronto Ontario M5G 2C4'])
        self.assertEqual(got['city'], 'Toronto')
        self.assertEqual(got['lines'], ['London ON N6A 3K7'])

    def test_phone_extension(self):
        for raw, want in (
                ('416-555-1234 ext. 22', ('416-555-1234', '22')),
                ('416-555-1234 x99', ('416-555-1234', '99')),
                ('416-555-1234 extension: 7', ('416-555-1234', '7')),
                ('416-555-1234', ('416-555-1234', ''))):
            with self.subTest(raw=raw):
                self.assertEqual(cpso_site.parse_phone(raw), want)


# ==============================================================
#  4. quick mode (JSON search API)
# ==============================================================

class TestSearchResult(unittest.TestCase):

    def _result(self, cpso_no):
        path = os.path.join(FIX_DIR, f'search_{cpso_no}.json')
        with open(path, encoding='utf-8') as fh:
            return json.load(fh)['results'][0]

    def test_active_location(self):
        loc = cpso_site.search_result_location(self._result(72500))
        self.assertIsNotNone(loc['address'])
        self.assertEqual(loc['address']['country'], 'Canada')
        self.assertEqual(loc['address']['prov'], 'ON')
        self.assertTrue(loc['address']['postal'].isupper())

    def test_shape_matches_detail_parser(self):
        # quick mode feeds the same process_address() path as the
        # full scrape, so the dicts must have the same keys
        loc = cpso_site.search_result_location(self._result(72500))
        detail = parse_fixture(72500)['locations'][0]
        self.assertEqual(set(loc), set(detail))
        self.assertEqual(set(loc['address']),
                         set(detail['address']))

    def test_missing_address(self):
        loc = cpso_site.search_result_location(
            {'name': 'X', 'phonenumber': ''})
        self.assertIsNone(loc['address'])
        self.assertEqual(loc['phone'], '')

    def test_us_province_mapped(self):
        loc = cpso_site.search_result_location(
            {'street1': '1 Elm', 'city': 'Buffalo',
             'province': 'New York', 'postalcode': '14203'})
        self.assertEqual(loc['address']['prov'], 'NY')
        self.assertEqual(loc['address']['country'],
                         'United States')

    def test_unknown_province_kept_verbatim(self):
        loc = cpso_site.search_result_location(
            {'street1': '1 Rue', 'city': 'Paris',
             'province': 'Ile-de-France', 'postalcode': '75001'})
        self.assertEqual(loc['address']['prov'], 'Ile-de-France')


# ==============================================================
#  5. process_record against the fake DB
# ==============================================================

class TestProcessRecord(unittest.TestCase):

    def run_one(self, cpso_no, refs=None):
        db = FakeDb(refs or REFS)
        with served(cpso_no), quiet():
            out = main.process_record(db, cpso_no, batch_id=1)
        return db, out

    def test_active_record(self):
        db, out = self.run_one(72500)
        self.assertEqual(len(out), 1)
        rec = out[0]
        self.assertEqual(rec[K.C_CPSO_NO], 72500)
        # 'Active' is aliased onto the existing 'Active Member'
        # code - reg_stat_code 1 must keep meaning active
        self.assertEqual(rec[K.C_REG_STAT_CODE], 1)
        self.assertTrue(rec[K.C_LNAME])
        self.assertTrue(rec[K.C_FNAME])

    def test_name_split(self):
        _, out = self.run_one(72500)
        rec = out[0]
        parsed = parse_fixture(72500)
        last, first_middle = parsed['name'].split(',', 1)
        bits = first_middle.split()
        self.assertEqual(rec[K.C_LNAME], last.strip())
        self.assertEqual(rec[K.C_FNAME], bits[0])
        if len(bits) > 1:
            self.assertEqual(rec[K.C_MNAME], ' '.join(bits[1:]))

    def test_deceased_gets_death_date_from_status(self):
        db, out = self.run_one(94138)
        rec = out[0]
        self.assertEqual(rec[K.C_REG_STAT_CODE], 3)
        parsed = parse_fixture(94138)
        self.assertEqual(rec[K.C_DATE_OF_DEATH],
                         main.reformat_date(parsed['status_date']))
        self.assertRegex(rec[K.C_DATE_OF_DEATH],
                         r'^\d{4}-\d{2}-\d{2}$')

    def test_committee_terms_alias(self):
        # the new site's wording must reuse the existing code 8
        db = FakeDb(REFS)
        code = main.retrieve_code_from_name(
            'Expired: Committee Terms & Conditions by '
            'Registration Committee',
            dict(REFS[K.REG_STAT_TABLE]), db,
            K.REG_STAT_TABLE, K.C_REG_STAT_CODE,
            K.C_REG_STAT_NAME, aliases=K.REG_STAT_ALIASES)
        self.assertEqual(code, 8)
        self.assertEqual(db.inserted, [])   # no new status created

    def test_gender_never_null(self):
        # gender is NOT NULL in z847e_MD_dir
        for no in (72500, 94138):
            with self.subTest(cpso=no):
                _, out = self.run_one(no)
                self.assertIn(K.C_MD_GENDER, out[0])
                self.assertIsNotNone(out[0][K.C_MD_GENDER])

    def test_gender_missing_falls_back_to_unknown(self):
        # the site leaves gender blank on some records; the column
        # is NOT NULL, so it must resolve to the 'Unknown' code
        db = FakeDb(REFS)
        with served_parsed(synthetic(gender='')), quiet():
            out = main.process_record(db, 99999, batch_id=1)
        self.assertEqual(out[0][K.C_MD_GENDER],
                         REFS[K.GENDER_TABLE][K.GENDER_UNKNOWN])

    def test_gender_alias_reuses_code(self):
        db = FakeDb(REFS)
        for site_value, want in (('Man', 1), ('Woman', 2)):
            with self.subTest(value=site_value):
                self.assertEqual(
                    main.retrieve_code_from_name(
                        site_value, dict(REFS[K.GENDER_TABLE]), db,
                        K.GENDER_TABLE, K.C_GENDER_CODE,
                        K.C_GENDER_NAME, aliases=K.GENDER_ALIASES),
                    want)

    def test_unknown_gender_falls_back(self):
        db = FakeDb(REFS)
        code = main.retrieve_code_from_name(
            K.GENDER_UNKNOWN, dict(REFS[K.GENDER_TABLE]), db,
            K.GENDER_TABLE, K.C_GENDER_CODE, K.C_GENDER_NAME,
            aliases=K.GENDER_ALIASES)
        self.assertEqual(code, 9)

    def test_case_insensitive_reference_match(self):
        db = FakeDb(REFS)
        code = main.retrieve_code_from_name(
            'ENGLISH', dict(REFS[K.LANGUAGE_TABLE]), db,
            K.LANGUAGE_TABLE, K.C_LANG_CODE, K.C_LANG_NAME)
        self.assertEqual(code, 1)
        self.assertEqual(db.inserted, [])

    def test_new_reference_value_inserted_once(self):
        refs = {K.SPEC_TABLE: {}}
        db = FakeDb(refs)
        arr = {}
        a = main.retrieve_code_from_name(
            'Nephrology', arr, db, K.SPEC_TABLE,
            K.C_SPEC_CODE, K.C_SPEC_NAME)
        b = main.retrieve_code_from_name(
            'Nephrology', arr, db, K.SPEC_TABLE,
            K.C_SPEC_CODE, K.C_SPEC_NAME)
        self.assertEqual(a, b)
        self.assertEqual(len(db.inserted), 1)

    def test_hospitals_named_with_location(self):
        # the reference table keeps the old program's
        # 'Hospital Name (Location)' convention
        parsed = parse_fixture(72500)
        wanted = {f'{h["name"]} ({h["location"]})' if h['location']
                  else h['name'] for h in parsed['hospitals']}
        db, _ = self.run_one(72500)
        names = {row.get(K.C_HOSP_NAME) for t, row in db.inserted
                 if t == K.HOSP_TABLE}
        self.assertTrue(wanted <= names,
                        f'missing {wanted - names}')

    def test_no_hospitals_gets_placeholder(self):
        _, out = self.run_one(94138)
        self.assertEqual(parse_fixture(94138)['hospitals'], [])
        self.assertEqual(out[0][K.MD_HOSP_TABLE],
                         [REFS[K.HOSP_TABLE][K.WEB_NO_HOSP]])

    def test_child_lists_deduped(self):
        for no in (72500, 83500, 91500):
            with self.subTest(cpso=no):
                _, out = self.run_one(no)
                rec = out[0]
                for tbl in (K.MD_HOSP_TABLE, K.MD_LANG_TABLE,
                            K.MD_REG_JURISDIC):
                    vals = rec.get(tbl, [])
                    self.assertEqual(len(vals), len(set(vals)),
                                     f'{tbl} has duplicates')

    def test_repeated_hospital_deduped(self):
        # the new register lists one row per appointment, so the
        # same hospital repeats; a duplicate code would violate
        # the unique (cpso_no, hosp_code) index
        rec = synthetic(hospitals=[
            {'name': 'Mount Sinai', 'location': 'Toronto'},
            {'name': 'Mount Sinai', 'location': 'Toronto'},
            {'name': 'Sunnybrook', 'location': 'Toronto'}])
        db = FakeDb(REFS)
        with served_parsed(rec), quiet():
            out = main.process_record(db, 99999, batch_id=1)
        codes = out[0][K.MD_HOSP_TABLE]
        self.assertEqual(len(codes), 2)
        self.assertEqual(len(codes), len(set(codes)))

    def test_repeated_language_deduped(self):
        db = FakeDb(REFS)
        with served_parsed(
                synthetic(languages=['English', 'English',
                                     'French'])), quiet():
            out = main.process_record(db, 99999, batch_id=1)
        self.assertEqual(out[0][K.MD_LANG_TABLE], [1, 2])

    def test_addresses_ordered_and_flagged(self):
        _, out = self.run_one(83500)
        addrs = out[0][K.MD_ADDR_TABLE]
        self.assertEqual(len(addrs), 2)
        self.assertEqual([a[K.C_ADDR_ORDER] for a in addrs], [1, 2])
        self.assertEqual([a[K.C_ADDR_IS_DEF] for a in addrs], [1, 0])

    def test_address_1_always_present(self):
        # address_1 is NOT NULL in MD_addresses
        for no in (72500, 83500, 94138, 110500, 119500):
            with self.subTest(cpso=no):
                _, out = self.run_one(no)
                for a in out[0][K.MD_ADDR_TABLE]:
                    self.assertIn(K.C_ADDR_PREFIX + '1', a)
                    self.assertIsNotNone(a[K.C_ADDR_PREFIX + '1'])

    def test_locality_only_address_still_fills_address_1(self):
        # an address can be a bare 'City PROV Postal' with no
        # street line at all - address_1 is still NOT NULL
        db = FakeDb(REFS)
        with served_parsed(
                synthetic(locations=[location(lines=[])])), quiet():
            out = main.process_record(db, 99999, batch_id=1)
        addr = out[0][K.MD_ADDR_TABLE][0]
        self.assertIn(K.C_ADDR_PREFIX + '1', addr)
        self.assertIsNotNone(addr[K.C_ADDR_PREFIX + '1'])
        self.assertEqual(addr[K.C_ADDR_CITY], 'Toronto')

    def test_no_address_gets_placeholder(self):
        db = FakeDb(REFS)
        blank = {'address': None, 'phone': '', 'ext': '', 'fax': ''}
        with served_parsed(
                synthetic(locations=[blank])), quiet():
            out = main.process_record(db, 99999, batch_id=1)
        addr = out[0][K.MD_ADDR_TABLE][0]
        self.assertEqual(addr[K.C_ADDR_PREFIX + '1'], K.NO_ADDR)

    def test_overflow_address_lines_folded(self):
        # more than four street lines must not be lost
        db = FakeDb(REFS)
        lines = ['L1', 'L2', 'L3', 'L4', 'L5']
        with served_parsed(
                synthetic(locations=[location(lines=lines)])), \
                quiet():
            out = main.process_record(db, 99999, batch_id=1)
        addr = out[0][K.MD_ADDR_TABLE][0]
        self.assertEqual(addr[K.C_ADDR_PREFIX + '4'], 'L4, L5')

    def test_geocode_linked(self):
        _, out = self.run_one(72500)
        addrs = out[0][K.MD_ADDR_TABLE]
        self.assertTrue(any(K.C_GEO_UNO in a for a in addrs))

    def test_missing_number_writes_nothing(self):
        db, out = self.run_one(10310)
        self.assertEqual(out, [])

    def test_fetch_failure_is_not_a_gap(self):
        # a site outage must NOT be mistaken for 'no such doctor':
        # flagging it would corrupt a good record and (with
        # --exclude-invalid) permanently exclude the number
        db = FakeDb(REFS)

        def fail(*a, **kw):
            raise cpso_site.CpsoFetchError('site down')

        flagged = []
        old_fetch = main.fetch_physician_page
        old_flag = main.flag_not_on_register
        main.fetch_physician_page = fail
        main.flag_not_on_register = \
            lambda *a, **kw: flagged.append(a)
        try:
            with quiet():
                out = main.process_record(db, 72500, batch_id=1)
        finally:
            main.fetch_physician_page = old_fetch
            main.flag_not_on_register = old_flag

        self.assertEqual(out, [])
        self.assertEqual(flagged, [], 'outage flagged as a gap')
        self.assertEqual(db.inserted, [])
        self.assertFalse([s for s, _ in db.statements
                          if s.upper().startswith(('UPDATE',
                                                   'DELETE'))])

    def test_missing_number_is_flagged(self):
        # the converse: a genuinely absent number MUST be flagged
        db = FakeDb(REFS)
        flagged = []
        old = main.flag_not_on_register
        main.flag_not_on_register = \
            lambda *a, **kw: flagged.append(a)
        try:
            with served(10310), quiet():
                main.process_record(db, 10310, batch_id=1)
        finally:
            main.flag_not_on_register = old
        self.assertEqual(len(flagged), 1)


# ==============================================================
#  5b. quick mode (JSON refresh) branch selection
# ==============================================================

class TestProcessRecordQuick(unittest.TestCase):
    """Quick mode must fall back to the full detail scrape in the
    two cases the search API cannot describe."""

    def _run(self, result, dir_rows, full=None):
        db = FakeDb(REFS, dir_rows=dir_rows)
        payload = {'totalcount': 1 if result else 0,
                   'results': [result] if result else []}
        calls = []
        old = (main.fetch_search_result, main.process_record,
               main.flag_not_on_register, main.update_detail_table)
        main.fetch_search_result = lambda *a, **kw: payload
        main.process_record = \
            lambda *a, **kw: calls.append('full') or ['REC']
        main.flag_not_on_register = \
            lambda *a, **kw: calls.append('flag')
        main.update_detail_table = \
            lambda *a, **kw: calls.append('detail')
        try:
            with quiet():
                out = main.process_record_quick(db, 99999,
                                                batch_id=1)
        finally:
            (main.fetch_search_result, main.process_record,
             main.flag_not_on_register,
             main.update_detail_table) = old
        return db, out, calls

    def test_unknown_doctor_triggers_full_scrape(self):
        _, out, calls = self._run(
            {'name': 'A, B', 'registrationstatus': 'Active'},
            dir_rows={})
        self.assertEqual(calls, ['full'])
        self.assertEqual(out, ['REC'])

    def test_active_to_inactive_triggers_full_scrape(self):
        # the JSON does not carry the detailed inactive reason
        _, out, calls = self._run(
            {'name': 'A, B', 'registrationstatus': 'Inactive'},
            dir_rows={99999: 1})          # 1 = 'Active Member'
        self.assertEqual(calls, ['full'])

    def test_already_inactive_stays_quick(self):
        _, out, calls = self._run(
            {'name': 'A, B', 'registrationstatus': 'Inactive'},
            dir_rows={99999: 3})          # already deceased
        self.assertEqual(calls, ['detail'])
        self.assertEqual(out, [])

    def test_no_results_is_flagged(self):
        _, out, calls = self._run(None, dir_rows={99999: 1})
        self.assertEqual(calls, ['flag'])
        self.assertEqual(out, [])

    def test_search_failure_is_not_a_gap(self):
        db = FakeDb(REFS, dir_rows={99999: 1})
        flagged = []
        old = (main.fetch_search_result, main.flag_not_on_register)

        def fail(*a, **kw):
            raise cpso_site.CpsoFetchError('search down')

        main.fetch_search_result = fail
        main.flag_not_on_register = \
            lambda *a, **kw: flagged.append(a)
        try:
            with quiet():
                out = main.process_record_quick(db, 99999,
                                                batch_id=1)
        finally:
            (main.fetch_search_result,
             main.flag_not_on_register) = old
        self.assertEqual(out, [])
        self.assertEqual(flagged, [])

    def test_active_refresh_replaces_default_address(self):
        db = FakeDb(REFS, dir_rows={99999: 1})
        old = (main.fetch_search_result, main.get_geocode_db_uno)
        main.fetch_search_result = lambda *a, **kw: {
            'totalcount': 1, 'results': [{
                'name': 'Testerton, Ada Mary',
                'registrationstatus': 'Active',
                'street1': '1 Main St', 'city': 'Toronto',
                'province': 'Ontario', 'postalcode': 'M5G 2C4',
                'phonenumber': '416-555-1234 ext. 22',
                'fax': '416-555-9999'}]}
        main.get_geocode_db_uno = lambda *a, **kw: (None, [999])
        try:
            with quiet():
                main.process_record_quick(db, 99999, batch_id=1)
        finally:
            (main.fetch_search_result,
             main.get_geocode_db_uno) = old

        addr = [r for t, r in db.inserted
                if t == K.MD_ADDR_TABLE]
        self.assertEqual(len(addr), 1)
        self.assertEqual(addr[0][K.C_ADDR_ORDER], 1)
        self.assertEqual(addr[0][K.C_ADDR_PHONE_NO],
                         '416-555-1234')
        self.assertEqual(addr[0][K.C_ADDR_EXT], '22')
        self.assertEqual(addr[0][K.C_ADDR_FAX_NO], '416-555-9999')
        # only the DEFAULT address is replaced; the additional
        # locations collected by a full sweep must survive
        deletes = [s for s, _ in db.statements
                   if s.upper().startswith('DELETE')
                   and K.MD_ADDR_TABLE in s]
        self.assertEqual(len(deletes), 1)
        self.assertIn(f'{K.C_ADDR_ORDER} = 1', deletes[0])

        # quick mode resolves the status too - 'Active' must reuse
        # the existing 'Active Member' code, not mint a new one.
        # (Match only the parameterized row update; FINAL_SQL also
        # issues an 'UPDATE z847e_MD_dir d JOIN ...' clean-up.)
        upd = [(s, p) for s, p in db.statements
               if s.startswith(f'UPDATE {K.MD_DIR_TABLE} SET')]
        self.assertEqual(len(upd), 1)
        self.assertIn(f'{K.C_REG_STAT_CODE} = ?', upd[0][0])
        self.assertIn(1, upd[0][1])
        self.assertNotIn(K.REG_STAT_TABLE,
                         [t for t, _ in db.inserted])


# ==============================================================
#  6. update_record write path
# ==============================================================

class TestUpdateRecord(unittest.TestCase):

    def test_replace_then_insert(self):
        db = FakeDb(REFS)
        rec = {K.C_LNAME: 'Smith', K.C_FNAME: 'Jane'}
        with quiet():
            main.update_record(db, [rec], K.MD_DIR_TABLE, 72500)
        # the write is a full replace, so the DELETE must come first
        kinds = [s.split()[0].upper() for s, _ in db.statements]
        self.assertEqual(kinds[0], 'DELETE')
        self.assertIn('INSERT', kinds)
        table, row = db.inserted[-1]
        self.assertEqual(table, K.MD_DIR_TABLE)
        self.assertEqual(row[K.C_CPSO_NO], 72500)
        self.assertEqual(row[K.C_LNAME], 'Smith')

    def test_child_rows_carry_parent_key(self):
        db = FakeDb(REFS)
        rec = {K.C_LNAME: 'Smith',
               K.MD_ADDR_TABLE: [{K.C_ADDR_PREFIX + '1': '1 Main',
                                  K.C_ADDR_ORDER: 1}]}
        with quiet():
            main.update_record(db, [rec], K.MD_DIR_TABLE, 72500)
        addr = [r for t, r in db.inserted if t == K.MD_ADDR_TABLE]
        self.assertEqual(len(addr), 1)
        self.assertEqual(addr[0][K.C_CPSO_NO], 72500)

    def test_autocommit_restored(self):
        db = FakeDb(REFS)
        db.autocommit = True
        with quiet():
            main.update_record(db, [{K.C_LNAME: 'S'}],
                               K.MD_DIR_TABLE, 1)
        self.assertTrue(db.autocommit)

    def test_x_table_dedupes(self):
        db = FakeDb(REFS)
        with quiet():
            main.update_x_table(db, K.MD_LANG_TABLE, K.C_CPSO_NO,
                                72500, K.C_LANG_CODE, [1, 2, 1, 2])
        inserts = [r for t, r in db.inserted if t == K.MD_LANG_TABLE]
        self.assertEqual(len(inserts), 2)


# ==============================================================
#  7. scheduling / control table  (the July 2026 rework)
# ==============================================================

class TestScheduling(unittest.TestCase):

    def test_ctl_built_from_schedule_row(self):
        db = FakeControlDb([CTL_ROW], now='20:00:00')
        ctl = main.read_control(db, 'KNST-MD-1')
        self.assertIsNotNone(ctl)
        self.assertTrue(ctl['go'])
        self.assertFalse(ctl['quick'])
        self.assertEqual(ctl['cpso_start'], 11000)
        self.assertEqual(ctl['cpso_stop'], 120000)
        self.assertEqual(ctl['batch_size'], 40)
        self.assertEqual(ctl['delay'], 2.5)
        self.assertTrue(ctl['random'])
        self.assertEqual(ctl['updated'], '2026-07-19 03:00:00')
        self.assertEqual(ctl['abort_check'], 25)
        self.assertEqual(ctl['interval'], 0)
        self.assertTrue(ctl['log_verbose'])
        self.assertEqual(ctl['auto_update_hrs'], 6)
        self.assertTrue(ctl['skip_gaps'])
        self.assertEqual(ctl['delay_jitter'], 0.4)

    def test_host_is_the_query_parameter(self):
        db = FakeControlDb([CTL_ROW])
        main.read_control(db, 'BrPC-MD-LEFT')
        stmt, params = db.statements[0]
        self.assertEqual(params, ('BrPC-MD-LEFT',))

    def test_schedule_sql_shape(self):
        db = FakeControlDb([CTL_ROW])
        main.read_control(db, 'X')
        stmt = db.statements[0][0]
        # highest priority wins, newest row breaks ties, one row
        self.assertIn('ORDER BY priority ASC, control_uno DESC',
                      stmt)
        self.assertIn('LIMIT 1', stmt)
        # the three matching criteria
        self.assertIn('? RLIKE agent_pattern', stmt)
        self.assertIn('RLIKE dow_pattern', stmt)
        self.assertIn('go_flag', stmt)
        # 24h, same-day and overnight window branches
        self.assertIn('run_from = run_until', stmt)
        self.assertIn('run_from < run_until', stmt)
        self.assertIn('run_from > run_until', stmt)
        # judged by the DB clock, never the agent's
        self.assertIn('CURTIME()', stmt)
        self.assertIn("DATE_FORMAT(NOW(), '%a')", stmt)

    def test_no_matching_schedule_means_idle(self):
        db = FakeControlDb([])          # query matches nothing
        self.assertIsNone(main.read_control(db, 'KNST-MD-1'))

    def test_falls_back_when_columns_absent(self):
        # a DB that has not had the scheduling ALTER applied yet
        legacy_cols = [c for c in ALL_CTL_COLS
                       if c not in ('agent_pattern', 'dow_pattern',
                                    'priority')]
        db = FakeControlDb([CTL_ROW], columns=legacy_cols,
                           now='20:00:00')
        ctl = main.read_control(db, 'KNST-MD-1')
        self.assertIsNotNone(ctl)
        self.assertEqual(ctl['cpso_start'], 11000)
        self.assertTrue(any('agent_pattern' in s
                            for s, _ in db.statements),
                        'should have tried the schedule query')

    def test_legacy_respects_go_flag(self):
        legacy_cols = [c for c in ALL_CTL_COLS
                       if c not in ('agent_pattern', 'dow_pattern',
                                    'priority')]
        row = dict(CTL_ROW, go_flag=0)
        db = FakeControlDb([row], columns=legacy_cols,
                           now='20:00:00')
        self.assertIsNone(main.read_control(db, 'KNST-MD-1'))

    def test_legacy_respects_window(self):
        legacy_cols = [c for c in ALL_CTL_COLS
                       if c not in ('agent_pattern', 'dow_pattern',
                                    'priority')]
        # window is 19:00-06:00; 12:00 is outside it
        db = FakeControlDb([CTL_ROW], columns=legacy_cols,
                           now='12:00:00')
        self.assertIsNone(main.read_control(db, 'KNST-MD-1'))

    def test_empty_control_table_is_idle(self):
        db = FakeControlDb([], columns=ALL_CTL_COLS)
        self.assertIsNone(main.read_control(db, 'KNST-MD-1'))
        self.assertIsNone(main.read_control(db))

    def test_auto_update_is_fleet_global(self):
        # read independently of any schedule, so an idle agent
        # still picks up a new version
        db = FakeControlDb([dict(CTL_ROW, auto_update_hrs=6),
                            dict(CTL_ROW, auto_update_hrs=12)])
        self.assertEqual(main.get_auto_update_hrs(db), 12)
        stmt = db.statements[0][0]
        self.assertIn('MAX(auto_update_hrs)', stmt)
        self.assertIn('WHERE go_flag', stmt)

    def test_auto_update_ignores_disabled_rows(self):
        db = FakeControlDb([dict(CTL_ROW, go_flag=0,
                                 auto_update_hrs=99)])
        self.assertEqual(main.get_auto_update_hrs(db), 0)

    def test_auto_update_absent_column_is_zero(self):
        cols = [c for c in ALL_CTL_COLS if c != 'auto_update_hrs']
        db = FakeControlDb([CTL_ROW], columns=cols)
        self.assertEqual(main.get_auto_update_hrs(db), 0)

    def test_in_time_window(self):
        def td(s):
            h, m, sec = (int(x) for x in s.split(':'))
            return timedelta(hours=h, minutes=m, seconds=sec)

        # 24h window
        self.assertTrue(main.in_time_window(
            td('03:00:00'), td('00:00:00'), td('00:00:00')))
        # same-day window
        self.assertTrue(main.in_time_window(
            td('10:00:00'), td('09:00:00'), td('17:00:00')))
        self.assertFalse(main.in_time_window(
            td('18:00:00'), td('09:00:00'), td('17:00:00')))
        # overnight window
        self.assertTrue(main.in_time_window(
            td('23:00:00'), td('19:00:00'), td('06:00:00')))
        self.assertTrue(main.in_time_window(
            td('02:00:00'), td('19:00:00'), td('06:00:00')))
        self.assertFalse(main.in_time_window(
            td('12:00:00'), td('19:00:00'), td('06:00:00')))


# ==============================================================
#  8. odds and ends
# ==============================================================

class TestMisc(unittest.TestCase):

    def test_reformat_date(self):
        self.assertEqual(main.reformat_date('02 Jul 2010'),
                         '2010-07-02')
        self.assertEqual(main.reformat_date('02-Jul-2010'),
                         '2010-07-02')

    def test_reformat_date_garbage_is_sentinel(self):
        self.assertEqual(main.reformat_date(''), '1900-01-01')
        self.assertEqual(main.reformat_date('unknown'),
                         '1900-01-01')

    def test_every_fixture_date_reformats(self):
        for name in fixture_names():
            rec = cpso_site.parse_physician_page(load_fixture(name))
            if rec is None:
                continue
            for field in ('status_date', 'expiry_date',
                          'reg_class_date'):
                if rec[field]:
                    with self.subTest(fixture=name, field=field):
                        self.assertRegex(
                            main.reformat_date(rec[field]),
                            r'^\d{4}-\d{2}-\d{2}$')

    def test_agent_version_never_raises(self):
        self.assertIsInstance(main.get_agent_version(), str)


# ==============================================================
#  8a. address family: the knock must match the connection
# ==============================================================

@contextlib.contextmanager
def fake_dns(mapping):
    """socket.getaddrinfo returning a chosen address list."""
    import socket as _s

    def gai(host, port, family=0, type=0, *a, **kw):
        rows = mapping.get(host)
        if rows is None:
            raise _s.gaierror(f'no such host {host}')
        return [(f, _s.SOCK_STREAM, 6, '', (addr, port))
                for f, addr in rows
                if family in (0, _s.AF_UNSPEC, f)]

    old = main.socket.getaddrinfo
    main.socket.getaddrinfo = gai
    try:
        yield
    finally:
        main.socket.getaddrinfo = old


@contextlib.contextmanager
def fake_connect(fail_times=0):
    """mariadb.connect that fails N times, recording its params."""
    calls = []

    def connect(**kw):
        calls.append(kw)
        if len(calls) <= fail_times:
            raise db_error('cannot connect', errno=2003)
        return FakeDb(REFS)

    old = main.mariadb.connect
    main.mariadb.connect = connect
    try:
        yield calls
    finally:
        main.mariadb.connect = old


@contextlib.contextmanager
def fake_knock():
    calls = []
    old = main.knock
    main.knock = lambda host, ports, proto='tcp', delay=0.3, \
        timeout=0.5, family=None: calls.append((host, family))
    try:
        yield calls
    finally:
        main.knock = old


@contextlib.contextmanager
def force_ipv4(on):
    old = main.FORCE_IPV4
    main.FORCE_IPV4 = on
    try:
        yield
    finally:
        main.FORCE_IPV4 = old


DUAL = {'db.example.com': [(main.socket.AF_INET6, '2001:db8::1'),
                           (main.socket.AF_INET, '203.0.113.7')]}


class TestAddressFamily(unittest.TestCase):
    """The knock daemon authorizes the SOURCE address it saw. A v4
    knock followed by a v6 connection authorizes one address and
    connects from another - the whole clinic then looks offline."""

    def test_resolve_orders_and_dedupes(self):
        dupes = {'h': [(main.socket.AF_INET6, '2001:db8::1'),
                       (main.socket.AF_INET6, '2001:db8::1'),
                       (main.socket.AF_INET, '203.0.113.7')]}
        with fake_dns(dupes):
            got = main.resolve_targets('h', 3306, force_ipv4=False)
        self.assertEqual(got, [(main.socket.AF_INET6, '2001:db8::1'),
                               (main.socket.AF_INET, '203.0.113.7')])

    def test_resolve_ipv4_only_when_forced(self):
        with fake_dns(DUAL):
            got = main.resolve_targets('db.example.com', 3306,
                                       force_ipv4=True)
        self.assertEqual(got, [(main.socket.AF_INET, '203.0.113.7')])

    def test_unresolvable_returns_empty(self):
        with fake_dns({}):
            self.assertEqual(
                main.resolve_targets('nope', 3306), [])

    def test_knocks_every_family_it_may_connect_from(self):
        os.environ['CPSO_KNOCK'] = 'tcp:1,2,3'
        try:
            with force_ipv4(False), fake_dns(DUAL), \
                    fake_knock() as knocks, \
                    fake_connect(fail_times=1), quiet():
                main.db_connect(_knock_retries=2, _knock_gap=0,
                                host='db.example.com', port=3306)
        finally:
            os.environ.pop('CPSO_KNOCK', None)
        self.assertEqual(knocks,
                         [('2001:db8::1', main.socket.AF_INET6),
                          ('203.0.113.7', main.socket.AF_INET)])

    def test_forced_ipv4_knocks_and_connects_v4_only(self):
        os.environ['CPSO_KNOCK'] = 'tcp:1,2,3'
        try:
            with force_ipv4(True), fake_dns(DUAL), \
                    fake_knock() as knocks, \
                    fake_connect(fail_times=1) as conns, quiet():
                main.db_connect(_knock_retries=2, _knock_gap=0,
                                host='db.example.com', port=3306)
        finally:
            os.environ.pop('CPSO_KNOCK', None)
        # knocked v4 only...
        self.assertEqual(knocks,
                         [('203.0.113.7', main.socket.AF_INET)])
        # ...and the connection is pinned to that same literal, so
        # it cannot slip back to v6
        self.assertTrue(conns)
        for kw in conns:
            self.assertEqual(kw['host'], '203.0.113.7')

    def test_not_forced_leaves_the_hostname_alone(self):
        with force_ipv4(False), fake_dns(DUAL), \
                fake_connect() as conns, quiet():
            main.db_connect(host='db.example.com', port=3306)
        self.assertEqual(conns[0]['host'], 'db.example.com')

    def test_no_knock_configured_still_raises(self):
        os.environ.pop('CPSO_KNOCK', None)
        with force_ipv4(False), fake_dns(DUAL), \
                fake_knock() as knocks, \
                fake_connect(fail_times=1), quiet():
            with self.assertRaises(Exception):
                main.db_connect(host='db.example.com', port=3306)
        self.assertEqual(knocks, [])

    def test_knock_accepts_a_family(self):
        # knock.py was hardcoded AF_INET, which is what caused this
        import inspect
        import knock as k
        self.assertIn('family',
                      inspect.signature(k.knock).parameters)

    def _knock_families(self, proto, family=None):
        """Families knock() actually opens sockets with (accepting
        the parameter is not the same as honouring it)."""
        import knock as k
        made = []

        class FakeSock:
            def __init__(self, fam, _type):
                made.append(fam)

            def settimeout(self, _t):
                pass

            def connect(self, _addr):
                raise OSError('filtered')     # expected for a knock

            def sendto(self, _data, _addr):
                pass

            def close(self):
                pass

        old_sock, old_sleep = k.socket.socket, k.time.sleep
        k.socket.socket = lambda fam, typ: FakeSock(fam, typ)
        k.time.sleep = lambda _s: None
        try:
            kw = {} if family is None else {'family': family}
            k.knock('198.51.100.1', [1, 2], proto, 0, **kw)
        finally:
            k.socket.socket, k.time.sleep = old_sock, old_sleep
        return made

    def test_knock_honours_the_family_tcp(self):
        got = self._knock_families('tcp', main.socket.AF_INET6)
        self.assertEqual(got, [main.socket.AF_INET6] * 2)

    def test_knock_honours_the_family_udp(self):
        got = self._knock_families('udp', main.socket.AF_INET6)
        self.assertEqual(got, [main.socket.AF_INET6] * 2)

    def test_knock_defaults_to_ipv4(self):
        # unchanged behaviour for any caller that does not care
        self.assertEqual(self._knock_families('tcp'),
                         [main.socket.AF_INET] * 2)

    def _resolved(self, force_env, knock_env):
        old = (os.environ.get('CPSO_FORCE_IPV4'),
               os.environ.get('CPSO_KNOCK'))
        for key, val in (('CPSO_FORCE_IPV4', force_env),
                         ('CPSO_KNOCK', knock_env)):
            if val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = val
        try:
            return main._resolve_force_ipv4()
        finally:
            for key, val in zip(('CPSO_FORCE_IPV4', 'CPSO_KNOCK'),
                                old):
                if val is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = val

    def test_explicit_env_wins(self):
        for raw in ('1', 'true', 'YES', 'on'):
            with self.subTest(raw=raw):
                self.assertTrue(self._resolved(raw, None))
        for raw in ('0', 'false', 'NO', 'off'):
            with self.subTest(raw=raw):
                # explicit opt-out beats the knocking default
                self.assertFalse(self._resolved(raw, 'tcp:1,2,3'))

    def test_defaults_on_when_knocking(self):
        # knocking needs an authorizable, stable address: knockd
        # needs separate rules for v6 and privacy extensions rotate
        # v6 addresses out from under an authorization
        self.assertTrue(self._resolved(None, 'tcp:1,2,3'))

    def test_defaults_off_without_knocking(self):
        # a machine that does not knock keeps ordinary dual-stack
        self.assertFalse(self._resolved(None, None))
        self.assertFalse(self._resolved(None, ''))

    def test_unparseable_env_falls_back_to_the_default(self):
        self.assertTrue(self._resolved('maybe', 'tcp:1,2,3'))
        self.assertFalse(self._resolved('maybe', None))


# ==============================================================
#  8b. agent.log timestamps
# ==============================================================

class TestSay(unittest.TestCase):
    """agent.log is the only record of what an unattended machine
    did; 'Agent: reconnected.' with no time says nothing."""

    STAMP = r'^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] '

    def test_stamps_the_line(self):
        with quiet() as out:
            main.say('Agent: reconnected.')
        self.assertRegex(out.getvalue(), self.STAMP)
        self.assertIn('Agent: reconnected.', out.getvalue())

    def test_one_line_per_call(self):
        with quiet() as out:
            main.say('a')
            main.say('b')
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertRegex(line, self.STAMP)

    def test_scrape_lines_stay_unstamped(self):
        # the per-doctor bulk keeps using plain print, which is what
        # makes the stamped operational lines stand out
        db = FakeDb(REFS)
        with served(72500), quiet() as out:
            main.process_record(db, 72500, batch_id=1)
        doctor = [l for l in out.getvalue().splitlines()
                  if 'CPSO: 72500' in l]
        self.assertTrue(doctor)
        for line in doctor:
            self.assertNotRegex(line, self.STAMP)

    def test_operational_lines_all_go_through_say(self):
        # a bare print() of an agent-state line would land in
        # agent.log with no timestamp - the bug this fixes
        src = open(os.path.join(ROOT, 'main.py'),
                   encoding='utf-8').read()
        markers = ('Agent mode:', 'Agent: idle', 'Agent: go!',
                   'Agent: reconnected', 'Agent: reconnect failed',
                   'Agent: database error', 'Agent: pool exhausted',
                   'DB unreachable', 'DB reachable after knock',
                   'Local log reached its size cap',
                   'Central DESTRUCT command',
                   'Updated - restarting')
        offenders = []
        for line in src.splitlines():
            for m in markers:
                if m in line and 'print(' in line:
                    offenders.append(line.strip()[:60])
        self.assertEqual(offenders, [],
                         'operational line(s) still using print()')

    def test_every_marker_is_actually_present(self):
        # guards the test above from silently passing if a message
        # gets reworded
        src = open(os.path.join(ROOT, 'main.py'),
                   encoding='utf-8').read()
        for m in ('Agent: reconnected', 'Agent: idle',
                  'DB unreachable', 'Agent mode:'):
            with self.subTest(marker=m):
                self.assertIn(m, src)


# ==============================================================
#  9. scheduled-task self-repair
# ==============================================================

class _Res:
    def __init__(self, returncode=0, stdout='', stderr=''):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@contextlib.contextmanager
def fake_run(result=None, raises=None):
    """Stand in for subprocess.run inside ensure_task_settings."""
    calls = []

    def run(cmd, **kw):
        calls.append((cmd, kw))
        if raises is not None:
            raise raises
        return result if result is not None else _Res()

    old = main.subprocess.run
    main.subprocess.run = run
    try:
        yield calls
    finally:
        main.subprocess.run = old


class TestTaskSelfRepair(unittest.TestCase):
    """This runs as SYSTEM on every fleet machine and touches the
    mechanism keeping the agent alive, so its failure modes matter
    more than its happy path."""

    def test_reports_what_changed(self):
        msg = 'task settings repaired: ExecutionTimeLimit PT72H->PT0S'
        with fake_run(_Res(0, msg + '\n')):
            self.assertEqual(main.ensure_task_settings(), msg)

    def test_silent_when_already_correct(self):
        with fake_run(_Res(0, '')):
            self.assertEqual(main.ensure_task_settings(), '')

    def test_invokes_the_repair_script(self):
        with fake_run(_Res(0, '')) as calls:
            main.ensure_task_settings()
        self.assertEqual(len(calls), 1)
        cmd = calls[0][0]
        self.assertEqual(cmd[0], 'powershell')
        self.assertIn('-Repair', cmd)
        self.assertIn('agent_task.ps1', ' '.join(cmd))
        # never -Register: recreating the task would kill the very
        # agent doing the repair
        self.assertNotIn('-Register', cmd)
        # must not block the agent forever, and no console window
        self.assertIn('timeout', calls[0][1])

    def test_nonzero_exit_is_a_warning_not_a_crash(self):
        with fake_run(_Res(1, 'ERROR Access is denied.')):
            got = main.ensure_task_settings()
        self.assertTrue(got.startswith('WARN'))
        self.assertIn('Access is denied', got)

    def test_subprocess_failure_never_raises(self):
        for boom in (OSError('powershell missing'),
                     main.subprocess.SubprocessError('boom'),
                     main.subprocess.TimeoutExpired('powershell', 120)):
            with self.subTest(err=type(boom).__name__):
                with fake_run(raises=boom):
                    got = main.ensure_task_settings()
                self.assertTrue(got.startswith('WARN'))

    def test_skipped_off_windows(self):
        old = main.os.name
        main.os.name = 'posix'
        try:
            with fake_run(_Res(0, 'should not be called')) as calls:
                self.assertEqual(main.ensure_task_settings(), '')
            self.assertEqual(calls, [])
        finally:
            main.os.name = old

    def test_missing_script_is_a_no_op(self):
        old = main.REPO_DIR
        main.REPO_DIR = os.path.join(HERE, 'no-such-dir')
        try:
            with fake_run(_Res(0, 'x')) as calls:
                self.assertEqual(main.ensure_task_settings(), '')
            self.assertEqual(calls, [])
        finally:
            main.REPO_DIR = old

    def test_output_is_bounded(self):
        with fake_run(_Res(0, 'x' * 5000)):
            self.assertLessEqual(len(main.ensure_task_settings()), 400)
        with fake_run(_Res(1, 'y' * 5000)):
            self.assertLessEqual(len(main.ensure_task_settings()), 400)

    def test_repair_script_is_shipped(self):
        # main.py invokes it by path; it must be in the repo
        self.assertTrue(os.path.exists(
            os.path.join(ROOT, 'agent_task.ps1')))

    def _start_agent(self, repair):
        """Enter run_agent far enough to pass the startup block,
        then bail out of the first poll."""
        args = types.SimpleNamespace(poll_interval=5, db_host='db')
        old = (main.ensure_task_settings, main.make_session,
               main.process_commands)
        main.ensure_task_settings = repair
        main.make_session = lambda *a, **kw: None
        main.process_commands = lambda *a, **kw: (_ for _ in ())\
            .throw(SystemExit(0))
        try:
            with quiet() as out:
                with self.assertRaises(SystemExit):
                    main.run_agent(args, FakeDb(REFS))
            return out.getvalue()
        finally:
            (main.ensure_task_settings, main.make_session,
             main.process_commands) = old

    def test_agent_startup_repairs_the_task(self):
        # the whole feature is inert if this call goes missing
        called = []
        printed = self._start_agent(
            lambda: called.append(1) and '' or 'task settings repaired: x')
        self.assertEqual(len(called), 1, 'startup did not repair')
        self.assertIn('task settings repaired', printed)

    def test_agent_starts_when_repair_reports_nothing(self):
        printed = self._start_agent(lambda: '')
        self.assertNotIn('task settings', printed)

    def test_repair_failure_does_not_stop_the_agent(self):
        # a WARN must be surfaced but must not prevent scraping -
        # reaching process_commands (SystemExit) proves it went on
        printed = self._start_agent(lambda: 'WARN denied')
        self.assertIn('WARN denied', printed)


def _main():
    global BLESS
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument('--bless', action='store_true')
    args, rest = ap.parse_known_args()
    BLESS = args.bless
    unittest.main(argv=[sys.argv[0]] + rest, verbosity=2)


if __name__ == '__main__':
    _main()
