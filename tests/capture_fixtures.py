# Capture / refresh the offline test fixtures from the live register.
#
# The offline harness (test_offline.py) must run with no network and
# no database, so it replays real physician-info pages saved here.
# Pages are stored gzipped: the raw HTML is ~220 KB each (the portal
# ships desktop + mobile + print copies of every section), which
# compresses to ~20 KB and keeps the repo small.
#
#   python tests/capture_fixtures.py            # refresh the set
#   python tests/capture_fixtures.py --scan     # hunt new candidates
#
# --scan fetches a spread of CPSO numbers, classifies what each page
# exercises and prints a table, so a human can pick replacements when
# a fixture doctor is purged from the register.

import argparse
import gzip
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))

import cpso_site                                    # noqa: E402

FIX_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       'fixtures')

# CPSO number -> fixture label. Chosen to cover the shapes the
# parser has to survive; see tests/README.md for what each one is.
DETAIL_FIXTURES = {
    10310: 'not_on_register',     # purged number -> parse gives None
    55000: 'expired_resigned',    # 'Expired: Resigned from ...'
    72500: 'active_3_hospitals',  # hospital-privilege rows
    79082: 'deceased_former',     # Deceased + a former name
    83500: 'active_2_locations',  # additional business location
    88000: 'expired_terms',       # 'Expired: Terms and Conditions'
    91500: 'active_jurisdiction',  # other-jurisdiction licence
    94138: 'deceased',            # the record verified in prod
    110500: 'active_fax',         # primary location carries a fax
    119500: 'expired_nonascii',   # non-ASCII payload (utf8mb4)
}

# numbers whose JSON search response is saved too (quick mode)
SEARCH_FIXTURES = [72500, 79082]

DELAY = 1.5             # courtesy gap between requests


def detail_path(cpso_no, label):
    return os.path.join(FIX_DIR, f'detail_{cpso_no}_{label}.html.gz')


def search_path(cpso_no):
    return os.path.join(FIX_DIR, f'search_{cpso_no}.json')


def save_html(path, html):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, 'wt', encoding='utf-8') as fh:
        fh.write(html)


def load_html(path):
    with gzip.open(path, 'rt', encoding='utf-8') as fh:
        return fh.read()


def classify(rec):
    """What does this page exercise?"""
    if rec is None:
        return {'found': False}
    prim = (rec['locations'][0]['address']
            if rec['locations'] and rec['locations'][0]['address']
            else None)
    blob = json.dumps(rec, ensure_ascii=False)
    return {
        'found': True,
        'name': rec['name'],
        'status': rec['status'],
        'locs': len(rec['locations']),
        'hosp': len(rec['hospitals']),
        'spec': len(rec['specialties']),
        'juris': len(rec['jurisdictions']),
        'former': bool(rec['former_name']),
        'dod': bool(rec['date_of_death']),
        'country': prim['country'] if prim else '',
        'ext': any(l['ext'] for l in rec['locations']),
        'fax': any(l['fax'] for l in rec['locations']),
        'nonascii': not blob.isascii(),
    }


def fetch_one(session, cpso_no):
    html = cpso_site.fetch_physician_page(session, cpso_no,
                                          max_attempts=2,
                                          retry_delay=5, timeout=45)
    return html, cpso_site.parse_physician_page(html)


def do_refresh():
    session = cpso_site.make_session()
    for cpso_no, label in DETAIL_FIXTURES.items():
        html, rec = fetch_one(session, cpso_no)
        save_html(detail_path(cpso_no, label), html)
        print(f'{cpso_no:>7} {label:<16} '
              f'{"MISSING" if rec is None else rec["name"]}')
        time.sleep(DELAY)
    for cpso_no in SEARCH_FIXTURES:
        data = cpso_site.fetch_search_result(session, cpso_no,
                                             max_attempts=2,
                                             retry_delay=5)
        with open(search_path(cpso_no), 'w', encoding='utf-8') as fh:
            json.dump(data, fh, ensure_ascii=False, indent=1)
        print(f'{cpso_no:>7} search  totalcount='
              f'{data.get("totalcount")}')
        time.sleep(DELAY)


def do_scan(numbers, out_dir):
    """Fetch candidates, stash them and print what each exercises."""
    session = cpso_site.make_session()
    os.makedirs(out_dir, exist_ok=True)
    index = {}
    for cpso_no in numbers:
        try:
            html, rec = fetch_one(session, cpso_no)
        except cpso_site.CpsoFetchError as e:
            print(f'{cpso_no:>7} FETCH-FAIL {e}')
            continue
        info = classify(rec)
        index[cpso_no] = info
        save_html(os.path.join(out_dir, f'{cpso_no}.html.gz'), html)
        if info['found']:
            print(f'{cpso_no:>7} {info["status"][:22]:<22} '
                  f'loc={info["locs"]} hosp={info["hosp"]} '
                  f'spec={info["spec"]} jur={info["juris"]} '
                  f'fmr={int(info["former"])} dod={int(info["dod"])} '
                  f'ext={int(info["ext"])} fax={int(info["fax"])} '
                  f'utf8={int(info["nonascii"])} '
                  f'{info["country"]} | {info["name"][:34]}')
        else:
            print(f'{cpso_no:>7} -- not on register --')
        time.sleep(DELAY)
    with open(os.path.join(out_dir, 'index.json'), 'w',
              encoding='utf-8') as fh:
        json.dump(index, fh, ensure_ascii=False, indent=1)
    print(f'\n{len(index)} pages cached in {out_dir}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scan', action='store_true',
                    help='probe candidate numbers instead of '
                         'refreshing the committed fixtures')
    ap.add_argument('--numbers', default='',
                    help='comma/space separated list for --scan')
    ap.add_argument('--out', default='scan_cache',
                    help='where --scan stashes pages')
    args = ap.parse_args()

    if args.scan:
        nums = [int(n) for n in
                args.numbers.replace(',', ' ').split() if n.strip()]
        do_scan(nums, args.out)
    else:
        do_refresh()


if __name__ == '__main__':
    main()
