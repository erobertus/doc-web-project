# Fetching and parsing of the new CPSO Physician Register
# (register.cpso.on.ca, Microsoft Power Pages based site that
# replaced doctors.cpso.on.ca).
#
# This module has no database dependencies: it downloads a
# physician-info page and turns it into a plain dict of raw
# (site-level) values. All reference-table code lookups and
# database writes stay in main.py.

import re
import json
import time
import requests
from bs4 import BeautifulSoup

BASE_URL = 'https://register.cpso.on.ca'
DETAIL_URL = BASE_URL + '/physician-info/?cpsonum={}'
SEARCH_URL = BASE_URL + '/Get-Search-Results/'

USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
              'AppleWebKit/537.36 (KHTML, like Gecko) '
              'Chrome/126.0 Safari/537.36')

# marker proving the portal served the physician-info page
# (and not e.g. a Cloudflare challenge or an error page); if it is
# present but the page has no physician banner, the CPSO number
# genuinely does not exist in the register
PAGE_MARKER = '/physician-info/'

NOT_AVAILABLE = 'No Information Available'
NO_ADDRESS = 'Address not Available'
NO_FRMR_NAME = 'No Former Name'

AS_OF = ' as of '

PROVINCE_MAP = {
    'ONTARIO': 'ON', 'QUEBEC': 'QC', 'QUÉBEC': 'QC',
    'BRITISH COLUMBIA': 'BC', 'ALBERTA': 'AB',
    'SASKATCHEWAN': 'SK', 'MANITOBA': 'MB',
    'NEW BRUNSWICK': 'NB', 'NOVA SCOTIA': 'NS',
    'PRINCE EDWARD ISLAND': 'PE',
    'NEWFOUNDLAND AND LABRADOR': 'NL', 'NEWFOUNDLAND': 'NL',
    'YUKON': 'YT', 'NORTHWEST TERRITORIES': 'NT', 'NUNAVUT': 'NU',
}
# also accept two-letter codes as-is ("Kingston ON  K7L 2V7")
PROVINCE_MAP.update({v: v for v in PROVINCE_MAP.values()})

US_STATES = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ',
    'ARKANSAS': 'AR', 'CALIFORNIA': 'CA', 'COLORADO': 'CO',
    'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL',
    'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
    'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
    'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA',
    'MAINE': 'ME', 'MARYLAND': 'MD', 'MASSACHUSETTS': 'MA',
    'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
    'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE',
    'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC',
    'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
    'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI',
    'SOUTH CAROLINA': 'SC', 'SOUTH DAKOTA': 'SD',
    'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
    'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA',
    'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY',
    'DISTRICT OF COLUMBIA': 'DC', 'PUERTO RICO': 'PR',
}
US_STATES.update({v: v for v in US_STATES.values()})

RE_CA_POSTAL = re.compile(
    r'\s*([A-Za-z]\d[A-Za-z])\s*(\d[A-Za-z]\d)\s*$')
RE_US_ZIP = re.compile(r'\s+(\d{5}(?:-\d{4})?)\s*$')
RE_PHONE_EXT = re.compile(
    r'(?:\b(?:ext|extension)\.?|\bx\.?)\s*:?\s*(\d+)\s*$', re.I)
RE_WS = re.compile(r'[\s\xa0]+')


class CpsoFetchError(Exception):
    """Raised when the register cannot be reached / keeps failing.
    Deliberately different from 'physician not found' so the caller
    does not permanently exclude the CPSO number."""
    pass


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})
    return session


def _norm(s: str) -> str:
    return RE_WS.sub(' ', s).strip()


def fetch_physician_page(session: requests.Session, cpso_no: int,
                         max_attempts=5, retry_delay=15,
                         timeout=90) -> str:
    """Download the physician-info page, retrying transient errors."""
    url = DETAIL_URL.format(cpso_no)
    last_err = ''
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200 \
                    and PAGE_MARKER in resp.text:
                return resp.text
            last_err = f'HTTP {resp.status_code}'
        except requests.RequestException as e:
            last_err = repr(e)
        if attempt < max_attempts:
            wait = retry_delay * attempt
            print(f'CPSO {cpso_no}: fetch failed ({last_err}), '
                  f'attempt {attempt}/{max_attempts}, '
                  f'retrying in {wait} sec...')
            time.sleep(wait)
    raise CpsoFetchError(
        f'CPSO {cpso_no}: giving up after {max_attempts} '
        f'attempts ({last_err})')


def _label_value(container, row_class: str) -> str:
    """Value of a 'Label: value' row (value may be a plain text node
    or a nested -value span). Returns '' when the row is missing."""
    row = container.find(class_=row_class)
    if row is None:
        return ''
    strings = [_norm(s) for s in row.stripped_strings]
    strings = [s for s in strings if s]
    # first string is the label itself
    value = ' '.join(strings[1:])
    if value in (NOT_AVAILABLE, NO_FRMR_NAME):
        return ''
    return value


def _split_as_of(s: str) -> tuple:
    """'Independent Practice as of 02 Jul 2010'
       -> ('Independent Practice', '02 Jul 2010')
       'as of 02 Jul 2010' -> ('', '02 Jul 2010')"""
    s = _norm(s)
    if s.lower().startswith('as of '):
        return '', s[len('as of '):].strip()
    if AS_OF in s:
        name, _, date = s.rpartition(AS_OF)
        return name.strip(' :'), date.strip()
    return s, ''


def parse_locality_line(line: str) -> dict:
    """Try to interpret an address line as 'City Province Postal'.
    Returns {} when the line does not look like a locality line."""
    result = {}
    line = _norm(line)

    def strip_suffix(text: str, suffix_map: dict) -> tuple:
        # match the longest known province/state name at the end
        upper = text.upper()
        for name in sorted(suffix_map, key=len, reverse=True):
            if upper.endswith(' ' + name) or upper == name:
                return (suffix_map[name],
                        text[:len(text) - len(name)].strip(' ,'))
        return '', text

    m = RE_CA_POSTAL.search(line)
    if m:
        result['postal'] = f'{m.group(1)} {m.group(2)}'.upper()
        rest = line[:m.start()].strip(' ,')
        result['country'] = 'Canada'
        prov, rest = strip_suffix(rest, PROVINCE_MAP)
        if prov:
            result['prov'] = prov
    else:
        m = RE_US_ZIP.search(line)
        if not m:
            return {}
        rest = line[:m.start()].strip(' ,')
        prov, rest = strip_suffix(rest, US_STATES)
        if not prov:
            # a five-digit code without a US state is some other
            # country's postal code - not a locality line we know
            return {}
        result['postal'] = m.group(1)
        result['country'] = 'United States'
        result['prov'] = prov

    result['city'] = rest
    return result


def parse_address_text(lines: list) -> dict:
    """Convert the <br>-separated lines of an address block into
    street lines + city/province/postal/country."""
    addr = {'lines': [], 'city': '', 'prov': '', 'postal': '',
            'country': '', 'raw': '\n'.join(lines)}

    locality_idx = -1
    # the locality line is the LAST line that carries a postal code
    for i, line in enumerate(lines):
        if parse_locality_line(line):
            locality_idx = i

    for i, line in enumerate(lines):
        line = _norm(line)
        if not line:
            continue
        if i == locality_idx:
            addr.update(parse_locality_line(line))
        elif locality_idx != -1 and i > locality_idx:
            # anything after 'City Prov Postal' is a country line
            addr['country'] = line
        else:
            addr['lines'].append(line)

    return addr


def parse_phone(s: str) -> tuple:
    """-> (phone, extension)"""
    s = _norm(s)
    ext = ''
    m = RE_PHONE_EXT.search(s)
    if m:
        ext = m.group(1)
        s = s[:m.start()].strip(' ,')
    return s, ext


def _parse_location_card(card) -> dict:
    """One address card: primary business location or an
    'additional business location' collapse row."""
    result = {'address': None, 'phone': '', 'ext': '', 'fax': ''}

    addr_el = card.find(class_='scrp-practiceaddress-value')
    if addr_el is not None:
        lines = [_norm(s) for s in addr_el.stripped_strings]
        lines = [s for s in lines if s]
        if lines and lines != [NO_ADDRESS]:
            result['address'] = parse_address_text(lines)

    phone = _label_value(card, 'scrp-phone')
    result['phone'], result['ext'] = parse_phone(phone)
    result['fax'] = _norm(_label_value(card, 'scrp-fax'))
    return result


def parse_physician_page(html: str):
    """Parse a physician-info page.

    Returns None when the register has no such CPSO number,
    otherwise a dict of raw site values:
        name, cpso_no, status, status_date, expiry_date,
        reg_class, reg_class_date, former_name, gender,
        languages [..], medical_school, grad_year, date_of_death,
        jurisdictions [..],
        locations [{address{lines,city,prov,postal,country,raw},
                    phone, ext, fax}, ..]   (primary first),
        specialties [{name, issued_date, certifying_body}, ..],
        hospitals [{name, location}, ..]
    """
    soup = BeautifulSoup(html, 'html.parser')

    banner = soup.find(class_='scrp-banner')
    if banner is None:
        return None                     # no such physician

    rec = {}

    el = banner.find(class_='scrp-contactname-value')
    rec['name'] = _norm(el.get_text()) if el else ''

    el = banner.find(class_='scrp-cpsonumber-value')
    rec['cpso_no'] = _norm(el.get_text()) if el else ''

    el = banner.find(class_='scrp-registrationstatus-value')
    pill_status = _norm(el.get_text()) if el else ''

    el = banner.find(class_='scrp-statusdate-value')
    status_detail = _norm(el.get_text()) if el else ''
    # active:   pill 'Active',   detail 'as of 02 Jul 2010'
    # inactive: pill 'Inactive', detail
    #           'Expired: Resigned from membership as of 18 May 2021'
    status_name, status_date = _split_as_of(status_detail)
    if not status_name:
        status_name = pill_status
    rec['status'] = status_name
    rec['status_date'] = status_date

    el = banner.find(class_='scrp-expirydate-value')
    rec['expiry_date'] = _norm(el.get_text()) if el else ''

    el = banner.find(class_='scrp-registrationclass-value')
    rc = _norm(el.get_text()) if el else ''
    rec['reg_class'], rec['reg_class_date'] = _split_as_of(rc)

    # ---- the rest of the page is duplicated (desktop + mobile
    # + print copies); always work inside the FIRST copy ----
    body = soup.find(class_='scrp-generalinfo') or soup

    general = body.find(id='general-information') or body
    rec['former_name'] = _label_value(general, 'scrp-formername')
    rec['gender'] = _label_value(general, 'scrp-gender')

    langs = _label_value(general, 'scrp-laguage')
    rec['languages'] = [s.strip() for s in langs.split(',')
                        if s.strip()]

    school = _label_value(general, 'scrp-education')
    rec['medical_school'], rec['grad_year'] = school, ''
    if ',' in school:
        left, _, right = school.rpartition(',')
        if right.strip().isdigit():
            rec['medical_school'] = left.strip()
            rec['grad_year'] = right.strip()

    # 'Date of Death:' has no dedicated scrp- class so far; look
    # for the label anywhere in the general-information section
    rec['date_of_death'] = ''
    dod_label = general.find(
        string=re.compile(r'Date of Death\s*:', re.I))
    if dod_label is not None:
        row = dod_label.find_parent(['div', 'span'])
        if row is not None:
            strings = [_norm(s) for s in row.stripped_strings]
            value = ' '.join(s for s in strings[1:] if s)
            rec['date_of_death'] = value

    # ---- practice locations (primary + additional) ----
    rec['locations'] = []
    practice = body.find(id='practice-information')
    if practice is not None:
        primary_zone = practice.find('div', class_='list-content')
        if primary_zone is not None:
            rec['locations'].append(
                _parse_location_card(primary_zone))
        for card in practice.find_all(
                class_='scrp-additionalinfo-row'):
            rec['locations'].append(_parse_location_card(card))

    # ---- specialties ----
    rec['specialties'] = []
    table = body.find('table', class_='scrp-specialties')
    if table is not None:
        for row in table.find_all(class_='scrp-specialty-row'):
            name = row.find(class_='scrp-specialtyname-value')
            issued = row.find(class_='scrp-issuedon-value')
            cert = row.find(class_='scrp-certifyingbody-value')
            issued_date = _norm(issued.get_text()) if issued else ''
            # 'Effective: 10 Feb 2020' -> '10 Feb 2020'
            issued_date = issued_date.split(':', 1)[-1].strip()
            rec['specialties'].append({
                'name': _norm(name.get_text()) if name else '',
                'issued_date': issued_date,
                'certifying_body':
                    _norm(cert.get_text()) if cert else '',
            })

    # ---- medical licences in other jurisdictions ----
    rec['jurisdictions'] = []
    for el in body.find_all(class_='scrp-jurisdiction-value'):
        val = _norm(el.get_text())
        if val and val not in rec['jurisdictions']:
            rec['jurisdictions'].append(val)

    # ---- hospital privileges ----
    rec['hospitals'] = []
    table = body.find('table', class_='scrp-hospitalprivileges')
    if table is not None:
        for row in table.find_all(
                class_='scrp-hospitalprivilege-row'):
            name = row.find(class_='scrp-hospitalname-value')
            loc = row.find(class_='scrp-location-value')
            rec['hospitals'].append({
                'name': _norm(name.get_text()) if name else '',
                'location': _norm(loc.get_text()) if loc else '',
            })

    return rec


def fetch_search_result(session: requests.Session, cpso_no: int,
                        max_attempts=5, retry_delay=15,
                        timeout=90) -> dict:
    """Query the JSON search API (the same endpoint the site's own
    results page calls), retrying transient errors. Returns the
    parsed response: {'totalcount': N, 'results': [{...}]} with
    name, registrationstatus, mostrecentformername, specialties
    and the PRIMARY address/phone/fax only."""
    last_err = ''
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.post(SEARCH_URL, data={
                'cpsoNumber': str(cpso_no),
                'cbx-includeinactive': 'on',
                'cbx-includeinactive-20years': 'on',
            }, timeout=timeout)
            if resp.status_code == 200:
                try:
                    return json.loads(resp.text)
                except ValueError as e:
                    last_err = f'bad JSON: {e}'
            else:
                last_err = f'HTTP {resp.status_code}'
        except requests.RequestException as e:
            last_err = repr(e)
        if attempt < max_attempts:
            wait = retry_delay * attempt
            print(f'CPSO {cpso_no}: search failed ({last_err}), '
                  f'attempt {attempt}/{max_attempts}, '
                  f'retrying in {wait} sec...')
            time.sleep(wait)
    raise CpsoFetchError(
        f'CPSO {cpso_no}: search giving up after {max_attempts} '
        f'attempts ({last_err})')


# backward-compatible name for diagnostics
search_by_number = fetch_search_result


def search_result_location(result: dict) -> dict:
    """Convert one JSON search result into the same location
    structure parse_physician_page produces, so the address can
    go through the same DB path."""
    lines = []
    for i in (1, 2, 3, 4):
        s = _norm(result.get(f'street{i}') or '')
        if s:
            lines.append(s)

    city = _norm(result.get('city') or '')
    prov_name = _norm(result.get('province') or '')
    postal = _norm(result.get('postalcode') or '').upper()

    prov = PROVINCE_MAP.get(prov_name.upper(), '')
    if prov:
        country = 'Canada'
    else:
        prov = US_STATES.get(prov_name.upper(), '')
        country = 'United States' if prov else ''
    if not prov:
        prov = prov_name        # keep verbatim rather than lose it

    address = None
    if lines or city or postal:
        locality = ' '.join(x for x in (city, prov_name, postal)
                            if x)
        address = {'lines': lines, 'city': city, 'prov': prov,
                   'postal': postal, 'country': country,
                   'raw': '\n'.join(lines + ([locality]
                                             if locality else []))}

    phone, ext = parse_phone(result.get('phonenumber') or '')
    return {'address': address, 'phone': phone, 'ext': ext,
            'fax': _norm(result.get('fax') or '')}
