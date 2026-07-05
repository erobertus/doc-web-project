# CPSO physician register scraper
# (rewritten for the new register.cpso.on.ca site, 2026)


import os
import sys
import mariadb
import time
import socket
import traceback
import subprocess
import argparse
from time import sleep
from datetime import timedelta
from constants import *
from GeoCoding import get_geocode_db_uno
from cpso_site import (make_session, fetch_physician_page,
                       parse_physician_page, fetch_search_result,
                       search_result_location, CpsoFetchError)
from knock import knock, knock_config_from_env


def db_connect(_knock_retries=6, _knock_gap=2.0, **conn_params):
    """mariadb.connect with a port-knock fallback for clinics
    behind a dynamic-IP firewall.

    If the first attempt fails and a knock sequence is configured
    (CPSO_KNOCK), knock the firewall to authorize this machine's
    current IP, then retry the connection several times spread
    across the firewall's open window (knockd opens the port for
    tens of seconds, and needs a moment to install the rule after
    seeing the sequence, so a single immediate retry can miss).
    Raises the last error if still unreachable - callers (the
    agent reconnect loop, the run_agent.bat restart loop) knock
    again on their own schedule. No sequence configured = a plain
    mariadb.connect."""
    try:
        return mariadb.connect(**conn_params)
    except mariadb.Error:
        ports, proto, delay = knock_config_from_env()
        if not ports:
            raise
        host = conn_params.get('host')
        print(f'DB unreachable - port-knocking {host} '
              f'({proto} {ports}) to authorize this IP...')
        knock(host, ports, proto, delay)
        last = None
        for attempt in range(_knock_retries):
            try:
                conn = mariadb.connect(**conn_params)
                if attempt > 0:
                    print(f'DB reachable after knock '
                          f'(attempt {attempt + 1}).')
                return conn
            except mariadb.Error as e:
                last = e
                time.sleep(_knock_gap)
        raise last


class DbLogger:
    """Best-effort central logging into MD_scrape_log so all fleet
    machines can be monitored from one place. Uses its OWN
    autocommit connection - a log write can never interfere with
    the scraper's transactions. Retries the connection once per
    write, then disables itself (e.g. when the table does not
    exist yet); scraping is never affected by logging problems."""

    def __init__(self):
        self.enabled = False
        self.verbose = False
        self.conn = None
        self.params = None
        self.version = 'unknown'
        self._version_col = True     # flips off if the column is
                                     # absent on this database
        # CPSO_AGENT_NAME (e.g. 'Clinic-Newmarket') beats the bare
        # Windows machine name, which says nothing about location
        self.host = os.environ.get('CPSO_AGENT_NAME') \
            or socket.gethostname()
        self._origin_added = bool(
            os.environ.get('CPSO_AGENT_NAME'))

    def debug(self, message, cpso_no=None, batch_uno=None):
        """Per-doctor detail rows; written only in verbose mode
        (log_verbose control column / --verbose-log)."""
        if self.verbose:
            self.log('DEBUG', message, cpso_no=cpso_no,
                     batch_uno=batch_uno)

    def init(self, **conn_params):
        self.params = conn_params
        self.enabled = True
        self.version = get_agent_version()
        self._connect(silent=False)

    def _connect(self, silent=True) -> bool:
        try:
            self.conn = db_connect(autocommit=True,
                                   **self.params)
            if not self._origin_added:
                # append the connection origin as the server sees
                # it (clinic public IP / reverse DNS) - the bare
                # machine name alone does not identify the office
                try:
                    curs = self.conn.cursor()
                    curs.execute(
                        "SELECT SUBSTRING_INDEX(USER(), '@', -1)")
                    (origin,) = curs.fetchone()
                    if origin and origin not in self.host:
                        self.host = (f'{self.host} @ '
                                     f'{origin}')[:128]
                    self._origin_added = True
                except mariadb.Error:
                    pass
            return True
        except mariadb.Error as e:
            if not silent:
                print(f'Central log unavailable ({e}); '
                      f'logging to console only.')
            self.conn = None
            return False

    def log(self, level, message, cpso_no=None, batch_uno=None):
        if not self.enabled:
            return
        attempt = 0
        while attempt < 2:
            attempt += 1
            if self.conn is None and not self._connect():
                return
            try:
                curs = self.conn.cursor()
                if self._version_col:
                    curs.execute(
                        f'INSERT INTO {LOG_TBL} (host, version, '
                        f'level, batch_uno, cpso_no, message) '
                        f'VALUES (?, ?, ?, ?, ?, ?)',
                        (self.host, self.version, level,
                         batch_uno, cpso_no,
                         str(message)[:60000]))
                else:
                    curs.execute(
                        f'INSERT INTO {LOG_TBL} (host, level, '
                        f'batch_uno, cpso_no, message) '
                        f'VALUES (?, ?, ?, ?, ?)',
                        (self.host, level, batch_uno, cpso_no,
                         str(message)[:60000]))
                return
            except mariadb.Error as e:
                # 1054 = no 'version' column on this DB: drop it
                # and retry without counting as a failure
                if getattr(e, 'errno', None) == 1054 \
                        and self._version_col:
                    self._version_col = False
                    attempt -= 1
                    continue
                self.conn = None      # reconnect once, then stop
                if attempt >= 2:
                    self.enabled = False
                    print(f'Central log disabled ({e}); '
                          f'logging to console only.')


DB_LOG = DbLogger()

# effective stale-batch threshold; __main__ overrides it from
# --stale-minutes so the auto-reap inside request_workload sees
# the operator's choice
STALE_MINUTES_ACTIVE = STALE_BATCH_MINUTES


def reformat_date(cpso_date: str) -> str:
    MONTHS = dict(Jan='01', Feb='02', Mar='03', Apr='04', May='05',
                  Jun='06', Jul='07', Aug='08', Sep='09', Oct='10',
                  Nov='11', Dec='12')

    t = cpso_date.replace('-', ' ').split()
    if len(t) == 3:
        t[1] = MONTHS[t[1]]
        x = t[0]
        t[0] = t[2]
        t[2] = x
    else:
        t = ('1900', '01', '01')
    return '-'.join(t)


def refresh_ref_from_db(in_db: 'connection', table: str,
                        code_col: str, name_col: str) -> dict:
    curs = in_db.cursor()
    curs.execute(f'SELECT {name_col}, {code_col} '
                 f'FROM {table}')
    statuses = {}
    for (reg_stat_name, reg_stat_code) in curs:
        statuses[reg_stat_name] = reg_stat_code
    return statuses


def update_reference(status: str, statuses: dict,
                     in_db: 'connection',
                     table: str, code_col: str, name_col: str) -> int:
    save_commit_state = in_db.autocommit
    in_db.autocommit = False

    curs = in_db.cursor()
    curs.execute(f'SELECT COUNT(*) AS cnt FROM {table} '
                 f'WHERE {name_col} = ?', (status,))
    for (cnt,) in curs:
        pass
    if cnt == 0:
        curs.execute(f'INSERT INTO {table} '
                     f'({name_col})'
                     'VALUES (?)', (status,))
        res = curs.lastrowid

        statuses[status] = res

    else:
        # cool
        curs.execute(f'SELECT {code_col} as code_col '
                     f'FROM {table} WHERE {name_col} = ?', (status,))
        for (res,) in curs:
            pass
    in_db.commit()
    in_db.autocommit = save_commit_state

    return res


def retrieve_code_from_name(member: str, array: dict,
                            cur_conn: 'connection',
                            table: str, code_col: str,
                            name_col: str, aliases=None) -> int:
    if member in array:
        return array[member]

    # case-insensitive match (the new site e.g. capitalizes
    # language names differently from the old one)
    for (name, code) in array.items():
        if name.upper() == member.upper():
            return code

    # the new site renamed some values (e.g. 'Active' instead of
    # 'Active Member'); reuse the existing code when the old name
    # is already in the reference table
    if aliases is not None:
        alias = aliases.get(member)
        if alias is not None and alias in array:
            return array[alias]

    return update_reference(
        member, array, cur_conn, table, code_col, name_col)


def process_address(conn_db: 'connection', locations: list) -> list:
    """Convert parsed practice locations (see cpso_site.
    parse_physician_page) into MD_addresses records and link them
    to geocodes, mirroring the address layout the old site used."""
    return_list = []

    for (i, loc) in enumerate(locations):
        addr_dict = {C_ADDR_ORDER: i + 1,
                     C_ADDR_IS_DEF: int(i == 0)}
        address = loc['address']

        if address is None:
            addr_dict[C_ADDR_PREFIX + '1'] = NO_ADDR
        else:
            lines = address['lines']
            # address_1 is NOT NULL in MD_addresses; an address
            # may consist of a locality line only
            addr_dict[C_ADDR_PREFIX + '1'] = BLANK
            for j in range(min(len(lines), 4)):
                if j == 3 and len(lines) > 4:
                    # never lose data: fold the overflow into
                    # the last address line (varchar(200))
                    addr_dict[C_ADDR_PREFIX + '4'] = \
                        ', '.join(lines[3:])[:200]
                else:
                    addr_dict[C_ADDR_PREFIX + str(j + 1)] = lines[j]

            if address['city']:
                addr_dict[C_ADDR_CITY] = address['city']
            if address['prov']:
                addr_dict[C_ADDR_PROV] = address['prov']
            if address['postal']:
                addr_dict[C_ADDR_POSTAL] = address['postal']
            if address['country']:
                addr_dict[C_ADDR_COUNTRY] = address['country']
            addr_dict[C_ADDR_RAW] = address['raw']

        if loc['phone']:
            addr_dict[C_ADDR_PHONE_NO] = loc['phone']
        if loc['ext']:
            addr_dict[C_ADDR_EXT] = loc['ext']
        if loc['fax']:
            addr_dict[C_ADDR_FAX_NO] = loc['fax']

        if address is not None:
            # same full-address layout the old code sent to the
            # geocoder: street lines, then 'City PROV  POSTAL',
            # then country
            t_str = addr_dict.get(C_ADDR_CITY, '')
            if C_ADDR_PROV in addr_dict:
                t_str = ' '.join((t_str, addr_dict[C_ADDR_PROV]))
            if C_ADDR_POSTAL in addr_dict:
                t_str = '  '.join((t_str, addr_dict[C_ADDR_POSTAL]))
            if C_ADDR_COUNTRY in addr_dict:
                t_str = '\n'.join((t_str, addr_dict[C_ADDR_COUNTRY]))

            full_addr = '\n'.join(
                address['lines'] + ([t_str] if t_str else []))

            _, geo_uno = get_geocode_db_uno(
                conn_db, full_addr,
                addr_dict.get(C_ADDR_PROV),
                addr_dict.get(C_ADDR_POSTAL),
                addr_dict.get(C_ADDR_COUNTRY))

            if len(geo_uno) > 0:
                addr_dict[C_GEO_UNO] = geo_uno[0]

        return_list.append(addr_dict)

    return return_list


def update_x_table(in_db: 'connection', table: str,
                   key_col: str, key: str,
                   val_col: str, values: list,
                   max_attempts=5, retry_delay=5):
    save_commit_state = in_db.autocommit
    in_db.autocommit = False
    curs = in_db.cursor()

    # the new register may list the same value twice (e.g. one
    # hospital-privilege row per appointment); a duplicate code
    # would violate the unique (key, value) index
    values = list(dict.fromkeys(values))

    str_list = ', '.join([str(s) for s in values])
    attempt = 1
    tryagain = True

    while attempt <= max_attempts and tryagain:
        try:
            curs.execute(BEGIN_TRAN)

            # DELETE VALUES NOT IN values
            stmt = f'DELETE FROM {table} WHERE {key_col} = {key} ' \
                   f'AND {val_col} NOT IN ({str_list})'

            curs.execute(stmt)

            # GET LIST OF EXISTING VALUES
            stmt = f'SELECT {val_col} FROM {table} WHERE {key_col} = ? ' \
                   f'AND {val_col} IN ({str_list})'

            curs.execute(stmt, (key,))
            db_values = [s for (s,) in curs]

            for value in values:
                if value not in db_values:
                    stmt = f'INSERT INTO {table} ({key_col}, {val_col}) ' \
                           f'VALUES (?, ?)'
                    curs.execute(stmt, (key, value))

            curs.execute(COMMIT_TRAN)
            tryagain = False
        except mariadb.OperationalError as e:
            print(f'Error: {e}.')
            print(f'Most recent statement: \n{stmt}')
            print(f'Attempt: {attempt}. '
                  f'Waiting {retry_delay} seconds to retry.')
            DB_LOG.log('WARN',
                       f'{table} update attempt {attempt} '
                       f'failed: {e}', cpso_no=key)
            time.sleep(retry_delay)
            tryagain = True
        except:
            raise
    in_db.autocommit = save_commit_state
    pass


def update_record(in_db: 'connection', records: list,
                  table: str, key_val) -> int:
    result = None
    iteration = 0
    save_commit_state = in_db.autocommit
    in_db.autocommit = False

    curs = in_db.cursor()

    key_col = DB_SCHEMA[table][T_KEY]

    stmt = f'DELETE FROM {table} WHERE {key_col} = ?'
    curs.execute(stmt, (key_val,))

    for record in records:
        iteration += 1
        cur_rec = {key_col: key_val}
        for (c_name, c_val) in record.items():
            if type(c_val) == list:
                if len(c_val) > 0:
                    if type(c_val[0]) == dict:
                        fkey_val = update_record(in_db, c_val,
                                                 c_name, key_val)
                        key_list = DB_SCHEMA[c_name].keys()

                        if T_FKEY in key_list:
                            cur_rec[DB_SCHEMA[c_name][T_FKEY]] = \
                                fkey_val
                    else:
                        update_x_table(in_db, c_name,
                                       DB_SCHEMA[c_name][T_KEY],
                                       key_val,
                                       DB_SCHEMA[c_name][T_VAL],
                                       c_val)
            else:
                cur_rec[c_name] = c_val

        if len(cur_rec) > 1:
            col_str = ', '.join(cur_rec.keys())
            val_str = ', '.join('?' * len(cur_rec))
            stmt = f'INSERT INTO {table} ({col_str}) ' \
                   f'VALUES({val_str})'
            try:
                curs.execute(stmt, tuple(cur_rec.values()))
                if T_OWNKEY in DB_SCHEMA[table].keys():
                    record[DB_SCHEMA[table][T_OWNKEY]] = \
                        curs.lastrowid
                if iteration == 1:
                    result = curs.lastrowid
            except mariadb.Error as e:
                print(f"!!! DB error: {e}")
                DB_LOG.log('ERROR',
                           f'{table} insert failed: {e}',
                           cpso_no=key_val)

    in_db.commit()
    in_db.autocommit = save_commit_state
    return result


def walk_data(d, fields: tuple) -> list:
    li = []
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, dict) or isinstance(v, list):
                li += walk_data(v, fields)
            else:
                if k in fields:
                    li.append((k, str(v)))
    elif isinstance(d, list):
        for v in d:
            if isinstance(v, dict) or isinstance(v, list):
                li += walk_data(v, fields)
    return li


def print_rec(rec: dict, fields: tuple, map=PRES_MAP) -> str:
    l = walk_data(rec, fields)
    s = ''
    for (i, (k, v)) in enumerate(l):
        if k in map:
            if LABEL in map[k]:
                s += map[k][LABEL]
            if PREFIX in map[k]:
                s = s.rstrip() + map[k][PREFIX]
            s += v
            if SUFFIX in map[k]:
                s += map[k][SUFFIX]
        else:
            s += v
        s += ' '
    return s


def request_workload(conn: 'connection',
                     batch_size=1000,
                     random=True,
                     interval=DEFAULT_INTERVAL_DAYS,
                     min_val=10000,
                     max_val=150000,
                     desc=False,
                     skip_gaps=False,
                     sweep_start=None) -> tuple:
    save_commit_state = conn.autocommit
    conn.autocommit = False
    curs = conn.cursor()

    stmt = f'SELECT COUNT(*) FROM {BATCH_HEAD_TBL} ' \
           f'WHERE host = "{ABORT_ALL}" and batch_size < 0'
    curs.execute(stmt)
    for (is_abort,) in curs:
        pass
    if is_abort:
        print('Halt requested. Aborting...')
        return (0, tuple())

    # self-healing: close batches abandoned by dead clients and
    # put their numbers back into play before allocating our own
    conn.autocommit = save_commit_state
    reap_stale_batches(conn, stale_minutes=STALE_MINUTES_ACTIVE)
    conn.autocommit = False

    # Serialize batch allocation across ALL fleet workers. The
    # pool SELECT (which numbers are free) and the claim INSERT
    # must be atomic as a pair: a plain transaction is not enough
    # because two workers can each run the SELECT before either
    # has INSERTed its claim, so both grab the same numbers. A
    # named lock makes the whole allocation mutually exclusive;
    # it is sub-second, so serializing it is cheap even with 20
    # workers, and it auto-releases if a worker's connection dies.
    curs.execute("SELECT GET_LOCK('cpso_batch_alloc', 60)")
    got = curs.fetchone()
    if not got or got[0] != 1:
        print('Could not acquire the allocation lock; '
              'retrying on the next request.')
        conn.autocommit = save_commit_state
        return (0, tuple())

    try:
        return _allocate_batch(conn, curs, batch_size, random,
                               interval, min_val, max_val, desc,
                               skip_gaps, sweep_start)
    finally:
        curs.execute("SELECT RELEASE_LOCK('cpso_batch_alloc')")
        conn.autocommit = save_commit_state


def _allocate_batch(conn, curs, batch_size, random, interval,
                    min_val, max_val, desc, skip_gaps,
                    sweep_start=None) -> tuple:
    # runs while the caller holds the allocation lock, with
    # conn.autocommit already False (restored by the caller)

    # curs.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
    curs.execute(BEGIN_TRAN)
    stmt = f'INSERT INTO {BATCH_HEAD_TBL} (batch_size, host) ' \
           f'VALUES ({batch_size}, LEFT(user(), 128))'
    curs.execute(stmt)
    batch_id = curs.lastrowid

    # make list
    if not random:

        # GET LIST OF CPSO NUMBERS
        # create simple range first
        src_list = [x for x in range(min_val, max_val)]
    else:
        # random mode refreshes doctors we already know about,
        # in shuffled order (the ORDER BY happens on the final
        # select below - the temp table would lose any insertion
        # order to its primary key anyway)
        stmt = 'SELECT d.CPSO_no ' \
               f'FROM {MD_DIR_TABLE} d ' \
               f'WHERE d.CPSO_no between {min_val} and {max_val}'

        curs.execute(stmt)

        src_list = [cpso_no for (cpso_no,) in curs]

    stmt = f'CREATE TEMPORARY TABLE _TMP_{batch_id} (' \
           f'cpso_no INT(11),' \
           f'PRIMARY KEY (cpso_no))'

    curs.execute(stmt)

    if len(src_list) > 0:
        stmt = f'INSERT INTO _TMP_{batch_id} VALUES (' \
               + '), ('.join(map(str,src_list)) \
               + ')'

        curs.execute(stmt)

    if random:
        order_clause = 'ORDER BY RAND()'
    elif desc:
        order_clause = 'ORDER BY cpso_no DESC'
    else:
        order_clause = 'ORDER BY cpso_no'

    # A number is skipped when it was COMPLETED "recently":
    #  - interval >= 1: within the freshness window (the normal
    #    refresh cadence - don't redo work from the last N days);
    #  - interval < 1: only if completed AT/AFTER this sweep began
    #    (sweep_start) - i.e. ignore prior-run recency and
    #    re-scrape everything, but still do each number once this
    #    pass so the pool advances.
    # Known gaps are re-checked by default (newly-assigned mid-
    # range numbers get picked up); skip_gaps also excludes
    # PermExcluded gaps for a fast re-sweep.
    if interval >= 1:
        done_recent = 'updated_date_time > ' \
                      f'(NOW() - INTERVAL {interval} DAY)'
    elif sweep_start is not None:
        done_recent = f"updated_date_time >= '{sweep_start}'"
    else:
        done_recent = '0'          # no marker: exclude none
    done_cond = f'(isCompleted AND ({done_recent}))'
    if skip_gaps:
        done_cond = f'({done_cond} OR PermExcluded)'

    # Also skip numbers that are CLAIMED but not yet completed by
    # a still-open batch (another live worker holds them). This is
    # interval-independent - it is what actually prevents two
    # workers from scraping the same number, so it holds even with
    # a zero/short freshness window. Reaped (abandoned) batches
    # have end_date set, so their numbers are free again.
    stmt = f'SELECT cpso_no FROM _TMP_{batch_id} ' \
           f'WHERE cpso_no NOT IN (' \
           f'  SELECT d.{C_CPSO_NO} FROM {BATCH_DET_TBL} d ' \
           f'  JOIN {BATCH_HEAD_TBL} h ' \
           f'  ON d.batch_uno = h.batch_uno ' \
           f'  WHERE d.{C_CPSO_NO} between {min_val} and {max_val} '\
           f'  AND NOT d.isCompleted AND h.end_date IS NULL) ' \
           f'AND cpso_no NOT IN (' \
           f'  SELECT DISTINCT {C_CPSO_NO} FROM {BATCH_DET_TBL} ' \
           f'  WHERE {C_CPSO_NO} between {min_val} and {max_val} ' \
           f'  AND {done_cond}) ' \
           f'{order_clause} ' \
           f'LIMIT {batch_size}'

    curs.execute(stmt)

#    for (cpso_no,) in curs:
#        if cpso_no in src_list:
#            src_list.remove(cpso_no)

    src_list = [cpso_no for (cpso_no,) in curs]

    stmt = f'INSERT INTO {BATCH_DET_TBL} (batch_uno, cpso_no, ' \
           f'updated_date_time) VALUES '
    for (i, j) in enumerate(src_list):
        if i > 0:
            stmt += DELIM_COMMA
        stmt += f'({batch_id}, {j}, NOW())'
    if len(src_list) > 0:
        curs.execute(stmt)
    curs.execute(COMMIT_TRAN)

    # Drop temp table (autocommit is restored by the caller)
    stmt = f'DROP TEMPORARY TABLE _TMP_{batch_id}'
    curs.execute(stmt)

    return (batch_id, tuple(src_list))


def finish_workload(conn: 'connection', batch_id):
    save_commit_state = conn.autocommit
    conn.autocommit = False
    curs = conn.cursor()
    s = f'UPDATE {BATCH_HEAD_TBL} SET Completed = 1, ' \
        f'InProgress = 0, end_date = now(),' \
        f'status_date = now() ' \
        f'WHERE batch_uno = ?'

    curs.execute(s, (batch_id,))
    conn.commit()
    conn.autocommit = save_commit_state


def update_detail_table(conn: 'connection',
                   cpso_no: int, perm_exclude=False,
                   batch_id=0):
    save_commit_state = conn.autocommit
    conn.autocommit = False
    curs = conn.cursor()
    stmt = f'UPDATE {BATCH_DET_TBL} ' \
           'SET isCompleted=1, ' \
           'updated_date_time = now()' \

    if perm_exclude:
        stmt += ', PermExcluded = 1'

    stmt += f' WHERE {C_CPSO_NO} = ? AND NOT isCompleted'

    if batch_id != 0:
        stmt += f' AND batch_uno = {batch_id}'
    curs.execute(stmt, (cpso_no,))

    if not perm_exclude:
        # a found doctor (or a not-found number above the known
        # max) is NOT a gap - clear any stale gap flag left on
        # older batch rows, so a --skip-gaps sweep includes it
        # again once a former gap has been assigned to a doctor.
        # Retry once on a transient deadlock (this touches every
        # row for the number, so it can briefly contend).
        for attempt in (1, 2):
            try:
                curs.execute(
                    f'UPDATE {BATCH_DET_TBL} SET PermExcluded = 0 '
                    f'WHERE {C_CPSO_NO} = ? AND PermExcluded',
                    (cpso_no,))
                break
            except mariadb.OperationalError:
                if attempt == 2:
                    raise
                time.sleep(0.5)

    conn.autocommit = save_commit_state
    conn.commit()


def flag_not_on_register(conn: 'connection', cur_CPSO: int,
                         db_statuses: dict, batch_id=0,
                         exclude_invalid=False):
    """The register genuinely has no such CPSO number. Unlike the
    old site, the new register also PURGES some historical
    doctors, so do not delete anything - keep the collected data
    and only flag the status."""
    print(f'({batch_id}) CPSO: {cur_CPSO} - '
          f'not on the register.')
    DB_LOG.log('INFO', 'not on the register',
               cpso_no=cur_CPSO, batch_uno=batch_id)

    stat_code = retrieve_code_from_name(
        NOT_ON_REGISTER_STAT, db_statuses, conn,
        REG_STAT_TABLE, C_REG_STAT_CODE, C_REG_STAT_NAME)

    curs = conn.cursor()
    curs.execute(f'UPDATE {MD_DIR_TABLE} '
                 f'SET {C_REG_STAT_CODE} = ?, '
                 f'{C_LAST_MODIF} = NOW() '
                 f'WHERE {C_CPSO_NO} = ?',
                 (stat_code, cur_CPSO))
    conn.commit()

    update_detail_table(conn, cur_CPSO, batch_id=batch_id,
                        perm_exclude=exclude_invalid)


def process_record_quick(conn: 'connection', cur_CPSO: int,
                         batch_id=0,
                         exclude_invalid=False,
                         session=None) -> list:
    """Refresh a doctor from the JSON search API only (~1 KB per
    doctor instead of a ~300 KB detail page). Updates the name,
    former name, registration status and the DEFAULT address /
    phone / fax; additional locations, specialties, education,
    languages and hospital privileges are left as previously
    collected. Falls back to the full detail-page scrape when the
    doctor is not in the database yet, or when the status changed
    away from active (the API does not carry the detailed
    inactive reason)."""
    db_statuses = refresh_ref_from_db(conn, REG_STAT_TABLE,
                                      C_REG_STAT_CODE,
                                      C_REG_STAT_NAME)

    if session is None:
        session = make_session()

    try:
        data = fetch_search_result(session, cur_CPSO)
    except CpsoFetchError as e:
        print(f'({batch_id}) {e}')
        DB_LOG.log('WARN', str(e), cpso_no=cur_CPSO,
                   batch_uno=batch_id)
        return []

    results = data.get('results') or []

    if len(results) == 0:
        flag_not_on_register(conn, cur_CPSO, db_statuses,
                             batch_id, exclude_invalid)
        return []

    result = results[0]
    is_active = (result.get('registrationstatus') == 'Active')

    curs = conn.cursor()
    curs.execute(f'SELECT {C_REG_STAT_CODE} FROM {MD_DIR_TABLE} '
                 f'WHERE {C_CPSO_NO} = ?', (cur_CPSO,))
    row = curs.fetchone()

    if row is None:
        # new doctor: collect the complete record
        return process_record(conn, cur_CPSO, batch_id=batch_id,
                              exclude_invalid=exclude_invalid,
                              session=session)

    if not is_active:
        code_names = {code: name
                      for (name, code) in db_statuses.items()}
        cur_stat_name = code_names.get(row[0], '')
        if cur_stat_name.upper().startswith('ACTIVE'):
            # went inactive since the last run: the JSON does not
            # say why, so fetch the detailed status once
            return process_record(conn, cur_CPSO,
                                  batch_id=batch_id,
                                  exclude_invalid=exclude_invalid,
                                  session=session)
        # already inactive with a detailed reason - nothing new
        update_detail_table(conn, cur_CPSO, batch_id=batch_id)
        return []

    save_commit_state = conn.autocommit
    conn.autocommit = False

    # default address, phone and fax
    addr_recs = process_address(
        conn, [search_result_location(result)])
    addr_uno = None
    if len(addr_recs) > 0:
        curs.execute(f'DELETE FROM {MD_ADDR_TABLE} '
                     f'WHERE {C_CPSO_NO} = ? '
                     f'AND {C_ADDR_ORDER} = 1', (cur_CPSO,))
        cur_rec = {C_CPSO_NO: cur_CPSO}
        cur_rec.update(addr_recs[0])
        col_str = ', '.join(cur_rec.keys())
        val_str = ', '.join('?' * len(cur_rec))
        try:
            curs.execute(f'INSERT INTO {MD_ADDR_TABLE} '
                         f'({col_str}) VALUES({val_str})',
                         tuple(cur_rec.values()))
            addr_uno = curs.lastrowid
        except mariadb.Error as e:
            print(f"!!! DB error: {e}")

    # name, former name, status
    upd = {}
    names = result.get('name', '').split(',')
    upd[C_LNAME] = names[0].strip()
    if len(names) > 1:
        first_middle = names[1].split()
        if len(first_middle) > 0:
            upd[C_FNAME] = first_middle[0]
            if len(first_middle) > 1:
                upd[C_MNAME] = ' '.join(first_middle[1:])

    former = result.get('mostrecentformername', '').strip()
    if former:
        upd[C_FRMR_NAME] = former

    upd[C_REG_STAT_CODE] = retrieve_code_from_name(
        result.get('registrationstatus'), db_statuses, conn,
        REG_STAT_TABLE, C_REG_STAT_CODE, C_REG_STAT_NAME,
        aliases=REG_STAT_ALIASES)

    if addr_uno is not None:
        upd[C_DEF_ADDR] = addr_uno

    set_str = ', '.join(f'{col} = ?' for col in upd.keys())
    curs.execute(f'UPDATE {MD_DIR_TABLE} '
                 f'SET {set_str}, {C_LAST_MODIF} = NOW() '
                 f'WHERE {C_CPSO_NO} = ?',
                 tuple(upd.values()) + (cur_CPSO,))

    for s in FINAL_SQL:
        curs.execute(s, (cur_CPSO,))

    conn.commit()
    conn.autocommit = save_commit_state

    record = {C_CPSO_NO: cur_CPSO, MD_ADDR_TABLE: addr_recs}
    record.update(upd)
    summary = '[quick] ' + print_rec(
        record, (C_CPSO_NO, C_FNAME, C_LNAME, C_MNAME,
                 C_ADDR_PREFIX + '1',
                 C_ADDR_PREFIX + '2',
                 C_ADDR_PREFIX + '3',
                 C_ADDR_PREFIX + '4',
                 C_ADDR_CITY,
                 C_ADDR_PROV, C_ADDR_POSTAL,
                 C_ADDR_COUNTRY,
                 C_ADDR_PHONE_NO, C_ADDR_FAX_NO))
    print(f'({batch_id})', summary)
    DB_LOG.debug(summary, cpso_no=cur_CPSO, batch_uno=batch_id)

    update_detail_table(conn, cur_CPSO, batch_id=batch_id)
    return []


def process_record(conn: 'connection', cur_CPSO: int,
                   batch_id=0,
                   exclude_invalid=False,
                   session=None) -> list:
    db_statuses = refresh_ref_from_db(conn, REG_STAT_TABLE,
                                      C_REG_STAT_CODE,
                                      C_REG_STAT_NAME)

    db_reg_classes = refresh_ref_from_db(conn, REG_CLASS_TABLE,
                                         C_REG_CLASS_CODE,
                                         C_REG_CLASS_NAME)

    db_genders = refresh_ref_from_db(conn, GENDER_TABLE,
                                     C_GENDER_CODE, C_GENDER_NAME)

    db_languages = refresh_ref_from_db(conn, LANGUAGE_TABLE,
                                       C_LANG_CODE, C_LANG_NAME)

    db_universities = refresh_ref_from_db(conn, UNIV_TABLE,
                                          C_UNIV_CODE, C_UNIV_NAME)

    db_reg_jurisdic = refresh_ref_from_db(conn, REG_JUR_TABLE,
                                          C_JUR_CODE, C_JUR_NAME)

    db_specialties = refresh_ref_from_db(conn, SPEC_TABLE,
                                         C_SPEC_CODE, C_SPEC_NAME)

    db_spec_types = refresh_ref_from_db(conn, STYPE_TABLE,
                                        C_STYPE_CODE, C_STYPE_NAME)

    db_hospitals = refresh_ref_from_db(conn, HOSP_TABLE,
                                       C_HOSP_CODE, C_HOSP_NAME)

    if session is None:
        session = make_session()

    try:
        html = fetch_physician_page(session, cur_CPSO)
    except CpsoFetchError as e:
        # transient site problem: leave the batch item incomplete
        # so the number is retried in a later run
        print(f'({batch_id}) {e}')
        DB_LOG.log('WARN', str(e), cpso_no=cur_CPSO,
                   batch_uno=batch_id)
        return []

    parsed = parse_physician_page(html)

    if parsed is None:
        flag_not_on_register(conn, cur_CPSO, db_statuses,
                             batch_id, exclude_invalid)
        return []

    record = {C_CPSO_NO: cur_CPSO}

    names = parsed['name'].split(',')
    record[C_LNAME] = names[0].strip()
    if len(names) > 1:
        first_middle = names[1].split()
        if len(first_middle) > 0:
            record[C_FNAME] = first_middle[0]

            if len(first_middle) > 1:
                record[C_MNAME] = ' '.join(first_middle[1:])

    record[C_REG_STAT_CODE] = retrieve_code_from_name(
        parsed['status'], db_statuses, conn,
        REG_STAT_TABLE, C_REG_STAT_CODE, C_REG_STAT_NAME,
        aliases=REG_STAT_ALIASES)

    if parsed['status_date']:
        record[C_REG_EFF_DATE] = reformat_date(
            parsed['status_date'])

    if parsed['expiry_date']:
        record[C_REG_EXP_DATE] = reformat_date(
            parsed['expiry_date'])

    if parsed['reg_class']:
        record[C_REG_CLASS_CODE] = retrieve_code_from_name(
            parsed['reg_class'], db_reg_classes, conn,
            REG_CLASS_TABLE, C_REG_CLASS_CODE, C_REG_CLASS_NAME)
        if parsed['reg_class_date']:
            record[C_REG_CLASS_DATE] = reformat_date(
                parsed['reg_class_date'])

    if parsed['former_name']:
        record[C_FRMR_NAME] = parsed['former_name']

    # gender is NOT NULL in z847e_MD_dir; fall back to 'Unknown'
    # (an existing code in MD_genders) when the site has none
    record[C_MD_GENDER] = retrieve_code_from_name(
        parsed['gender'] or GENDER_UNKNOWN, db_genders, conn,
        GENDER_TABLE, C_GENDER_CODE, C_GENDER_NAME,
        aliases=GENDER_ALIASES)

    lang_codes = []
    for language in parsed['languages']:
        lang_codes.append(retrieve_code_from_name(
            language, db_languages, conn,
            LANGUAGE_TABLE, C_LANG_CODE, C_LANG_NAME))
    record[MD_LANG_TABLE] = list(dict.fromkeys(lang_codes))

    if parsed['medical_school']:
        record[C_UNIV_CODE] = retrieve_code_from_name(
            parsed['medical_school'], db_universities, conn,
            UNIV_TABLE, C_UNIV_CODE, C_UNIV_NAME)
        if parsed['grad_year']:
            record[C_GRAD_YEAR] = parsed['grad_year']

    if parsed['date_of_death']:
        record[C_DATE_OF_DEATH] = reformat_date(
            parsed['date_of_death'])
    elif parsed['status'] == DECEASED_STAT \
            and parsed['status_date']:
        # the new register shows no separate 'Date of Death'
        # field; the 'Deceased as of ...' date is the death date
        record[C_DATE_OF_DEATH] = reformat_date(
            parsed['status_date'])

    jur_codes = []
    for jurisdiction in parsed['jurisdictions']:
        jur_codes.append(retrieve_code_from_name(
            jurisdiction, db_reg_jurisdic, conn,
            REG_JUR_TABLE, C_JUR_CODE, C_JUR_NAME))
    record[MD_REG_JURISDIC] = list(dict.fromkeys(jur_codes))

    record[MD_ADDR_TABLE] = process_address(
        conn, parsed['locations'])

    spec_list = []
    for spec in parsed['specialties']:
        spec_dict = {C_SPEC_CODE: retrieve_code_from_name(
            spec['name'], db_specialties, conn,
            SPEC_TABLE, C_SPEC_CODE, C_SPEC_NAME)}
        if spec['issued_date']:
            spec_dict[C_SPEC_DATE] = reformat_date(
                spec['issued_date'])
        if spec['certifying_body']:
            spec_dict[C_STYPE_CODE] = retrieve_code_from_name(
                spec['certifying_body'], db_spec_types, conn,
                STYPE_TABLE, C_STYPE_CODE, C_STYPE_NAME)
        if spec_dict not in spec_list:
            spec_list.append(spec_dict)

    record[MD_SPEC_TABLE] = spec_list

    hosp_list = []
    for hospital in parsed['hospitals']:
        hosp_name = hospital['name']
        if hospital['location']:
            # same 'Hospital Name (Location)' convention the old
            # program used for the hospital reference table
            hosp_name += f' ({hospital["location"]})'
        hosp_list.append(retrieve_code_from_name(
            hosp_name, db_hospitals, conn,
            HOSP_TABLE, C_HOSP_CODE, C_HOSP_NAME))

    if len(hosp_list) == 0:
        hosp_list.append(retrieve_code_from_name(
            WEB_NO_HOSP, db_hospitals, conn,
            HOSP_TABLE, C_HOSP_CODE, C_HOSP_NAME))

    record[MD_HOSP_TABLE] = list(dict.fromkeys(hosp_list))

    update_x_table(conn, MD_LANG_TABLE, C_CPSO_NO, cur_CPSO,
                   C_LANG_CODE, record[MD_LANG_TABLE])

    summary = print_rec(record,
                        (C_CPSO_NO, C_FNAME, C_LNAME, C_MNAME,
                         C_ADDR_PREFIX + '1',
                         C_ADDR_PREFIX + '2',
                         C_ADDR_PREFIX + '3',
                         C_ADDR_PREFIX + '4',
                         C_ADDR_CITY,
                         C_ADDR_PROV, C_ADDR_POSTAL,
                         C_ADDR_COUNTRY,
                         C_ADDR_PHONE_NO, C_ADDR_FAX_NO))
    print(f'({batch_id}) ', summary)
    DB_LOG.debug(summary, cpso_no=cur_CPSO, batch_uno=batch_id)

    # NOTE: the batch item is deliberately NOT marked completed
    # here - the caller does that after update_record/FINAL_SQL
    # succeed, so a crash mid-write leaves the number unclaimed
    # and the next run re-scrapes it (the full-replace write
    # makes the retry idempotent)

    return [record]


def check_abort_requested(conn: 'connection') -> bool:
    """True when an --abort flag row is present (same check
    request_workload performs at the start of every batch)."""
    curs = conn.cursor()
    curs.execute(f'SELECT COUNT(*) FROM {BATCH_HEAD_TBL} '
                 f'WHERE host = "{ABORT_ALL}" '
                 f'AND batch_size < 0')
    (cnt,) = curs.fetchone()
    # end the read snapshot so the next check sees fresh data
    conn.commit()
    return cnt > 0


def release_unfinished(conn: 'connection', batch_id,
                       interval=DEFAULT_INTERVAL_DAYS):
    """Return the unprocessed numbers of a batch to the pool by
    backdating their claim stamp past the freshness interval, so
    the next run can pick them up immediately."""
    curs = conn.cursor()
    curs.execute(f'UPDATE {BATCH_DET_TBL} '
                 f'SET updated_date_time = '
                 f'NOW() - INTERVAL ? DAY '
                 f'WHERE batch_uno = ? AND NOT isCompleted',
                 (interval + 1, batch_id))
    conn.commit()


def reap_stale_batches(conn: 'connection',
                       stale_minutes=STALE_BATCH_MINUTES,
                       release_days=366) -> int:
    """Close batches abandoned by dead clients (crash, power
    loss, ...) and return their unfinished numbers to the pool.
    A live client completes a number every second or two, so an
    OPEN batch with no completed numbers for stale_minutes can
    only belong to a dead process. Runs automatically whenever a
    client requests a new batch - the ecosystem heals itself."""
    if stale_minutes <= 0:
        return 0

    save_commit_state = conn.autocommit
    conn.autocommit = False
    curs = conn.cursor()

    curs.execute(
        f'SELECT h.batch_uno, h.host '
        f'FROM {BATCH_HEAD_TBL} h '
        f'WHERE h.end_date IS NULL '
        f'AND h.host <> "{ABORT_ALL}" '
        f'AND h.start_date < NOW() - INTERVAL ? MINUTE '
        f'AND NOT EXISTS ('
        f'  SELECT 1 FROM {BATCH_DET_TBL} d '
        f'  WHERE d.batch_uno = h.batch_uno '
        f'  AND d.isCompleted '
        f'  AND d.updated_date_time > '
        f'      NOW() - INTERVAL ? MINUTE)',
        (stale_minutes, stale_minutes))
    dead = [(batch_uno, host) for (batch_uno, host) in curs]

    for (batch_uno, host) in dead:
        curs.execute(f'UPDATE {BATCH_DET_TBL} '
                     f'SET updated_date_time = '
                     f'NOW() - INTERVAL ? DAY '
                     f'WHERE batch_uno = ? AND NOT isCompleted',
                     (release_days, batch_uno))
        released = curs.rowcount
        curs.execute(f'UPDATE {BATCH_HEAD_TBL} '
                     f'SET InProgress = 0, Abandoned = 1, '
                     f'end_date = NOW(), status_date = NOW() '
                     f'WHERE batch_uno = ? '
                     f'AND end_date IS NULL', (batch_uno,))
        print(f'Reaped stale batch {batch_uno} ({host}): '
              f'released {released} unfinished number(s).')
        DB_LOG.log('WARN',
                   f'reaped stale batch from {host}; released '
                   f'{released} unfinished numbers',
                   batch_uno=batch_uno)

    conn.commit()
    conn.autocommit = save_commit_state
    return len(dead)


def force_abort_all(conn: 'connection', release_days=366):
    """--force-abort: unconditionally close ALL open batches,
    release their unfinished numbers and clear any abort flags.
    For when the operator knows no clients are alive."""
    save_commit_state = conn.autocommit
    conn.autocommit = False
    curs = conn.cursor()

    curs.execute(f'UPDATE {BATCH_DET_TBL} d '
                 f'JOIN {BATCH_HEAD_TBL} h '
                 f'ON d.batch_uno = h.batch_uno '
                 f'SET d.updated_date_time = '
                 f'NOW() - INTERVAL ? DAY '
                 f'WHERE NOT d.isCompleted '
                 f'AND h.end_date IS NULL '
                 f'AND h.host <> "{ABORT_ALL}"',
                 (release_days,))
    released = curs.rowcount

    curs.execute(f'UPDATE {BATCH_HEAD_TBL} '
                 f'SET InProgress = 0, Abandoned = 1, '
                 f'end_date = NOW(), status_date = NOW() '
                 f'WHERE end_date IS NULL '
                 f'AND host <> "{ABORT_ALL}"')
    closed = curs.rowcount

    curs.execute(ABORT_DEL_SQL)

    conn.commit()
    conn.autocommit = save_commit_state

    print(f'Force abort: closed {closed} open batch(es), '
          f'released {released} unfinished number(s), '
          f'cleared abort flags.')
    DB_LOG.log('WARN',
               f'force abort: closed {closed} open batches, '
               f'released {released} unfinished numbers')


def run_sweep(conn: 'connection', http_session,
              cpso_start: int, cpso_stop: int, batch_size: int,
              use_random=True, descending=False,
              delay=DEFAULT_DELAY, quick=False,
              control_check=None,
              check_every=0,
              interval=DEFAULT_INTERVAL_DAYS,
              skip_gaps=False) -> int:
    """Work the CPSO number pool until it is exhausted. When
    control_check is given it is consulted between batches; a
    falsy result stops the sweep after the current batch. With
    check_every > 0 the abort flag (and the control check, in
    agent mode) is also consulted after every check_every doctors
    WITHIN a batch; on a stop the batch's unprocessed numbers are
    released back to the pool. Returns the number of doctors
    processed."""
    curs = conn.cursor()

    # CPSO numbers are ever-increasing; a missing number below
    # the highest one we have ever seen is a permanent gap,
    # while a missing number above it may belong to a future
    # newly registered doctor
    curs.execute(f'SELECT COALESCE(MAX({C_CPSO_NO}), 0) '
                 f'FROM {MD_DIR_TABLE}')
    known_max_cpso = curs.fetchone()[0]
    print(f"Highest CPSO number in database: {known_max_cpso} "
          f"(not-found numbers below it are flagged as gaps; "
          f"{'skipped' if skip_gaps else 're-checked'} this run)")

    # sweep epoch (database clock, so it is comparable to the
    # stored timestamps regardless of each machine's timezone).
    # With interval < 1 the pool excludes only numbers completed
    # at/after this instant, so the sweep re-scrapes everything
    # done before it started, exactly once.
    curs.execute('SELECT NOW()')
    sweep_start = curs.fetchone()[0]
    if interval < 1:
        print(f'interval={interval}: re-scraping everything in '
              f'range, ignoring prior-run recency (each number '
              f'once this pass).')

    DB_LOG.log('INFO',
               f'Sweep start: range {cpso_start}-{cpso_stop}, '
               f'batch {batch_size}, quick={quick}, '
               f'delay={delay}, random={use_random}, '
               f'interval={interval}d, '
               f'known_max={known_max_cpso}')

    processed = 0
    stopping = False
    scrape_one = process_record_quick if quick \
        else process_record

    workload = request_workload(conn, random=use_random,
                                batch_size=batch_size,
                                min_val=cpso_start,
                                max_val=cpso_stop,
                                desc=descending,
                                interval=interval,
                                skip_gaps=skip_gaps,
                                sweep_start=sweep_start)

    while len(workload[1]) > 0:

        batch_no = workload[0]
        print(f"Running batch {batch_no} "
              f"({cpso_start}-{cpso_stop}:{batch_size})\n"
              f"======================================")

        for (done, cpso_no) in enumerate(workload[1], start=1):
            try:
                all_recs = scrape_one(
                    conn, cpso_no,
                    batch_id=batch_no,
                    exclude_invalid=(cpso_no <= known_max_cpso),
                    session=http_session)

                if len(all_recs) > 0:
                    update_record(conn, all_recs,
                                  MD_DIR_TABLE, cpso_no)

                    save_commit_state = conn.autocommit
                    conn.autocommit = False
                    for s in FINAL_SQL:
                        curs.execute(s, (cpso_no,))

                    conn.commit()
                    conn.autocommit = save_commit_state

                    # the 'super-transaction' commit: only now,
                    # with every write for this CPSO number in
                    # place, is the batch item marked completed
                    update_detail_table(conn, cpso_no,
                                        batch_id=batch_no)
            except mariadb.Error as e:
                # distinguish data-level errors (collation
                # conflicts, values a column cannot hold, ...)
                # from a lost connection: only the latter must
                # abort the sweep
                try:
                    probe = conn.cursor()
                    probe.execute('SELECT 1')
                    probe.fetchone()
                except mariadb.Error:
                    # connection is gone: let the agent's
                    # reconnect logic (or the operator) handle it
                    raise
                try:
                    conn.rollback()
                except mariadb.Error:
                    pass
                print(f'({batch_no}) CPSO {cpso_no}: '
                      f'database error: {e} - skipped, will '
                      f'be re-scraped in a later run')
                DB_LOG.log('ERROR',
                           f'database error: '
                           f'{traceback.format_exc()}',
                           cpso_no=cpso_no, batch_uno=batch_no)
            except Exception as e:
                # one broken doctor must not kill an unattended
                # sweep: log centrally, leave the number
                # unmarked so a later run retries it, move on
                print(f'({batch_no}) CPSO {cpso_no}: '
                      f'unexpected error: {e} - skipped, will '
                      f'be re-scraped in a later run')
                DB_LOG.log('ERROR',
                           f'unexpected error: '
                           f'{traceback.format_exc()}',
                           cpso_no=cpso_no, batch_uno=batch_no)

            processed += 1

            if check_every > 0 and done % check_every == 0 \
                    and done < len(workload[1]):
                if check_abort_requested(conn) or \
                        (control_check is not None
                         and not control_check()):
                    remaining = len(workload[1]) - done
                    print(f'Stop requested - releasing '
                          f'{remaining} unfinished number(s) '
                          f'of batch {batch_no}.')
                    DB_LOG.log('INFO',
                               f'stop requested; released '
                               f'{remaining} unfinished numbers',
                               batch_uno=batch_no)
                    release_unfinished(conn, batch_no,
                                       interval=interval)
                    stopping = True
                    break

            if delay > 0:
                time.sleep(delay)

        finish_workload(conn, batch_no)

        if stopping:
            break

        if control_check is not None and not control_check():
            print('Stop signal (go flag / run window) - '
                  'stopping this sweep.')
            DB_LOG.log('INFO', 'stop signal between batches')
            break

        workload = request_workload(conn, random=use_random,
                                    batch_size=batch_size,
                                    min_val=cpso_start,
                                    max_val=cpso_stop,
                                    desc=descending,
                                    interval=interval,
                                    skip_gaps=skip_gaps,
                                    sweep_start=sweep_start)

    DB_LOG.log('INFO', f'Sweep finished: {processed} processed')
    return processed


def read_control(conn: 'connection'):
    """Read the newest row of the central control table. Returns
    a dict or None when the table is empty. The BIT columns are
    cast to integers server-side so the connector returns plain
    numbers."""
    curs = conn.cursor()
    base_cols = 'go_flag+0, quick_mode+0, cpso_start, ' \
                'cpso_stop, batch_size, delay_sec, ' \
                'use_random+0, updated'
    # newest optional columns first; fall back for control tables
    # created before they were introduced. CURTIME() rides along
    # so the schedule window is judged by the DATABASE clock -
    # consistent across the whole fleet regardless of how each
    # machine's local timezone is set.
    variants = (
        (f'{base_cols}, abort_check, run_from, run_until, '
         f'interval_days, log_verbose+0, auto_update_hrs, '
         f'skip_gaps+0, CURTIME()', 'skipgaps'),
        (f'{base_cols}, abort_check, run_from, run_until, '
         f'interval_days, log_verbose+0, auto_update_hrs, '
         f'CURTIME()', 'update'),
        (f'{base_cols}, abort_check, run_from, run_until, '
         f'interval_days, log_verbose+0, CURTIME()', 'verbose'),
        (f'{base_cols}, abort_check, run_from, run_until, '
         f'interval_days, CURTIME()', 'interval'),
        (f'{base_cols}, abort_check, run_from, run_until, '
         f'CURTIME()', 'window'),
        (f'{base_cols}, abort_check, CURTIME()', 'abort_check'),
        (f'{base_cols}, CURTIME()', 'base'),
    )
    row = None
    level = 'base'
    try:
        for (cols, lvl) in variants:
            try:
                curs.execute(
                    f'SELECT {cols} FROM {CONTROL_TBL} '
                    f'ORDER BY control_uno DESC LIMIT 1')
                row = curs.fetchone()
                level = lvl
                break
            except mariadb.Error:
                if lvl == 'base':
                    raise
    finally:
        # leave the read snapshot behind so the next poll sees
        # fresh data (REPEATABLE READ would keep serving the old
        # snapshot otherwise)
        conn.commit()

    if row is None:
        return None

    ctl = {'go': bool(row[0]),
           'quick': bool(row[1]),
           'cpso_start': row[2] if row[2] is not None else 10000,
           'cpso_stop': row[3] if row[3] is not None else 200000,
           'batch_size': row[4] if row[4] is not None else 50,
           'delay': float(row[5]) if row[5] is not None
                    else DEFAULT_DELAY,
           'random': bool(row[6]),
           'updated': row[7],
           'abort_check': None,
           'run_from': None,
           'run_until': None,
           'interval': None,
           'log_verbose': None,
           'auto_update_hrs': None,
           'skip_gaps': False,
           'now': row[-1]}

    optional = ('abort_check', 'window', 'interval', 'verbose',
                'update', 'skipgaps')
    if level in optional:
        ctl['abort_check'] = row[8]
    if level in optional[1:]:
        ctl['run_from'] = row[9]
        ctl['run_until'] = row[10]
    if level in optional[2:]:
        ctl['interval'] = row[11]
    if level in optional[3:]:
        ctl['log_verbose'] = bool(row[12])
    if level in optional[4:]:
        ctl['auto_update_hrs'] = row[13]
    if level == 'skipgaps':
        ctl['skip_gaps'] = bool(row[14])

    ctl['in_window'] = in_time_window(ctl['now'],
                                      ctl['run_from'],
                                      ctl['run_until'])
    return ctl


def in_time_window(now_td, from_td, until_td) -> bool:
    """True when 'now' falls inside the daily run window. TIME
    values arrive from the connector as timedeltas. Both limits
    NULL (or equal) = no restriction; from > until = an overnight
    window that wraps midnight (e.g. 19:00 -> 06:30)."""
    if from_td is None and until_td is None:
        return True
    start = from_td if from_td is not None else timedelta(0)
    end = until_td if until_td is not None else timedelta(hours=24)
    if start == end:
        return True
    if start < end:
        return start <= now_td < end
    return now_td >= start or now_td < end


REPO_DIR = os.path.dirname(os.path.abspath(__file__))
# suppress console flashes when the SYSTEM task shells out to git
_NO_WINDOW = getattr(subprocess, 'CREATE_NO_WINDOW', 0)


def _find_git():
    """Locate git under the SYSTEM account (it may not be on the
    task's PATH even though the installer added it)."""
    candidates = ['git',
                  os.path.join(os.environ.get('ProgramFiles', ''),
                               'Git', 'cmd', 'git.exe'),
                  os.path.join(os.environ.get('ProgramFiles(x86)',
                                              ''),
                               'Git', 'cmd', 'git.exe')]
    for git in candidates:
        try:
            subprocess.run([git, '--version'],
                           capture_output=True, timeout=15,
                           creationflags=_NO_WINDOW)
            return git
        except (OSError, subprocess.SubprocessError):
            continue
    return None


def get_agent_version() -> str:
    """Short git commit hash of the deployed code, for the version
    column in MD_scrape_log. 'nogit'/'unknown' when unavailable."""
    git = _find_git()
    if git is None:
        return 'nogit'
    try:
        r = subprocess.run([git, '-C', REPO_DIR, 'rev-parse',
                            '--short', 'HEAD'],
                           capture_output=True, text=True,
                           timeout=15, creationflags=_NO_WINDOW)
        v = (r.stdout or '').strip()
        return v[:40] if v else 'unknown'
    except (OSError, subprocess.SubprocessError):
        return 'unknown'


def git_pull() -> tuple:
    """Fast-forward the local repo. Returns (ok, changed, text)."""
    git = _find_git()
    if git is None:
        return False, False, 'git not found'
    try:
        r = subprocess.run([git, '-C', REPO_DIR, 'pull',
                            '--ff-only'],
                           capture_output=True, text=True,
                           timeout=180, creationflags=_NO_WINDOW)
        out = ((r.stdout or '') + (r.stderr or '')).strip()
        changed = (r.returncode == 0
                   and 'up to date' not in out.lower())
        return r.returncode == 0, changed, out
    except (OSError, subprocess.SubprocessError) as e:
        return False, False, repr(e)


def _cmd_pos_path():
    return os.path.join(REPO_DIR, CMD_POS_FILE)


def _read_cmd_pos():
    """Highest command id already acted on, or None on first run."""
    try:
        with open(_cmd_pos_path()) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _write_cmd_pos(uno) -> bool:
    try:
        with open(_cmd_pos_path(), 'w') as f:
            f.write(str(uno))
        return True
    except OSError as e:
        print(f'Could not persist command position: {e}')
        return False


def process_commands(conn: 'connection', host: str):
    """Check MD_scrape_command for rows targeting this host
    (host_pattern is a SQL LIKE) that have not been acted on yet.
    Returns 'destruct', 'update', or None.

    The last acted-on id is kept in a local pos file, persisted
    BEFORE acting so a command never re-runs after a restart.
    With no pos file yet (first ever run, or the command table was
    created after this agent started) it starts from 0 and
    processes everything pending - which is why 'destruct' is
    guarded by age (DESTRUCT_TTL_MIN): a freshly deployed machine
    obeys a recent destruct but ignores a stale one. 'update' is
    always safe to replay (git pull just reports up to date).

    Returns None (without latching) when the table is absent, so
    creating it later is picked up on the next poll with no
    restart."""
    curs = conn.cursor()
    try:
        last = _read_cmd_pos()
        if last is None:
            last = 0
        curs.execute(
            f'SELECT cmd_uno, command, '
            f'(created >= NOW() - INTERVAL ? MINUTE) '
            f'FROM {CMD_TBL} '
            f'WHERE cmd_uno > ? AND ? LIKE host_pattern '
            f'ORDER BY cmd_uno',
            (DESTRUCT_TTL_MIN, last, host))
        rows = curs.fetchall()
    except mariadb.Error as e:
        # 1146 = table missing: the command feature is not set up
        # here yet; skip this poll (no latch - a table created
        # later is seen next time)
        if getattr(e, 'errno', None) == 1146:
            return None
        raise
    finally:
        try:
            conn.commit()
        except mariadb.Error:
            pass

    action = None
    for (uno, command, fresh) in rows:
        if not _write_cmd_pos(uno):
            # cannot record progress - stop rather than risk a
            # re-run loop; retry next poll
            break
        cmd = (command or '').strip().lower()
        if cmd == 'destruct':
            if fresh:
                return 'destruct'  # recent destruct wins, act now
            # stale destruct: pos already advanced, ignore it
        elif cmd == 'update':
            action = 'update'      # collapse repeats to one pull
    return action


def agent_log_oversized() -> bool:
    """True when run_agent.bat's redirected log (path in
    CPSO_AGENT_LOG) has grown past CPSO_LOG_MAX_MB. Windows will
    not let anything rotate the file while the wrapper holds it
    open, so the agent hands control back (exits
    AGENT_ROTATE_EXIT) and the wrapper rotates + restarts it."""
    path = os.environ.get('CPSO_AGENT_LOG')
    if not path:
        return False
    try:
        mb = float(os.environ.get('CPSO_LOG_MAX_MB',
                                  str(DEFAULT_LOG_MAX_MB)))
    except ValueError:
        mb = DEFAULT_LOG_MAX_MB
    try:
        return os.path.getsize(path) >= mb * 1024 * 1024
    except OSError:
        return False


def run_agent(args, conn: 'connection'):
    """Unattended mode for fleet machines: poll the control table
    and run sweeps with the parameters stored there while go_flag
    is set. Reconnects automatically when the database connection
    drops. Runs until interrupted (Ctrl-C / service stop)."""
    http_session = make_session()
    completed_marker = None      # control 'updated' stamp of the
                                 # last fully exhausted sweep
    completed_time = 0.0
    waiting_logged = False
    last_auto_update = time.time()

    print(f'Agent mode: polling {CONTROL_TBL} on {args.db_host} '
          f'every {args.poll_interval} sec. Ctrl-C to stop.')
    # record the effective config so each machine's settings are
    # visible centrally (run_agent.bat exports the log-rotation
    # values into the environment, so os.environ reflects what is
    # actually in force)
    kports, kproto, _ = knock_config_from_env()
    knock_desc = f'{kproto} x{len(kports)}' if kports else 'off'
    log_cap = os.environ.get('CPSO_LOG_MAX_MB',
                             str(DEFAULT_LOG_MAX_MB))
    log_keep = os.environ.get('CPSO_LOG_KEEP', '3')
    DB_LOG.log('INFO',
               f'agent started (version {DB_LOG.version}, '
               f'poll {args.poll_interval}s, knock {knock_desc}, '
               f'log cap {log_cap}MB x{log_keep})')

    def do_update(reason):
        """git pull; restart (exit 43) only if code changed."""
        ok, changed, out = git_pull()
        DB_LOG.log('INFO', f'{reason}: {out[:200]}')
        print(f'{reason}: {out[:200]}')
        if changed:
            print('Updated - restarting to load new code.')
            sys.exit(AGENT_UPDATE_EXIT)

    while True:
        try:
            # central fleet commands (update / destruct), targeted
            # by host pattern - checked before anything else
            action = process_commands(conn, DB_LOG.host)
            if action == 'destruct':
                print('Central DESTRUCT command - uninstalling '
                      'this agent.')
                DB_LOG.log('WARN', 'destruct command received - '
                           'uninstalling')
                sys.exit(AGENT_DESTRUCT_EXIT)
            elif action == 'update':
                do_update('update command')

            ctl = read_control(conn)

            # periodic self-update (auto_update_hrs > 0)
            hrs = None
            if ctl is not None:
                hrs = ctl.get('auto_update_hrs')
            if hrs is None:
                hrs = DEFAULT_AUTO_UPDATE_HRS
            if hrs and (time.time() - last_auto_update
                        >= hrs * 3600):
                last_auto_update = time.time()
                do_update(f'periodic update ({hrs}h)')

            if ctl is None:
                print(f'Agent: {CONTROL_TBL} is empty; waiting '
                      f'for a control row...')
            elif not ctl['go']:
                completed_marker = None
                completed_time = 0.0
                waiting_logged = False
            elif not ctl['in_window']:
                if not waiting_logged:
                    print(f"Agent: go is set but outside the "
                          f"run window "
                          f"({ctl['run_from']} - "
                          f"{ctl['run_until']}, db time "
                          f"{ctl['now']}); waiting...")
                    waiting_logged = True
            else:
                waiting_logged = False
                resweep_due = (time.time() - completed_time
                               >= AGENT_RESWEEP_SECS)
                if ctl['updated'] != completed_marker \
                        or resweep_due:
                    DB_LOG.verbose = (
                        ctl['log_verbose']
                        if ctl['log_verbose'] is not None
                        else args.verbose_log)
                    print(f"Agent: go! quick={ctl['quick']} "
                          f"range={ctl['cpso_start']}-"
                          f"{ctl['cpso_stop']} "
                          f"batch={ctl['batch_size']} "
                          f"delay={ctl['delay']} "
                          f"random={ctl['random']}\n"
                          f"======================================")

                    def keep_running():
                        c = read_control(conn)
                        return (c is not None and c['go']
                                and c['in_window']
                                and not agent_log_oversized())

                    n = run_sweep(
                        conn, http_session,
                        ctl['cpso_start'], ctl['cpso_stop'],
                        ctl['batch_size'],
                        use_random=ctl['random'],
                        delay=ctl['delay'],
                        quick=ctl['quick'],
                        control_check=keep_running,
                        check_every=(
                            ctl['abort_check']
                            if ctl['abort_check'] is not None
                            else args.abort_check),
                        interval=(
                            ctl['interval']
                            if ctl['interval'] is not None
                            else args.interval),
                        skip_gaps=ctl['skip_gaps'])

                    after = read_control(conn)
                    if after is not None and after['go'] \
                            and after['in_window']:
                        # pool exhausted while still 'go' inside
                        # the window: note the control stamp so we
                        # do not spin; re-sweep when the row
                        # changes or after the idle period
                        completed_marker = after['updated']
                        completed_time = time.time()
                        print(f'Agent: pool exhausted '
                              f'({n} processed). Idle until the '
                              f'control row changes or '
                              f'{AGENT_RESWEEP_SECS // 3600} h '
                              f'passes.')
                    else:
                        # stopped by flag drop or window close:
                        # forget the marker so the sweep resumes
                        # as soon as go/window allows
                        completed_marker = None
                        completed_time = 0.0

            # hand control back to run_agent.bat so it can rotate
            # the (Windows-locked) local log, then restart us
            if agent_log_oversized():
                print('Local log reached its size cap - exiting '
                      'for rotation; run_agent.bat restarts '
                      'immediately.')
                DB_LOG.log('INFO', 'local log rotation restart')
                sys.exit(AGENT_ROTATE_EXIT)

            time.sleep(args.poll_interval)

        except mariadb.Error as e:
            print(f'Agent: database error: {e}. '
                  f'Reconnecting in 60 sec...')
            DB_LOG.log('WARN',
                       f'agent database error, '
                       f'reconnecting: {e}')
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(60)
            while True:
                try:
                    conn = db_connect(
                        user=args.db_user,
                        password=args.db_pass,
                        host=args.db_host,
                        port=args.db_port,
                        database=args.db_name,
                        compress=True)
                    print('Agent: reconnected.')
                    DB_LOG.log('INFO', 'agent reconnected')
                    break
                except mariadb.Error as e2:
                    print(f'Agent: reconnect failed: {e2}; '
                          f'retrying in 60 sec...')
                    time.sleep(60)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Process parameters here
    parser = argparse.ArgumentParser(
        description='CPSO web site parse utility '
                    '(c) Eugene Robertus, 2021')
    parser.add_argument('-s', '--cpso-start', type=int,
                        help='CPSO number to start from '
                             '(default=10000)',
                        default=10000)
    parser.add_argument('-e', '--cpso-stop', type=int,
                        default=200000,
                        help='CPSO number to stop at '
                             '(default=200000; raised from the '
                             'old 158100 to catch newly '
                             'registered doctors)')
    parser.add_argument('--db-host', type=str,
                        default='faxcomet.com',
                        help='host name where database is located '
                             '(default=faxcomet.com)')
    parser.add_argument('-d','--db-name', type=str,
                        default='faxcomet_MD_list',
                        help='database name '
                             '(default=faxcomet_MD_list)')
    parser.add_argument('-u', '--db-user', type=str,
                        default='faxcomet_scrape',
                        help='database user '
                             '(default=faxcomet_scrape)')
    parser.add_argument('-p', '--db-pass',
                        '--db-password', '-db-pwd', type=str,
                        default='NnBgmX$t^+tG',
                        help='database user password '
                             '(default <hidden>)')
    parser.add_argument('--db-port', type=int,
                        default=3306,
                        help='database port for connection '
                             '(default=3306)')
    parser.add_argument('-r', '--random',
                        action='store_true',
                        help='if set, use random iteration order '
                             '(default: iterate in '
                             'ascending sequence)')
    parser.add_argument('--descending',
                        action='store_true',
                        help='if set, use descending iteration order '
                             '(default: iterate in '
                             'ascending sequence)')
    parser.add_argument('-z', '--batch-size', type=int,
                        default=50,
                        help='iteration batch size '
                             '(default=50)')
    parser.add_argument('-a', '--abort',
                        action='store_true',
                        help='request abort of all running scrapes '
                             '(waits for live clients to stop; '
                             'batches of dead clients are reaped '
                             'once they age past --stale-minutes)')
    parser.add_argument('--force-abort',
                        action='store_true',
                        help='immediately close ALL open batches, '
                             'release their unfinished numbers '
                             'and clear abort flags, without '
                             'waiting - use when no clients are '
                             'alive')
    parser.add_argument('--stale-minutes', type=int,
                        default=STALE_BATCH_MINUTES,
                        metavar='N',
                        help='an open batch with no completed '
                             'numbers for N minutes is considered '
                             'abandoned by a dead client and is '
                             'automatically closed (its numbers '
                             'return to the pool); checked every '
                             'time a client requests a batch; '
                             '0 disables '
                             f'(default={STALE_BATCH_MINUTES})')
    parser.add_argument('--knock', type=str, default=None,
                        metavar='SEQ',
                        help='port-knock sequence to open the '
                             'database firewall for this '
                             "machine's current IP when a "
                             'connection fails (clinics behind a '
                             'dynamic-IP firewall). Format '
                             '"[proto:]p1,p2,p3" e.g. '
                             '"tcp:7001,8002,9003" (ports may be '
                             "separated by ',' or ';'). Overrides "
                             'the CPSO_KNOCK environment variable')
    parser.add_argument('--knock-delay', type=float, default=None,
                        metavar='SEC',
                        help='seconds between individual knocks '
                             '(default 0.3)')
    parser.add_argument('-v', '--verbose-log',
                        action='store_true',
                        help='also write each doctor\'s summary '
                             'line to the central log table as '
                             'DEBUG rows (default: only '
                             'lifecycle, warnings and errors go '
                             'to the central log); in agent mode '
                             'the log_verbose control column '
                             'takes precedence')
    parser.add_argument('-i', '--interval', type=int,
                        default=DEFAULT_INTERVAL_DAYS,
                        metavar='DAYS',
                        help='freshness window: a doctor is only '
                             're-scraped when the last update is '
                             'older than this many days; in '
                             'agent mode the interval_days '
                             'column of the control table takes '
                             'precedence '
                             f'(default={DEFAULT_INTERVAL_DAYS})')
    parser.add_argument('-c', '--abort-check', type=int,
                        default=0, metavar='N',
                        help='also check for an abort request '
                             '(and the go flag in agent mode) '
                             'after every N doctors WITHIN a '
                             'batch; unprocessed numbers of the '
                             'interrupted batch are released '
                             'back to the pool immediately '
                             '(default 0 = check only between '
                             'batches)')
    parser.add_argument('--agent',
                        action='store_true',
                        help='unattended fleet mode: poll the '
                             f'{CONTROL_TBL} table and scrape '
                             'with the parameters stored there '
                             'while its go_flag is set; range/'
                             'batch/delay/quick options on the '
                             'command line are ignored. Runs '
                             'until interrupted')
    parser.add_argument('--poll-interval', type=int,
                        default=AGENT_POLL_SECS,
                        help='seconds between control-table '
                             'checks in --agent mode '
                             f'(default={AGENT_POLL_SECS})')
    parser.add_argument('-q', '--quick',
                        action='store_true',
                        help='refresh from the JSON search API '
                             'only (much lighter/faster): '
                             'updates name, status and the '
                             'default address/phone/fax; leaves '
                             'additional locations, specialties, '
                             'education, languages and hospital '
                             'privileges as previously '
                             'collected. Falls back to a full '
                             'page scrape for new doctors and '
                             'for doctors that went inactive '
                             'since the last run')
    parser.add_argument('--delay', type=float,
                        default=DEFAULT_DELAY,
                        help='seconds to wait between two '
                             'physician downloads '
                             f'(default={DEFAULT_DELAY})')
    parser.add_argument('--skip-gaps',
                        action='store_true',
                        help='skip known gaps (CPSO numbers below '
                             'the database maximum that were not '
                             'found on the register) for a fast '
                             'close-in-time re-sweep. By DEFAULT '
                             'those gaps are re-checked, so mid-'
                             'range numbers newly assigned to a '
                             'doctor are picked up. Agent mode is '
                             'governed by the skip_gaps control '
                             'column instead of this flag')
    args = parser.parse_args()

    STALE_MINUTES_ACTIVE = args.stale_minutes

    # CLI knock options feed the same env-based config the knock
    # helper reads, so there is one code path
    if args.knock is not None:
        os.environ['CPSO_KNOCK'] = args.knock
    if args.knock_delay is not None:
        os.environ['CPSO_KNOCK_DELAY'] = str(args.knock_delay)

    # central fleet log (best-effort; falls back to console)
    DB_LOG.init(user=args.db_user,
                password=args.db_pass,
                host=args.db_host,
                port=args.db_port,
                database=args.db_name)
    DB_LOG.verbose = args.verbose_log

    # Connect to MariaDB Platform
    try:
        connect_db = db_connect(
            user=args.db_user,
            password=args.db_pass,
            host=args.db_host,
            port=args.db_port,
            database=args.db_name,
            compress=True
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)


    curs = connect_db.cursor()
    CPSO_START = args.cpso_start
    CPSO_STOP = args.cpso_stop
    USE_RANDOM = args.random
    BATCH_SIZE = args.batch_size
    # for cpso_no in range(108493,150000): # TEST_CPSO:

    if not args.agent:
        print(f"Running with following parameters:\n"
              f"From CPSO  : {CPSO_START}\n"
              f"To CPSO    : {CPSO_STOP}\n"
              f"Host       : {args.db_host}\n"
              f"Database   : {args.db_name}\n"
              f"User       : {args.db_user}\n"
              f"Random     : {USE_RANDOM}\n"
              f"Batch size : {BATCH_SIZE}\n"
              f"Delay      : {args.delay}\n"
              f"Skip gaps  : {args.skip_gaps}\n"
              f"Quick mode : {args.quick}\n"
              f"======================================")

    if args.abort:
        save_commit_state = connect_db.autocommit
        connect_db.autocommit = True

        curs.execute(ABORT_SET_SQL)
        running_tasks = 1

        while running_tasks > 0:
            print(f"Waiting {SECONDS_TO_WAIT} seconds for all tasks "
                  f"to finish...")
            sleep(SECONDS_TO_WAIT)
            # batches of dead clients would keep this loop
            # waiting forever - reap them as they age out
            reap_stale_batches(connect_db,
                               stale_minutes=args.stale_minutes)
            curs.execute(CHECK_STOP_STATUS)
            running_tasks = curs.fetchone()[0]
            print(f"Number of running tasks: {running_tasks}")

        print("All tasks stopped. Clearing database...")
        curs.execute(ABORT_DEL_SQL)
        connect_db.autocommit = save_commit_state
        print("Done.")

    elif args.force_abort:
        force_abort_all(connect_db)

    elif args.agent:
        run_agent(args, connect_db)

    else:
        http_session = make_session()
        run_sweep(connect_db, http_session,
                  CPSO_START, CPSO_STOP, BATCH_SIZE,
                  use_random=USE_RANDOM,
                  descending=args.descending,
                  delay=args.delay,
                  quick=args.quick,
                  check_every=args.abort_check,
                  interval=args.interval,
                  skip_gaps=args.skip_gaps)
    curs.close()
    connect_db.close()
