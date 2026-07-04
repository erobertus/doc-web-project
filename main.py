# CPSO physician register scraper
# (rewritten for the new register.cpso.on.ca site, 2026)


import sys
import mariadb
import time
import argparse
from time import sleep
from datetime import timedelta
from constants import *
from GeoCoding import get_geocode_db_uno
from cpso_site import (make_session, fetch_physician_page,
                       parse_physician_page, fetch_search_result,
                       search_result_location, CpsoFetchError)


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
                     desc=False) -> tuple:
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

    # curs.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
    curs.execute(BEGIN_TRAN)
    stmt = f'INSERT INTO {BATCH_HEAD_TBL} (batch_size, host) ' \
           f'VALUES ({batch_size}, user())'
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

    stmt = f'SELECT cpso_no FROM _TMP_{batch_id} ' \
           f'WHERE cpso_no not in (' \
           f'SELECT DISTINCT {C_CPSO_NO} FROM {BATCH_DET_TBL} ' \
           f'WHERE {C_CPSO_NO} between {min_val} and {max_val} ' \
           'AND (updated_date_time > (NOW() - INTERVAL ' \
           f'{interval} DAY) ' \
           'OR PermExcluded) ) ' \
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
    # curs.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')

    # conn.commit()
    conn.autocommit = save_commit_state

    # Drop temp table
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
    print(f'({batch_id}) [quick]',
          print_rec(record, (C_CPSO_NO, C_FNAME, C_LNAME, C_MNAME,
                             C_ADDR_PREFIX + '1',
                             C_ADDR_PREFIX + '2',
                             C_ADDR_PREFIX + '3',
                             C_ADDR_PREFIX + '4',
                             C_ADDR_CITY,
                             C_ADDR_PROV, C_ADDR_POSTAL,
                             C_ADDR_COUNTRY,
                             C_ADDR_PHONE_NO, C_ADDR_FAX_NO)))

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

    print(f'({batch_id}) ',
          print_rec(record, (C_CPSO_NO, C_FNAME, C_LNAME, C_MNAME,
                             C_ADDR_PREFIX + '1',
                             C_ADDR_PREFIX + '2',
                             C_ADDR_PREFIX + '3',
                             C_ADDR_PREFIX + '4',
                             C_ADDR_CITY,
                             C_ADDR_PROV, C_ADDR_POSTAL,
                             C_ADDR_COUNTRY,
                             C_ADDR_PHONE_NO, C_ADDR_FAX_NO)))

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


def run_sweep(conn: 'connection', http_session,
              cpso_start: int, cpso_stop: int, batch_size: int,
              use_random=True, descending=False,
              delay=DEFAULT_DELAY, quick=False,
              perm_exclude=False, control_check=None,
              check_every=0,
              interval=DEFAULT_INTERVAL_DAYS) -> int:
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
          f"(missing numbers below it are excluded permanently)")

    processed = 0
    stopping = False
    scrape_one = process_record_quick if quick \
        else process_record

    workload = request_workload(conn, random=use_random,
                                batch_size=batch_size,
                                min_val=cpso_start,
                                max_val=cpso_stop,
                                desc=descending,
                                interval=interval)

    while len(workload[1]) > 0:

        batch_no = workload[0]
        print(f"Running batch {batch_no} "
              f"({cpso_start}-{cpso_stop}:{batch_size})\n"
              f"======================================")

        for (done, cpso_no) in enumerate(workload[1], start=1):
            all_recs = scrape_one(
                conn, cpso_no,
                batch_id=batch_no,
                exclude_invalid=(perm_exclude or
                                 cpso_no <= known_max_cpso),
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

                # the 'super-transaction' commit: only now, with
                # every write for this CPSO number in place, is
                # the batch item marked completed
                update_detail_table(conn, cpso_no,
                                    batch_id=batch_no)

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
            break

        workload = request_workload(conn, random=use_random,
                                    batch_size=batch_size,
                                    min_val=cpso_start,
                                    max_val=cpso_stop,
                                    desc=descending,
                                    interval=interval)
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
           'now': row[-1]}

    if level in ('abort_check', 'window', 'interval'):
        ctl['abort_check'] = row[8]
    if level in ('window', 'interval'):
        ctl['run_from'] = row[9]
        ctl['run_until'] = row[10]
    if level == 'interval':
        ctl['interval'] = row[11]

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

    print(f'Agent mode: polling {CONTROL_TBL} on {args.db_host} '
          f'every {args.poll_interval} sec. Ctrl-C to stop.')

    while True:
        try:
            ctl = read_control(conn)

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
                                and c['in_window'])

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
                            else args.interval))

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

            time.sleep(args.poll_interval)

        except mariadb.Error as e:
            print(f'Agent: database error: {e}. '
                  f'Reconnecting in 60 sec...')
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(60)
            while True:
                try:
                    conn = mariadb.connect(
                        user=args.db_user,
                        password=args.db_pass,
                        host=args.db_host,
                        port=args.db_port,
                        database=args.db_name,
                        compress=True)
                    print('Agent: reconnected.')
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
                        help='request abort of all running scrapes')
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
    parser.add_argument('--perm-exclude',
                        action='store_true',
                        help='permanently exclude ALL CPSO '
                             'numbers that are not found on the '
                             'register. By default only numbers '
                             'BELOW the highest CPSO number '
                             'already in the database are '
                             'excluded (CPSO numbers are ever-'
                             'increasing, so gaps between '
                             'existing doctors are never filled '
                             'in); numbers above it are re-'
                             'checked on the next run because '
                             'they may be issued to newly '
                             'registered doctors')
    args = parser.parse_args()

    # Connect to MariaDB Platform
    try:
        connect_db = mariadb.connect(
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
              f"Perm excl. : {args.perm_exclude}\n"
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
            curs.execute(CHECK_STOP_STATUS)
            running_tasks = curs.fetchone()[0]
            print(f"Number of running tasks: {running_tasks}")

        print("All tasks stopped. Clearing database...")
        curs.execute(ABORT_DEL_SQL)
        connect_db.autocommit = save_commit_state
        print("Done.")

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
                  perm_exclude=args.perm_exclude,
                  check_every=args.abort_check,
                  interval=args.interval)
    curs.close()
    connect_db.close()
