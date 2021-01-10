# This is a sample Python script.


import sys
import mechanicalsoup as ms
import re
import mariadb
import googlemaps
from constants import *


# NnBgmX$t^+tG

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
                            name_col: str) -> int:
    if member not in array:
        result = update_reference(
            member, array, cur_conn, table, code_col, name_col)
    else:
        result = array[member]

    return result


def is_numeric(s: str) -> bool:
    return re.sub(r'\W|_', '', s).isnumeric()


def is_at_top_of_address(addr: list, position: int) -> bool:
    result = False
    if (position == 0) or (position >= len(addr)):
        result = True
    elif addr[position] in (PHONE_TAG, FAX_TAG, COUNTY_TAG,
                            E_DISTR_TAG):
        result = False
    elif addr[position - 1] in (PHONE_TAG, FAX_TAG, E_DISTR_TAG) \
            and not is_numeric(addr[position]):
        result = True
    elif addr[position - 2] in (PHONE_TAG, FAX_TAG, E_DISTR_TAG) \
            and is_numeric(addr[position - 1]):
        result = True

    return result


def next_value(in_lst: list):
    if len(in_lst) > 1:
        return in_lst[1]
    else:
        return ''


def is_postal(s: str) -> bool:
    re_pattern = r'^.+\xa0.+\xa0{2}'
    return re.search(re_pattern, s) is not None


def processs_address(source: 'ResultSet') -> list[dict]:
    addr_dict = {}
    return_list = []
    idx = 0
    addr_order = 0

    while idx < len(source):
        addr_list = [s for s in source[idx].stripped_strings]

        j = 0
        at_top_of_addr = True

        while j < len(addr_list):
            if at_top_of_addr:
                addr_line = 1
                addr_order += 1
                addr_dict[C_ADDR_ORDER] = addr_order
                addr_dict[C_ADDR_IS_DEF] = int(addr_order == 1)
                streets_filled = False

            if addr_list[j].find(POSTAL_SEPARATOR) > -1 \
                    and ((len(addr_list) > j+1 \
                    and addr_list[j+1].find(POSTAL_SEPARATOR) == -1) \
                    or len(addr_list) == j+1) :
                # we are on the line:
                # the line has \xa0 and may be one of the lines
                # with city, province, postal. The logic
                # for checking is either last line with \xa0 or
                # the line with \xa0 with no following line
                t_info = addr_list[j].split(POSTAL_SEPARATOR)
                addr_dict[C_ADDR_CITY] = t_info[0]
                if len(t_info) > 3:
                    addr_dict[C_ADDR_PROV] = t_info[1]
                    addr_dict[C_ADDR_POSTAL] = t_info[3]
                else:
                    addr_dict[C_ADDR_POSTAL] = t_info[1]

                if next_value(addr_list[j:]) in (
                        PHONE_TAG, FAX_TAG,
                        E_DISTR_TAG, COUNTY_TAG, BLANK):
                    addr_dict[C_ADDR_COUNTRY] = CANADA
                else:
                    j += 1
                    addr_dict[C_ADDR_COUNTRY] = addr_list[j]
                streets_filled = True
            elif addr_list[j] in (PHONE_TAG, FAX_TAG, E_DISTR_TAG):
                t_info = next_value(addr_list[j:]). \
                    split(POSTAL_SEPARATOR)

                if is_numeric(t_info[0]):
                    addr_dict[WEB2DB_MAP[addr_list[j]]] = t_info[0]
                    if addr_list[j] == PHONE_TAG and \
                            len(t_info) > 1:
                        addr_dict[C_ADDR_EXT] = t_info[2]
                    j += 1
                streets_filled = True
            elif addr_list[j] == COUNTY_TAG and \
                    next_value(addr_list[j:]) != E_DISTR_TAG:
                addr_dict[WEB2DB_MAP[COUNTY_TAG]] = \
                    next_value(addr_list[j:])
                j += 1
                streets_filled = True

            if not streets_filled:
                addr_dict[C_ADDR_PREFIX + str(addr_line)] = \
                    addr_list[j]
                addr_line += 1
                at_top_of_addr = False

            j += 1
            if is_at_top_of_address(addr_list, j):
                if len(addr_dict) > 0:
                    return_list.append(addr_dict)
                    addr_dict = {}
                    at_top_of_addr = True

        idx += 1

    return return_list


def update_x_table(in_db: 'connection', table: str,
                   key_col: str, key: str,
                   val_col: str, values: list):
    save_commit_state = in_db.autocommit
    in_db.autocommit = False
    curs = in_db.cursor()

    str_list = ', '.join([str(s) for s in values])

    # DELETE VALUES NOT IN values
    stmt = f'DELETE FROM {table} WHERE {key_col} = ? ' \
           f'AND {val_col} NOT IN ({str_list})'

    curs.execute(stmt, (key,))

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

    in_db.commit()
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
                     interval=20,
                     min_val=10000,
                     max_val=150000,
                     desc=False) -> tuple:
    curs = conn.cursor()
    stmt = f'INSERT INTO {BATCH_HEAD_TBL} (batch_size, host) ' \
           f'VALUES ({batch_size}, user())'
    curs.execute(stmt)
    batch_id = curs.lastrowid

    # make list
    if not random:

        # GET LIST OF CPSO NUMBERS
        # create simple range first
        src_list = [x for x in range(min_val, max_val)]
        if desc:
            src_list.reverse()
    else:
        stmt = 'SELECT d.CPSO_no, floor(RAND() * 10000000) r ' \
               f'FROM {MD_DIR_TABLE} d ' \
               f'WHERE d.CPSO_no between {min_val} and {max_val} ' \
               'ORDER BY r ' \

        curs.execute(stmt)

        src_list = [cpso_no for (cpso_no, order) in curs]

    stmt = f'SELECT DISTINCT {C_CPSO_NO} FROM {BATCH_DET_TBL} ' \
           f'WHERE {C_CPSO_NO} between {min_val} and {max_val} ' \
           'AND ((isCompleted AND ' \
           'updated_date_time > NOW() - INTERVAL ' \
           f'{interval} DAY) ' \
           'OR (NOT isCompleted AND ' \
           f'updated_date_time <= NOW()-INTERVAL {interval} DAY) ' \
           f'OR PermExcluded) ' \
           f'ORDER BY {C_CPSO_NO}'

    curs.execute(stmt)

    for (cpso_no,) in curs:
        if cpso_no in src_list:
            src_list.remove(cpso_no)

    src_list = src_list[:batch_size]

    stmt = f'INSERT INTO {BATCH_DET_TBL} (batch_uno, cpso_no, ' \
           f'updated_date_time) VALUES ({batch_id}, ?, NOW())'
    curs.executemany(stmt, [(x,) for x in src_list])
    conn.commit()

    return (batch_id, tuple(src_list))


def update_detail_table(conn: 'connection',
                   cpso_no: int, perm_exclude=False,
                   batch_id=0):

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
    conn.commit()


def link_geocode(conn_db: 'connection', record: list,
                 incl_prov=(ALL,), incl_cntry=(ALL,)):
    ADDR_COMPONENTS = 'address_components'
    STR_NO = 'street_number'
    TYPES = 'types'
    LONG_NAME = 'long_name'
    SHORT_NAME = 'short_name'
    STR_NAME = 'route'
    CITY_NAME = 'locality'
    COUNTY_NAME = 'administrative_area_level_2'
    PROV_NAME = 'administrative_area_level_1'
    COUNTRY_NAME = 'country'
    POSTAL_NAME = 'postal_code'
    GEOM = 'geometry'
    LOC = 'location'
    LAT = 'lat'
    LNG = 'lng'

    curs = conn_db.cursor()
    gmaps = googlemaps.Client(key=API_KEY)
    for cur_addr in record[MD_ADDR_TABLE]:
        # get if postal already stored
        stmt = f'SELECT geo_uno `cnt` FROM {GEO_TABLE} ' \
               f'WHERE postal_code = ? and country = ?'
        curs.execute(stmt,
                     (cur_addr[C_ADDR_POSTAL],
                      cur_addr[C_ADDR_COUNTRY]))

        if curs.rowcount > 0:
            geo_unos = [x for (x,) in curs]




        do_geocode = (ALL in incl_prov or
                      cur_addr[C_ADDR_PROV] in incl_prov) and \
                     (ALL in incl_cntry or
                      cur_addr[C_ADDR_COUNTRY] in incl_cntry)

        if cur_addr[C_ADDR_PREFIX+'1'] != NO_ADDR and do_geocode:
            addr = print_rec(cur_addr, (
                               C_ADDR_PREFIX + '1',
                               C_ADDR_PREFIX + '2',
                               C_ADDR_PREFIX + '3',
                               C_ADDR_PREFIX + '4',
                               C_ADDR_CITY,
                               C_ADDR_PROV, C_ADDR_POSTAL,
                               C_ADDR_COUNTRY),
                             map=ADDR_MAP)
            geocode_result = gmaps.geocode(addr)

            streets = []
            for geocode in geocode_result:
                for addr_part in geocode[ADDR_COMPONENTS]:
                    for types in addr_part[TYPES]:
                        if types == STR_NO:
                            str_no = addr_part[SHORT_NAME]
                        elif types == STR_NAME:
                            streets.append(addr_part[SHORT_NAME])
                        elif types == CITY_NAME:
                            city = addr_part[SHORT_NAME]
                        elif types == COUNTY_NAME:
                            county = addr_part[SHORT_NAME]
                        elif types == PROV_NAME:
                            prov = addr_part[SHORT_NAME]
                        elif types == COUNTRY_NAME:
                            country = addr_part[LONG_NAME]
                        elif types == POSTAL_NAME:
                            postal = addr_part[SHORT_NAME]
                for i in range(len(streets),4):
                    streets.append(BLANK)
                lat = geocode[GEOM][LOC][LAT]
                lng = geocode[GEOM][LOC][LNG]

    conn_db.commit()

    pass

def process_record(conn: 'connection', cur_CPSO: int,
                   batch_id=0,
                   exclude_invalid=True) -> list:
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

    result_dict = {}

    browser = ms.StatefulBrowser(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0')
    url = 'https://doctors.cpso.on.ca/?search=general'

    browser.open(url)
    # page_html = page.soup
    # form = browser.select_form('form[action="/?search=general"]')
    form = browser.select_form()
    form[ALL_DR_SEARCH_CHECKMARK] = True
    form[CPSONO_FIELD] = cur_CPSO
    response = browser.submit_selected()
    record = {C_CPSO_NO: cur_CPSO}
    page = response.soup
    error_str = ''

    if response.status_code == 200:
        error_str = page.find_all(DIV, class_=CL_ERROR)

    if response.status_code == 200 and len(error_str) == 0:
        name = re.sub(r'(\r\n|\t|\s)', ' ', page.h1.string).strip()
        names = name.split(',')
        record[C_LNAME] = names[0].strip(',')
        if len(names) > 1:
            names[1] = names[1].split()
            if len(names[1]) > 0:
                record[C_FNAME] = names[1][0]

                if len(names[1]) > 1:
                    record[C_MNAME] = ' '.join(names[1][1:])

        all_info = page.find_all(DIV, class_=CL_DR_INFO)

        for cur_info in all_info:
            info = [re.sub(r'[\r\n\t\s]+as of[\r\n\t\s]*',
                           ' as of ', s)
                    for s in cur_info.stripped_strings]
            info[1] = info[1].split(' as of ')

            if info[0] == WEB_MEMBER_STAT:
                record[C_REG_STAT_CODE] = retrieve_code_from_name(
                    info[1][0], db_statuses, conn,
                    REG_STAT_TABLE, C_REG_STAT_CODE, C_REG_STAT_NAME)

                record[C_REG_EFF_DATE] = reformat_date(info[1][1])
            elif info[0] == WEB_EXPIRY_DATE:
                record[C_REG_EXP_DATE] = reformat_date(info[1][0])
            elif info[0] == WEB_CPSO_REG_CL:
                record[C_REG_CLASS_CODE] = retrieve_code_from_name(
                    info[1][0], db_reg_classes, conn,
                    REG_CLASS_TABLE, C_REG_CLASS_CODE,
                    C_REG_CLASS_NAME)
                if len(info[1][1]) > 0:
                    record[C_REG_CLASS_DATE] = reformat_date(
                        info[1][1])

            # print(info)

        all_info = page.find_all(DIV, class_=CL_INFO)

        for cur_info in all_info:
            info = [s for s in cur_info.stripped_strings]
            i = 0
            while i < len(info) - 1:
                if info[i] == WEB_FRMR_NAME:
                    record[C_FRMR_NAME] = next_value(info[i:])
                    i += 1
                elif info[i] == WEB_GENDER:
                    record[WEB2DB_MAP[WEB_GENDER]] = \
                        retrieve_code_from_name(
                            next_value(info[i:]), db_genders, conn,
                            GENDER_TABLE, C_GENDER_CODE, C_GENDER_NAME
                        )
                    i += 1
                elif info[i] == WEB_LANGUAGES:
                    md_languages = re.sub(', +', DELIM_COMMA,
                                          next_value(info[i:])
                                          ).split(DELIM_COMMA)
                    lang_codes = []
                    for language in md_languages:
                        lang_codes.append(
                            retrieve_code_from_name(
                                language, db_languages, conn,
                                LANGUAGE_TABLE, C_LANG_CODE,
                                C_LANG_NAME
                            )
                        )
                    record[MD_LANG_TABLE] = lang_codes
                    i += 1
                elif info[i] == WEB_UNIVERSITY:
                    education = \
                        re.sub(', +', DELIM_COMMA,
                               next_value(info[i:])
                               ).split(DELIM_COMMA)
                    if len(education[0]) > 0:
                        record[C_UNIV_CODE] = retrieve_code_from_name(
                            ', '.join(education[:-1]),
                            db_universities,
                            conn, UNIV_TABLE, C_UNIV_CODE, C_UNIV_NAME
                        )
                        if len(education[-1]) > 0:
                            record[C_GRAD_YEAR] = education[-1]
                    i += 1
                elif info[i] == WEB_DATE_OF_DEATH:
                    record[WEB2DB_MAP[WEB_DATE_OF_DEATH]] = \
                        reformat_date(next_value(info[i:]))
                    i += 1
                elif info[i] == WEB_REG_IN_OTHER_JUR:

                    jur_codes = []
                    for jurisdiction in info[i + 2:]:
                        jur_codes.append(retrieve_code_from_name(
                            jurisdiction, db_reg_jurisdic,
                            conn, REG_JUR_TABLE, C_JUR_CODE,
                            C_JUR_NAME
                        ))
                    record[MD_REG_JURISDIC] = jur_codes
                    i += len(info[i + 1:])
                i += 1

        all_info = page.find_all(DIV, class_=CL_ADD_PR_LOC)
        record[MD_ADDR_TABLE] = processs_address(all_info)

        all_info = page.find_all(SECT,
                                 class_=CL_SPECIALTIES,
                                 id=ID_SPECIALTIES)

        if len(all_info) > 0:
            cur_info = [s for s in all_info[0].stripped_strings][4:]
        else:
            cur_info = []

        i = 0
        spec_list = []

        while i < len(cur_info):
            spec_dict = {C_SPEC_CODE:
                             retrieve_code_from_name(cur_info[i],
                                                     db_specialties,
                                                     conn, SPEC_TABLE,
                                                     C_SPEC_CODE,
                                                     C_SPEC_NAME)
                         }
            if i + 2 < len(cur_info):
                s = cur_info[i + 1].split(':')
                if len(s) > 1:
                    spec_dict[C_SPEC_DATE] = reformat_date(s[1])
                spec_dict[C_STYPE_CODE] = retrieve_code_from_name(
                    cur_info[i + 2], db_spec_types, conn,
                    STYPE_TABLE, C_STYPE_CODE, C_STYPE_NAME
                )

            spec_list.append(spec_dict)
            i += 3

        record[MD_SPEC_TABLE] = spec_list

        all_info = page.find_all(SECT,
                                 class_=CL_HOSPITALS, id=ID_HOSPITALS)

        if len(all_info) > 0:
            cur_info = [s for s in all_info[0].stripped_strings]
        else:
            cur_info = []

        hosp_list = []

        if len(cur_info) > 0:
            if cur_info[1] == WEB_NO_HOSP:
                hosp_list.append(retrieve_code_from_name(
                    cur_info[1], db_hospitals, conn,
                    HOSP_TABLE, C_HOSP_CODE, C_HOSP_NAME
                ))
            else:
                i = 3
                while i < len(cur_info):
                    hosp_list.append(retrieve_code_from_name(
                        cur_info[i] + f' ({cur_info[i + 1]})',
                        db_hospitals, conn,
                        HOSP_TABLE, C_HOSP_CODE, C_HOSP_NAME
                    ))
                    i += 2
        record[MD_HOSP_TABLE] = hosp_list

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
#        link_geocode(conn, record, incl_prov=('ON',))

        update_detail_table(conn, cur_CPSO, batch_id=batch_id)
    else:
        print(f'({batch_id}) CPSO: {cur_CPSO} - cannot obtain. '
              f'Response: {response.status_code}')

        if exclude_invalid:
            update_detail_table(conn, cur_CPSO,
                           batch_id=batch_id,
                           perm_exclude=True)
    return [record]


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Connect to MariaDB Platform
    try:
        connect_db = mariadb.connect(
            user="faxcomet_scrape",
            password="NnBgmX$t^+tG",
            host="faxcomet.com",
            port=3306,
            database="faxcomet_MD_list",
            compress=True
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)


    curs = connect_db.cursor()
    # for cpso_no in range(108493,150000): # TEST_CPSO:

    workload = request_workload(connect_db, random=False,
                                batch_size=20,
                                min_val=CPSO_START, max_val=CPSO_STOP)

    while len(workload[1]) > 0:

        batch_no = workload[0]

        for cpso_no in workload[1]:
            all_recs = process_record(connect_db, cpso_no,
                                      batch_id=batch_no)
            update_record(connect_db, all_recs, MD_DIR_TABLE, cpso_no)

            for s in FINAL_SQL:
                curs.execute(s, (cpso_no,))

            connect_db.commit()

        workload = request_workload(connect_db, random=False,
                                    batch_size=20,
                                    min_val=CPSO_START,
                                    max_val=CPSO_STOP)

    connect_db.close()
