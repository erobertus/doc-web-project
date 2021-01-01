# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import urllib.request
import sys
import mechanicalsoup as ms
import re
import mariadb
from constants import *

# NnBgmX$t^+tG


def refresh_ref_from_db(in_db: 'mariadb.connection', table: str,
                         code_col:str, name_col: str) -> dict:
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
        result = curs.lastrowid

        statuses[status] = result

    else:
        # test comment here
        curs.execute(f'SELECT {code_col} as code_col '
                     f'FROM {table} WHERE {name_col} = ?', (status,))
        for (result,) in curs:
            pass
    in_db.commit()
    in_db.autocommit = save_commit_state

    return result

def retrieve_code_from_name(member: str, array: dict,
                            conn: 'connection',
                            table: str, code_col: str,
                            name_col: str) -> int:
    if member not in array:
        result = update_reference(
            member, array, conn, table, code_col, name_col)
    else:
        result = array[member]

    return result

def is_numeric(s: str) -> bool:
    return re.sub('\W|_', '', s).isnumeric()


def processs_address(source: 'bs4.element.ResultSet') -> dict:

    addr_dict = {}
    addr_list = []
    i = 0

    while i < len(source):
        addr_list = [s for s in source[i].stripped_strings]

        j = 0
        at_top_of_addr = True

        while j < len(addr_list):
            if at_top_of_addr:
                # init vars
                streets = []
                addr_line = 1
                streets_filled = False

            if addr_list[j].find(POSTAL_SEPARATOR) > -1:
                # we are on the line with city, province, postal
                cur_info = addr_list[j].split(POSTAL_SEPARATOR)
                addr_dict[C_ADDR_CITY] = cur_info[0]
                addr_dict[C_ADDR_PROV] = cur_info[1]
                addr_dict[C_ADDR_POSTAL] = cur_info[3]

                # we are on the line with postal code, so the
                # next one is country - increase j and fill country
                j += 1
                addr_dict[C_ADDR_COUNTRY] = addr_list[j]
                streets_filled = True
            elif addr_list[j] in (PHONE_TAG, FAX_TAG, E_DISTR_TAG):
                if is_numeric(addr_list[j+1]):
                    addr_dict[WEB2DB_MAP[addr_list[j]]] = \
                        addr_list[j+1]
                    j += 1

            if not streets_filled:
                addr_dict[C_ADDR_PREFIX+str(addr_line)] = addr_list[j]
                addr_line += 1
                at_top_of_addr = False

            j += 1

        i += 1

    return addr_dict

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user="faxcomet_scrape",
            password="NnBgmX$t^+tG",
            host="faxcomet.com",
            port=3306,
            database="faxcomet_MD_list"
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

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
    # test = update_reference('test status', db_statuses, conn)

    browser = ms.StatefulBrowser()
    url = 'https://doctors.cpso.on.ca/?search=general'
    browser.open(url)
    # page_html = page.soup
    # form = browser.select_form('form[action="/?search=general"]')
    form = browser.select_form()
    cur_CPSO =  60387
    form[ALL_DR_SEARCH_CHECKMARK] = True
    form[CPSONO_FIELD] = cur_CPSO
    response = browser.submit_selected()
    record = {C_CPSO_NO: cur_CPSO}
    page = response.soup
    name = re.sub('(\r\n|\t|\s)',' ', page.h1.string).strip()
    names = name.split()
    name_dict = {C_LNAME: names[0].strip(','), C_FNAME: names[1],
                 C_MNAME: ' '.join(names[2:])}

    all_info = page.find_all('div', class_=CL_DR_INFO)

    for cur_info in all_info:
        info = [re.sub('[\r\n\t\s]+as of[\r\n\t\s]*', ' as of ', s)
                for s in cur_info.stripped_strings]
        info[1] = info[1].split(' as of ')

        if info[0] == WEB_MEMBER_STAT:
            record[C_REG_STAT_CODE] = retrieve_code_from_name(
                info[1][0], db_statuses, conn,
                REG_STAT_TABLE, C_REG_STAT_CODE, C_REG_STAT_NAME)

            record[C_REG_EFF_DATE] = info[1][1]
        elif info[0] == WEB_EXPIRY_DATE:
            record[C_REG_EXP_DATE] = info[1][0]
        elif info[0] == WEB_CPSO_REG_CL:
            record[C_REG_CLASS_CODE] = retrieve_code_from_name(
                info[1][0], db_reg_classes, conn,
                REG_CLASS_TABLE, C_REG_CLASS_CODE, C_REG_CLASS_NAME)
            record[C_REG_CLASS_DATE] = info[1][1]

        # print(info)

    all_info = page.find_all('div', class_=CL_INFO)

    for cur_info in all_info:
        info = [s for s in cur_info.stripped_strings]
        i = 0
        while i < len(info) - 1:
            if info[i] == WEB_FRMR_NAME:
                record[C_FRMR_NAME] = info[i+1]
                i += 1
            elif info[i] == WEB_GENDER:
                record[C_MD_GENDER] = retrieve_code_from_name(
                    info[i+1], db_genders, conn, GENDER_TABLE,
                    C_GENDER_CODE, C_GENDER_NAME)
                i += 1
            elif info[i] == WEB_LANGUAGES:
                md_languages = re.sub(', +', DELIM_COMMA,
                                      info[i+1]).split(DELIM_COMMA)
                lang_codes = []
                for language in md_languages:
                    lang_codes.append(
                        retrieve_code_from_name(language, db_languages,
                                            conn, LANGUAGE_TABLE,
                                            C_LANG_CODE, C_LANG_NAME)
                    )
                record[MD_LANGUAGES] = lang_codes
                i += 1
            elif info[i] == WEB_UNIVERSITY:
                education = re.sub(', +', DELIM_COMMA,
                                  info[i+1]).split(DELIM_COMMA)
                record[C_UNIV_CODE] = retrieve_code_from_name(
                    education[0], db_universities,
                    conn, UNIV_TABLE, C_UNIV_CODE, C_UNIV_NAME
                )
                record[C_GRAD_YEAR] = education[1]
                i += 1
            elif info[i] == WEB_DATE_OF_DEATH:
                record[C_DATE_OF_DEATH] = info[i+1]
                i += 1
            elif info[i] == WEB_REG_IN_OTHER_JUR:

                jur_codes = []
                for jurisdiction in info[i+2:]:
                    jur_codes.append(retrieve_code_from_name(
                        jurisdiction, db_reg_jurisdic,
                        conn, REG_JUR_TABLE, C_JUR_CODE, C_JUR_NAME
                    ))
                record[MD_REG_JURISDIC] = jur_codes
                i += len(info[i+1:])
            i += 1

    all_info = page.find_all('div', class_=CL_ADD_PR_LOC)
    processs_address(all_info)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
