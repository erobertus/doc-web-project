import googlemaps
from datetime import datetime
import mariadb
from constants import *
CONST_SQL = "SELECT a.postal_code, COUNT(*) `cnt`,	\n" \
            "CONCAT(a.address_1,\n" \
            "IF(COALESCE(a.address_2, '') <> '', " \
            "CONCAT(', ', a.address_2), ''),\n" \
            "IF(COALESCE(a.address_3, '') <> '', " \
            "CONCAT(', ', a.address_3), ''),\n" \
            "IF(COALESCE(a.address_4, '') <> '', " \
            "CONCAT(', ', a.address_4), ''),\n" \
            "', ', a.city, ' ', a.prov_code, '  ', " \
            "a.postal_code, ', ', a.country\n" \
            ") `full_address`, a.prov_code, a.country\n" \
            "FROM MD_addresses a\n" \
            "WHERE a.postal_code IS NOT NULL\n" \
            "	AND a.prov_code = 'ON'\n" \
            "	AND a.country = 'Canada'\n" \
            f"	AND a.address_1 <> '{NO_ADDR}'\n" \
            "GROUP BY a.postal_code_clean\n" \
            "HAVING COUNT(*) > 1"

MD_SQL = "SELECT a.postal_code, a.row_uno `addr_uno`,\n" \
         "	CONCAT(a.address_1,\n" \
         "	IF(COALESCE(a.address_2, '') <> '', " \
         "CONCAT(', ', a.address_2), ''),\n" \
         "	IF(COALESCE(a.address_3, '') <> '', " \
         "CONCAT(', ', a.address_3), ''),\n" \
         "	IF(COALESCE(a.address_4, '') <> '', " \
         "CONCAT(', ', a.address_4), ''),\n" \
         "	', ', a.city, ' ', a.prov_code, '  ', a.postal_code, " \
         "', ', a.country) `full_address`, a.prov_code, a.country\n" \
         "FROM z847e_MD_dir d\n" \
         "JOIN MD_addresses a ON d.CPSO_no = a.CPSO_no\n" \
         "LEFT JOIN MD_addr_x_geo ag ON a.row_uno = ag.addr_uno\n" \
         "LEFT JOIN MD_geo_pos gp ON ag.geo_uno = gp.geo_uno\n" \
         "WHERE d.reg_stat_code = 1\n" \
         f"AND a.address_1 <> '{NO_ADDR}'\n" \
         "AND a.country = 'Canada'\n" \
         "AND a.prov_code = 'ON'\n" \
         "-- AND a.postal_code IS NOT NULL\n" \
         "AND (ag.row_uno IS NULL)\n" \
         "ORDER BY d.CPSO_no"

LINK_POSTAL = f'INSERT INTO {ADDR_X_GEO_TABLE} (addr_uno, geo_uno)\n' \
               'SELECT a.row_uno, gp.geo_uno\n' \
               'FROM MD_geo_pos gp\n' \
               '	JOIN MD_addresses a ' \
               'ON gp.user_postal_code = a.postal_code\n' \
               '	LEFT JOIN MD_addr_x_geo ag ' \
               'ON a.row_uno = ag.addr_uno ' \
               'AND gp.geo_uno = ag.geo_uno\n' \
               'WHERE gp.user_postal_code = ?\n' \
               '	AND ag.row_uno IS NULL'

LINK_BY_UNO = f'INSERT INTO {ADDR_X_GEO_TABLE} ' \
              f'(addr_uno, geo_uno)\n' \
              f'VALUES (?, ?)'

API_KEY = 'AIzaSyBkKoxxJxWNpPluVYD0HRt3ya05HctSTn4'

def get_geocode(in_addr: str):
    pass


def link_geocode_all(conn_db: 'connection',
                     incl_prov=(ALL,), incl_cntry=(ALL,),
                     use_postal=True):
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

    save_commit_state = conn_db.autocommit
    conn_db.autocommit = False

    curs = conn_db.cursor()


    gmaps = googlemaps.Client(key=API_KEY)

    if use_postal:
        stmt = CONST_SQL
    else:
        stmt = MD_SQL
    curs.execute(stmt)
    result_set = [(postal, cnt_or_id, addr, prov, cntry) for
                  (postal, cnt_or_id, addr, prov, cntry) in curs]
    iteration = 1
    google_calls = 0
    total_iterations = len(result_set)
    for (postal, cnt_or_id, addr, prov, cntry) \
            in result_set:
        print(f"{iteration}(G:{google_calls})/{total_iterations}: "
              f"=== Checking address: ", addr)
        # get if postal already stored
        curs.execute(BEGIN_TRAN)
        stmt = f'SELECT geo_uno FROM {GEO_TABLE} ' \
               f'WHERE user_postal_code = ? and prov_code = ? ' \
               f'and country = ? FOR UPDATE'
        curs.execute(stmt, (postal, prov, cntry))

        geo_unos = [x for (x,) in curs]

        do_geocode = len(geo_unos) == 0

        if addr != NO_ADDR and do_geocode:
            print("\t...not found, asking Google...", end='')
            geocode_result = gmaps.geocode(addr)
            google_calls += 1
            s = addr

            while len(geocode_result) == 0:
                # try to reformat address and make sens e of it
                s = s.split(DELIM_COMMA)
                if len(s) > 1:
                    s = DELIM_COMMA.join(s[1:])
                else:
                    break
                print("Hmmmm....Trying to make sense of address:")
                print(f'Maybe this will be better: {s}')
                geocode_result = gmaps.geocode(s)
                google_calls += 1

            g_data = {}
            for geocode in geocode_result:
                for type in (STR_NO, STR_NAME, CITY_NAME,
                                     COUNTY_NAME, PROV_NAME,
                                     POSTAL_NAME, COUNTRY_NAME):
                    g_data[type] = ''
                for addr_part in geocode[ADDR_COMPONENTS]:
                    for types in addr_part[TYPES]:
                        if types in (STR_NO, STR_NAME, CITY_NAME,
                                     COUNTY_NAME, PROV_NAME,
                                     POSTAL_NAME):
                            g_data[types] = addr_part[SHORT_NAME]
                        elif types in (COUNTRY_NAME,):
                            g_data[types] = addr_part[LONG_NAME]


                if len(g_data[POSTAL_NAME]) < 7:
                    # presume priority of user data over Google
                    g_data[POSTAL_NAME] = postal

                g_streets = [g_data[STR_NAME]]

                for i in range(len(g_streets),4):
                    g_streets.append(BLANK)

                lat = geocode[GEOM][LOC][LAT]
                lng = geocode[GEOM][LOC][LNG]
                print(f' --> \t\t Lat: {lat}, Lng: {lng}')
                # INSERT GEOCODE
                stmt = f'INSERT INTO {GEO_TABLE} (\n' \
                       f'   lattitude, longitude, ' \
                       f'   geo_point, ' \
                       f'   address_1, address_2, ' \
                       f'address_3, address_4, \n' \
                       f'   street_no, street_name, ' \
                       f'city, prov_code, ' \
                       f'   county, postal_code, ' \
                       f'user_postal_code, postal_code_clean, ' \
                       f'   country)\n' \
                       f'VALUES (\n' \
                       f'   ?, ?, ' \
                       'POINTFROMTEXT(' \
                       'CONCAT("POINT(",?, " ", ?, ")")),' \
                       f' ?, ?, ?, ?,\n' \
                       f'   ?, ?, ?, ?, ?,?, ?, ?, ?)'
                v = (lat, lng, str(lat), str(lng))
                v += tuple([x for x in g_streets])
                v += (g_data[STR_NO], g_data[STR_NAME],
                      g_data[CITY_NAME], g_data[PROV_NAME],
                      g_data[COUNTY_NAME])
                v += (g_data[POSTAL_NAME], postal,
                      g_data[POSTAL_NAME].replace(' ', ''),
                      g_data[COUNTRY_NAME])

                curs.execute(stmt, v)
                geo_unos.append(curs.lastrowid)
                print(f"\tGeo table {GEO_TABLE} updated with ID {curs.lastrowid}")
        # GET LIST OF ADDRESS UNOS

        print(f'...Updating link table {ADDR_X_GEO_TABLE}... ', end='')
        if use_postal:
            stmt = LINK_POSTAL
            curs.execute(stmt, (postal,))
        else:
            stmt = LINK_BY_UNO
            val_t = [(cnt_or_id, x) for x in geo_unos]
            curs.executemany(stmt, val_t)

        print(f'{curs.rowcount} row(s) affected.')
        curs.execute(COMMIT_TRAN)
        iteration += 1
    # conn_db.commit()
    conn_db.autocommit = save_commit_state


if __name__ == '__main__':
    gmaps = googlemaps.Client(key=API_KEY)
    # Connect to MariaDB Platform
    try:
        connect_db = mariadb.connect(
            user="faxcomet_scrape",
            password="NnBgmX$t^+tG",
            host="faxcomet.com",
            port=3306,
            database="faxcomet_MD_list"
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")

    link_geocode_all(connect_db, use_postal=False)


    """
    stmt = "UPDATE MD_addresses SET lat = ?, lng = ?, " \
           "point_coordinates = " \
           'POINTFROMTEXT(CONCAT("POINT(", ?, " ", ?, ")")) ' \
           "WHERE row_uno = ?"


        # save_commit_state = connect_db.autocommit
    # connect_db.autocommit = False

    curs = connect_db.cursor()
    curs2 = connect_db.cursor()
    curs.execute(CONST_SQL)
    l = []
    for (CPSO_no, row_uno, address_1, address_2, address_3,
         address_4, city, prov_code, postal_code, postal_code_clean,
         country) in curs:
        s = f'{address_1} {address_2} {address_3} {address_4}, {city} ' \
            f'{prov_code}  {postal_code}, {country}'

        s = 'Postgraduate Medical Education Queen\'s University, 70 ' \
            'Barrie Street, Kingston, ON  K7L 3N6'

        print(s)

        # Geocoding an address
        geocode_result = gmaps.geocode(s)
        print(geocode_result[0]['geometry']['location'])
        lattitude = geocode_result[0]['geometry']['location']['lat']
        longitude = geocode_result[0]['geometry']['location']['lng']
        l.append({'lat': lattitude, 'lng': longitude, 'uno': row_uno})

    # connect_db.commit()

    for location in l:
        curs.execute(stmt, (location['lat'], location['lng'],
                             location['lat'], location['lng'],
                             location['uno']))

    connect_db.commit()

    # connect_db.autocommit = save_commit_state

    """
    connect_db.close()
"""
gmaps = googlemaps.Client(key='AIzaSyBkKoxxJxWNpPluVYD0HRt3ya05HctSTn4')

# Geocoding an address
geocode_result = gmaps.geocode('9651 Yonge Street ,Richmond Hill, ON')

print(geocode_result[0]['geometry']['location'])

"""
