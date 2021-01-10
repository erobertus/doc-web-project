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
            "	AND a.address_1 <> 'Practice Address Not Available'\n" \
            "GROUP BY a.postal_code_clean\n" \
            "HAVING COUNT(*) > 4"

API_KEY = 'AIzaSyBkKoxxJxWNpPluVYD0HRt3ya05HctSTn4'

def get_geocode(in_addr: str):
    pass


def link_geocode_all(conn_db: 'connection',
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

    save_commit_state = conn_db.autocommit
    conn_db.autocommit = False

    curs = conn_db.cursor()
    curs.execute(BEGIN_TRAN)

    gmaps = googlemaps.Client(key=API_KEY)

    curs.execute(CONST_SQL)
    result_set = ((postal, cnt, addr, prov, cntry) for
                  (postal, cnt, addr, prov, cntry) in curs)

    for (postal, cnt, addr, prov, cntry) \
            in result_set:
        # get if postal already stored

        stmt = f'SELECT geo_uno `cnt` FROM {GEO_TABLE} ' \
               f'WHERE postal_code = ? and country = ?'
        curs.execute(stmt, (prov, cntry))

        geo_unos = []
        if curs.rowcount > 0:
            geo_unos = [x for (x,) in curs]


        do_geocode = len(geo_unos) == 0

        if addr != NO_ADDR and do_geocode:

            geocode_result = gmaps.geocode(addr)

            g_streets = []
            for geocode in geocode_result:
                for addr_part in geocode[ADDR_COMPONENTS]:
                    for types in addr_part[TYPES]:
                        if types == STR_NO:
                            g_str_no = addr_part[SHORT_NAME]
                        elif types == STR_NAME:
                            g_streets.append(addr_part[SHORT_NAME])
                        elif types == CITY_NAME:
                            g_city = addr_part[SHORT_NAME]
                        elif types == COUNTY_NAME:
                            g_county = addr_part[SHORT_NAME]
                        elif types == PROV_NAME:
                            g_prov = addr_part[SHORT_NAME]
                        elif types == COUNTRY_NAME:
                            g_country = addr_part[LONG_NAME]
                        elif types == POSTAL_NAME:
                            g_postal = addr_part[SHORT_NAME]
                for i in range(len(g_streets),4):
                    g_streets.append(BLANK)

                lat = geocode[GEOM][LOC][LAT]
                lng = geocode[GEOM][LOC][LNG]

                # INSERT GEOCODE
                stmt = f'INSERT INTO {GEO_TABLE} (\n' \
                       f'   lattitude, longitude, ' \
                       f'   geo_point, ' \
                       f'   address_1, address_2, ' \
                       f'address_3, address_4, \n' \
                       f'   street_no, street_name, ' \
                       f'city, prov_code, ' \
                       f'   county, postal_code, ' \
                       f'postal_code_clean, ' \
                       f'   country)\n' \
                       f'VALUES (\n' \
                       f'   ?, ?, ' \
                       'POINTFROMTEXT(' \
                       'CONCAT("POINT(",?, " ", ?, ")")),' \
                       f' ?, ?, ?, ?,\n' \
                       f'   ?, ?, ?, ?, ?, ?, ?, ?)'
                v = (lat, lng, str(lat), str(lng))
                v += tuple([x for x in g_streets])
                v += (g_str_no, g_streets[0], g_city, g_prov, g_county)
                v += (g_postal, g_postal.replace(' ', ''), g_country)

                curs.execute(stmt, v)
                geo_unos.append(curs.lastrowid)

    curs.execute(COMMIT_TRAN)
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

    link_geocode_all(connect_db)


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
