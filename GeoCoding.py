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


    outer_curs = conn_db.cursor()
    gmaps = googlemaps.Client(key=API_KEY)

    outer_curs.execute(CONST_SQL)

    for (postal_code, cnt, cur_addr, prov, cntry) in outer_curs:
        # get if postal already stored
        curs = conn_db.cursor()
        stmt = f'SELECT geo_uno `cnt` FROM {GEO_TABLE} ' \
               f'WHERE postal_code = ? and country = ?'
        curs.execute(stmt, (prov, cntry))

        if curs.rowcount > 0:
            geo_unos = [x for (x,) in curs]


        do_geocode = len(geo_unos) > 0

        if cur_addr != NO_ADDR and do_geocode:

            geocode_result = gmaps.geocode(cur_addr)

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
        curs.close()
    conn_db.commit()


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
