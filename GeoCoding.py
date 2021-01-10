import googlemaps
from datetime import datetime
import mariadb
from constants import *
CONST_SQL = 'SELECT CPSO_no, row_uno, address_1, ' \
            'COALESCE(address_2, "") as address_2, ' \
            'COALESCE(address_3, "") as address_3, '\
            'COALESCE(address_4, "") as address_4, city, prov_code, ' \
            'postal_code, ' \
            'postal_code_clean, '\
            'country FROM MD_addresses ' \
            'WHERE cpso_no BETWEEN 100100 AND 100125 ' \
            'AND address_1 <> "Practice Address Not Available"'
API_KEY = 'AIzaSyBkKoxxJxWNpPluVYD0HRt3ya05HctSTn4'


def get_geocode(in_addr: str):
    pass


def link_geocode_all(conn_db: 'connection', record: list,
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

    connect_db.close()
"""
gmaps = googlemaps.Client(key='AIzaSyBkKoxxJxWNpPluVYD0HRt3ya05HctSTn4')

# Geocoding an address
geocode_result = gmaps.geocode('9651 Yonge Street ,Richmond Hill, ON')

print(geocode_result[0]['geometry']['location'])

"""
