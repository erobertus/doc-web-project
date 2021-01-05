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
