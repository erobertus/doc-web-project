import googlemaps
from datetime import datetime
import mariadb
from constants import *

gmaps = googlemaps.Client(key='AIzaSyBkKoxxJxWNpPluVYD0HRt3ya05HctSTn4')

# Geocoding an address
geocode_result = gmaps.geocode('9651 Yonge Street ,Richmond Hill, ON')

print(geocode_result[0]['geometry']['location'])
# Look up an address with reverse geocoding
reverse_geocode_result = gmaps.reverse_geocode((40.714224, -73.961452))

# Request directions via public transit
now = datetime.now()
directions_result = gmaps.directions("Sydney Town Hall",
                                     "Parramatta, NSW",
                                     mode="transit",
                                     departure_time=now)
