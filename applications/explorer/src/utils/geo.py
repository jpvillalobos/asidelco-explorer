from geopy.geocoders import Nominatim
geo = Nominatim(user_agent="my-cr-geocoder")
loc = geo.geocode("Grecia, Alajuela, Costa Rica", country_codes="cr")
print(loc.latitude, loc.longitude)
