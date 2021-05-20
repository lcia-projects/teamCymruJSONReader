import geoip2.database
import pygeoip

import time

class getGeoIP:
    def __init__(self):
        self.reader = geoip2.database.Reader('./geoip/GeoLite2-City.mmdb')

    def getGeoIPCity(self,ip):

        responseData={}
        try:
            response = self.reader.city(ip)
            responseData['country_code'] = response.country.iso_code
            responseData['most_specific.name'] = response.subdivisions.most_specific.name
            responseData['most_specific.iso_code'] = response.subdivisions.most_specific.iso_code
            responseData['response.city.name'] = response.city.name
            responseData['postal.code'] = response.postal.code
            responseData['location']={}
            responseData['location']['lat'] = response.location.latitude
            responseData['location']['lon'] = response.location.longitude
            responseData['country.name'] = response.country.name
            return responseData.copy()
        except:
            responseData=None
            return responseData

    def __del__(self):
        self.reader.close()