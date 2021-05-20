import time

from elasticsearch import Elasticsearch
from tqdm import tqdm
from datetime import datetime
from libGeoIP import getGeoIP
import geoip2.database
from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing
import dateutil.parser

from pprint import pprint
# todo: bytes sent
# todo: duration
# todo: connections

class teamCymruJSON:
    record_count=0
    total_records=0

    def __init__(self, jsonData):
        print ("object created")
        # for multi-processing
        self.threadCount = (multiprocessing.cpu_count() - 2)
        print("Number of Cores:", self.threadCount)

        self.jsonData = jsonData.copy()
        self.reader = geoip2.database.Reader('./geoip/GeoLite2-City.mmdb')
        #self.processFullJSONData()
        self.processFullJSONData_Multi()

    def processFullJSONData(self):

        geoIP_Obj=getGeoIP()

        for data_type in self.jsonData.keys():
            print ( "Submitting Data Type:", data_type)
            es_index_name="teamcymru_query_"+data_type
            print("index:", es_index_name)
            for item in tqdm(self.jsonData[data_type]):
                item['geo']={}
                for key in item.keys():
                    if "time" in key or "date" in key:
                        item[key] = self.convertDataString(item[key])
                    if "ip_addr" in key:
                        geoKeyName="geo_"+key
                        item['geo'][geoKeyName]=geoIP_Obj.getGeoIPCity(item[key])
                if "start_time" in item.keys():
                    item['timestamp'] = item['start_time']
                self.submitToES(item, es_index_name)

    def processFullJSONData_Multi(self):
        # records start time
        appStartTime = datetime.now()

        # creates multi-processingPool using number of cores calculated
        pool = ThreadPool(self.threadCount)

        # run the multi-processing pool on an array
        #   pool.map(<method to run>, <array of data to run it on>)

        for data_type in self.jsonData.keys():
            self.total_records=len(self.jsonData[data_type])
            with tqdm( total=self.total_records) as self.pbar:
                print("Submitting Data Type:", data_type, " Total Records to Process:", self.total_records)
                # pool.map(self.multiDo, self.jsonData[data_type])
                pool.map(self.multiDo, self.jsonData[data_type])
                self.record_count=0

        # stop the clock
        appEndTime = datetime.now()

        # print results
        print("Records in File:", len(self.dataArray))
        print("Total Time Taken:", (appEndTime - appStartTime))

    def multiDo(self, data):
        geoIP_Obj = getGeoIP()
        data['geo'] = {}
        data['timestamp']="time will go here"
        es_index_name="teamcymru_query_"+data['query_type']

        for key in data.keys():
            if "time" in key or "date" in key:
                try:
                    data[key] = self.convertDataStringParser(data[key])
                except:
                    print ("Time Conversaion Error on:", data[key])
            if "ip_addr" in key:
                geoKeyName = "geo_" + key
                data['geo'][geoKeyName] = self.getGeoIPCity(data[key])
            if "start_time" in data.keys():
                data['timestamp'] = data['start_time']
            #self.record_count+=1
            #print ("Record: [", self.record_count,"/",self.total_records,"]")
        self.pbar.update(1)
        self.submitToES(data, es_index_name)

    def convertDataString(self, dateString):
        # format: 2021-03-10 21:56:44
        date_time_obj = datetime.strptime(dateString, '%Y-%m-%d %H:%M:%S')
        return date_time_obj.strftime('%Y-%m-%dT%H:%M:%S%z')

    def convertDataStringParser(self, dateString):
        # format: 2021-03-10 21:56:44
        date_time_obj= dateutil.parser.parse(dateString)
        #date_time_obj = datetime.strptime(dateString, '%Y-%m-%d %H:%M:%S')
        return date_time_obj.strftime('%Y-%m-%dT%H:%M:%S%z')


    def submitToES(self, data, index_name):
        try:
            es = Elasticsearch(
                ['192.168.1.95'],
                port=9200,
                http_auth=('mike', 'Monday@1')
            )
            es.index(index=index_name, body=data)
        except Exception as ex:
            print("Error:", ex, ":", "\n\n\n\n")

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
