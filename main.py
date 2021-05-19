# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import json
from libCymruProcessor import teamCymruJSON

from pprint import pprint

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
   #dataReader=open('/Users/darrellmiller/fusionData/CymruData/Malwar3Ninja.json',"r")

   count=1

   # builds python dictionary from team cymru "Export as full query as JSON"
   jsonDataDict={}
   file='Adam_LPSS.json'
   file2='Malwar3Ninja.json'
   with open(('/Users/darrellmiller/fusionData/CymruData/'+ file)) as fp:
      query_name=file
      Lines = fp.readlines()
      for line in Lines:
         count += 1
         jsonData=json.loads(line)
         jsonData['query_name']=query_name
         #print (jsonData['query_type'])
         if jsonData['query_type'] in jsonDataDict.keys(): #this datatype already in master dictionary
            jsonDataDict[jsonData['query_type']].append(jsonData.copy())
         else: #new data type, create an array for data
            print ("Creating New Data type in Master Dictionary:", jsonData['query_type'])
            jsonDataDict[jsonData['query_type']]=[]
            jsonDataDict[jsonData['query_type']].append(jsonData)

      teamCymruJSON_Obj=teamCymruJSON(jsonDataDict)

      # print ("Keys:", jsonDataDict.keys())
      # for qtype in jsonDataDict.keys():
      #    print (qtype,"::", len(jsonDataDict[qtype]))


