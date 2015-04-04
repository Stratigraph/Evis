import httplib2
import pprint
import sys
from client import *

from apiclient.discovery import build
from apiclient.errors import HttpError

from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools

import datetime as dt
from collections import defaultdict

import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

def main():
  headers = ["Year", "Actor1Code", "Actor2Code", "EventCode", "QuadClass", "GoldsteinScale", "Actor1Geo_Lat", "Actor1Geo_Long", "Actor2Geo_Lat", "Actor2Geo_Long", "ActionGeo_Long", "ActionGeo_Lat"]

  #, EventCode, QuadCategory, GoldsteinScale, Actor1Geo_Lat, Actor1Geo_Long, Actor2Geo_Lat, Actor2Geo_Long, ActionGeo_Long, ActionGeo_Lat,
  try:
    query_request = bigquery_service.jobs()
    #most edited wikipedia articles
    #query_data = {'query':'SELECT TOP( title, 10) as title, COUNT(*) as revision_count FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;'}
    #most defining events per year since 1979
    query_data = {'query':'''SELECT Year, Actor1Code, Actor2Code, EventCode, QuadClass, GoldsteinScale, Actor1Geo_Lat, Actor1Geo_Long, Actor2Geo_Lat, Actor2Geo_Long, ActionGeo_Long, ActionGeo_Lat, Count FROM ( 
                             SELECT Year, Actor1Code, Actor2Code, EventCode, QuadClass, GoldsteinScale, Actor1Geo_Lat, Actor1Geo_Long, Actor2Geo_Lat, Actor2Geo_Long, ActionGeo_Long, ActionGeo_Lat, COUNT(*) Count, RANK() OVER(PARTITION BY YEAR ORDER BY Count DESC) rank FROM (
                             SELECT Year, Actor1Code, Actor2Code, EventCode, QuadClass, GoldsteinScale, Actor1Geo_Lat, Actor1Geo_Long, Actor2Geo_Lat, Actor2Geo_Long, ActionGeo_Long, ActionGeo_Lat
                             FROM [gdelt-bq:full.events] 
                             WHERE Actor1Code < Actor2Name 
                               and Actor1CountryCode != \'\' 
                               and Actor2CountryCode != \'\' 
                               and Actor1CountryCode!=Actor2CountryCode), (
                             SELECT Actor2Code Actor1Code, Actor1Code Actor2Code, Year, EventCode, QuadClass, GoldsteinScale, Actor1Geo_Lat, Actor1Geo_Long, Actor2Geo_Lat, Actor2Geo_Long, ActionGeo_Long, ActionGeo_Lat
                             FROM [gdelt-bq:full.events] 
                             WHERE Actor1Code > Actor2Code 
                               and Actor1CountryCode != \'\' 
                               and Actor2CountryCode != \'\' 
                               and Actor1CountryCode!=Actor2CountryCode), 
                             WHERE Actor1Code IS NOT null 
                               AND Actor2Code IS NOT null GROUP EACH BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 HAVING Count > 100 ) 
                             WHERE rank=1 ORDER BY Year'''}
    query_response = query_request.query(projectId=PROJECT_NUMBER,
                                         body=query_data).execute()
    #pretty printer
    print 'Query Results:'
    for row in query_response['rows']:
      result_row = []
      for field in row['f']:
        result_row.append(field['v'])
      print ('\t').join(result_row)

  except HttpError as err:
    print 'Error:', pprint.pprint(err.content)

  except AccessTokenRefreshError:
    print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")

if __name__ == '__main__':
  main()