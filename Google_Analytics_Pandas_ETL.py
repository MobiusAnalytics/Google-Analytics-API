"""A simple example of how to access the Google Analytics API."""
""" Use pip install pyparsing==2.4.7 """
try:
  from apiclient.discovery import build
except:
  from googleapiclient.discovery import build #pip install google-api-python-client
from oauth2client.service_account import ServiceAccountCredentials #pip install oauth2client==1.5.2
import pandas as pd
import csv
import sys
import numpy as np
import argparse
import sys
import csv
import re
import os
import string
import mysql.connector as sql
import json
from datetime import datetime
import time

def get_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.

    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.

    Returns:
        A service that is connected to the specified API.
    """

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=scopes)
    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)
    return service
  
def get_results(service, profile_id,start_date,end_date):
    start_time = datetime.now()
    GA_data = []
    prev_data={}
    pag_index=0
    stop_page=100
    for pag_index in range(0,stop_page):
      #print(f"Processing {pag_index+1}...")
      newData=service.data().ga().get(
              ids='ga:228140852',
              start_date=start_date,
              end_date=end_date,
              dimensions = 'ga:clientId,ga:pagePath,ga:sourceMedium,ga:date,ga:dateHour',
              metrics='ga:avgTimeOnPage,ga:sessionsPerUser,ga:sessionDuration,ga:pageviews,ga:users',
              #filters='ga:dimension2!~0',
              start_index=str(pag_index*5000+1),
              max_results=str(pag_index*5000+5000)).execute()
      GA_data.append(newData)
      if newData.get("totalResults") < 5000: # to find the stop page and exit from navigating page
        break
    end_time=datetime.now()
    return {"data":GA_data,"start_time":start_time,"end_time":end_time}
        
def print_results(results,date):
  start_time = datetime.now()
  header = [h['name'][3:] for h in results[0].get('columnHeaders')]
  df_temp=pd.DataFrame()
  for i in results:
    df = pd.DataFrame(i.get('rows'), columns = header)
    try:
      if not df_temp.empty:
        df_temp = pd.concat([df,df_temp])
      else:
        df_temp = df
    except AttributeError:
        pass
  end_time= datetime.now()
  df_temp.to_csv(r'Output/GA_Report_Output_{}.csv'.format(re.sub("\_+","_",re.sub("\W","_",date,flags=re.I|re.M))))
  return {"start_time":start_time,"end_time":end_time}
    
def main():
    if not os.path.exists("output"):
      os.mkdir("output")
    # Define the auth scopes to request.
    scope = 'https://www.googleapis.com/auth/analytics.readonly'
    key_file_location = 'mobius-ga-api-f9d768fd43fc.json'
        
    # Authenticate and construct service.
    service = get_service(
            api_name='analytics',
            api_version='v3',
            scopes=[scope],
            key_file_location=key_file_location)

    profile_id = '228140852' #get_first_profile_id(service) #view id
    etlm=open("ETL_Time_Complexity_Monthly.txt","w",encoding="utf-8")
    with open("ETL_Time_Complexity_Daily.txt","w",encoding="utf-8") as etld:
      etld.write("\t\tExtraction\t\t\tTransformation\n")
    with open("ETL_Time_Complexity_Daily.txt","a",encoding="utf-8") as etld:
      etld.write("Date\tStart time\tEnd time\tTotal time\tStart time\tEnd time\tTotal time\tComplete time taken\n")
    dateRange= pd.date_range(start ='2021-01-01',end ='2021-01-31', freq ='24H')
    etld=open("ETL_Time_Complexity_Daily.txt","a",encoding="utf-8")
    start_time = datetime.now()
    for date in dateRange:
      date=datetime.strftime(date,"%Y-%m-%d")
      print(date)
      extraction=get_results(service,profile_id,date,date)
      transform=print_results(extraction['data'],date)
      etld.write(f"{date}\t{extraction['start_time']}\t{extraction['end_time']}\t{(extraction['end_time']-extraction['start_time']).total_seconds()}\t{transform['start_time']}\t{transform['end_time']}\t{(transform['end_time']-transform['start_time']).total_seconds()}\t{(extraction['end_time']-extraction['start_time']).total_seconds()+(transform['end_time']-transform['start_time']).total_seconds()}\n")
    end_time=datetime.now()
    etld.close()
    etlm.write("Start Time\tEnd Time\tTotal Time Taken in Seconds\n{}\t{}\t{}\n".format(start_time,end_time,(end_time-start_time).total_seconds() ))
    etlm.close()
 
main()  

