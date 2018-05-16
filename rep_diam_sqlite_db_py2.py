#!/usr/bin/env python

from pygraylog.graylogapi import GraylogAPI
import sys
import json
import pandas as pd
from pandas import DataFrame
import numpy as np
import datetime as dt
import sqlite3

version = '0.0.1'

#Variables

log_level = 0
rep_interval = 24 #hours
days_back = 1 #days
period = 1 #number of intervals
db_path = '/opt/sqlite/nsstat.sqlite'

#Functions

graylog_con={
        "user":"merger",
        "password":"pc@pM8rger",
        "url":"http://graylog-ui.roamability.com:9000/api",
        "offset":0,
        "limit":1
    }

api = GraylogAPI(
    graylog_con["url"],
    graylog_con["user"], 
    graylog_con["password"])

def outLog(message, level=1):
    if(level <= log_level):
        print (message)
        sys.stdout.flush()

def LoadGraylogResult(exportsearch_results):
        jsonResult = json.loads(exportsearch_results)
        outputResult = list()
        i=0
        if "total_results" in jsonResult:
            outLog("Graylog search found {} messages".format(jsonResult["total_results"]),0)
        # Get the results and store them
            for result in jsonResult["messages"]:  
                if "message" in  result and isinstance(result["message"], dict):
                    outputResult.append(result["message"])
                    i+=1
                    if i%100==0: outLog("loaded {} records from Graylog".format(i),0)
            outLog("Graylog search load {} messages".format(len(outputResult)),0)
        else:
            outLog("Graylog search error: {}".format(exportsearch_results),0)
        return outputResult
    
#Prepare a report

start_time = (dt.datetime.now()-dt.timedelta(days=days_back))
start_time=start_time.strftime("%m/%d/%Y 00:00:00")
date_range=pd.date_range(start=start_time,periods=period,freq=str(rep_interval) + 'H')

res_diam=np.array([[0,1,2,3]])

for d in date_range:
    dateFrom = dt.datetime.strptime(d.strftime("%Y-%m-%dT%H:%M"), '%Y-%m-%dT%H:%M')
    dateTo = dt.datetime.strptime((d+dt.timedelta(hours=rep_interval)).strftime("%Y-%m-%dT%H:%M"), '%Y-%m-%dT%H:%M')

    for rb in ['176.10.116.194','5.148.189.250']:
        for cf in ['195.211.12.98','91.221.82.98']:
            query='protocol:diameter AND ip_src:{} AND ip_dst:{}'.format(rb,cf)
            gres=api.search.universal.absolute.get(query=query, from_=dateFrom, to=dateTo, offset=graylog_con["offset"], limit=graylog_con["limit"])
            jsonResult = json.loads(gres)
            query_res=jsonResult["total_results"]
            temp=np.array([[dateTo,rb,cf,query_res]])
            res_diam=np.concatenate([res_diam,temp],axis=0)
            
            query='protocol:diameter AND ip_src:{} AND ip_dst:{}'.format(cf,rb)
            gres=api.search.universal.absolute.get(query=query, from_=dateFrom, to=dateTo, offset=graylog_con["offset"], limit=graylog_con["limit"])
            jsonResult = json.loads(gres)
            query_res=jsonResult["total_results"]
            temp=np.array([[dateTo,cf,rb,query_res]])
            res_diam=np.concatenate([res_diam,temp],axis=0)

res_diam=DataFrame(res_diam[1:],columns=['Rep_end_time','IP_SRC','IP_DST','MSG'])

conn = sqlite3.connect(db_path)

#---For the fist time to create the table
#cur = conn.cursor()
#cur.execute('DROP TABLE IF EXISTS REP_DIAM_MSG ')
#cur.execute('CREATE TABLE REP_DIAM_MSG (REP_END_TIME TEXT, IP_SRC TEXT, IP_DST TEXT, MSG INTEGER)')
#---

res_diam.to_sql(name='REP_DIAM_MSG', con=conn, if_exists = 'append', index=False)
conn.commit()
conn.close()