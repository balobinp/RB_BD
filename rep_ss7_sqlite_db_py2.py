#!/usr/bin/env python

from pygraylog.graylogapi import GraylogAPI
import sys
import json
import pandas as pd
from pandas import DataFrame
import numpy as np
import datetime as dt
import sqlite3

version = '0.0.2'

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
start_time = start_time.strftime("%m/%d/%Y 00:00:00")
date_range = pd.date_range(start=start_time,periods=period,freq=str(rep_interval) + 'H')

res_ss7=np.array([[0,1,2,3]])

conn = sqlite3.connect(db_path)
cur = conn.cursor()

sql_str = "SELECT DISTINCT PC FROM DICT_SS7_PC WHERE DEST_NAME != 'OWN'"
cur.execute(sql_str)
result = cur.fetchall()
dest_pc = [i[0] for i in result]

sql_str = "SELECT PC FROM DICT_SS7_PC WHERE DEST_NAME='OWN'"
cur.execute(sql_str)
result = cur.fetchall()
own_pc = [i[0] for i in result]

for d in date_range:
    dateFrom = dt.datetime.strptime(d.strftime("%Y-%m-%dT%H:%M"), '%Y-%m-%dT%H:%M')
    dateTo = dt.datetime.strptime((d+dt.timedelta(hours=rep_interval)).strftime("%Y-%m-%dT%H:%M"), '%Y-%m-%dT%H:%M')

    for rb in own_pc:
        for cf in dest_pc:
            query='protocol:ss7 AND m3ua_OPC:{} AND m3ua_DPC:{}'.format(rb,cf)
            gres=api.search.universal.absolute.get(query=query, from_=dateFrom, to=dateTo, offset=graylog_con["offset"], limit=graylog_con["limit"])
            jsonResult = json.loads(gres)
            query_res_1=jsonResult["total_results"]
                        
            query='protocol:ss7 AND m3ua_OPC:{} AND m3ua_DPC:{}'.format(cf,rb)
            gres=api.search.universal.absolute.get(query=query, from_=dateFrom, to=dateTo, offset=graylog_con["offset"], limit=graylog_con["limit"])
            jsonResult = json.loads(gres)
            query_res_2=jsonResult["total_results"]
            
            if (query_res_1 + query_res_2) > 0:
                temp=np.array([[dateTo,rb,cf,query_res_1]])
                res_ss7=np.concatenate([res_ss7,temp],axis=0)
                temp=np.array([[dateTo,cf,rb,query_res_2]])
                res_ss7=np.concatenate([res_ss7,temp],axis=0)
    
df_ss7=DataFrame(res_ss7[1:],columns=['Rep_end_time','OPC','DPC','MSU'])

#---
#cur.execute('DROP TABLE IF EXISTS REP_SS7_MSU ')
#cur.execute('CREATE TABLE REP_SS7_MSU (REP_END_TIME TEXT, OPC INTEGER, DPC INTEGER, MSU INTEGER)')
#---

df_ss7.to_sql(name='REP_SS7_MSU', con=conn, if_exists = 'append', index=False)
conn.commit()
conn.close()