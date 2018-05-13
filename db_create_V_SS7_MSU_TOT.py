#!/usr/bin/env python

import sqlite3
#db_path = '../Documents/GITLAB/RB_BD/DATA/test.sqlite'
db_path = '/opt/sqlite/nsstat.sqlite'
conn = sqlite3.connect(db_path)

cur = conn.cursor()
cur.execute('DROP VIEW IF EXISTS V_SS7_MSU_TOT ')
cur.execute('''CREATE VIEW V_SS7_MSU_TOT
AS
SELECT DATE(REP_END_TIME,'-1 day') AS REP_DATE,'COMFONE' AS DEST,
SUM(CASE WHEN OPC IN (1961,1962) THEN MSU END )'MSU_IN',
SUM(CASE WHEN DPC IN (1961,1962) THEN MSU END) 'MSU_OUT'
FROM REP_SS7_MSU
GROUP BY REP_END_TIME
ORDER BY REP_END_TIME DESC''')

conn.commit()
conn.close()