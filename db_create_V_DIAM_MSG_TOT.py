#!/usr/bin/env python

import sqlite3
#db_path = '../Documents/GITLAB/RB_BD/DATA/test.sqlite'
db_path = '/opt/sqlite/nsstat.sqlite'
conn = sqlite3.connect(db_path)

cur = conn.cursor()
cur.execute('DROP VIEW IF EXISTS V_DIAM_MSG_TOT')
cur.execute('''CREATE VIEW V_DIAM_MSG_TOT
AS
SELECT DATE(REP_END_TIME,'-1 day') AS REP_DATE,'COMFONE' AS DEST,
SUM(CASE WHEN IP_DST IN ('176.10.116.194','5.148.189.250') THEN MSG END )'MSG_IN',
SUM(CASE WHEN IP_SRC IN ('176.10.116.194','5.148.189.250') THEN MSG END )'MSG_OUT'
FROM REP_DIAM_MSG
GROUP BY REP_END_TIME
ORDER BY REP_END_TIME DESC''')

conn.commit()
conn.close()