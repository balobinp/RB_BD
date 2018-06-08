#!/usr/bin/env python

import sqlite3
db_path = '/opt/sqlite/nsstat.sqlite'
conn = sqlite3.connect(db_path)

cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS DICT_SS7_PC')
cur.execute('''
CREATE TABLE DICT_SS7_PC (DEST_NAME TEXT,PC_NAME TEXT,PC INTEGER)
''')
cur.execute('''
INSERT INTO DICT_SS7_PC (DEST_NAME,PC_NAME, PC)
VALUES
('OWN','RB_STP1',2505),
('OWN','RB_STP2',2506),
('OWN','RB_STP2',700),
('OWN','RB_STP',200),
('OWN','RB_STP1',1093),
('OWN','RB_STP2',1094),
('OWN','RB_STP1',2000),
('OWN','RB_STP2',2001),
('OWN','RB_STP1',40),
('OWN','RB_STP',800),
('COMFONE','COMFONE_STP1',1961),
('COMFONE','COMFONE_STP2',1962),
('LAB','LAB_STP',40),
('DMI','DMI_STP',199),
('C9','C9_STP',16281),
('MAXCOM','MAXCOM_STP',6009),
('CITICTEL','CITICTEL_STP',200),
('NAKA','NAKA_STP1',781),
('NAKA','NAKA_STP2',782),
('TELZAR','TELZAR_STP1',7138)
''')

conn.commit()
conn.close()