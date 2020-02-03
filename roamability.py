import http
import re
from bs4 import BeautifulSoup
import sys
import os
from os.path import isfile, isdir, join, normpath
import pyodbc # for MS SQL
import mysql.connector # for MySQL. pip install mysql-connector-python-rf
import cx_Oracle

# SendMsu variables

S6A_URL = '172.18.11.90'
S6A_PATH = '/cgi-bin/http.fcgi'
ss7_url = "172.18.11.10"
ss7_path = "/cgi-bin/ss7gw.fcgi"
get_info = "027000001F0D00010000BFFF01000000000000102F978372C730834EF445172BD26B8C8F"
get_info_mo = "027000001F0D00210000BFFF01000000000000102F978372C730834EF445172BD26B8C8F"

switch_01_payload = '027000001F0D00010000BFFF0100000000000010F5375954EF29FB9A7F41ADE8444236A3' #From Denis
#switch_01_payload = '027000001f0d00010000bfff0100000000000010554f212dbbad49a1caf2573e1a626139' #From FSM
switch_02_payload = '027000001F0D00010000BFFF0100000000000040a740AD174703383798A7A6545BB36491' #From FSM

ogt = {'p4':'48790993070',
       'partner':'97254120624',
       'sure':'447797706411',
       'tot':'66893773228',
       'porto_seguro':'550549900000',
       'rusec_rus':'79028710069',
       'rusec_int':'417999880000024',
       'multi_byte_sponsor':'85263347864',
       'smart1':'639180009880',
       'smart2':'639180009881',
       'smart3':'639180009882',
       'smart4':'639180009883',
       'telzar':'972559900040',
       'x2one':'972553316228',
       'cellact':'972557016315',
       'netmore':'46731726312',
       'p4_naka_01':'48790998145',
       'p4_naka_02':'48790998146',
       'p4_naka_03':'48790998147',
       'partner_naka_01':'97254120634',
       'partner_naka_02':'97254120635',
       'partner_naka_03':'97254120636',
       'partner_naka_04':'97254120637',
       'mb':'852633477591',
       'maxcom':'525575709019'}

# NRT CDR variables

nrt_cdr_ver4_col = ['CDR_ID',
            'USAGE_TIMESTAMP',
            'Subscriber ID',
            'IMSI',
            'ICCID',
            'MSISDN',
            'Account ID',
            'Account Name',
            'USAGE_TYPE_ID',
            'USAGE_TYPE_DESCRIPTION',
            'MCC',
            'MNC',
            'Network Name',
            'Country Name',
            'Tadig Code',
            'Destination phone number',
            'Session ID',
            'APN',
            'RAT (RadioAccessType)',
            'IMEI',
            'Download Bitrate',
            'Upload Bitrate',
            'QUANTITY',
            'QUANTITY_DESCRIPTION',
            'COST',
]

nrt_cdr_ver3_col = ['CDR_ID',
            'USAGE_TIMESTAMP',
            'Subscriber ID',
            'IMSI',
            'ICCID',
            'MSISDN',
            'Account ID',
            'Account Name',
            'USAGE_TYPE_ID',
            'USAGE_TYPE_DESCRIPTION',
            'MCC',
            'MNC',
            'Network Name',
            'Country Name',
            'Destination phone number',
            'Session ID',
            'QUANTITY',
            'QUANTITY_DESCRIPTION',
            'COST',
]

# Greetings function

def greetings_func():
    """Test function to say Greetings from Roamability!"""
    print('Greetings from Roamability!!!', '\n')
    return None


# DataBase connections Classes

class MySqlConnect:
    """
    #This method is to connect to MySQL DB.
    #Usage example. Connect to OCSDBREP1 (BSS):
    sql_srt='SELECT MSISDN, VisitedNetworkTadig FROM TAP.GPRS_CALL LIMIT 5'
    with rb.MySqlConnect('172.18.11.40', 'BSS', 'noc', 'WcQUzkXiXwoxnFfGnRxb') as cnxn:
        df = pd.read_sql_query(sql_srt, cnxn)
    """

    def __init__(self, host, db, uid, pwd):
        self.user = uid
        self.password = pwd
        self.host = host
        self.database = db
        
    def __enter__(self):
        self.cnxn = mysql.connector.connect(user=self.user, password=self.password, host=self.host, database=self.database)
        return self.cnxn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cnxn:
            self.cnxn.close()


class MssqlConnect:
    """
    #This method is to connect to MS SQL DB.
    #Usage example. Connect to OCSDBREP1 (BSS):
    sql_srt='SELECT TOP(5) * FROM USAGE_TYPE;'
    with rb.MssqlConnect('172.18.11.82', '10028', 'BSS', 'iKQVm40AZAmyRaw72LeY') as cnxn:
        df = pd.read_sql_query(sql_srt, cnxn, coerce_float=False)
    """
    
    def __init__(self, server, db, uid, pwd):
        self.server = server
        self.db = db
        self.uid = uid
        self.pwd = pwd
        
    def __enter__(self):
        con_str = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.db};UID={self.uid};PWD={self.pwd}'
        self.cnxn = pyodbc.connect(con_str)
        return self.cnxn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cnxn:
            self.cnxn.close()
			

class OracleConnect:
    """
    #This method is to connect to Oracle DB.
    #Usage example. Connect to DMI Oracle DB:
    sql_srt = 'SELECT COUNT(*) FROM s_imsi si'
    with OracleConnect('DMI', 'dd607605ce341', 'DMI_TEST') as cnxn:
        df = pd.read_sql_query(sql_srt, cnxn, coerce_float=False)
    """
    
    def __init__(self, uid, pwd, sid):
        self.sid = sid
        self.uid = uid
        self.pwd = pwd
        
    def __enter__(self):
        con_str = f'{self.uid}/{self.pwd}@{self.sid}'
        self.cnxn = cx_Oracle.connect(con_str)
        return self.cnxn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cnxn:
            self.cnxn.close()
	
# Search in files by Tags functions

def find_files_by_tags(paths, all_tags, any_tags):
    """
    This function search given tags in given folders and print out the description.
    paths parameter - Список файлов, в которых нужно искать файлы с расширением .txt
    any_tags parameter - Если хотя бы один из тэгов присутствует. Нужно указать минимум один.
    all_tags parameter - Если все теги присутствуют. Нужно оставить лист пустым [] если нет обязательных тэгов.
    """
    for path in paths:
        files, folders = find_txt_files_and_folders(path)
        search_tags_in_file(path, files, all_tags, any_tags)

        for folder in folders:
            folder_path = join(path, folder)
            files, _ = find_txt_files_and_folders(folder_path)
            search_tags_in_file(folder_path, files, all_tags, any_tags)

def find_txt_files_and_folders(path_name):
    file_names_all = os.listdir(path_name)
    files = [file for file in file_names_all if isfile(join(path_name, file)) and file.find('.txt') != -1]
    folders = [folder for folder in file_names_all if isdir(join(path_name, folder))]
    return files, folders


def search_tags_in_file(path_name, files_list, all_tags, any_tags):
    for file in files_list:
        #print(f"{normpath(join(path_name, file))}")
        with open(join(path_name, file)) as inf:
            # Преобразовать файл в список. В качествер разделителя пробел. read читает весь файл.
            words_in_file = inf.read().lower().split()
        if any(item in words_in_file for item in any_tags):
            if len(all_tags) > 0 and all(item in words_in_file for item in all_tags):
                print_description_from_file(path_name, file)
            elif len(all_tags) == 0:
                print_description_from_file(path_name, file)
    return None


def print_description_from_file(path_name,file_name):
    print(100*'*')
    print(normpath(join(path_name, file_name)))
    print(100*'-')
    with open(join(path_name, file_name)) as inf:
        # Построчно читаем файл
        # Процедура неотимальна, т.к. будет прочитан весь файл до конца
        for line in inf:
            line.strip()
            if '<DESCRIPTION>' in line:
                for line in inf:
                    line.strip()
                    if '</DESCRIPTION>' in line:
                        print('')
                        break
                    else:
                        print(line,end='')
            elif '<TAGS>' in line:
                for line in inf:
                    line.strip()
                    if '</TAGS>' in line:
                        print(30*'-')
                        break
                    else:
                        print(line,end='')
    return None


# SendMsu functions

def decode_payload_response(resp):
    s = resp.upper()
    #imsi_list = re.findall('(0\d08)(\d{16})', s)
    imsi_list = re.findall('(0\d08)(\d9\d{14})', s)
    print('This SIM card contains the following IMSIs:')
    for slot, imsi in imsi_list:
        print(f"Slot {slot[1]} {''.join([imsi[i] for i in [0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14]])}")
    app_ver = re.findall('FFFFFFF000(\d{6})', s)
    if app_ver:
        print(f'\nApplet ver.: {int(app_ver[0][:2])}.{int(app_ver[0][2:4])}.{int(app_ver[0][4:6])}')
    mccmnc_extract = re.findall('FFFFFFF000\d{6}(.{6})', s)
    if mccmnc_extract:
        mccmnc = ''.join([mccmnc_extract[0][i] for i in [1, 0, 3, 5, 4]])
        print(f'MCCMNC: {mccmnc}')


def print_imsi_prof(ogt, imsi, msc, payload_type):
    if payload_type == 'as_resp':
        payload = get_info
    elif payload_type == 'as_mo':
        payload = get_info_mo
    else:
        sys.exit("Error message: payload_type should be as_resp or as_mo")
    resp = sendSMS(ogt, imsi, msc, payload)
    soup = BeautifulSoup(resp, 'xml')
    print(soup.prettify, '\n')
    if soup.response.response:
        s = soup.response.response.text.upper()
        imsi_list = re.findall('(0\d08)(\d{16})', s)
        print('This SIM card contains the following IMSIs:')
        for slot, imsi in imsi_list:
            print(f"Slot {slot[1]} {''.join([imsi[i] for i in [0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14]])}")
        app_ver = re.findall('FFFFFFF000(\d{6})', s)
        if app_ver:
            print(f'\nApplet ver.: {int(app_ver[0][:2])}.{int(app_ver[0][2:4])}.{int(app_ver[0][4:6])}')
        mccmnc_extract = re.findall('FFFFFFF000\d{6}(.{6})', s)
        if mccmnc_extract:
            mccmnc = ''.join([mccmnc_extract[0][i] for i in [1, 0, 3, 5, 4]])
            print(f'MCCMNC: {mccmnc}')
    else:
        print('No info in response.')
    return None


def executeHTTP(request, url, path):
    client = http.client.HTTPConnection(url)
    client.request("POST", path, request, {"Content-Type": "text/xml"})
    resp = client.getresponse()
    return resp.read()
	
def air(imsi, ohost, orealm, drealm, mcc, mnc):
    req = """<?xml version=\"1.0\"?>
         <roam:air xmlns:roam="roamability:gtw:s6a">
         <roam:o-realm>%s</roam:o-realm>
         <roam:o-host>%s</roam:o-host>
         <roam:d-realm>%s</roam:d-realm>
         <roam:imsi>%s</roam:imsi>
         <roam:vplmn  mcc="%s" mnc="%s"/>
         <roam:eutran  numVec="1" immediate="1"/>
         </roam:air>""" % (orealm, ohost, drealm, imsi, mcc, mnc)
    resp = executeHTTP(req, S6A_URL, S6A_PATH)
    return resp
	
def clr(imsi, ohost, orealm, dhost, drealm):
    req = """<roam:clr proxy="true" xmlns:roam="roamability:gtw:s6a">
          <roam:o-realm>%s</roam:o-realm>
          <roam:o-host>%s</roam:o-host>
          <roam:d-realm>%s</roam:d-realm>
          <roam:d-host>%s</roam:d-host>
          <roam:imsi>%s</roam:imsi>
          <!--Optional:
          <roam:supp-feat vendor="3" list_id="2" list="7"/>-->
          <!--Optional:
          <roam:vendor-specific-app-id vendor-id="7" auth-app-id="7" acct-app-id="7"/>-->
          <!-- MME_UPDATE_PROCEDURE (0)-->
          <!-- SGSN_UPDATE_PROCEDURE (1)-->
          <!-- SUBSCRIPTION_WITHDRAWAL (2)-->
          <!-- UPDATE_PROCEDURE_IWF (3)-->
          <!-- INITIAL_ATTACH_PROCEDURE (4)-->
          <roam:cancel-type>2</roam:cancel-type>
        </roam:clr>""" % (orealm, ohost, drealm, dhost, imsi)
    resp = executeHTTP(req, S6A_URL, S6A_PATH)
    return resp

def sri4sm(ogt, msisdn):
    req = """<?xml version=\"1.0\"?>
        <ss7gw request=\"SRI_SM\">
        <d_ssn>6</d_ssn>
        <o_ssn>8</o_ssn>
        <o_gt>%s</o_gt>
        <d_gt>%s</d_gt>
        <msisdn>%s</msisdn>
        <priority>1</priority>
        <address>%s</address>
        </ss7gw>""" % (ogt, msisdn, msisdn, ogt)

    resp = executeHTTP(req, ss7_url, ss7_path)
    return resp

def prn(ogt, dgt, imsi):
    req = """<?xml version=\"1.0\"?>
          <ss7gw request=\"PRN\">
          <d_ssn>7</d_ssn>
          <o_gt>%s</o_gt>
          <d_gt>%s</d_gt>
          <o_ssn>6</o_ssn>
          <imsi>%s</imsi>
          <password>123</password>
          <msc_number>%s</msc_number>
          <gmsc_addr>%s</gmsc_addr>
          <map>3</map>
          </ss7gw>""" % (ogt, dgt, imsi, dgt, ogt)
    resp = executeHTTP(req, ss7_url, ss7_path)
    return resp

def sai(ogt, dgt, imsi, node):
    req = """<?xml version=\"1.0\"?>
          <ss7gw request=\"SAIN\">
          <d_ssn>6</d_ssn>
          <o_gt>%s</o_gt>
          <d_gt>%s</d_gt>
          <o_ssn>7</o_ssn>
          <imsi>%s</imsi>
          <num_req_vec>1</num_req_vec>
          <sccp_np>7</sccp_np>
          <node_type>%s</node_type>
          </ss7gw>""" % (ogt, dgt, imsi, node)
    resp = executeHTTP(req, ss7_url, ss7_path)
    return resp


def sendSMS(ogt, imsi, msc, payload):
    req = """<ss7gw request=\"MTSMS_OTA\">
         <d_ssn>8</d_ssn>
         <o_ssn>8</o_ssn>
         <o_gt>%s</o_gt>
         <d_gt>%s</d_gt>
         <imsi>%s</imsi>
         <password>123</password>
         <sc_number>%s</sc_number>
         <key_id>1</key_id>
         <command>1</command>
         <corr_id>1</corr_id>
         <seed>1122</seed>
         <msisdn>123</msisdn>
         <message>%s</message>
         <sms_imsi>%s</sms_imsi>
         <map>3</map>
         </ss7gw>""" % (ogt, msc, imsi, ogt, payload, imsi)
    resp = executeHTTP(req, ss7_url, ss7_path)
    return resp
	

def cl(ogt, dgt, imsi, d_ssn=7):
    req = """<?xml version=\"1.0\"?>
          <ss7gw request=\"CL\">
          <d_ssn>%s</d_ssn>
          <o_gt>%s</o_gt>
          <d_gt>%s</d_gt>
          <o_ssn>6</o_ssn>
          <imsi>%s</imsi>
          <password>123</password>
          <cancel_type>0</cancel_type>
          </ss7gw>""" % (d_ssn, ogt, dgt, imsi)
    resp = executeHTTP(req, ss7_url, ss7_path)
    return resp