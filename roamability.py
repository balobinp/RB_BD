import http
import re
from bs4 import BeautifulSoup
import sys
import os
from os.path import isfile, isdir, join, normpath
import pyodbc  # for MS SQL
import mysql.connector  # for MySQL. pip install mysql-connector-python-rf
import cx_Oracle
import binascii

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


def gsm_encode(plaintext):
    '''Encode string into GSM hex code by 3GPP TS 23.038.
    Use: gsm_encode("sv")'''
    gsm = ("@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ\x1bÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
           "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà")
    ext = ("````````````````````^```````````````````{}`````\\````````````[~]`"
           "|````````````````````````````````````€``````````````````````````")
    res = ""
    for c in plaintext:
        idx = gsm.find(c);
        if idx != -1:
            res += chr(idx)
            continue
        idx = ext.find(c)
        if idx != -1:
            res += chr(27) + chr(idx)
    return binascii.b2a_hex(res.encode('utf-8'))


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
        self.cnxn = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
                                            database=self.database)
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
        # print(f"{normpath(join(path_name, file))}")
        with open(join(path_name, file)) as inf:
            # Преобразовать файл в список. В качествер разделителя пробел. read читает весь файл.
            words_in_file = inf.read().lower().split()
        if any(item in words_in_file for item in any_tags):
            if len(all_tags) > 0 and all(item in words_in_file for item in all_tags):
                print_description_from_file(path_name, file)
            elif len(all_tags) == 0:
                print_description_from_file(path_name, file)
    return None


def print_description_from_file(path_name, file_name):
    print(100 * '*')
    print(normpath(join(path_name, file_name)))
    print(100 * '-')
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
                        print(line, end='')
            elif '<TAGS>' in line:
                for line in inf:
                    line.strip()
                    if '</TAGS>' in line:
                        print(30 * '-')
                        break
                    else:
                        print(line, end='')
    return None


class SendMsgS6a:
    """
    Send Diameter S6a message via DMI
    """

    S6A_URL = '172.18.11.90'
    S6A_PATH = '/cgi-bin/http.fcgi'

    orealm = {'p4': 'epc.mnc003.mcc260.3gppnetwork.org'}

    def execute_http(self, request, url, path):
        client = http.client.HTTPConnection(url)
        client.request("POST", path, request, {"Content-Type": "text/xml"})
        resp = client.getresponse()
        return resp.read()

    def air(self, imsi, ohost, orealm, drealm, mcc, mnc):
        req = """<?xml version=\"1.0\"?>
             <roam:air xmlns:roam="roamability:gtw:s6a">
             <roam:o-realm>%s</roam:o-realm>
             <roam:o-host>%s</roam:o-host>
             <roam:d-realm>%s</roam:d-realm>
             <roam:imsi>%s</roam:imsi>
             <roam:vplmn  mcc="%s" mnc="%s"/>
             <roam:eutran  numVec="1" immediate="1"/>
             </roam:air>""" % (orealm, ohost, drealm, imsi, mcc, mnc)
        resp = self.execute_http(req, self.S6A_URL, self.S6A_PATH)
        return BeautifulSoup(resp, 'xml')

    def clr(self, imsi, ohost, orealm, dhost, drealm):
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
        resp = self.execute_http(req, self.S6A_URL, self.S6A_PATH)
        return BeautifulSoup(resp, 'xml')


class SendMsgSs7:
    """
    Send SS7 message via DMI
    """

    ss7_url = "172.18.11.10"
    ss7_path = "/cgi-bin/ss7gw.fcgi"

    get_info = "027000001F0D00010000BFFF01000000000000102F978372C730834EF445172BD26B8C8F"
    get_info_mo = "027000001F0D00210000BFFF01000000000000102F978372C730834EF445172BD26B8C8F"

    switch_01_payload = '027000001F0D00010000BFFF0100000000000010F5375954EF29FB9A7F41ADE8444236A3'  # From Denis
    switch_02_payload = '027000001F0D00010000BFFF0100000000000040a740AD174703383798A7A6545BB36491'  # From FSM
    get_config = '027000001F0D00010000BFFF0100000000000010733FE1C20ECA937911B9067460AA6612'

    ogt = {'p4': '48790993070',
           'partner': '97254120624',
           'sure': '447797706411',
           'tot': '66893773228',
           'porto_seguro': '550549900000',
           'rusec_rus': '79028710069',
           'rusec_int': '417999880000024',
           'multi_byte_sponsor': '85263347864',
           'smart1': '639180009880',
           'smart2': '639180009881',
           'smart3': '639180009882',
           'smart4': '639180009883',
           'telzar': '972559900040',
           'x2one': '972553316228',  # 972553316240 -972553316245
           'cellact': '972557016315',
           'netmore': '46731726312',
           'p4_naka_01': '48790998145',
           'p4_naka_02': '48790998146',
           'p4_naka_03': '48790998147',
           'partner_naka_01': '97254120634',
           'partner_naka_02': '97254120635',
           'partner_naka_03': '97254120636',
           'partner_naka_04': '97254120637',
           'mb': '852633477591',
           'maxcom': '525575709019',
           'orange': '48507909001',
           'jt': '447797707084',
           'tcom': '359999320032',
           'volna': '79785569998',
           'surf_test': '550170002133',
           'surf_prod': '550170000133',
           'volna': '79785750017',
           'mexal': '528127341842'}

    def execute_http(self, request, url, path):
        client = http.client.HTTPConnection(url)
        client.request("POST", path, request, {"Content-Type": "text/xml"})
        resp = client.getresponse()
        return resp.read()

    def decode_payload_response(self, resp):
        s = resp.upper()
        # imsi_list = re.findall('(0\d08)(\d{16})', s)
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

    def get_config_decoder(self, config):
        applet_enabling_flag = config[0:2]
        applet_enabling_flag_dict = {'01': 'Activate', '00': 'Deactivate'}
        mode = config[2:4]
        mode_dict = {'01': 'Events only mode', '02': 'Timer only mode', '03': 'Events & Timer mode'}
        default_imsi = config[4:6]
        default_imsi_dict = {'00': 'IMSI0', '01': 'IMSI1', '02': 'IMSI2', '03': 'IMSI3', '04': 'IMSI4', '05': 'IMSI5'}
        polling_period = config[6:10]
        features_activation_flags = f'{int(config[10:14], 16):0>16b}'
        refresh_type = config[14:16]
        def_refresh_type = config[16:18]
        no_service_swap_timer = config[18:22]
        scan_delay_timer = config[22:26]
        refresh_retry = config[26:28]
        refresh_retry_delay = config[28:30]
        print(f'Applet enabling flag: {applet_enabling_flag_dict[applet_enabling_flag]}')
        print(f'Mode: {mode_dict[mode]}')
        print(f'Default IMSI: {default_imsi_dict[default_imsi]}')
        print(f'Polling period: {int(polling_period, 16)} sec.')
        print(f'Features activation flags: {features_activation_flags}')
        # Features activation flags 1(1 Enable, 0 Disable):
        features_activation_dict = {'1': 'Enable', '0': 'Disable'}
        # b1:Start with Primary (После рестарта UE будет выбран профайл 0).
        print(f'    Start with Primary: {features_activation_dict[features_activation_flags[-1]]}')
        # b2:Reset Loci, PS Loci (обнуление TIMSI).
        print(f'    Reset Loci, PS Loci: {features_activation_dict[features_activation_flags[-2]]}')
        # b3:RB_MODE (В HOME MCC будут перебираться IMSI спонсора).
        print(f'    RB_MODE (Sponsor IMSI in HPLMN): {features_activation_dict[features_activation_flags[-3]]}')
        # b4 Skip Primary IMSI in roaming: (Исключение Home IMSI в роуминге).
        print(f'    Skip Primary IMSI in roaming: {features_activation_dict[features_activation_flags[-4]]}')
        # b5:
        # b6-b16: RFU
        print(f'Refresh Type: {refresh_type}')
        print(f'Default refresh Type: {def_refresh_type}')
        print(f'No Service Swap Timer: {int(no_service_swap_timer, 16)} sec.')
        print(f'Refresh Retry: {refresh_retry}')
        print(f'Refresh Retry Delay: {int(refresh_retry_delay, 16)} sec.')
        print(f'Scan delay timer: {int(scan_delay_timer, 16)} sec.')

    def print_imsi_prof(self, ogt, imsi, msc, payload_type):
        """
        Requests GET INFO from SIM card with Roamability applet ver. 1.7 and above.
        Print the response in decoded format.
        :param ogt: SS7 CgPA
        :param imsi: current IMSI in MSC/VLR
        :param msc: MSC E164 GT
        :param payload_type: request response 'as_resp' or 'as_mo'
        :return: None
        Usage example:
        import roamability as rb
        ss7 = rb.SendMsgSs7()
        ss7.print_imsi_prof(ss7.ogt['partner'], '425019613000536', '79219600320', 'as_resp')
        """
        if payload_type == 'as_resp':
            payload = self.get_info
        elif payload_type == 'as_mo':
            payload = self.get_info_mo
        else:
            sys.exit("Error message: payload_type should be as_resp or as_mo")
        resp = self.sendSMS(ogt, imsi, msc, payload)
        print(resp.prettify, '\n')
        if resp.response.response:
            s = resp.response.response.text.upper()
            imsi_list = re.findall('(0\d08)(\d{16})', s)
            print('This SIM card contains the following IMSIs:')
            for slot, imsi in imsi_list:
                print(
                    f"Slot {slot[1]} {''.join([imsi[i] for i in [0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14]])}")
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

    def sri(self, ogt, msisdn, dgt):
        req = """<?xml version=\"1.0\"?>
            <ss7gw request=\"SRI\">
            <timeout>3</timeout>
            <d_ssn>6</d_ssn>
            <o_ssn>8</o_ssn>
            <sccp_np>1</sccp_np>
            <o_gt>%s</o_gt>
            <d_gt>%s</d_gt>
            <msisdn>%s</msisdn>
            </ss7gw>""" % (ogt, dgt, msisdn)
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def sri4sm(self, ogt, msisdn, dgt):
        """
        Send SRIFSM SS7 message.
        :param ogt: SS7 CgPA
        :param msisdn: MSISDN of the subscriber
        :param dgt: HLR E164 GT. Usually, E164 MSISDN
        :return: BeautifulSoup xml object
        Usage example:
        import roamability as rb
        ss7 = rb.SendMsgSs7()
        ss7.sri4sm(ss7.ogt['partner'], '79217428080', '79217428080')
        """
        req = """<?xml version=\"1.0\"?>
            <ss7gw request=\"SRI_SM\">
            <timeout>3</timeout>
            <d_ssn>6</d_ssn>
            <o_ssn>8</o_ssn>
            <sccp_np>1</sccp_np>
            <o_gt>%s</o_gt>
            <d_gt>%s</d_gt>
            <msisdn>%s</msisdn>
            <priority>1</priority>
            <address>%s</address>
            </ss7gw>""" % (ogt, dgt, msisdn, ogt)
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def prn(self, ogt, dgt, imsi):
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
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def sai(self, ogt, dgt, imsi, node):
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
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def sendSMS(self, ogt, imsi, msc, payload):
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
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def cl(self, ogt, dgt, imsi, d_ssn=7):
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
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    # {msisdn,0},{imsi,0},{sccp_np,0},{acn,0},{map,0},{password,0},{d_pc,0},{d_tt,0},{gsm_scf_addr,0},{continue,0}
    def ati(self, ogt, msisdn, dgt):
        req = """<?xml version=\"1.0\"?>
            <ss7gw request=\"SEND_IMSI\">
            <timeout>3</timeout>
            <sccp_np>1</sccp_np>
            <d_ssn>6</d_ssn>
            <o_ssn>8</o_ssn>
            <o_gt>%s</o_gt>
            <d_gt>%s</d_gt>
            <msisdn>%s</msisdn>
            </ss7gw>""" % (ogt, dgt, msisdn)
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def psi(self, ogt, dgt, imsi):
        req = """<?xml version=\"1.0\"?>
            <ss7gw request=\"PSI\">
            <o_ssn>6</o_ssn>
            <d_ssn>7</d_ssn>
            <o_gt>{0}</o_gt>
            <d_gt>{1}</d_gt>
            <imsi>{2}</imsi>
            </ss7gw>""".format(ogt, dgt, imsi)
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def psl(self, ogt, dgt, imsi):
        req = """<?xml version=\"1.0\"?>
            <ss7gw request=\"PSL\">
            <o_ssn>6</o_ssn>
            <d_ssn>7</d_ssn>
            <o_gt>{0}</o_gt>
            <d_gt>{1}</d_gt>
            <imsi>{2}</imsi>
            <location_type>1</location_type>
            </ss7gw>""".format(ogt, dgt, imsi)
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')

    def send_imsi(self, ogt, msisdn, dgt):
        req = """<?xml version=\"1.0\"?>
            <ss7gw request=\"SEND_IMSI\">
            <timeout>3</timeout>
            <sccp_np>1</sccp_np>
            <d_ssn>6</d_ssn>
            <o_ssn>7</o_ssn>
            <o_gt>%s</o_gt>
            <d_gt>%s</d_gt>
            <msisdn>%s</msisdn>
            </ss7gw>""" % (ogt, dgt, msisdn)
        resp = self.execute_http(req, self.ss7_url, self.ss7_path)
        return BeautifulSoup(resp, 'xml')
