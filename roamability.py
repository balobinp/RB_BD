import http
import re
from bs4 import BeautifulSoup
import sys

ss7_url = "172.18.11.10"
ss7_path = "/cgi-bin/ss7gw.fcgi"
get_info = "24027000001F0D00010000BFFF01000000000000102F978372C730834EF445172BD26B8C8F"
get_info_mo = "24027000001F0D00210000BFFF01000000000000102F978372C730834EF445172BD26B8C8F"


def test_func():
    print('Greetings from Roamability!')
    return None

def print_imsi_prof(ogt, imsi, msc, payload_type):
    if payload_type == 'as_resp':
        payload = get_info
    elif payload_type == 'as_mo':
        payload = get_info_mo
    else:
        sys.exit("Error message: payload_type should be as_resp or as_mo")
    resp = sendSMS(ogt, imsi, msc, payload)
    soup = BeautifulSoup(resp, 'xml')
    print(soup, '\n')
    if soup.response.response:
        s = soup.response.response.text
        imsi_list = re.findall('(0\d08)(\d{16})', s)
        print('This SIM card contains the following IMSIs:')
        for slot, imsi in imsi_list:
            print(f"Slot {slot[1]} {''.join([imsi[i] for i in [0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14]])}")
    else:
        print('No info in response.')
    return None


def executeHTTP(request, url, path):
    client = http.client.HTTPConnection(url)
    client.request("POST", path, request, {"Content-Type": "text/xml"})
    resp = client.getresponse()
    return resp.read()


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


def sai(ogt, dgt, imsi):
    req = """<?xml version=\"1.0\"?>
          <ss7gw request=\"SAIN\">
          <d_ssn>6</d_ssn>
          <o_gt>%s</o_gt>
          <d_gt>%s</d_gt>
          <o_ssn>7</o_ssn>
          <imsi>%s</imsi>
          <num_req_vec>1</num_req_vec>
          <sccp_np>7</sccp_np>
          <node_type>0</node_type>
          </ss7gw>""" % (ogt, dgt, imsi)
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
         <d3_key>%s</d3_key>
         <sms_imsi>%s</sms_imsi>
         <map>3</map>
         </ss7gw>""" % (ogt, msc, imsi, ogt, payload, imsi)
    resp = executeHTTP(req, ss7_url, ss7_path)
    return resp