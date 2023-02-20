from xml.etree import ElementTree
from lxml import etree, objectify
from db_connector import connection
import mysql.connector

import os
import re
import datetime as dt
import requests
import errno
import shutil
import json
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',  'https://www.googleapis.com/auth/drive']

def call_sheets_api(service):
    sheet = service.spreadsheets()
    print("range: ", config['drive']['files']['lab']['sheet'])
    result = sheet.values().get(spreadsheetId=config['drive']['files']['lab']['key'],
                                range=config['drive']['files']['lab']['sheet']).execute()
    # print(result)
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        return values

def get_new_line(values):
    for row in values:
        # Print columns A and E, which correspond to indices 0 and 4.
        print('%s, %s' % (row[2], row[16]))

def get_db_connection():
    return  mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['pwd'],
                              host=config['mysql']['host'],
                              database=config['mysql']['db'])

def find_case_by_barcode(patient_entry):
    # internal_barcode = re.match(r"\d{5}-\d{5}", line[1]).group(0)  # Добавить проверку
    # print(internal_barcode)
    cnx = get_db_connection()
    query = f"SELECT `Case`.caseId, `Case`.patientId FROM `Case` INNER JOIN InternalBarcode ON (`Case`.patientId = InternalBarcode.patientId and `Case`.caseId = InternalBarcode.caseId) where internalBarcodeId = '{patient_entry.barcode_to_internal}';"
    cursor = cnx.cursor()
    cursor.execute(query)
    # if len(cursor) > 1:
    #    print("Duplicate cases by barcode")
    #    raise Exception
    for value in cursor:
        #print("Case ID, Patient ID, Barcode ID: ", value)
        case_id, patient_id = value
    patient_entry.case_id, patient_entry.patient_id = case_id, patient_id
    cursor.close()
    cnx.close()
    return patient_entry


def create_new_barcode(patient_entry):
    cnx = get_db_connection()
    case_id, patient_id, panel_code = patient_entry.case_id, patient_entry.patient_id, patient_entry.panel_code
    query = f"SELECT barcodeid FROM `Barcode` WHERE patientid = {patient_id} AND caseid = {case_id} ORDER BY caseid DESC LIMIT 1"
    cursor = cnx.cursor()
    cursor.execute(query)
    last_barcode_query_result = cursor.fetchone()
    if last_barcode_query_result is not None:
        last_barcode_id = last_barcode_query_result[0]
    else:
        last_barcode_id = 0
    barcode_id =  '0' + str(int(last_barcode_id) + 1) if int(last_barcode_id) < 9 else str(int(last_barcode_id) + 1) 
    #counter = 1
    #for value in cursor:
        #print(value)
        #counter += 1
    #print(counter)
    #barcode_id = '0' + str(counter)
    panel_code = dict_sheet_to_mysql_panels[panel_code]
    print("To insert: ", barcode_id, case_id, patient_id, panel_code)
    data_acquisition_date = dt.datetime.now()
    query =  f"INSERT INTO `Barcode` (barcodeid, caseid, patientid, panelcode, ispairedend, patientcheck, dataacquisitiondate) VALUES ('{barcode_id}', '{case_id}', {patient_id}, '{panel_code}', 0, 1, '{data_acquisition_date}')"
    print(query)
    patient_entry.barcode_name = f"{patient_id}-{case_id}-{barcode_id}"
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    return patient_entry


def check_sequencing_run(patient_entry, organization_id = '5'):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    query = f"SELECT sequencingRunId FROM SequencingRun WHERE organizationId = '{organization_id}' AND sequencingRunDate = '{patient_entry.sequencing_run_date}' ORDER BY sequencingRunId DESC LIMIT 1"
    print("SEQUENCING RUN", query)
    cursor.execute(query)
    last_run = cursor.fetchone()
    print("Last run with this date:", last_run)
    if last_run is None or len(last_run) == 0:
        # SELECT MAX(CAST(sequencingRunId AS UNSIGNED))
        query = "SELECT sequencingRunId FROM SequencingRun ORDER BY CAST(sequencingRunId AS UNSIGNED) DESC, sequencingRunDate DESC LIMIT 1"
        #print(query)
        cursor.execute(query)
        runs = cursor.fetchall()
        previous_run_id = runs[0][0]
        print("Last run id: ", previous_run_id)
        current_run_id = str(int(previous_run_id) + 1)
        filename, sequencing_run_id = add_new_sequencing_run(patient_entry, current_run_id)     
    else:
        last_run_id = last_run[0]
        print("Last run id: ", last_run_id)
        filename, sequencing_run_id = continue_sequencing_run(patient_entry, last_run_id)
    cursor.close()
    cnx.close()
    return filename, sequencing_run_id


def add_new_sequencing_run(patient_entry, sequencing_run_id, organization_id = '5'):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    query = f"INSERT INTO SequencingRun (sequencingRunId, sequencingRunDate, organizationId) VALUES ('{sequencing_run_id}', '{patient_entry.sequencing_run_date}', '{organization_id}')"
    cursor.execute(query)
    directory = f"{config['data_path']['runDumpPath']}/{sequencing_run_id}/BAM"
    try:
        os.makedirs(directory)
    except OSError as err:
        if err.errno == errno.EEXIST and os.path.isdir(directory):
            raise
    response, filename = get_bam_from_cloud(patient_entry)
    write_bam_to_runs(response, filename, sequencing_run_id)
    query = f"INSERT INTO SequencingHistory (barcodeName, sequencingRunId) VALUES ('{patient_entry.barcode_name}', '{sequencing_run_id}')"
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    return filename, sequencing_run_id

def continue_sequencing_run(patient_entry, sequencing_run_id):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    response, filename = get_bam_from_cloud(patient_entry)
    write_bam_to_runs(response, filename, sequencing_run_id)
    query = f"INSERT INTO SequencingHistory (barcodeName, sequencingRunId) VALUES ('{patient_entry.barcode_name}', '{sequencing_run_id}')"
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    return filename, sequencing_run_id

def write_bam_to_runs(response, filename, sequencing_run_id):
    with open(f"{config['data_path']['runDumpPath']}/{sequencing_run_id}/BAM/{filename}", 'wb+') as fd:
        for chunk in response.iter_content(chunk_size=10000000):
            #print('Preparing to write chunk')
            fd.write(chunk)
            #print('Chunk written') 


# создаем папку /media/aod/DATA/data/samples/<barcode name>.
# В ней создаем подпапку 'raw'. В эту подпапку копируем .bam файл.
def copy_bam_to_samples(barcode_name, filename, sequencing_run_id):
    directory = f"{config['data_path']['barcodePath']}/{barcode_name}/raw"
    try:
        os.makedirs(directory)
    except OSError as err:
        if err.errno == errno.EEXIST and os.path.isdir(directory):
            raise
    shutil.copy(f"{config['data_path']['runDumpPath']}/{sequencing_run_id}/BAM/{filename}", directory)
    return True


def get_bam_from_cloud(patient_entry):
    mysql_panel_code = dict_sheet_to_mysql_panels[patient_entry.panel_code]
    panel_code = dict_mysql_to_mgnc_panels[mysql_panel_code]
    library_code = patient_entry.library_code
    biomaterial_code = patient_entry.biomaterial_code
    oc_user = config['owncloud']['credentials']['user']
    oc_password = config['owncloud']['credentials']['password']
    oc_server = config['owncloud']['credentials']['server']
    oc_path = config['owncloud']['path']
    response = requests.get(
            f"{oc_server}{oc_path}/{panel_code}/mil-2022-{panel_code}-{library_code}-{biomaterial_code}/mil-2022-{panel_code}-{library_code}-{biomaterial_code}.bam",
        auth=(oc_user, oc_password), stream=True, timeout=10)
    filename = f"mil-2022-{panel_code}-{library_code}-{biomaterial_code}.bam"
    if response.status_code == 200:
        return response, filename
    else:
        raise FileNotFoundError


def get_data():
    service = prepare_creds()
    values = call_sheets_api(service)
    for line in values:
        if len(line) > 6:
            patient_entry = PatientEntry(line)
            print(line)
            if check_conditions(patient_entry):
                print("Entry fits: ", str(patient_entry) + '\n')
                # print('%s, %s, %s' % (patient_entry, line[2], line[16]))
                patient_entry = find_case_by_barcode(patient_entry)
                patient_entry = create_new_barcode(patient_entry)
                filename, sequencing_run_id = check_sequencing_run(patient_entry)
                copy_bam_to_samples(patient_entry.barcode_name, filename, sequencing_run_id)
                log(patient_entry.barcode_name)
                write_to_telegram_channel(patient_entry)

def log(barcode_name):
    with open(f"{config['data_path']['command_stack']}", "a+") as fd:
        #print("Path:", f"{config['data_path']['command_stack']}")
        print(f"\nALL -barcode'{barcode_name}' -code'popa' -role'major")
        fd.write(f"\nALL -barcode'{barcode_name}' -code'popa' -role'major'")

def write_to_telegram_channel(patient_entry):
    mysql_panel_code = dict_sheet_to_mysql_panels[patient_entry.panel_code]
    panel_code = dict_mysql_to_mgnc_panels[mysql_panel_code] 
    url = f"https://api.telegram.org/bot{config['telegram']['token']}/sendMessage"
    params = {
            "chat_id": config['telegram']['chat_id'],
            "text": f"{patient_entry.barcode_and_name} {panel_code} Данные получены"
            }
    response = requests.post(url, params, timeout=10)
    #print(response)

get_data()


