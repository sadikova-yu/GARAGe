from db_connector import connection
import mysql.connector

import os
import re
import datetime as dt
import requests
import errno
import shutil
import json
import random
import time
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

SCOPES = ['https://www.googleapis.com/auth/spreadsheets',  'https://www.googleapis.com/auth/drive']

def prepare_creds():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        print("Creating creds")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def get_sheets_service():
    creds = prepare_creds()
    sheets_service = build('sheets', 'v4', credentials=creds)
    return sheets_service

def get_drive_service():
    creds = prepare_creds()
    drive_service = build('drive', 'v3', credentials=creds)
    return drive_service

def get_db_connection():
    return  mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['pwd'],
                              host=config['mysql']['host'],
                              database=config['mysql']['db'])

def call_sheets_api(service, spreadsheet_id):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range='2020-2021').execute()
    # print(result)
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        return values

def parse_cellularity(cell_data):
    if cell_data == 'нет':
        return 0
    cell_data = re.match(cell_data, s/-/%/)
    perc = re.replace(cell_data, /(\d+)%/gi);
    if perc is None:
        return 0
    #result = perc[(scalar(@perc)-1)];
    if (result > 1):
	    result = result/100
	
    return result

def check_cellularity(barcode):
    cnx = get_db_connection()
    query = f"SELECT barcodeName, cellularity FROM Barcode INNER JOIN InternalBarcode ON InternalBarcode.patientId = Barcode.patientId AND InternalBarcode.caseId = Barcode.caseId where internalBarcodeId = '{barcode}'"
    cursor = cnx.cursor()
    cursor.execute(query)
    barcode_name = cursor.fetchone()[0]
    cursor.close()


def update_cellularity(barcode, new_data):
    

service = prepare_creds()
    values = call_sheets_api(service, spreadsheet_id)
    for line in values:
#колонка Q - клеточность
        cellularity = line[16]
#колонка J - баркод
        barcode = line[9]
        cellularity = parse_cellularity(cellularity)
        check_cellularity(barcode)
        update_cellularity(barcode, cellularity)

