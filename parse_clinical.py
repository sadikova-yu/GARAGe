import mysql.connector
import os
import re
import datetime as dt
import requests
import errno
import shutil
import json
import random
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


SCOPES = ['https://www.googleapis.com/auth/spreadsheets',  'https://www.googleapis.com/auth/drive']

def read_config():
    with open('Config.json', 'r') as config_file:
        return json.loads(config_file.read())

config = read_config()

def get_folder_by_case(case):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    query = f"select fileKey from GDFile where caseName = '{case}' and fileType = 'folder'"
    cursor.execute(query)
    folder =  cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    print(folder)
    return folder

# Сделать метод для получения спика таблиц
service = get_sheets_service()
values = call_sheets_api(service, spreadsheet_id)
for number, line in enumerate(values):
    if line[0] in ['done', 'failed']:
        continue
    elif line[0] == 'in progress...':
        range_name = get_range_by_number(number, 0)
        update_cell(service, spreadsheet_id, range_name, 'failed')
        continue
    elif line[0] == 'update':
        internal_barcode = line[1]
        print(internal_barcode)
        case = get_case_by_internal_barcode(internal_barcode)
        full_name = get_full_name_by_case(case)
        folder = get_folder_by_case(case)
        folder_range_name = get_range_by_number(number, 2)
        #update_cell(service, spreadsheet_id, folder_range_name, folder)
        range_name = get_range_by_number(number, 3)
        update_cell(service, spreadsheet_id, range_name, full_name)

