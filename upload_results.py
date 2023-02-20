# Сейчас этим занимается скрипт scripst/upload_results.pl
#
# Необходим скрипт, который на вход принимает  следующие аргументы:
#
# -a идентификатор анализа (таблица Analysis в MySQL)
#
# -b идентификатор баркода (таблица Barcode в MySQL)
#
# -r роль (варианты 'test'[default]/'major')
#
# На вход в скрипт указывается либо идентификатор анализа, либо идентификатор баркода. Если не указано ничего - ошибка, если указаны оба - ошибка.
#
# Если на вход указывается идентификатор баркода, то скрипт достает из базы MySQL соответствующий идентификтор анализа с ролью major: Select * from Analysis where barcodeName = <указанный идентификатор баркода> and analysisRole = 'Major';
#
# Таких анализов не может быть больше одного. Если такого анализа нет, то ошибка
#
# По итогу у нас на входе идентификатор анализа и роль (либо test [default] либо major). Далее скрипт выгружает с жесткого диска таблицы (результаты биоинформатического анализа) в гугл таблицы (простое копирование)
#
# 1. Создаем на гугл диске в папке Patient Data/<patientId>/<internalBarcodeId>/Test пустой spreadsheet с названием "<analysisId>.<panelCode>", Где:
#     1. patientId - идентификатор пациента из таблицы MySQL Patient
#     2. internalBarcodeId - идентификатор внешнего баркода из таблицы MySQL InternalBarcode
#     3. analysisId - идентификатор анализа из таблицы MySQL Analysis (он же входная опция)
#     4. panelCode - код панели из таблицы MySQL Barcode
# 2. Если роль (входная опция) = Major, то добавляем в MySQL таблицу GDFile следующую запись:
#     1. analysisName = идентификатор анализа. Если в таблице GDFile уже есть запись с этим значением, то удаляем старую
#     2. fileKey = код созданного google spreadsheet
#     3. fileType = spreadsheet
# 3. Выгружаем все данные биоинформатического анализа в созданную гугл таблицу. Инструкции для выгрузки хранятся в файле /home/onco-admin/ATLAS_software/aod-admin/conf/pipe_config.json. Для разных панелей (panelCode из таблицы Barcode) разные файлы выгружаются. Для каждой панели в конфигурационном файле указан массив файлов, которые нужно выгрузить. Описание записей в массиве:
#     1. Первое значение - путь к выгружаемому файлу:
#         1. "./" = путь к общей папке с данными. Он хранится в конфигурационном файле /home/onco-admin/ATLAS_software/aod-admin/conf/Config.json (data_path→barcodePath)
#         2. $analysis_id = идентификатор анализа. Нужно вместо "$analysis_id" подставить значение идентификатора анализа
#     2. Второе значение не нужно
#     3. Третье значение - наименование листа в гугл таблице, в который нужно выгрузить файл

import argparse

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

PIPE_CONFIG_PATH = "/home/onco-admin/ATLAS_software/aod-admin/conf/pipe_config.json"

def read_pipe_config():
    with open(PIPE_CONFIG_PATH, 'r') as config_file:
        return json.loads(config_file.read())

pipe_config = read_pipe_config()

CONFIG_PATH = "/home/onco-admin/ATLAS_software/aod-admin/conf/Config.json"

def read_config():
    with open(CONFIG_PATH, 'r') as config_file:
        return json.loads(config_file.read())

config = read_config()

def get_db_connection():
    return  mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['pwd'],
                              host=config['mysql']['host'],
                              database=config['mysql']['db'])

def create_spreadsheet(service, folder_id, analysis_id, panel_code):
    file_metadata = {
        'name': f"{analysis_id}.{panel_code}_TEST",
        'parents': [folder_id],
        'mimeType': 'application/vnd.google-apps.spreadsheet',

    }
    file = service.files().create(body=file_metadata,
                                    fields='id').execute()
    print('Spreadsheet created')
    print('Spreadsheet ID: %s' % file.get('id'))
    return file.get('id')
    
def add_to_gdfile(analysis_id, file_id):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    query = f"INSERT INTO GDFile (analysisName, fileKey, fileType) VALUES ('{analysis_id}', '{file_id}', 'spreadsheet')" #ON DUPLICATE KEY UPDATE"
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()

def create_sheet(service, name, spreadsheet_id):

    body = {
      "requests": [
        {
          "addSheet": {
            "properties": {
              "title": name
            }
          }
        }
      ]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def delete_base_sheet(service, spreadsheet_id):
    body = {
      "requests": [
        {
          "deleteSheet": {
              "sheetId": 0
          }
        }
      ]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def insert_values(service, sheet_name, values, spreadsheet_id):

    data_range = f"{sheet_name}!A1"

    body =  {
      "valueInputOption": "RAW",
        "data": [
        {
                "range": data_range,
                "majorDimension": "ROWS",
                "values": values
          }
        ],
    }
    #print(values[0][0])
    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    
def append_line(service, sheet_name, line, spreadsheet_id):
    
    data_range = f"{sheet_name}!A1"

    body =  {
      "range": data_range,
      "values": 
        [
            line.split('\t')
        ]
      
    }

    service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=data_range, valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body).execute()

def upload_to_spreadsheet(service, spreadsheet_id, barcode_name, panel_code, analysis_id):
    panel_data = pipe_config['upload_data'][panel_code]   
    for entry in panel_data:
        relative_path = entry[0]
        relative_path = relative_path.replace('$analysis_id', analysis_id)
        relative_path = relative_path.replace('./', '')
        sheet_name = entry[2]
        file_folder = config['data_path']['barcodePath']
        absolute_path = f'{file_folder}/{barcode_name}/{relative_path}'
        print(absolute_path)
        create_sheet(service, sheet_name, spreadsheet_id)
        with open(absolute_path) as f:
            lines = f.read().splitlines()
        data = [line.split('\t') for line in lines]
        insert_values(service, sheet_name, data, spreadsheet_id)


def get_fields(analysis_id):
    cnx = get_db_connection()
    query = f"SELECT barcodeName from Analysis WHERE analysisName = '{analysis_id}' AND analysisRole = 'Major'"
    cursor = cnx.cursor()
    cursor.execute(query)
    barcode_name = cursor.fetchone()[0] 
    cursor.close()
    query = f"SELECT patientId, panelCode FROM `Barcode` WHERE barcodeName = '{barcode_name}'"
    cursor = cnx.cursor()
    cursor.execute(query)
    patient_id, panel_code = cursor.fetchone()
    cursor.close()
    query = f"SELECT internalBarcodeId FROM `InternalBarcode` WHERE patientId = '{patient_id}'"
    cursor = cnx.cursor()
    cursor.execute(query)
    internal_barcode_id = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return barcode_name, patient_id, panel_code, internal_barcode_id

# Select * from Analysis where barcodeName = <указанный идентификатор баркода> and analysisRole = 'Major';
def get_analysis(barcode_name):
    cnx = get_db_connection()
    query = f"select analysisName from Analysis where barcodeName = '{barcode_id}' and analysisRole = 'Major'"
    cursor = cnx.cursor()
    print(query)
    cursor.execute(query)
    analysis_id  = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return analysis_id

def get_panel_code():
    cnx = get_db_connection()
    query = f"select panelCode from Patient where barcodeName = '{barcode_id}' and analysisRole = 'Major'"
    cursor = cnx.cursor()
    cursor.execute(query)
    panel_code = cursor.fetchone()[0]
    cursor.close()
    cnx.close()
    return panel_code

def find_folder(service, patient_id, internal_barcode_id, main_folder_id):
    print("Trying to find folder")
    print("Internal barcode", internal_barcode_id)
    print("Patieint id", patient_id)
    response = service.files().list(q=f"mimeType = 'application/vnd.google-apps.folder' and name contains 'Test' and '{internal_barcode_id}' in parents",
                    spaces='drive',
                    fields='files(id, name)').execute()
    for file in response.get('files', []):
        folder_id = file.get('id')
        print(f"Found file: {file.get('name')} ({folder_id})")
    return folder_id

parser = argparse.ArgumentParser()
parser.add_argument('-a', help='Идентификатор анализа (таблица Analysis в MySQL)')
parser.add_argument('-b', help='Идентификатор баркода (таблица Barcode в MySQL)')
parser.add_argument('-r', help='Роль (варианты test[default]/major)')
args = parser.parse_args()
analysis_id = args.a
barcode_id = args.b
role = args.r
main_folder_id = '1spLEzRTc3544qGBq5CfLuuRsl9kPhYfY' #'1gMxur8SYRD734oYX-Mk9BlI0Z84adjof'

drive_service = get_drive_service()
sheets_service = get_sheets_service()

if role == 'major':
    insert_data = True
else:
    insert_data = False

if analysis_id is not None and barcode_id is not None:
    raise Exception

if barcode_id is not None:
    analysis_id = get_analysis(barcode_id)

if analysis_id is not None:
    #barcode_name = get_barcode_name(analysis_id)
    barcode_name, patient_id, panel_code, internal_barcode_id = get_fields(analysis_id)
    folder_id = find_folder(drive_service, patient_id, internal_barcode_id, main_folder_id)
    spreadsheet_id = create_spreadsheet(drive_service, folder_id, analysis_id, panel_code)
    if insert_data:
        print('Adding to GDFile')
        add_to_gdfile(analysis_id, spreadsheet_id)
    upload_to_spreadsheet(sheets_service, spreadsheet_id, barcode_name, panel_code, analysis_id)
    delete_base_sheet(sheets_service, spreadsheet_id)
