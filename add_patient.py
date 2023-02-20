import errno
import shutil
import json
import random
import sys
import Levenshtein
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


SCOPES = ['https://www.googleapis.com/auth/spreadsheets',  'https://www.googleapis.com/auth/drive']

def read_config():
        return json.loads(config_file.read())

config = read_config()

def get_db_connection():
    return mysql.connector.connect(user=config['mysql']['user'], password=config['mysql']['pwd'],
                              host=config['mysql']['host'],
                              database=config['mysql']['db'])

    
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

def call_sheets_api(service, spreadsheet_id):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range='main').execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        return values

def generate_TB_internal_barcode():
    barcode = None
    while(1):
        barcode = 'TB' + str(random.randint(100,999)) + '-' + str(random.randint(10000,99999))
        cnx = get_db_connection()
        cursor = cnx.cursor()
        cmd = f"SELECT COUNT(*) FROM InternalBarcode WHERE internalBarcodeId = '{barcode}'"
        cursor.execute(cmd)
        res = None
        try:
            res = cursor.fetchone()[0]
        except:
            res = None
        cursor.close()
        cnx.close()
        if res == 0:
            break
    return barcode


def check_unique_patient():
    cnx = get_db_connection()
    query = f"SELECT patientId from Patient"
    cursor = cnx.cursor()
    cursor.execute(query)
    patient_ids = [patient_id[0] for patient_id in cursor]
    return patient_ids

def patient_search(line):
    cur_ref = {}
    cur_ref['family'] = line[2];
    cur_ref['given'] = line[3];
    cur_ref['add'] = line[4];
    cur_ref['dob'] = line[5];
    for key in cur_ref:
        cur_ref[key] = str(cur_ref[key])
        if len(cur_ref[key]) < 1:
            cur_ref[key] = None
        if (cur_ref[key][-1] == '.'):
            cur_ref[key] = cur_ref[key][:-1]
    cnx = get_db_connection()
    query = f"SELECT patientid, patientGivenName, patientFamilyName, patientAddName, patientDOB FROM Patient"
    cursor = cnx.cursor()
    cursor.execute(query)
    patients = cursor.fetchall()
    pts = []
    for pt in patients:
        cur = cur_ref.copy()
        ref = {}
        ref['given'] = pt[1]
        ref['family'] = pt[2]
        ref['add'] = pt[3]
        ref['dob'] = pt[4]
        for key in ref:
            if ref[key] is None:
                continue
            else:
                ref[key] = str(ref[key])
            if len(ref[key]) < 1:
                ref[key] = None
            if (ref[key][-1] == '.'):
                ref[key] = ref[key][:-1]
            if (str(ref[key]).upper() == 'NULL'):
                ref[key] = None
        for key in ('given', 'family', 'add'):
            if (ref[key] is None)or(cur[key] is None):
                ref[key] = None
                cur[key] = None
                continue
            ref[key] = ref[key].upper()
            cur[key] = cur[key].upper()
            if (len(ref[key]) == 1) or (len(cur[key]) == 1):
                ref[key] = ref[key][0]
                cur[key] = cur[key][0]
        match = 0
        if (ref['family'] is not None) and (cur['family'] is not None):
            if ref['dob'] is not None and cur['dob'] is not None:
                match = 1
            elif ref['given'] is not None and cur['given'] is not None and ref['add'] is not None and cur['add'] is not None:
                match = 1
        for key in ('family', 'given', 'add'):
            if (ref[key] is not None) and (cur[key] is not None):
                if len(ref[key]) == 1 and ref[key] != cur[key]:
                    match = 0
                if len(ref[key]) > 1 and (Levenshtein.distance(ref[key], cur[key])/len(ref[key])) > 0.2:
                    match = 0
        if ref['dob'] is not None and cur['dob'] is not None:
            if Levenshtein.distance(ref['dob'], cur['dob']) > 1:
                match = 0

        if match == 1:
            pts.append(pt[0])
    return(pts)
    

def generate_patient_id():
    patient_id = None
    existing_ids = check_unique_patient()
    while patient_id is None or patient_id in existing_ids:
           patient_id = random.randrange(10000, 99999)
    return patient_id

def insert_to_patient(line, patient_Id):
        cnx = get_db_connection()
        cursor = cnx.cursor()
        data = {}
        data['pFN'] = line[2]
        data['pGN'] = line[3]
        data['pAN'] = line[4]
        data['pDOB'] = line[5]
        data['pSex'] = line[6]
        for key in data:
            if (data[key] == '' or data[key] is None):
                data[key] = 'NULL'
            else:
                data[key] = f"'{data[key]}'"

        query = f"INSERT INTO Patient (patientId, patientFamilyName, patientGivenName, patientAddName, patientDOB, sexId) VALUES ({patient_id}, {data['pFN']}, {data['pGN']}, {data['pAN']}, {data['pDOB']}, {data['pSex']})"
        cursor.execute(query)
        cnx.commit()
        cursor.close()
        cnx.close()

def insert_patient_to_gdfile(patient_id, file_key):
         cnx = get_db_connection()
         cursor = cnx.cursor()
         query = f"INSERT INTO GDFile (patientId, fileKey, fileType) VALUES ({patient_id}, '{file_key}', 'folder')"  # ON DUPLICATE KEY UPDATE"
         cursor.execute(query)
         cnx.commit()
         cursor.close()
         cnx.close()


def create_patient_subfolder(patient_id, service):
    folder_metadata = {
        'name': patient_id,
        'parents': [folder_id],
        'mimeType': 'application/vnd.google-apps.folder'

    }
    folder = service.files().create(body=folder_metadata,
                                    fields='id').execute()
    print('Patient folder ID: %s' % folder.get('id'))
    return folder.get('id')

def calculate_case(patient_id):
    cnx = get_db_connection()
    query = f"SELECT caseid FROM `Case` WHERE patientid = {patient_id} ORDER BY caseid DESC LIMIT 1"
    cursor = cnx.cursor()
    cursor.execute(query)
    last_case_query_result = cursor.fetchone()
    if last_case_query_result is not None:
        last_case_id = last_barcode_query_result[0]
    else:
        last_case_id = 0
    case_id =  '0' + str(int(last_case_id) + 1) if int(last_case_id) < 9 else str(int(last_case_id) + 1)
    print('CASE_ID', case_id)
    return case_id

def insert_to_case(case_id, patient_id, caseType):
         cnx = get_db_connection()
         cursor = cnx.cursor()
         query = f"INSERT INTO `Case` (caseId, patientId, caseType) VALUES ('{case_id}', {patient_id}, '{caseType}')"  # ON DUPLICATE KEY UPDATE"
         cursor.execute(query)
         cnx.commit()
         cursor.close()
         cnx.close()

def insert_case_to_gdfile(patient_id, file_key, case_id):
         cnx = get_db_connection()     
         cursor = cnx.cursor()
         query = f"INSERT INTO GDFile (caseName, fileKey, fileType) VALUES ('{patient_id}-{case_id}', '{file_key}', 'folder')"  # ON DUPLICATE KEY UPDATE"
         cursor.execute(query)
         cnx.commit()
         cursor.close()
         cnx.close()

def insert_to_barcode(internal_barcode_id, patient_id, case_id):
         cnx = get_db_connection()
         cursor = cnx.cursor()
         current_date = dt.date.today().strftime('%Y-%m-%d')
         query = f"INSERT INTO InternalBarcode (internalBarcodeId, caseId, patientId, applicationDate) VALUES ('{internal_barcode_id}', '{case_id}', '{patient_id}', '{current_date}')"  # ON DUPLICATE KEY UPDATE"
         cursor.execute(query)
         cnx.commit()
         cursor.close()
         cnx.close()

def create_barcode_subfolder(barcode_id, patient_subfolder_id, service):
    folder_metadata = {
        'name': barcode_id,
        'parents': [patient_subfolder_id],
        'mimeType': 'application/vnd.google-apps.folder'

    }
    folder = service.files().create(body=folder_metadata,
                                    fields='id').execute()
    print('Barcode folder ID: %s' % folder.get('id'))
    return folder.get('id')

def add_subfolders_to_barcode(barcode_subfolder_id, service):
    folder_metadata = {
        'name': 'Test',
        'parents': [barcode_subfolder_id],
        'mimeType': 'application/vnd.google-apps.folder'

    }
    folder = service.files().create(body=folder_metadata,
                                    fields='id').execute()
    print('Test folder ID: %s' % folder.get('id'))
    folder_metadata = {
        'name': 'Report',
        'parents': [barcode_subfolder_id],
        'mimeType': 'application/vnd.google-apps.folder'

    }
    folder = service.files().create(body=folder_metadata,
                                    fields='id').execute()
    print('Report folder ID: %s' % folder.get('id'))
    folder_metadata = {
        'name': 'Info',
        'parents': [barcode_subfolder_id],
        'mimeType': 'application/vnd.google-apps.folder'

    }
    folder = service.files().create(body=folder_metadata,
                                    fields='id').execute()
    print('Info folder ID: %s' % folder.get('id'))


def add_requisition(barcode_subfolder_id, sheets_service, drive_service):
    template_version = config['drive']['files']['requisition_template']['latest']
    template_id = config['drive']['files']['requisition_template'][template_version]['key']
    spreadsheet_id = create_spreadsheet_in_barcode(barcode_subfolder_id, drive_service)
    sheet_id = copy_from_main_to_requisition(sheets_service, spreadsheet_id, template_id)
    delete_base_sheet(sheets_service, spreadsheet_id)
    rename_sheet(sheets_service, spreadsheet_id, 'Requisition', sheet_id)
    return spreadsheet_id
    

def add_sheet_to_requisition(service, sheet_id):
    sheet = service.spreadsheets()
    print("Sheet id", sheet_id)
    request_body = {
            "requests": {
                "add_sheet": {
                    "properties": {
                            "title": "Requisition"
                     }
                }
            }
        } 
    res = sheet.batchUpdate(spreadsheetId=sheet_id, body=request_body).execute()
    print(res)


def copy_from_main_to_requisition(service, spreadsheet_id, template_id):
    print(service, spreadsheet_id)
    request_body = { 'destinationSpreadsheetId': spreadsheet_id }
    res = service.spreadsheets().sheets().copyTo(spreadsheetId=template_id, sheetId=0, body=request_body).execute()
    return res['sheetId']

def rename_sheet(service, spreadsheet_id, new_name, sheet_id):
    requests = {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "title": new_name,
                },
                "fields": "title",
            }
        }

    body = {
            'requests': requests
    }
    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

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

def insert_template_to_gdfile(patient_id, case_id, file_id):
         template_version = config['drive']['files']['requisition_template']['latest']
         cnx = get_db_connection()
         cursor = cnx.cursor()
         query = f"INSERT INTO GDFile (caseName, fileKey, fileType, dataType, templateVersion) VALUES ('{patient_id}-{case_id}', '{file_id}', 'spreadsheet', 'requisition', '{template_version}')"  # ON DUPLICATE KEY UPDATE"
         cursor.execute(query)
         cnx.commit()
         cursor.close()
         cnx.close()

def create_spreadsheet_in_barcode(barcode_subfolder_id, service, surname = ''):
    file_metadata = {
        'name': 'Направление ' + surname,
        'parents': [barcode_subfolder_id],
        'mimeType': 'application/vnd.google-apps.spreadsheet',

    }
    file = service.files().create(body=file_metadata,
                                    fields='id').execute()
    print('Spreadsheet created')
    sheet_id = file.get('id')
    print('Spreadsheet ID ', sheet_id)
    return sheet_id

def update_cell(service, range_name, value, spreadsheet_id, hyperlink_flag = False, valueInputOption = 'RAW'):
    if hyperlink_flag is False:
        cells = [
            [
                value
            ]
        ]
        body = {
            'values': cells
        }
    else:
        hyperlink = f"https://drive.google.com/drive/u/0/folders/{value['folder_id']}"
        text = value['barcode_id']

        body = {
                 'values': [[
                            {
                                'userEnteredValue': {'formulaValue': f"=HYPERLINK({hyperlink},{text})"}
                            }
                          ]]
              }
    print(range_name, value, body)
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption=valueInputOption,
        body=body
    ).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))

def update_cell_with_hyperlink(service, range_name, value, spreadsheet_id, hyperlink_flag = False):
    request = {
      "rows": [
        {
            'values': [
                        {
                            'userEnteredValue': {'formulaValue': f"=HYPERLINK({hyperlink},{text})"}
                        }
                      ]
        }
      ],
      "fields": "userEnteredValue",
      "range": {
        object (GridRange)
      }
    }
    print(range_name, value, body)
    result = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))

def get_range_by_number(row, col_num):
        row += 1
        col_num += 1
        col_chr =  chr(64 + col_num)
        range_name = str(col_chr) + str(row)
        print(range_name)
        return range_name

def add_link_to_folder():
    pass

def check_if_patient_exists():
    pass

sheets_service = get_sheets_service()
drive_service = get_drive_service()
values = call_sheets_api(sheets_service, spreadsheet_id)
for number, line in enumerate(values):
    patient_id = None
    if (line[0] == 'add' or line[0] == 'add-force'):
        if (len(line) > 7 and str(line[7]).upper() in ('RISK','CDX','ONCOTYPE','TB','THYROSEQ','NA')):
            pass
        else:
            update_cell(sheets_service, get_range_by_number(number, 0), 'FAIL: unknown or empty case type', spreadsheet_id)
            continue
        #check_if_patient_exists
        patient_id = generate_patient_id()
        if line[0] == 'add':
            pts_found = patient_search(line)
            print(pts_found)
            if len(pts_found) > 0:
                pts_found = ', '.join([str(item) for item in pts_found])
                update_cell(sheets_service, get_range_by_number(number, 0), f"FAIL: found matching patients (patient id: {pts_found}). Now use either 'add-force' to add the following patient anyway or 'add <patient id>' to add case to existing patient", spreadsheet_id)
                continue
        print(patient_id)
        insert_to_patient(line, patient_id)
        patient_subfolder_id = create_patient_subfolder(patient_id, drive_service)
        insert_patient_to_gdfile(patient_id, patient_subfolder_id)
        correct_line = True 
    elif re.search(r"add\s\d{5}", line[0]) is not None:
        #check_if_patient_exists
        patient_id = re.search(r"\d{5}", line[0]).group(0)
        print(patient_id)
        correct_line = True
    if patient_id is not None and correct_line is True:
        case_id = calculate_case(patient_id)
        insert_to_case(case_id, patient_id, line[7])
        internal_barcode_id = line[1] #проверка
        if (str(line[7]).upper() == 'TB'):
            internal_barcode_id = generate_TB_internal_barcode()
            range_name = get_range_by_number(number, 1)
            update_cell(sheets_service, range_name, internal_barcode_id, spreadsheet_id)
        insert_to_barcode(internal_barcode_id, patient_id, case_id)
        barcode_subfolder_id = create_barcode_subfolder(internal_barcode_id, patient_subfolder_id, drive_service)
        range_name = get_range_by_number(number, 8)
        update_cell(sheets_service, range_name, '=HYPERLINK("https://drive.google.com/drive/u/0/folders/' + barcode_subfolder_id + '";"' + str(patient_id) + '")', spreadsheet_id, hyperlink_flag = False, valueInputOption = 'USER_ENTERED')
        #update_cell(sheets_service, range_name, {'barcode_id': internal_barcode_id, 'folder_id': barcode_subfolder_id}, spreadsheet_id, hyperlink_flag = True) 
        #add_link_to_folder(sheets_service, barcode_subfolder_id)
        insert_case_to_gdfile(patient_id, barcode_subfolder_id, case_id)
        add_subfolders_to_barcode(barcode_subfolder_id, drive_service)
        if (str(line[7]).upper() == 'CDX'):
            requisition_id = add_requisition(barcode_subfolder_id, sheets_service, drive_service)
            insert_template_to_gdfile(patient_id, case_id, requisition_id)
        range_name = get_range_by_number(number, 0)
        update_cell(sheets_service, range_name, 'done', spreadsheet_id)

# TODO Точный поиск существующего пациента
