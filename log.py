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
data = {}
data['pFN'] = ''
data['pGN'] = 'asd'
data['pAN'] = None
data['pDOB'] = ''
data['pSex'] = ''

for key in data:
    if (data[key] == '' or data[key] is None):
        data[key] = 'NULL'
    else:
        data[key] = f"'{data[key]}'"
    print(data[key])

