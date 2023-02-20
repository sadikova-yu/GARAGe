import os

class Config:
    MYSQL_SELF_HOST = os.getenv('MYSQL_SELF_HOST')
    MYSQL_SELF_PORT = os.getenv('MYSQL_SELF_PORT')
    MYSQL_SELF_DB = os.getenv('MYSQL_SELF_DB')
    MYSQL_SELF_USER = os.getenv('MYSQL_SELF_USER')
    MYSQL_SELF_PASS = os.getenv('MYSQL_SELF_PASS')