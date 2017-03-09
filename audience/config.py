"""
config -- credentials and paths
"""
import os

# base directory

_basedir = os.path.abspath(os.path.dirname(__file__))

# database information 

DATABASE_PATH = os.path.join(_basedir, "customers.db")
DATABASE_SCHEMA = os.path.join(_basedir, "customers.sql")

# ftp credentials

FTP_HOST = None
FTP_USER = None
FTP_PASSWORD = None
FTP_DIR = None

# facebook credentials

APP_ID = None
APP_SECRET = None
ACCESS_TOKEN = None
SITE_ID = None
TESTING_SITE_ID = None

# client-specific variables

CURRENT = None
LAPSED = None
EXTRA = None

# If testing, mark as True

DEBUG = False














