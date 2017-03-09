from . import models 

import io
import os
import ftplib
import subprocess

from openpyxl import load_workbook
from datetime import datetime, timedelta, date


def write_database(config):
	""" Write the customer datbase using a sql schema file.
	If database exists, do nothing.
	
	:params config: config module, application configuration module
	"""
	if not os.path.isfile(config.DATABASE_PATH):
		file = open(config.DATABASE_PATH, 'w')
		file.close()
		database, schema = config.DATABASE_PATH, config.DATABASE_SCHEMA
		subprocess.call('sqlite3 "{}" < "{}"'.format(database, schema), shell=True)


def process_csv_bytestring(file_object, file_date=None):
	""" Read a io.BytesIO object and return an import-ready
	list of dictionaries.

	:params file_object: io.BytesIO, file like object
	:params file_date: str, date in file

	return :: list of dictionaries
	"""
	store = []
	headers = []
	_str = str(file_object.getvalue()).split('\\r\\n')
	parse_out = ('b"', ' ', '-', "\\xef\\xbb\\xbf")
		
	for header in _str[0].split(','):
		parsed = header
		for char in parse_out:
			if char in (' ', '-'):
				parsed = parsed.replace(char, '_')
			else:
				parsed = parsed.replace(char, '')
		headers.append(parsed.lower())
	
	for item in _str[1:]:
		record = item.split(',')
		if len(record) == len(headers):
			record = dict(zip(headers, record))
			try:
				order_date = datetime.strptime(record['last_order_date'],
											   '%m/%d/%Y').date()
			except ValueError:
				order_date = datetime.strptime(record['last_order_date'],
											   '%Y-%m-%d').date()
			record['last_order_date'] = order_date.isoformat()
			record['segment'] = return_segment(order_date)
			record['record_create_date'] = date.today().isoformat()
			record['file_parse_date'] = file_date
			if len(record) > 11:
				del record['address_2']
				del record['address_1']
				del record['state']
				del record['post_code']
				del record['country']
				del record['city']
			store.append(record)

	return store


def process_xlsx_bytestring(file_obj):
	""" Read a io.BytesIO object and return an import-ready
	list of dictionaries.

	:params file_object: io.BytesIO, file like object

	return :: list of dictionaries
	"""
	store = []
	wb = load_workbook(filename=file_obj, read_only=True)
	data = [[cell.value for cell in r] for r in wb['Sheet1'].rows]
	headers = [header.replace('-', '_').replace(' ', '_').lower()
			  for header in data[0]]

	for row in data[1:]:
		record = dict(zip(headers, row))
		if record['last_order_date'] is None:
			continue
		record['last_order_date'] = record['last_order_date'].date()
		record['segment'] = return_segment(record['last_order_date'])
		record['record_create_date'] = date.today()
		record['file_parse_date'] = '1900-01-01' # Arbitrary old date.
		store.append(record)

	return store


def stream_ftp(config, keyword="vendor"):
	""" Connect to specified FTP server, stream file(s) into
	file-like objects and return a list or single file-like object.

	:params config: module, contains all relevant variables
	:params keyword: str, keyword to filter files

	return :: list of dictionaries containing records
	"""
	store = []
	preprocess = []
	csvs = []
	xlsxs = []
	with ftplib.FTP(config.FTP_HOST) as ftp:
		ftp.login(config.FTP_USER, config.FTP_PASSWORD)
		ftp.cwd(config.FTP_DIR)
		for name in ftp.nlst(): 
			if ('_' in name or '-' in name) and keyword in name.lower():
				preprocess.append(name)
		for file in preprocess:
			if '_' in file:
				csvs.append((datetime.strptime(file.split('_')[1].replace('.csv',''),'%Y%m%d').date(), file))
			else:
				xlsxs.append((int(file[-9:].replace('.xlsx','')), file))
		csvs.sort()
		xlsxs.sort()
		files = xlsxs+csvs
		for file in files:
			print('Processing File: {}'.format(file[1]))
			file_obj = io.BytesIO()
			ftp.retrbinary('RETR {}'.format(file[1]), file_obj.write)
			if 'xlsx' in file[1]:
				store.extend(process_xlsx_bytestring(file_obj))
			else:
				filedate = str(datetime.strptime(file[1].split('_')[1].split('.')[0],
                    '%Y%m%d').date())
				store.extend(process_csv_bytestring(file_obj, filedate))
	ftp.close()

	return store


def return_segment(file_date):
	""" Takes in a datetime.date object, compares and
	returns a string value of the segment.

	:params date: datetime.date, last order date

	return :: str, segment
	"""
	_today = date.today()
	_90_days = _today - timedelta(days=90)
	_2_years = _today - timedelta(days=730)

	if file_date < _today and file_date >= _90_days:
		return 'current'
	if file_date < _90_days and file_date >= _2_years:
		return 'lapsed'
	if file_date < _2_years:
		return 'extra lapsed'


def sqlite_import(table, data):
	""" Performs a REPLACE INTO or UPSERT given a table
	and list of dictionaries.

	:params table: str, name of table in database
	:params data: list, list of dictionaries containing records to be "pushed"
	"""
	model = models.__dict__[table]
	record_sets = data_generator(data)  # SQLite has SQL variable limits (999).

	with model._meta.database.atomic():
		for chunk in record_sets:
			model.insert_many(chunk, validate_fields=True).upsert(True).on_conflict(action='IGNORE').execute()


def sqlite_truncate(table):
	""" A redundant and transparent function for
	a SQL Truncate statement.

	:params table: str, table name from database
	"""
	model = models.__dict__[table]
	models.database.truncate_table(model)


def data_generator(data, size=90):
	""" A generator that chunks large sets of data.

	:params data: list, list of dictionaries containing records
	:params size: integer, number of records per chunk
	"""
	for step in range(0, len(data), size):
		try:
			yield data[step:step + size]
		except IndexError:
			yield data[step:len(data)]











	





	


