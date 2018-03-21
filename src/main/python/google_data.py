#!/usr/bin/env/python 2
# This script gets the published date from Google Books API
# by passing the title and author as input. The output publish date 
# is verified against the author's birth and death date. After 
# verification, the published date is updated on the book's document
# in elastic search
# Author: Amit Nandan Periyapatna (periy003@umn.edu)
# Created: 15th March, 2018
# Updated: 15th March, 2018

import requests, urllib
import os, json
import pandas as pd
import traceback
from datetime import datetime
from elasticsearch import Elasticsearch
# Google Books API Keys: 
# AIzaSyBYF2xU3IR3vmzmUBFUzD6ss_FMNls4DL4 - Amit	  
# AIzaSyB02Bwa_lqrTevZjcs9Y9SwywuWOh0AOoY - Amit
# AIzaSyDUbmbH1j3GcH9U3wrWVDtBaqgr4aNGAcc - Max
# AIzaSyBi4C6enT0GG-IdE1g3Z6LpC0zxPTdR7-0 - Calvin
# AIzaSyBigx7sFaGYKUzmTyd-015DjqPeBC7y90E - Calvin
# AIzaSyB19gc7odN-AcQylCiUm-HlA05-bWLdx0U - Calvin
# AIzaSyCilj5-BnSNTm0eLBVJCTfwr-Ju6oCGFGk - Amit
API_KEY = '&key=AIzaSyBYF2xU3IR3vmzmUBFUzD6ss_FMNls4DL4'
API_URL = 'https://www.googleapis.com/books/v1/volumes?q='
JSON_DIR = 'json_dir_books_we_have/'
LAST_READ_FILE = 'last_read_files.txt'
UTF_8 = 'utf8'
READ_MODE = 'r'
INTITLE = 'intitle:'
INAUTHOR = 'inauthor:'
PLUS = '+'
DATE_FORMATS = ['%Y', '%Y-%m-%d', '%Y-%d-%m']

# Get all JSON files in the json directory
# Input: JSON DIRECTORY
# Output: Data Frame containing all the files
def get_json_files(json_dir):
	json_files = json_files = [pos_json for pos_json in os.listdir(json_dir) if pos_json.endswith('.json')]
	return json_files


# Get the remaining files to be parsed in the directory
# Input: The file containing the last read entity
# Output: Data frame containing the remaining files to be read
def get_remaining_files(last_read_file):
	last_read = get_latest_run_index(last_read_file)
	remaining_files = get_json_files(JSON_DIR)
	return remaining_files[int(last_read):]

def get_latest_run_index(last_read_file):
	file_data = open(last_read_file, READ_MODE)
	value = file_data.read()
	print(value)
	file_data.close()
	value = value.replace("\n","")
	print(value)
	return value

def replace_spaces_with_plus(value):
	return value.replace(" ", "+")

def enclose_double_quotes(value):
	return '"' + value + '"'

# Access the Google Books API to get the Publish date of a book. 
# Input: Title and Author of a Book
# Output: Publish Year or None
def get_publish_date(title, author):
	title = replace_spaces_with_plus(title)
	title = enclose_double_quotes(title)
	#if author is None or author == 'Various':
	url = urllib.quote(API_URL.encode(UTF_8) + INTITLE + title.encode(UTF_8) + API_KEY, safe="%/:=&?~#+!$,;'@()*[]")
	# else:
	# 	author = replace_spaces_with_plus(author)
	# 	author = enclose_double_quotes(author)
	# 	url = urllib.quote(API_URL.encode(UTF_8) + INTITLE + title.encode(UTF_8) + PLUS + INAUTHOR + author.encode(UTF_8) + API_KEY, safe="%/:=&?~#+!$,;'@()*[]")
	req = requests.get(url)
	reply = req.json()
	print(url)
	if int(reply["totalItems"]) == 1:
		if 'publishedDate' in reply["items"][0]["volumeInfo"]:
			return get_least_year(reply["items"][0]["volumeInfo"]["publishedDate"])
		else: 
			return None
	elif int(reply["totalItems"]) > 1:
		published_dates = []
		for item in reply["items"]:
			if 'publishedDate' in item["volumeInfo"]:
				published_dates.append(item["volumeInfo"]["publishedDate"])
				return get_least_year(published_dates)
			else:
				return None
	else:
		return None

# If the Google Books API returns many publish dates, get the least publish year
# Input: List of publish dates returned by Google Books API
# Output: Least of the publish years
def get_least_year(published_dates):
	published_year = []
	for published_date in published_dates:
		published_year.append(get_year_from_date_string(published_date))
	return min(published_year)

# Get the year from date string
# Input: Date as string
# Output: Year
def get_year_from_date_string(date_string):
	date_format = get_date_format(date_string)
	if date_format:
		dt = datetime.strptime(date_string, date_format)
		return dt.year
	else:
		return 9999

# Check if the date format is valid
# Input: Date String
# Output: Format
def get_date_format(date_string):
	format_ok = False
	for mask in DATE_FORMATS:
		try:
			datetime.strptime(date_string, mask)
			format_ok = True
			break
		except ValueError:
			pass

	if format_ok:
		return mask
	else:
		return None

# Run the script to iterate through all the JSON metadata files in our file system
# Input: none
# Output: none
def bulk_get_publish_date():
	end = 950
	index = 0
	previous_index = int(get_latest_run_index(LAST_READ_FILE))
	try:
		for json_file in get_remaining_files(LAST_READ_FILE):
			if(index >= end):
				break
			temp_json = json.load(open(JSON_DIR+json_file))
			title = temp_json["title"]
			author = temp_json["author"]

			published_date = get_publish_date(title, author)
			print("Publish year of " + str(temp_json["id"]) +  " is: " + str(published_date))	
			published_date = validate_and_return_publish_date(temp_json["authoryearofbirth"], temp_json["authoryearofdeath"], published_date)
			print(str(temp_json["authoryearofbirth"]) + " " + str(temp_json["authoryearofdeath"]) + " " + str(published_date))
			update_elastic_document(temp_json["id"], "publishedDate", published_date)
			index = index + 1
	except TypeError as e:
		print("Type Error: {0}".format(e))
		traceback.print_exc()
		final_index = index + previous_index
		write_last_read_to_file(LAST_READ_FILE, final_index)
		exit()
	finally:
		final_index = index + previous_index
		write_last_read_to_file(LAST_READ_FILE, final_index)

def write_last_read_to_file(last_read_file, index):
	with open(LAST_READ_FILE, 'w') as last_read_file:
			print("Total files parsed through", index)
			last_read_file.write(str(index))

# Validate that the published date is inbetween the author's birth and death date
# Input: Author Birth Date, Author Death Date, Publish Date from Google Books API
# Output: True or False
def validate_and_return_publish_date(author_birth_date, author_death_date, published_date):
	if author_birth_date is None and author_death_date is None:
		if published_date is None:
			return 9999
		else:
			return published_date
	elif author_birth_date is None: 
		if published_date <= author_death_date + 50:
			return published_date
		else:
			return author_death_date - 30
	elif author_death_date is None:
		if published_date != 9999 and published_date > author_birth_date:
			return published_date
		else:
			return author_birth_date + 30
	elif author_birth_date < published_date <= author_death_date + 50:
		return published_date
	elif not(author_birth_date is None) and not(author_death_date is None):
		return int((author_birth_date + author_death_date)/2)
	else:
		return 9999

# Update document on elastic search
# Input: Document ID, paramater to be updated, Value of the paramater
# Output: None
def update_elastic_document(id, param, value):
	es = Elasticsearch(['http://localhost:9200'], http_auth=('elastic', 'MSSE2018'), scheme="http")
	es_index = 'books'
	es_doc_type = 'book'
	es.update(index=es_index, doc_type=es_doc_type, id=id, body={"doc": {param: value}})

