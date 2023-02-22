'''
This script detects the language from all chat data present in twitch.chat and twitch.vod_chat
'''

import cx_Oracle
import argparse
import time
import yaml
from datetime import datetime
from oci.config import from_file
import oci
from collections import Counter
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
import os
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)


# wallet location (default is HOME/wallets/wallet_X)
os.environ['TNS_ADMIN'] = '{}/{}'.format(home, process_yaml()['WALLET_DIR'])
print(os.environ['TNS_ADMIN'])


parser = argparse.ArgumentParser()
parser.add_argument('-l', '--language', help='Language detection method', choices=['oracle_service', 'langdetect'], required=True)
parser.add_argument('-o', '--offset', help='Offset from where to start to fetch rows', required=False)
parser.add_argument('-b', '--base', help='Number of rows to fetch upon execution', required=False)
parser.add_argument('-t', '--table', help='Table to process', choices=['twitch.chat', 'twitch.vod_chat'], required=False)
args = parser.parse_args()




def init_oracle_lang_service():
	config = from_file()
	# Initialize service client with default config file
	ai_language_client = oci.ai_language.AIServiceLanguageClient(config)
	return ai_language_client


def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



def detect_language_oracle(ai_language_client, cursor, tables_to_process, base=None, offset=None):
	assert type(tables_to_process) == type(list())

	for i in tables_to_process:
		sql_get_tweets = str()
		if not offset and not base:
			sql_get_tweets = """select distinct message from {} where language='UNKNOWN'""".format(i)
		else:
			sql_get_tweets = """select distinct message from {} where language='UNKNOWN' OFFSET {} ROWS FETCH NEXT {} ROWS ONLY""".format(i, offset, base)
		try:
			print(sql_get_tweets)
			cursor.execute(sql_get_tweets)
		except Exception as e:
			print('@{}: {}'.format(detect_language_oracle.__name__, e))
		result = cursor.fetchall()
		iterator = -1
		print('Obtained {} results to process in table {}'.format(len(result), i))
		for x in result:
			iterator += 1
			s_data = str()
			# We can only process texts with more than 10 unique words. So let's get rid of the smaller texts altogether.
			if len(set(x[0].split(' '))) < 10:
				continue
			try:
				detect_language_sentiments_response = ai_language_client.detect_dominant_language(
				detect_dominant_language_details=oci.ai_language.models.DetectDominantLanguageDetails(
				text=x[0]))
				s_data = detect_language_sentiments_response.data
			except oci.exceptions.ServiceError as e:
				print(e)
				continue
			except AttributeError:
				# In this case, we didn't get a proper response.
				continue

			row = ['{}'.format(s_data.languages[0].code), x[0]]
			print('Processing {}'.format(row))
			print('Currently on row: {}/{}; Currently iterated {}% of rows in table {}'.format(iterator, len(result), (iterator + 1)/len(result) * 100, i))
			cursor.execute("update {} set language = :1 where message = :2".format(i), row)



def detect_language_langdetect(cursor, tables_to_process, base=None, offset=None):
	assert type(tables_to_process) == type(list())

	for i in tables_to_process:
		sql_get_tweets = str()
		if not offset and not base:
			sql_get_tweets = """select distinct message from {} where language='UNKNOWN'""".format(i)
		else:
			sql_get_tweets = """select distinct message from {} where language='UNKNOWN' OFFSET {} ROWS FETCH NEXT {} ROWS ONLY""".format(i, offset, base)
		try:
			print(sql_get_tweets)
			cursor.execute(sql_get_tweets)
		except Exception as e:
			print('@{}: {}'.format(detect_language_langdetect.__name__, e))
		result = cursor.fetchall()
		iterator = -1
		print('Obtained {} results to process in table {}'.format(len(result), i))
		for x in result:
			iterator += 1
			try:
				detect(x[0])
			except LangDetectException:
				print('[DBG] SKIP {}'.format(x[0]))
				continue
			row = [detect(x[0]), x[0]]
			print('Processing {}'.format(row))
			print('Currently on row: {}/{}; Currently iterated {}% of rows in table {}'.format(iterator, len(result), (iterator + 1)/len(result) * 100, i))
			cursor.execute("update {} set language = :1 where message = :2".format(i), row)



def main():
	data = process_yaml()
	connection = init_db_connection(data)
	cursor = connection.cursor()

	tables_to_process = str()
	if not args.table:
		tables_to_process = ['twitch.chat', 'twitch.vod_chat']
	else:
		tables_to_process = ['{}'.format(args.table)]

	if args.language == 'oracle_service':
		ai_language_client = init_oracle_lang_service()
		if not args.offset and not args.base:
			detect_language_oracle(ai_language_client, cursor, tables_to_process)
		else:
			detect_language_oracle(ai_language_client, cursor, tables_to_process, args.base, args.offset)
	elif args.language == 'langdetect':
		if not args.offset and not args.base:
			detect_language_langdetect(cursor, tables_to_process)
		else:
			detect_language_langdetect(cursor, tables_to_process, args.base, args.offset)

	connection.close()



if __name__ == '__main__':
	main()



#url = 'https://www.youtube.com/watch?v=5qap5aO4i9A' # youtube URL example
