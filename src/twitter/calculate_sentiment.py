import cx_Oracle
import yaml
from oci.config import from_file
import oci
import argparse
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
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
parser.add_argument('-l', '--language-mode', help='Language mode, either online or offline', choices=['online', 'offline'], required=True)
parser.add_argument('-o', '--offset', help='Offset from where to start to fetch rows', required=False)
parser.add_argument('-b', '--base', help='Number of rows to fetch upon execution', required=False)
parser.add_argument('-t', '--table', help='Table to process' , required=False)
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



def execute_table_processing(ai_language_client, cursor, tables_to_process, base=None, offset=None):
	assert type(tables_to_process) == type(list())
	for i in tables_to_process:
		variable_name = str()
		if i in ['twitch.chat', 'twitch.vod_chat']:
			variable_name = 'message'
		elif i in ['public_sentiment', 'user_sentiment']:
			variable_name = 'tweet'
		sql_get_tweets = str()
		if not offset and not base:
			sql_get_tweets = """select {} from {} where sentiment = '-1' and how_positive = '-1' and how_neutral = '-1' and how_negative = '-1' and language='en'""".format(variable_name, i)
		else:
			sql_get_tweets = """select {} from {} where sentiment = '-1' and how_positive = '-1' and how_neutral = '-1' and how_negative = '-1' and language='en' OFFSET {} ROWS FETCH NEXT {} ROWS ONLY""".format(variable_name, i, offset, base)
		try:
			print(sql_get_tweets)
			cursor.execute(sql_get_tweets)
		except Exception as e:
			print('@{}: {}'.format(execute_table_processing.__name__, e))
		result = cursor.fetchall()
		iterator = 0
		print('Obtained {} results to process in table {}'.format(len(result), i))
		for x in result:
			s_data = str()
			try:
				detect_language_sentiments_response = ai_language_client.detect_language_sentiments(
					detect_language_sentiments_details=oci.ai_language.models.DetectLanguageSentimentsDetails(
					text=x[0])
				)
				s_data = detect_language_sentiments_response.data
			except oci.exceptions.ServiceError as e:
				print(e)
			f_pos = float()
			f_neu = float()
			f_neg = float()
			final_sentiment = float()
			try:
				overall_positive = list()
				overall_neutral = list()
				overall_negative = list()
				for y in s_data.aspects:
					overall_positive.append(y.scores.get('Positive'))
					overall_neutral.append(y.scores.get('Neutral'))
					overall_negative.append(y.scores.get('Negative'))
				f_pos = sum(overall_positive) / len(overall_positive)
				f_neu = sum(overall_neutral) / len(overall_neutral)
				f_neg = sum(overall_negative) / len(overall_negative)
				final_sentiment = max(f_pos, f_neu, f_neg)
				if final_sentiment == f_neg: 
					final_sentiment = -f_neg # put it in negative
			except AttributeError:
				iterator += 1
				continue
			except ZeroDivisionError:
				f_pos = 0
				f_neu = 1
				f_neg = 0
				final_sentiment = 0

			row = [str(final_sentiment), str(f_pos), str(f_neu), str(f_neg), x[0]]
			print('Processing {}'.format(row))
			print('Currently on row: {}/{}; Currently iterated {}% of rows in table {}'.format(iterator, len(result), (iterator + 1)/len(result) * 100, i))
			cursor.execute("update {} set sentiment = :1, how_positive = :2, how_neutral = :3, how_negative = :4 where tweet = :5".format(i), row)
			iterator += 1



def execute_offline_processing(cursor, tables_to_process, base=None, offset=None):
	sia = SentimentIntensityAnalyzer()
	assert type(tables_to_process) == type(list())
	for i in tables_to_process:
		variable_name = str()
		if i in ['twitch.chat', 'twitch.vod_chat']:
			variable_name = 'message'
		elif i in ['public_sentiment', 'user_sentiment']:
			variable_name = 'tweet'
		sql_get_tweets = str()
		if not offset and not base:
			#sql_get_tweets = """select {} from {} where sentiment = '-1' and how_positive = '-1' and how_neutral = '-1' and how_negative = '-1' and language='en'""".format(variable_name, i)
			sql_get_tweets = """select {} from {} where sentiment = '-1' and how_positive = '-1' and how_neutral = '-1' and how_negative = '-1' and language='en' FETCH FIRST 50000 ROWS ONLY""".format(variable_name, i)
		else:
			sql_get_tweets = """select {} from {} where sentiment = '-1' and how_positive = '-1' and how_neutral = '-1' and how_negative = '-1' and language='en' OFFSET {} ROWS FETCH NEXT {} ROWS ONLY""".format(variable_name, i, offset, base)
		try:
			print(sql_get_tweets)
			cursor.execute(sql_get_tweets)
		except Exception as e:
			print('@{}: {}'.format(execute_offline_processing.__name__, e))
		result = cursor.fetchall()
		iterator = 0
		print('Obtained {} results to process in table {}'.format(len(result), i))
		for x in result:
			analysis = sia.polarity_scores(x[0])
			row = [analysis.get('compound'), analysis.get('pos'), analysis.get('neu'), analysis.get('neg'), x[0]]
			print('Processing {}'.format(row))
			print('Currently on row: {}/{}; Currently iterated {}% of rows in table {}'.format(iterator, len(result), (iterator + 1)/len(result) * 100, i))
			cursor.execute("update {} set sentiment = :1, how_positive = :2, how_neutral = :3, how_negative = :4 where {} = :5".format(i, variable_name), row)
			iterator += 1



def main():
	tables_to_process = list()
	data = process_yaml()
	connection = init_db_connection(data)
	# Get cursor.
	cursor = connection.cursor()

	if not args.table:
		tables_to_process = ['public_sentiment', 'user_sentiment', 'tweets']
	else:
		tables_to_process = ['{}'.format(args.table)]
	

	if args.language_mode == 'online':
		ai_language_client = init_oracle_lang_service()
		if not args.offset and not args.base:
			execute_table_processing(ai_language_client, cursor, tables_to_process)
		else:
			execute_table_processing(ai_language_client, cursor, tables_to_process, args.base, args.offset)
	elif args.language_mode == 'offline':
		if not args.offset and not args.base:
			execute_offline_processing(cursor, tables_to_process)
		else:
			execute_offline_processing(cursor, tables_to_process, args.base, args.offset)

	connection.close()



if __name__ == '__main__':
	main()
