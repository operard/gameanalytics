# This file is used to reprocess any JSON file as input, which needs to be reprocessed. Backup is in Ignacio_M compartment in devreldev's Object Storage
import json 
import time 
import datetime 
import cx_Oracle
import yaml
from sys import argv
import pandas as pd
import os
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)



# wallet location (default is HOME/wallets/wallet_X)
os.environ['TNS_ADMIN'] = '{}/{}'.format(home, process_yaml()['WALLET_DIR'])
print(os.environ['TNS_ADMIN'])



def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



# auxiliary function to get hashtags from a string. (removes duplicates using set then converts to list)
def extract_hash_tags(s):
	return list(set(part[1:] for part in s.split() if part.startswith('#')))



# auxiliary function to convert from zulu time to datetime object
def convert_zulu(date_str):
	# remove the Z
	date_str = date_str[:-1]

	aux = date_str.split('T')
	first_part = aux[0] # years months and days
	second_part = aux[1] # hours minutes and second

	year = first_part.split('-')[0]
	month = first_part.split('-')[1]
	day = first_part.split('-')[2]


	hour = second_part.split(':')[0]
	minute = second_part.split(':')[1]
	second = second_part.split(':')[2][:2]

	#print(year, month, day, hour, minute, second) # testing
	#return year, month, day, hour, minute, second
	return datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), 0)



# Reprocess a specific user file in a date
# date format must be YYYYmmdd format.
def reprocess_user(connection, twitter_handle, handle_type, date_to_process = time.strftime('%Y%m%d')):
	print('+++ Processing user {} +++'.format(twitter_handle))

	f = open('../data/json/{}_{}.json'.format(twitter_handle, date_to_process) , 'r')
	res_json = json.loads(f.read())
	f.close()

	try:
		res_json.get('meta').get('result_count')
	except AttributeError:
		print('Problem with request. Exiting. Request: {}'.format(res_json))
		return
	# Only continue if the number of results is greater than 1.
	if res_json.get('meta').get('result_count') == 0:
		print('0 results for user {}'.format(twitter_handle))
		return 


	# We process the tweets to get only the texts.
	tweets = res_json.get('data')
	print('{}: Printing tweets for user {}: --------\n {}'.format(time.strftime("%H:%M:%S", time.localtime()), twitter_handle, tweets))
	cursor = connection.cursor()

	for tweet in tweets:
		# Check if there are URLs in the tweet.
		contains_urls_bool = False
		try:
			tweet.get('entities').get('urls')
			contains_urls_bool = True
		except Exception as e:
			pass

		# References other tweets? Lets find out.
		references_other_tweets = False
		try:
			tweet.get('referenced_tweets')
			references_other_tweets = True
		except Exception as e:
			pass
		

		possibly_sensitive = tweet.get('possibly_sensitive')
		tweet_creation_time = tweet.get('created_at')
		# Metrics
		public_metrics = tweet.get('public_metrics') # total number of interactions is the sum of all of the 4 categories.
		num_rts = public_metrics.get('retweet_count')
		num_likes = public_metrics.get('like_count')
		num_replies = public_metrics.get('reply_count')
		num_quotes = public_metrics.get('quote_count')

		tweet_id = tweet.get('id') # string.

		device = tweet.get('source')
		language = tweet.get('lang')

		right_now = datetime.datetime.now()
		hashtags_list = extract_hash_tags(tweet.get('text'))

		tweet_text = tweet.get('text')
		tweet_text = tweet_text.replace("'", "\'")
		# Insert into hashtags tables
		for k in hashtags_list:
			row_hashtags = [
				twitter_handle, tweet_text, k
			]
			try:
				if handle_type == 'sprinklr':
					cursor.execute("insert into visitor.hashtags values (:1, :2, :3)", row_hashtags)
				elif handle_type == 'oracle':
					cursor.execute("insert into admin.hashtags values (:1, :2, :3)", row_hashtags)
				else:
					cursor.execute("insert into racing.hashtags values (:1, :2, :3)", row_hashtags)
			except Exception as e:
				print('Can not insert {}. Continuing. {}'.format(row_hashtags, e))

		# Insert into tweets tables

		row = [
			twitter_handle, tweet_text, str(-1), str(-1), str(-1), str(-1), right_now, str(hashtags_list),
			convert_zulu(tweet_creation_time), str(contains_urls_bool), str(references_other_tweets), str(possibly_sensitive), device, language,
			num_rts, num_likes, num_replies, num_quotes,
			tweet_id
		]
		try:
			if handle_type == 'sprinklr':
				cursor.execute("insert into visitor.tweets values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)", row)
			elif handle_type == 'oracle':
				cursor.execute("insert into admin.tweets values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)", row)
			else:
				cursor.execute("insert into racing.tweets values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)", row)
		except Exception as e:
			print('Can not insert {}. Continuing. {}'.format(row, e))

	connection.commit()
	connection.close()
	# after writing the data, we wait for 15 seconds for the next request due to Twitter API's rate limiting.
	time.sleep(15)




def process_batch_users(connection, handles, to_process_date):
	to_read_file = str()
	if handles == 'sprinklr':
		to_read_file = '../data/sprinklr.handles'
	elif handles == 'oracle':
		to_read_file = '../data/oracle.handles'
	elif handles == 'formula1':
		to_read_file = '../data/formula1.handles'

	df = pd.read_csv(to_read_file)

	print(df)
	for idx, i in df.iterrows():
		twitter_handle = i['final_twitter']
		if '@' in twitter_handle:
			twitter_handle = twitter_handle.replace('@', '')
		# We execute the processing of that user -> Twitter API -> Azure Cognitive Services -> CSV -> Autonomous.
		reprocess_user(connection, twitter_handle, handles, to_process_date)


def main():
	# read type of processing
	action = str()
	to_process_date = str()
	try:
		action = argv[1]
		to_process_date = argv[2]
	except Exception as e:
		print('Could not read any input. Specify either "sprinklr", "formula1" or "oracle" as input types.')

	data = process_yaml()
	connection = init_db_connection(data)
	process_batch_users(connection, action, to_process_date)

if __name__ == '__main__':
	main()

