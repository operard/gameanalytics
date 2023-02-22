import requests
import pandas as pd
import yaml
import json
import ast
import os
import time
import datetime
import pickle
import csv
import time
# db connection
import cx_Oracle
from sys import argv
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

# v2
def find_recent_tweets(twitter_handle):
	max_results = 100
	mrf = 'max_results={}'.format(max_results)
	query = 'query=from:{}'.format(twitter_handle)
	tweet_fields = 'tweet.fields={}'.format(('attachments,author_id,conversation_id,created_at,entities,geo,id,'
		'in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld'))
	url = 'https://api.twitter.com/2/tweets/search/recent?{}&{}&{}'.format(
		mrf, query, tweet_fields
	)
	return url


# v2
def find_all_tweets():
	handle = 'ocigeek'
	max_results = 100
	mrf = 'max_results={}'.format(max_results)
	q = 'query=from:{}'.format(handle)
	url = 'https://api.twitter.com/2/tweets/search/all?{}&{}'.format(
		mrf, q
	)
	return url


# v1.1
def premium_full_archive():
	url = 'https://api.twitter.com/1.1/tweets/search/fullarchive/dev.json'
	return url


def create_bearer_token(data):
	return data['sentiment_analyzer_api']['bearer_token']


	
def twitter_auth_and_connect_v2(bearer_token, url):
	headers = {"Authorization": "Bearer {}".format(bearer_token)}
	response = requests.request("GET", url, headers=headers)
	return response.json()



def twitter_auth_and_connect_v1(twitter_handle, bearer_token, url):
	headers = {
		'Authorization': 'Bearer {}'.format(bearer_token),
		'Content-Type': 'application/json'
	}

	data = {
		"query":"from:{} lang:en".format('ocigeek'),
		"maxResults":"100",
		"fromDate":"200603210000", # minimum date acceptable
		"toDate":"202106092359"
	}

	# Necessary to convert to JSON object before passing it to the POST request.
	data = json.dumps(data)

	print('Headers: {}, type {}'.format(headers, type(data)))
	print('Data: {}, type {}'.format(data, type(data)))

	response = requests.post(url, headers=headers, data=data)
	return response.json()



def process_tweets(json_obj):
	#json_obj = json.loads(json_object)
	tweets = list()
	assert len(json_obj.get('data')) > 0
	for i in json_obj.get('data'):
		tweets.append(i.get('text'))

	print(tweets)
	return tweets



def test_premium(twitter_handle):
	url= premium_full_archive()
	data = process_yaml()
	bearer_token = create_bearer_token(data)
	res_json = twitter_auth_and_connect_v1(twitter_handle, bearer_token, url)
	#print(res_json)
	# Since we have a limited amount of requests per month, everything that's done is put in a file.
	timestr = time.strftime('%Y%m%d-%H%M%S')
	f = open('../data/user/{}_{}.txt'.format(twitter_handle, timestr), 'w+')
	f.write(json.dumps(res_json))
	f.close()



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


# test v1
def extract_weekly_tweets(connection, twitter_handle, handle_type):
	print('+++ Processing user {} +++'.format(twitter_handle))
	url = find_recent_tweets(twitter_handle)
	#url = find_all_tweets() # not available in Standard Basic Tier
	# https://documenter.getpostman.com/view/9956214/T1LMiT5U#e8d36c1b-9f1b-42b3-be14-cd624ffd038a
	data = process_yaml()
	bearer_token = create_bearer_token(data)
	res_json = twitter_auth_and_connect_v2(bearer_token, url)

	#print(res_json)
	try:
		res_json.get('meta').get('result_count')
	except AttributeError:
		print('Problem with request. Exiting. Request: {}'.format(res_json))
		return
	# Only continue if the number of results is greater than 1.
	if res_json.get('meta').get('result_count') == 0:
		print('0 results for user {}'.format(twitter_handle))
		return 

	# Write to json file
	f = open('../data/json/{}_{}.json'.format(twitter_handle, time.strftime('%Y%m%d')) , 'w+')
	f.write(json.dumps(res_json))
	f.close()


	# We process the tweets to get only the texts.
	tweets = res_json.get('data')
	print('{}: Printing tweets for user {}: --------\n {}'.format(time.strftime("%H:%M:%S", time.localtime()), twitter_handle, tweets))

	# Insert into the database.
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
	



def process_batch_users(connection, handles):
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
		extract_weekly_tweets(connection, twitter_handle, handles)



# this process can be executed for the last week. The granularity is variable but hour by default.
# table will have this form: twitter_handle|
def get_tweets_timeline(twitter_handle, query, granularity='hour'):
	assert granularity in ['minute', 'hour', 'day']
	assert len(query) < 512
	pass



if __name__ == "__main__":
	# read type of processing
	action = str()
	try:
		action = argv[1]
	except Exception as e:
		print('Could not read any input. Specify either "sprinklr", "formula1" or "oracle" as input types.')

	data = process_yaml()
	connection = init_db_connection(data)
	process_batch_users(connection, action)

