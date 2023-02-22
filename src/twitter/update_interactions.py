# This code intends to update all tweets less than a month old when executed. It will retrieve the tweets that currently have
# some fields missing, or its creation date is less than a month ago, and update the data present in the DB.

import requests
import pandas as pd
import yaml
import json
import ast
import os
import time
import pickle
import csv
import time
# db connection
import cx_Oracle
import os
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)



def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



# wallet location (default is HOME/wallets/wallet_X)
os.environ['TNS_ADMIN'] = '{}/{}'.format(home, process_yaml()['WALLET_DIR'])
print(os.environ['TNS_ADMIN'])



def create_bearer_token(data):
	return data['sentiment_analyzer_api']['bearer_token']


# test v1
def get_tweets(connection):

	data = process_yaml()
	bearer_token = create_bearer_token(data)

	# https://documenter.getpostman.com/view/9956214/T1LMiT5U#e8d36c1b-9f1b-42b3-be14-cd624ffd038a
	# Insert into the database.
	cursor = connection.cursor()

	import datetime
	today = datetime.datetime.today()
	last_month = today - datetime.timedelta(days=30)

	accounts = ['admin', 'racing', 'visitor']
	for i in accounts:
		tweets_to_process = list()
		query_sql = 'select tweet_id, created_at from {}.tweets where tweet_id is not null'.format(i)

		try:
			cursor.execute(query_sql)
		except Exception as e:
			print('Can not select from table {}'.format(i))

		rows = cursor.fetchall()
		if rows:
			for x in rows:
				if last_month <= x[1]:
					tweets_to_process.append({
						'tweet_id':x[0],
						'created_at':x[1]
						})
					#print(x[0], x[1])
		print('Processing {} tweets for table {}'.format(len(tweets_to_process), i))
		for x in tweets_to_process:
			connection.commit()
			# Make call to endpoint

			url = 'https://api.twitter.com/2/tweets/{}?tweet.fields={}'.format(x['tweet_id'], 'public_metrics')

			headers = {
			  'Authorization': 'Bearer {}'.format(bearer_token)
			}

			response = requests.request("GET", url, headers=headers)

			# Process object
			public_metrics = dict()
			try:
				public_metrics = response.json().get('data').get('public_metrics')
			except Exception as e:
				print('Error in tweet with ID {}. Continuing...'.format(x['tweet_id']))
				continue

			updated_rts = public_metrics.get('retweet_count')
			updated_likes = public_metrics.get('like_count')
			updated_replies = public_metrics.get('reply_count')
			updated_quotes = public_metrics.get('quote_count')


			# Update in database
			update_sql = "update {}.tweets set num_rts={}, num_likes={}, num_replies={}, num_quotes={} where tweet_id='{}'".format(
				i, updated_rts, updated_likes, updated_replies, updated_quotes, x['tweet_id'])
			#print(update_sql)
			try:
				cursor.execute(update_sql)
				print('Executed: {};'.format(update_sql))
			except Exception as e:
				print('Could not update table {}, tweet ID {}: {}'.format(i, x['tweet_id'], e))

			time.sleep(5) # wait for rate limiting purposes

	connection.close()



def main():
	data = process_yaml()
	connection = init_db_connection(data)
	get_tweets(connection)



if __name__ == '__main__':
	main()