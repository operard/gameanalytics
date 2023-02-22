# This code will implement automation (RTs, favs, interactions) with other tweets.

import time
import os 
import json 
import yaml 

from requests_oauthlib import OAuth1Session
# db connection
import cx_Oracle
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



def get_oauth(consumer_key, consumer_secret):
	# Get request token
	request_token_url = 'https://api.twitter.com/oauth/request_token'
	oauth = OAuth1Session(consumer_key, client_secret=consumer_secret)

	try:
		fetch_response = oauth.fetch_request_token(request_token_url)
	except ValueError:
		print(
			'There may have been an issue with the consumer_key or consumer_secret you entered.'
		)

	resource_owner_key = fetch_response.get('oauth_token')
	resource_owner_secret = fetch_response.get('oauth_token_secret')
	print('Got OAuth token: {}'.format(resource_owner_key))

	# Get authorization
	base_authorization_url = 'https://api.twitter.com/oauth/authorize'
	authorization_url = oauth.authorization_url(base_authorization_url)
	print('Please go here and authorize: {}'.format(authorization_url))
	verifier = input('Paste the PIN here: ')

	# Get the access token
	access_token_url = 'https://api.twitter.com/oauth/access_token'
	oauth = OAuth1Session(
		consumer_key,
		client_secret=consumer_secret,
		resource_owner_key=resource_owner_key,
		resource_owner_secret=resource_owner_secret,
		verifier=verifier,
	)
	oauth_tokens = oauth.fetch_access_token(access_token_url)

	access_token = oauth_tokens['oauth_token']
	access_token_secret = oauth_tokens['oauth_token_secret']

	# Make the request
	oauth = OAuth1Session(
		consumer_key,
		client_secret=consumer_secret,
		resource_owner_key=access_token,
		resource_owner_secret=access_token_secret,
	)
	return oauth



'''
create table interaction_tweets(
    tweet_id varchar2(30)
);
alter table interaction_tweets add constraint pk_interaction_tweets primary key (tweet_id);

'''
def retweet_tweets(connection, oauth):
	
	# Get tweets from db.
	cursor = connection.cursor()

	query_sql = 'select tweet_id from admin.interaction_tweets where do_rt=1'
	try:
		cursor.execute(query_sql)
	except Exception:
		print('@retweet_tweets: Error')

	rows = cursor.fetchall()
	if rows:
		for x in rows:
			payload = dict(tweet_id=x[0])

			response = oauth.post(
				'https://api.twitter.com/2/users/{}/retweets'.format(id), json=payload
			)

			if response.status_code != 200:
				raise Exception(
					'Request returned an error: {} {}'.format(response.status_code, response.text)
				)

			print('Response {}, code: {}'.format(json.dumps(response.json(), indent=4, sort_keys=True), response.status_code))



def like_tweets(connection, oauth):
	# Get tweets from db.
	cursor = connection.cursor()

	query_sql = 'select tweet_id from admin.interaction_tweets where do_like=1'
	try:
		cursor.execute(query_sql)
	except Exception:
		print('@retweet_tweets: Error')

	rows = cursor.fetchall()
	if rows:
		for x in rows:
			payload = dict(tweet_id=x[0])

			response = oauth.post(
    			'https://api.twitter.com/2/users/{}/likes'.format(id), json=payload
			)


			if response.status_code != 200:
				raise Exception(
					'Request returned an error: {} {}'.format(response.status_code, response.text)
				)

			print('Response {}, code: {}'.format(json.dumps(response.json(), indent=4, sort_keys=True), response.status_code))



def main():


	# Get keys and information
	yaml_data = process_yaml()
	consumer_key = yaml_data['sentiment_analyzer_api']['api_key']
	consumer_secret = yaml_data['sentiment_analyzer_api']['api_secret_key']
	id = yaml_data['automator']['account_id'] # testing with @jupiterwanderer

	connection = init_db_connection(yaml_data)

	oauth = get_oauth(consumer_key, consumer_secret)

	retweet_tweets(connection, oauth)

	like_tweets(connection, oauth)

	connection.close()



if __name__ == '__main__':
	main()