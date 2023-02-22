import twint
import time
import yaml
import cx_Oracle
from datetime import datetime, timedelta
import argparse
import json
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
parser.add_argument('-g', '--group', help="Specific keyword group to process", required=False)
parser.add_argument('-t', '--time', help="Number of days to retrieve relative to the current date", required=False)
parser.add_argument('-k', '--keyword', help='Keyword to process', required=False)
parser.add_argument('-a', '--at', help='At to process', required=False)
args = parser.parse_args()



def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



def to_timestamp(timestamp):
	dt_object = datetime.fromtimestamp(timestamp/1000)
	return dt_object



# auxiliary function to get hashtags from a string. (removes duplicates using set then converts to list)
def extract_hash_tags(s):
	return list(set(part[1:] for part in s.split() if part.startswith('#')))



def search(filter_search, connection):
	print ("Fetching Tweets")
	c = twint.Config()
	# choose username (optional)
	#c.Username = x
	# choose search term (optional)
	c.Search = '{}'.format(filter_search)
	# choose beginning time (narrow results)
	if not args.time:
		c.Since = "2018-01-01"
	else:
		since_date = datetime.today() - timedelta(days=int(args.time))
		c.Since = since_date.strftime('%Y-%m-%d')
	# set limit on total tweets
	#c.Limit = 10
	c.Pandas = True
	# we want to hide the output, there will be a lot of tweets
	c.Hide_output = True
	
	twint.run.Search(c)
	tweets_df = twint.storage.panda.Tweets_df
	print('[DBG] {} items | {} | keyword'.format(len(tweets_df.index), filter_search))

	# Get tweets from db.
	cursor = connection.cursor()
	'''
	['id', 'conversation_id', 'created_at', 'date', 'timezone', 'place',
       'tweet', 'language', 'hashtags', 'cashtags', 'user_id', 'user_id_str',
       'username', 'name', 'day', 'hour', 'link', 'urls', 'photos', 'video',
       'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'quote_url',
       'search', 'near', 'geo', 'source', 'user_rt_id', 'user_rt',
       'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src',
       'trans_dest']
      '''
	list_tweets = json.loads(tweets_df.to_json(orient = 'records')) # get a list of dicts instead of a dict of dicts
	# iterrows and pandas iteration deprecated due to performance issues
	#for index, x in tweets_df.iterrows():
	index = 0
	failed_insertions = 0
	for x in list_tweets:
		geoloc = str()
		try:
			geoloc = x['geo'].get('coordinates')
		except AttributeError:
			pass
		row = [
			filter_search, x['link'], x['username'], x['name'], x['place'], x['tweet'], -1, -1, -1, -1, str(extract_hash_tags(x['tweet'])), to_timestamp(x['created_at']), 
			x['source'], x['language'], x['nretweets'], x['nlikes'], x['nreplies'], geoloc, x['near'], x['retweet_date'], x['tweet'], 0, x['language']
		]
		try:
			cursor.execute("insert into public_sentiment values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23)", row)
			#print('[DBG][{:.2f}%] INSERT ID {}'.format((index + 1)/len(list_tweets) * 100, x['link']))
		except Exception:
			#print('[DBG][{:.2f}%] SKIP ID {}'.format((index + 1)/len(list_tweets) * 100, x['link']))
			failed_insertions += 1
		index+=1
	print('[DBG] OK {} DUPLICATE {} | {} | keyword'.format(len(list_tweets) - failed_insertions, failed_insertions, filter_search))




def user(user, connection):
	print ("Fetching Tweets")
	c = twint.Config()
	# choose username (optional)
	c.Username = user
	# choose search term (optional)
	#c.Search = '{}'.format(filter_search)
	# choose beginning time (narrow results)
	if not args.time:
		c.Since = "2018-01-01"
	else:
		since_date = datetime.today() - timedelta(days=int(args.time))
		c.Since = since_date.strftime('%Y-%m-%d')
	# set limit on total tweets
	#c.Limit = 10
	c.Pandas = True
	# we want to hide the output, there will be a lot of tweets
	c.Hide_output = True
	
	twint.run.Search(c)
	tweets_df = twint.storage.panda.Tweets_df
	print('[DBG] {} items | {} | user'.format(len(tweets_df.index), user))

	# Get tweets from db.
	cursor = connection.cursor()
	'''
	['id', 'conversation_id', 'created_at', 'date', 'timezone', 'place',
       'tweet', 'language', 'hashtags', 'cashtags', 'user_id', 'user_id_str',
       'username', 'name', 'day', 'hour', 'link', 'urls', 'photos', 'video',
       'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'quote_url',
       'search', 'near', 'geo', 'source', 'user_rt_id', 'user_rt',
       'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src',
       'trans_dest']
      '''
	# iterrows and pandas iteration deprecated due to performance issues
	#for index, x in tweets_df.iterrows():
	list_tweets = json.loads(tweets_df.to_json(orient = 'records')) # get a list of dicts instead of a dict of dicts
	index = 0
	failed_insertions = 0
	for x in list_tweets:
		geoloc = str()
		try:
			geoloc = x['geo'].get('coordinates')
		except AttributeError:
			pass
		row = [
			user, x['link'], x['username'], x['name'], x['place'], x['tweet'], -1, -1, -1, -1, str(extract_hash_tags(x['tweet'])), to_timestamp(x['created_at']), 
			x['source'], x['language'], x['nretweets'], x['nlikes'], x['nreplies'], geoloc, x['near'], x['retweet_date'], x['tweet'], 0, x['language']
		]
		try:
			cursor.execute("insert into user_sentiment values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23)", row)
			#print('[DBG][{:.2f}%] INSERT ID {}'.format((index + 1)/len(list_tweets) * 100, x['link']))
		except Exception:
			#print('[DBG][{:.2f}%] SKIP ID {}'.format((index + 1)/len(list_tweets) * 100, x['link']))
			failed_insertions += 1
		index += 1

	print('[DBG] OK {} DUPLICATE {} | {} | user'.format(len(list_tweets) - failed_insertions, failed_insertions, user))



def read_file(path):
	f = open(path)
	word_list = f.readlines()
	for x in range(len(word_list)):
		word_list[x] = word_list[x].rstrip() # Clean spaces and delimiting characters
	f.close()
	return word_list


def read_params_from_db(connection, group=None):
	keyword_list = list()
	at_list = list()

	sql_query_1 = str()
	sql_query_2 = str()
	if group:
		sql_query_1 = """select distinct keyword from keywords where keyword_group = '{}'""".format(group)
		sql_query_2 = """select distinct at from ats where at_group = '{}'""".format(group)
	else:
		sql_query_1 = """select distinct keyword from keywords"""
		sql_query_2 = """select distinct at from ats"""
	
	cursor = connection.cursor()

	try:
		cursor.execute(sql_query_1)
	except Exception as e:
		print('@{}: Exception: {}'.format(read_params_from_db.__name__), e)

	rows = cursor.fetchall()
	if rows:
		for x in rows:
			keyword_list.append(x[0])

	try:
		cursor.execute(sql_query_2)
	except Exception as e:
		print('@{}: Exception: {}'.format(read_params_from_db.__name__), e)

	rows = cursor.fetchall()
	if rows:
		for x in rows:
			at_list.append(x[0])

	return keyword_list, at_list



def main():
	yaml_data = process_yaml()
	conn = init_db_connection(yaml_data)

	ats = list()
	keywords = list()

	if not args.at and not args.keyword:
		if not args.group:
			keywords, ats = read_params_from_db(conn)
		else:
			# Then we will process the specified keyword group.
			keywords, ats = read_params_from_db(conn, args.group)
			
		if keywords:
			for x in keywords:
				search(x, conn)
		else:
			print('No keywords found with filter {}. Skipping...'.format(args.group))

		if ats:
			for x in ats:
				#search('@{}'.format(x), conn) # look for @ mentions and analyze sentiment
				#search('#{}'.format(x), conn) # also look for the hashtags
				user(x, conn)
		else:
			print('No ats found with filter {}. Skipping...'.format(args.group))
	if args.at:
		user(args.at, conn)
	if args.keyword:
		search(args.keyword, conn)
	
	conn.close()



if __name__ == '__main__':
	main()
