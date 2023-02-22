import cx_Oracle
import yaml
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



data = process_yaml()
connection = init_db_connection(data)
# Insert into the database.
cursor = connection.cursor()

tables_to_process = ['visitor.tweets', 'admin.tweets', 'racing.tweets']

for i in tables_to_process:
	print('Inserting into table {}...'.format(i))
	sql_get_tweets = """select twitter_handle, tweet, hashtags from {}""".format(i)
	#sql_query = 'update {} where twitter_handle = {} and tweet = {}'.format(i)
	cursor.execute(sql_get_tweets)
	result = cursor.fetchall()
	#print(result)
	for j in result:
		hashtags_list = str(extract_hash_tags(j[1])) # str type to make it insertable into the db
		#print('Found hashtag list: {}'.format(hashtags_list))
		row = [hashtags_list, j[0], j[1]]
		print('Processing {}'.format(row))
		cursor.execute("update {} set hashtags = :1 where twitter_handle = :2 and tweet = :3".format(i), row)

connection.close()

