import os
import sys
import pandas as pd
import numpy as np
from pprint import pprint
import requests
import time
from threading import Thread
import cx_Oracle
import yaml
import csv
from datetime import datetime
import codecs
import twint
import json
import re
import unicodedata
import re
import sys
import argparse

version = "1.0.1"

verbose = False
debug = False
data_path = '/home/opc/data_twitter/users'

def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)

# auxiliary function to get hashtags from a string. (removes duplicates using set then converts to list)
def extract_hash_tags(s):
	return list(set(part[1:] for part in s.split() if part.startswith('#')))

def is_json_key_present(json, key):
    try:
        buf = json[key]
    except KeyError:
        return False
    return True

def analyze_by_user(us):
	print ("Reading Tweets for {}".format(us))
    #
	fname = str.format("{}.json", us)
	tweets_save = os.path.join(data_path, fname)

	df = pd.read_json(tweets_save, lines=True)
	print (df.head())


def analyze_by_user2(us):
	print ("Reading Tweets for {}".format(us))
    #
	fname = str.format("{}.json", us)
	tweets_save = os.path.join(data_path, fname)

    # JSON file
	f = open (tweets_save, "r")
    
    # Reading from file
	data = json.loads(f.read())
	'''
	['id', 'conversation_id', 'created_at', 'date', 'timezone', 'place',
       'tweet', 'language', 'hashtags', 'cashtags', 'user_id', 'user_id_str',
       'username', 'name', 'day', 'hour', 'link', 'urls', 'photos', 'video',
       'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'quote_url',
       'search', 'near', 'geo', 'source', 'user_rt_id', 'user_rt',
       'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src',
       'trans_dest']
      '''    
    # Iterating through the json
    # list
	n_replies = 0
	n_main = 0
	for x in data:
		print(x)
		geoloc = str()
		try:
			geoloc = x['geo'].get('coordinates')
		except AttributeError:
			pass
		if is_json_key_present(x,'conversation_id'):
			n_replies += 1
		else:
			n_main += 1
		row = [
			us, x['link'], x['username'], x['name'], x['place'], x['tweet'], -1, -1, -1, -1, str(extract_hash_tags(x['tweet'])), to_timestamp(x['created_at']), 
			x['source'], x['language'], x['nretweets'], x['nlikes'], x['nreplies'], geoloc, x['near'], x['retweet_date'], x['tweet'], 0, x['language']
		]

    # Closing file
	f.close()
    #
	print('[analyze_by_user] {}  user, {} main , {} replies '.format(us, n_main, n_replies))



def main():
    parser = argparse.ArgumentParser()
    #parser.add_argument('-g', '--group', help="Specific keyword group to process", required=False)
    #parser.add_argument('-t', '--time', help="Number of days to retrieve relative to the current date", required=False)
    #parser.add_argument('-k', '--keyword', help='Keyword to process', required=False)
    parser.add_argument('-u', '--user', help='Twitter User Account', required=True)
    args = parser.parse_args()

    init_time2 = datetime.now()
    # read yaml config
    data = process_yaml()
    # Execute Method
    analyze_by_user(args.user)
    print("Done!")
    #
    end_time2 = datetime.now()
    dif_time2=end_time2-init_time2
    #
    temp=dif_time2.seconds
    hours = temp//3600
    temp = temp - 3600*hours
    minutes = temp//60
    seconds = temp - 60*minutes
    print('Execution Time to extract user twitter : %d hour:%d min:%d sec' %(hours,minutes,seconds))


if __name__ == "__main__":
    main()

