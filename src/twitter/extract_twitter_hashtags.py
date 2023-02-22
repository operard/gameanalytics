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
import cx_Oracle

version = "1.0.1"

verbose = False
debug = False
data_path = '/home/opc/data_twitter/hashtags'
data_csv_path = '/home/opc/data_twitter_csv/hashtags'


def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)

def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection

def close_db_connection(connection):
    connection.close()

def to_timestamp(timestamp):
	dt_object = datetime.fromtimestamp(timestamp/1000)
	return dt_object

# auxiliary function to get hashtags from a string. (removes duplicates using set then converts to list)
def extract_hash_tags(s):
	return list(set(part[1:] for part in s.split() if part.startswith('#')))


def analyze_all_tags(df):
    hashElem = []
    #
    for i, row in df.iterrows():
        # String to list
        id = row['ID']
        v = row['HASHTAGS']
        #if v != '[]':'tag_count'
        #    tag = v.strip('][').split(',')
        #    print('{} : {}'.format(v, len(tag))) 
        #else:
        #    print('{} : {}'.format(v, 'NOT ELEMENTS'))
        # new_df.loc[i,'TWEET_ID']
        if v != '[]':
            tag = v.strip('][').split(',')
            for elem in tag:
                #dfObj = dfObj.append({'TWEET_ID': id, 'TWITTER_NAME': name, 'tags': elem, 'tag_count': 1}, ignore_index=True)
                hashtag = elem.replace("'", "")
                add_tuple = (id, hashtag, 1)
                hashElem.append(add_tuple)
    #Create a DataFrame object
    dfObj = pd.DataFrame(hashElem, 
                        columns = ['ID', 'HASHTAG', 'count'])
    return dfObj


def getHashtagsForTweets(connection):
    query = '''SELECT ID, HASHTAGS from TWEETS_SENTIMENT'''
    df = pd.read_sql(query, con=connection)
    return df

def getRepliesHashtagsForTweets(connection):
    query = '''SELECT ID, HASHTAGS from TWEETS_REPLIES_SENTIMENT'''
    df = pd.read_sql(query, con=connection)
    return df


def main():
    parser = argparse.ArgumentParser()
    #parser.add_argument('-g', '--group', help="Specific keyword group to process", required=False)
    #parser.add_argument('-t', '--time', help="Number of days to retrieve relative to the current date", required=False)
    #parser.add_argument('-k', '--keyword', help='Keyword to process', required=True)
    #parser.add_argument('-u', '--user', help='Twitter User Account', required=True)
    args = parser.parse_args()

    # Check Directory exists
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    # Check Directory exists
    if not os.path.exists(data_csv_path):
        os.mkdir(data_csv_path)

    init_time2 = datetime.now()
    # read yaml config
    data = process_yaml()
    # Initialize DB
    conn = init_db_connection(data)
    # Execute Method
    df_tweets = getHashtagsForTweets(conn)
    df_hash_tweets = analyze_all_tags(df_tweets)
    fname = 'hashtags_tweets.csv'
    tweets_save = os.path.join(data_csv_path, fname)
    df_hash_tweets.to_csv(tweets_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    #
    df_replies = getRepliesHashtagsForTweets(conn)
    df_hash_Replies = analyze_all_tags(df_replies)
    fname = 'hashtags_replies.csv'
    tweets_save = os.path.join(data_csv_path, fname)
    df_hash_Replies.to_csv(tweets_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)    
    print("Done!")
    # Close
    close_db_connection(conn)
    #
    end_time2 = datetime.now()
    dif_time2=end_time2-init_time2
    #
    temp=dif_time2.seconds
    hours = temp//3600
    temp = temp - 3600*hours
    minutes = temp//60
    seconds = temp - 60*minutes
    print('Execution Time to extract hashtags : %d hour:%d min:%d sec' %(hours,minutes,seconds))


if __name__ == "__main__":
    main()

