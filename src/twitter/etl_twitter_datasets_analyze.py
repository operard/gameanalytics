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
import json
from sklearn.feature_extraction import text
from sklearn.feature_extraction.text import CountVectorizer
from nltk import *
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import nltk
from nltk.collocations import BigramCollocationFinder, BigramAssocMeasures
import re
from collections import OrderedDict
from operator import itemgetter
import unicodedata
import re
import sys


version = "1.0.1"

verbose = False
debug = False
data_path = './data'

def sentiment_scores(analyser, sentence):
    snt = analyser.polarity_scores(sentence)
    return snt

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


def find_number_rows(connection, table_name):
    cursor = connection.cursor()
    sql_query = """select count(*) from {} where sentiment = '-1' and how_positive = '-1' and how_neutral = '-1' and how_negative = '-1' and language='en'""".format(table_name)
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('@{}: {}'.format(find_number_rows.__name__, e))
    
    result = cursor.fetchall()
    print(result[0][0])
    print('Obtained {} rows to process'.format(result[0][0]))
    return int(result[0][0])


def getPublicSentimentRows(connection):
    query = '''SELECT * from PUBLIC_SENTIMENT'''
    df = pd.read_sql(query, con=connection)
    return df


## Get Tweet Hashtag Key Terms by User
def generateHashtagTerms(df):
    global data_path
    print("Begin generateHashtagTerms ")

    new_df = df[['TWEET_ID', 'HASHTAGS', 'TWITTER_NAME']]
    hashElem = []
    #
    for i, row in new_df.iterrows():
        # String to list
        id = row['TWEET_ID']
        name = row['TWITTER_NAME']
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
                add_tuple = (id, name, elem, 1)
                hashElem.append(add_tuple)
    #Create a DataFrame object
    dfObj = pd.DataFrame(hashElem, 
                        columns = ['TWEET_ID', 'TWITTER_NAME', 'tags', 'tag_count'])
    # save to CSV
    tag_save = os.path.join(data_path, 'data_Tweet_Hashtag_Terms_by_ID.csv')
    dfObj.to_csv(tag_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    print("Hashtag Terms Dataset saved to: ", tag_save)

## Get Users Tagged by User
def generateTweetsUsersTagged(df):
    global data_path
    print("Begin generateTweetsUsersTagged ")
    new_df = df[['TWEET_ID', 'TWITTER_HANDLE', 'TWITTER_NAME']]
    s = new_df.apply(lambda x: pd.Series(x['TWITTER_HANDLE']), axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'users_tagged'
    #df_user_tags = new_df.drop('TWITTER_NAME', axis=1).join(s)
    df_user_tags = new_df.drop('TWITTER_HANDLE', axis=1).join(s)
    df_user_tags['users_tagged_count'] = 1

    user_save = os.path.join(data_path, 'data_Tweet_Users_Tagged_ID.csv')
    #df_user_tags.to_csv(user_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    df_user_tags.to_csv(user_save, index=True, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    print("Hashtag Users Dataset saved to: ", user_save)

def generateMapTweetTopTerms(df):
    global data_path
    print("Begin generateMapTweetTopTerms ")
    # nltk.data.path.append("/home/opc/nltk_data")
    token_df = df[['TWEET_ID', 'TWEET', 'TWITTER_NAME']]
    stops = set(stopwords.words('custom'))
    maplist = []
    tokenlist = dict()
    regex = re.compile('\W')
    for index, row in token_df.iterrows():
        text = row['TWEET']
        normalized = unicodedata.normalize('NFKC', text)
        if normalized != text:
            text = ''.join([c for c in normalized if not unicodedata.combining(c)])
        terms = regex.sub(' ', text.lower()).split()
        for term in terms:
            if term not in stops and re.match('^\W|^$', term) is None:
                if term in tokenlist:
                    tokenlist[term] += 1
                else:
                    tokenlist[term] = 1
                maplist.append((row['TWEET_ID'], term))
    top50dict = OrderedDict(sorted(tokenlist.items(), key=itemgetter(1), reverse=True)[:50])
    top50 = list(top50dict.items())
    top50maplist = []
    for i in range(0, len(maplist) - 1):
        if maplist[i][1] in top50dict:
            top50maplist.append(maplist[i])
    mapdf = pd.DataFrame(top50maplist, columns=['TWEET_ID', 'term'])
    mapdf.rename(index=str, columns={"TWEET_ID": "Tweet ID", "term": "Tweet Term"})
    mapdf.columns = ['Tweet ID', 'Tweet Term']
    #
    map_file = os.path.join(data_path, 'data_Map_Tweet_Top_Terms.csv')
    mapdf.to_csv(map_file, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    print("Top Terms Mapping Dataset saved to: ", map_file)
    #
    termdf = pd.DataFrame(top50, columns=['term', 'frequency'])
    termdf.rename(index=str, columns={"term": "Tweet Term", "frequency": "Term Frequency"})
    termdf.columns = ['Tweet Term', 'Term Frequency']

    term_file = os.path.join(data_path, 'data_Tweet_Text_Top_Terms.csv')
    termdf.to_csv(term_file, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    print("Top Terms Dataset saved to: ", term_file)


def analyzeTweetsSentiment(df):
    global data_path
    print("Begin analyzeTweetsSentiment ")
    #nltk.data.path.append("/home/opc/nltk_data")
    analyser = SentimentIntensityAnalyzer()
    # process
    for index, row in df.iterrows():
        text = row['TWEET']
        sentiment = float(row['SENTIMENT'])
        fl_positive= float(row['HOW_POSITIVE'])
        fl_neutral= float(row['HOW_NEUTRAL'])
        fl_negative= float(row['HOW_NEGATIVE'])
        if sentiment == -1 and fl_positive == -1 and fl_neutral == -1 and fl_negative == -1:
            score = sentiment_scores(analyser, text)
            score_coumpound = score['compound']
            row['sent_compound'] = score_coumpound
            if score_coumpound >= 0.05:
                row['sentiment_label'] = 'Positive'
            elif score_coumpound <= -0.05:
                row['sentiment_label'] = 'Negative'    
            else:
                row['sentiment_label'] = 'Neutral'

            row['SENTIMENT'] = score_coumpound
            row['HOW_POSITIVE'] = score['pos']
            row['HOW_NEUTRAL'] = score['neu']
            row['HOW_NEGATIVE'] = score['neg']
        else:
            # {'neg': 0.0, 'neu': 0.432, 'pos': 0.568, 'compound': 0.8476}
            score_coumpound = sentiment
            row['sent_compound'] = score_coumpound
            if score_coumpound >= 0.05:
                row['sentiment_label'] = 'Positive'
            elif score_coumpound <= -0.05:
                row['sentiment_label'] = 'Negative'    
            else:
                row['sentiment_label'] = 'Neutral'
    # Save
    tweets_save = os.path.join(data_path, 'data_all_Tweets.csv')
    df.to_csv(tweets_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    print("All tweets Dataset saved to: ", tweets_save)
    # Save
    tweets_df = df[['TWEET_ID', 'TWEET', 'NUM_RTS','NUM_LIKES', 'TWEET_PLACE', 'CREATED_AT', 
                           'TWITTER_NAME', 'HASHTAGS', 'NUM_REPLIES', 'TWITTER_HANDLE',
                           'sent_score','sent_compound','sentiment_label','SEARCH_FILTER']]
    tweets_df = pd.DataFrame(tweets_df)
    tweets_df.rename(index=str, columns={
            'TWEET_ID': 'Tweet ID',
            'TWEET': 'Tweet Text',
            'NUM_RTS': 'Retweet Count',
            'NUM_LIKES': 'Favorite Count',
            'TWEET_PLACE': 'Tweet Source',
            'CREATED_AT': 'Tweet Created',
            'TWITTER_NAME': 'User ID',
            'HASHTAGS': 'Tweet Tags ',
            'NUM_REPLIES': 'Tweet Replies',
            'TWITTER_HANDLE': 'Users Tagged',
            'sent_score': 'Sentiment score',
            'sent_compound': 'Sentiment Compound Score',
            'sentiment_label': 'Sentiment Label',
            'SEARCH_FILTER': 'Search Term'
        })
    tweets_df.columns = ['Tweet ID', 'Tweet Text', 'Retweet Count', 'Favorite Count', 'Tweet Source', 'Tweet Created',
                            'User ID',
                            'Tweet Tags ', 'Tweet Replies', 'Users Tagged', 'Sentiment score',
                            'Sentiment Compound Score', 'Sentiment Label', 'Search Term']
    ds_save = os.path.join(data_path, 'data_Twitter_DataSet.csv')
    tweets_df.to_csv(ds_save, index=True, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    print("Main Dataset saved to: ", ds_save)
    

def main():
    global data_path
    # args is a list of the command line args
    args = sys.argv[1:]
    # 1. Check for the arg pattern:
    if len(args) >= 1:
        data_path = args[0]
    else:
        data_path = './data'
    # Check Directory exists
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    init_time2 = datetime.now()
    # read yaml config
    data = process_yaml()
    # Get cursor.
    connection = init_db_connection(data)
    # read the public_sentiment table
    df_public = getPublicSentimentRows(connection)
    print("getPublicSentimentRows rows: ", len(df_public))
    # Initialize columns
    df_public['sent_score'] = 'null'
    df_public['sent_compound'] = 'null'
    df_public['sentiment_label'] = 'null'
    analyzeTweetsSentiment(df_public)

    # Close connection
    close_db_connection(connection)
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
    print('Execution Time for ETL_TWITTER_DATASETS dataset: %d hour:%d min:%d sec' %(hours,minutes,seconds))


if __name__ == "__main__":
    main()

