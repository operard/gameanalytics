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
from sklearn.feature_extraction import text
from sklearn.feature_extraction.text import CountVectorizer
from nltk import *
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import nltk
from nltk.collocations import BigramCollocationFinder, BigramAssocMeasures

version = "1.0.1"

verbose = False
debug = False
data_path = '/home/opc/data_discord/'
data_path_out = '/home/opc/data_discord_out/'

analyser = SentimentIntensityAnalyzer()

def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)


def sentiment_scores(analyser, sentence):
    snt = analyser.polarity_scores(sentence)
    return snt

def to_timestamp(timestamp):
	dt_object = datetime.fromtimestamp(timestamp/1000)
	return dt_object

def analyze_sentiment(text):
	score = sentiment_scores(analyser, text)
	score_coumpound = score['compound']
	sentiment = score_coumpound
	sentiment_label = 'Neutral'
	if score_coumpound >= 0.05:
		sentiment_label = 'Positive'
	elif score_coumpound <= -0.05:
		sentiment_label = 'Negative'    
	else:
		sentiment_label = 'Neutral'
	return [sentiment_label, sentiment, score['pos'], score['neu'], score['neg']]
   

def extendDataframe(df, channel_id):
	df['channel_id'] = '{}'.format(channel_id)
	df['language'] = ''
	df['sentiment_label'] = df['Content'].dropna().apply(lambda x: analyze_sentiment(x)[0] )
	df['sentiment'] = df['Content'].dropna().apply(lambda x: analyze_sentiment(x)[1] )
	df['how_positive'] = df['Content'].dropna().apply(lambda x: analyze_sentiment(x)[2] )
	df['how_neutral'] = df['Content'].dropna().apply(lambda x: analyze_sentiment(x)[3] )
	df['how_negative'] = df['Content'].dropna().apply(lambda x: analyze_sentiment(x)[4] )
	#df['created_at'] = df['created_at'].apply(lambda x: to_timestamp(x) )
	# 
	#out_df= pd.DataFrame()
	#out_df = pd.concat([out_df,df])
	#return out_df
	#df.to_csv(tweets_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
	return df

def process_batch_discord(fname, channel_id, msgid):
	to_read_file = os.path.join(data_path, fname)
	df = pd.read_csv(to_read_file)

	print(df)
	# Add Msg Id
	ini_msgid = int(msgid)
	df['id'] = 0
	#for idx, i in df.iterrows():
	for i in df.index:
		df['id'][i] = ini_msgid
		ini_msgid = ini_msgid+1
	print(df)
	df_csv = extendDataframe(df, '{}'.format(channel_id) )

	# Save
	fname_out = str.format("{}_out.csv", channel_id)
	discord_save = os.path.join(data_path_out, fname_out)
	df_csv.to_csv(discord_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
	print("Next MSG ID : {}",ini_msgid)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--filename', help='filename', required=True)
    parser.add_argument('-C', '--Channel', help='Channel Id', required=True)
    parser.add_argument('-i', '--msgid', help='Message Id Begin', required=True)
    args = parser.parse_args()

    # Check Directory exists
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    init_time2 = datetime.now()
    # read yaml config
    #data = process_yaml()
    # Execute Method
    process_batch_discord(args.filename, args.Channel, args.msgid)
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
    print('Execution Time to convert Discord Messages : %d hour:%d min:%d sec' %(hours,minutes,seconds))


if __name__ == "__main__":
    main()

