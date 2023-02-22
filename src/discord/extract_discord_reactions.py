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
import emoji
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
data_path_react = '/home/opc/data_discord_react/'

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

def get_filename_without_extension(file_path):
    file_basename = os.path.basename(file_path)
    filename_without_extension = file_basename.split('.')[0]
    return filename_without_extension
	
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

def analyze_reactions(text):
	# emoji, count, me
	return ["", 0, False]

def extendReactionsDataframe(df):
	df['reaction_emoji'] = df['Reactions'].dropna().apply(lambda x: analyze_reactions(x)[0] )
	df['reaction_count'] = df['Content'].dropna().apply(lambda x: analyze_reactions(x)[1] )
	df['reaction_me'] = df['Content'].dropna().apply(lambda x: analyze_reactions(x)[2] )
	return df

def process_extract_discord_react(fname):
	to_read_file = os.path.join(data_path_out, fname)
	base = get_filename_without_extension(to_read_file)
 
	df = pd.read_csv(to_read_file)

	#print(df)
	# Filter all msg with Reactions
	df2=df.dropna(subset=['Reactions']).reset_index(drop=True)
	#print(df2)
	if df2.shape[0] > 0:
		df_csv = pd.DataFrame(columns=['msgid', 'emoji', 'emoji_translated', 'counter'])
		for i in df2.index:
			id = df2['id'][i]
			#print("id: {} ; Reactions: {}".format(df2['id'][i],df2['Reactions'][i]))
			#chunks = df2['Reactions'][i].split(',')
			chunks = re.split(',+', df2['Reactions'][i])
			#print(chunks)
			for c in chunks:
				emo, sp1, count, sp2 = re.split('[\s()]', c)
				#print ("emoji: {} ; translate: {} ; count: {}".format(emo, emoji.demojize(emo), count))
				#print(re.split('[\s()]', c))
		#df_csv = extendReactionsDataframe(df2)
				#add row to end of DataFrame
				df_csv.loc[len(df_csv.index)] = [id, emo, emoji.demojize(emo), int(count)]

		#print(df_csv)
		# Save
		fname_out = str.format("{}_react.csv", base)
		discord_save = os.path.join(data_path_react, fname_out)
		df_csv.to_csv(discord_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
	else:
		print("No reactions in this file")
	
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--filename', help='filename', required=True)
    args = parser.parse_args()

    # Check Directory exists
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    init_time2 = datetime.now()
    # read yaml config
    #data = process_yaml()
    # Execute Method
    process_extract_discord_react(args.filename)
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
    print('Execution Time to extract Reactions Messages : %d hour:%d min:%d sec' %(hours,minutes,seconds))


if __name__ == "__main__":
    main()

