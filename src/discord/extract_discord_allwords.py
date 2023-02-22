import os
import sys
import pandas as pd
import numpy as np
from os import listdir
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

counts = dict()

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

def word_count(str):
    global counts
    words = str.split()

    for word in words:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1

	
def process_extract_discord_allwords(dirname):
    global counts
    onlyfiles = [f for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]
    #print(onlyfiles)
    for f in onlyfiles:
        #print(f)
        to_read_file = os.path.join(dirname, f)
        df = pd.read_csv(to_read_file)
		# Analyze Content
        df['Content'].dropna().apply(lambda x: word_count(x) )
	
    #print (counts)
    #df_csv = pd.DataFrame.from_dict(counts, orient="index").reset_index()
    df_csv = pd.DataFrame({'Word' : counts.keys() , 'Counter' : counts.values() })	
    #df_csv.columns = ['Word', 'Counter']
	
    allwords_save = os.path.join(data_path_react, "allwords_out.csv")
    df_csv.to_csv(allwords_save, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', help='directory', required=True)
    args = parser.parse_args()

    # Check Directory exists
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    init_time2 = datetime.now()
    # read yaml config
    #data = process_yaml()
    # Execute Method
    process_extract_discord_allwords(args.directory)
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
    print('Execution Time to extract All words for all Messages : %d hour:%d min:%d sec' %(hours,minutes,seconds))


if __name__ == "__main__":
    main()

