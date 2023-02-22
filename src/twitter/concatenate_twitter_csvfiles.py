import os
import sys
import glob
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


keyword_csv_path = '/home/opc/data_twitter_csv/keywords'
data_csv_path = '/home/opc/data_twitter_csv/users'
from_csv_path = '/home/opc/data_twitter_csv/fromusers'


def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)


def contenate_allcsv(dirpath, fname_output):
    csvfiles = glob.glob(dirpath + "/*.csv")
    df = pd.concat( (pd.read_csv(f, header=0) for f in csvfiles) )
    df.to_csv(fname_output, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)


def main():
    parser = argparse.ArgumentParser()
    #parser.add_argument('-g', '--group', help="Specific keyword group to process", required=False)
    #parser.add_argument('-t', '--time', help="Number of days to retrieve relative to the current date", required=False)
    #parser.add_argument('-k', '--keyword', help='Keyword to process', required=True)
    #parser.add_argument('-u', '--user', help='Twitter User Account', required=True)
    parser.add_argument('-d', '--directory', help='CSV Directory to process', required=True)
    parser.add_argument('-f', '--file_output', help='Output files to save csv', required=True)
    args = parser.parse_args()

    init_time2 = datetime.now()
    # read yaml config
    # data = process_yaml()
    # Execute Method
    contenate_allcsv(args.directory, args.file_output)
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

