from facebook_scraper import get_posts, get_profile, get_group_info
import argparse
import yaml
import cx_Oracle
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import json
import os
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("config.yaml") as file:
		return yaml.safe_load(file)



# wallet location (default is HOME/wallets/wallet_X)
os.environ['TNS_ADMIN'] = '{}/{}'.format(home, process_yaml()['WALLET_DIR'])
print(os.environ['TNS_ADMIN'])

COOKIE_PATH = './cookies.json'


def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



# get login info from config.yaml
login_info = process_yaml()
USERNAME = login_info['username']
PASSWORD = login_info['password']


parser = argparse.ArgumentParser()
parser.add_argument('-o', '--option', help='Option to choose from', choices=['page_data', 'profile_info', 'group_info'], required=True)
parser.add_argument('-t', '--target', help='Send the unique page name, profile name, or ID', required=False)
args = parser.parse_args()

options = {
    'comments': 1000000,
    'reactors': 1000000,
    'progress': False
}



def insert_post_db(connection, post):
    cursor = connection.cursor()
    num_haha = 0; num_like = 0; num_love = 0; num_sorry = 0; num_wow = 0
    detected_language = str()
    sentiment = -1; how_positive = -1; how_neutral = -1; how_negative = -1
    # get language
    try:
        detected_language = detect(post['text'])
    except LangDetectException:
        detected_language = 'UNKNOWN'
    # get sentiment using VADER if the source language is english.
    sentiment, how_positive, how_neutral, how_negative = analyze_sentiment_vader(post['text'])
    try:
        num_haha = post['reactions']['haha']
        num_like = post['reactions']['like']
        num_love = post['reactions']['love']
        num_sorry = post['reactions']['sorry']
        num_wow = post['reactions']['wow']
    except (KeyError, TypeError):
        print('No reactions found.')
    finally:
        row = [
            post['post_id'], post['post_url'],
            post['text'], post['time'],
            post['comments'], post['likes'],
            post['shares'], num_haha,
            num_like, num_love,
            num_sorry, num_wow,
            post['link'],
            sentiment, how_positive, how_neutral, how_negative, detected_language
        ]
        try:
            cursor.execute(
                """INSERT INTO facebook.posts VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18)""",
                row
            )
            print('[DBG][POST][{}][{}] INSERT {}'.format(detected_language, sentiment, post['post_id']))
        except Exception as e:
            print('[DBG][POST] SKIP {}: {}'.format(post['post_id'], e))

    try:
        post['comments_full'][0]
    except IndexError:
        print('No comments found.')
        return
    try:
        for x in post['comments_full']:
            sentiment = -1; how_positive = -1; how_neutral = -1; how_negative = -1 # reset values
            try:
                detected_language = detect(x['comment_text'])
            except LangDetectException:
                detected_language = 'UNKNOWN'
            sentiment, how_positive, how_neutral, how_negative = analyze_sentiment_vader(x['comment_text'])
            row = [
                post['post_id'], x['comment_id'],
                x['comment_url'], x['comment_text'],
                x['comment_time'], x['commenter_id'],
                sentiment, how_positive, how_neutral, how_negative, detected_language
            ]
            try:
                cursor.execute(
                    """INSERT INTO facebook.comments VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)""",
                    row
                )
                print('[DBG][COMMENT][{}][{}] INSERT {}'.format(detected_language, sentiment, x['comment_id']))
            except Exception as e:
                print('[DBG][COMMENT] SKIP {}: {}'.format(x['comment_id'], e))
    except (TypeError, KeyError):
        return # no comments available in post.
    cursor.close()



def get_page_data(connection, filter):
    # Send the unique page name, profile name, or ID as the first parameter and you're good to go:
    for post in get_posts(filter, cookies=COOKIE_PATH, options=options):
        insert_post_db(connection, post)



def get_profile_info(filter):
    returning = get_profile(filter, cookies=COOKIE_PATH)
    print(json.dumps(returning, indent=4))



def get_group_info(filter):
    returning = get_group_info(filter, cookies=COOKIE_PATH)
    print(json.dumps(returning, indent=4))



def analyze_sentiment_vader(text):
    sia = SentimentIntensityAnalyzer()
    analysis = sia.polarity_scores(text)
    return analysis.get('compound'), analysis.get('pos'), analysis.get('neu'), analysis.get('neg')



def find_all_pages(connection):
    pages_list = list()
    cursor = connection.cursor()
    sql_query = """select distinct facebook_keyword from {}""".format('admin.keywords')
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('{}'.format(e))
    
    result = cursor.fetchall()
    for x in result:
        pages_list.append(x[0])
    print('[DBG] PAGES {}'.format(pages_list))
    return pages_list



def main():
    connection = init_db_connection(login_info)

    all_pages = find_all_pages(connection)

    if args.option == 'page_data':
        if args.target:
            get_page_data(connection, args.target)
        else:
            # get all pages
            for x in all_pages:
                get_page_data(connection, x)
    elif args.option == 'profile_info':
        get_profile_info(args.target) # just printing.
    elif args.option == 'group_info':
        get_group_info(args.target) # just printing.
    
    connection.close()



if __name__ == '__main__':
    main()