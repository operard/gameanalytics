from chat_downloader import ChatDownloader
import cx_Oracle
import argparse
import time
import yaml
from datetime import datetime
import os
from pathlib import Path
home = str(Path.home())

'''
Supported sites:

    YouTube.com - Livestreams, past broadcasts and premieres.
    Twitch.tv - Livestreams, past broadcasts and clips.
    Reddit.com - Livestreams, past broadcasts
    Facebook.com (currently in development) - Livestreams and past broadcasts.

'''

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url', help='Twitch URL', required=True)
parser.add_argument('-m', '--mode', help='Mode', choices=['online', 'vod'], required=True)
args = parser.parse_args()




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



def calculate_date(timestamp):
    return datetime.fromtimestamp(timestamp/1000000)



def download_stream(connection, url):
    cursor = connection.cursor()
    chat = ChatDownloader().get_chat(url)       # create a generator
    
    for message in chat:
        start = time.time()
        #chat.print_formatted(message)           # print the formatted message
        #print(message)

        try:
            message['message']
        except KeyError:
            print('[DBG] Found empty message. Skipping')
            continue
        sql_insert = str()
        if args.mode == 'online':
            sql_insert = """insert into twitch.chat values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20)"""
        elif args.mode == 'vod':
            sql_insert = """insert into twitch.vod_chat values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)"""
        row = list()
        if args.mode == 'online':
            row = [
                args.url,
                message['channel_id'],
                message['timestamp'],
                calculate_date(message['timestamp']),
                message['message_id'],
                message['message_type'],
                message['message'],
                message['author']['id'],
                message['author']['name'],
                message['author']['is_moderator'],
                message['author']['is_subscriber'],
                message['author']['is_turbo'],
                -1,
                -1,
                -1,
                -1,
                'UNKNOWN',
                message['message'],
                0,
                'UNKNOWN'
            ]
        elif args.mode == 'vod':
            row = [
                args.url,
                message['timestamp'],
                calculate_date(message['timestamp']),
                message['message_type'],
                message['message'],
                message['author']['id'],
                message['author']['name'],
                -1,
                -1,
                -1,
                -1,
                message['message_id'],
                'UNKNOWN',
                message['message'],
                0,
                'UNKNOWN'
            ]

        end = time.time()
        try:
            # print(sql_insert)
            cursor.execute(sql_insert, row)
            print('[DBG] +{} | {} ID {} NEW'.format((end-start), args.url, message['message_id']))
        except Exception as e:
            print('[DBG] ERR: {}'.format(e))


# if it's a vod, update the processed bit
def update_processed_bit(connection, vod_url):
    cursor = connection.cursor()
    sql_query = """update twitch.vods set processed='1' where vod_url='{}'""".format(vod_url)
    try:
        cursor.execute(sql_query)
        print('[DBG] UPDATE {} PROCESSED'.format(vod_url))
    except Exception as e:
        print('{}'.format(e))




def main():
    data = process_yaml()
    connection = init_db_connection(data)
    url = args.url
    download_stream(connection, url)
    if args.mode == 'vod':
        update_processed_bit(connection, url)
    connection.close()



if __name__ == '__main__':
    main()



#url = 'https://www.youtube.com/watch?v=5qap5aO4i9A' # youtube URL example
