import threading
import time
import os
import yaml
import cx_Oracle
import argparse
import os
from pathlib import Path
home = str(Path.home())

parser = argparse.ArgumentParser()
parser.add_argument('-t', '--threads', help='Number of threads', required=True)
parser.add_argument('-l', '--language', help='Language detection method', choices=['oracle_service', 'langdetect'], required=True)
parser.add_argument('-m', '--mode', help='Mode to execute', choices=['twitter', 'twitch'], required=True)
args = parser.parse_args() 

global TABLES
if args.mode == 'twitter':
    TABLES = ['admin.public_sentiment', 'admin.user_sentiment']
elif args.mode == 'twitch':
    TABLES = ['twitch.chat', 'twitch.vod_chat']
else:
    TABLES =  ['admin.public_sentiment', 'admin.user_sentiment', 'twitch.chat', 'twitch.vod_chat']

NUM_THREADS = int(args.threads)



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



def thread_function(table, base, offset):
    os.system('python detect_language.py --table "{}" --offset "{}" --base "{}" --language "{}"'.format(table, offset, base, args.language))



def find_number_rows(connection, table_name):
    cursor = connection.cursor()
    sql_query = """select count(*) from (select distinct message from {} where language='UNKNOWN')""".format(table_name)
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('@{}: {}'.format(find_number_rows.__name__, e))
    
    result = cursor.fetchall()
    print(result[0][0])
    print('Obtained {} rows to process'.format(result[0][0]))
    return int(result[0][0])



def main():
    data = process_yaml()
    # Get cursor.
    connection = init_db_connection(data)
    for x in TABLES:
        offset = 0
        threads = list()
        total_rows = find_number_rows(connection, x)
        rows_per_thread = int(total_rows / NUM_THREADS) + 1
        print('[STAT] Table: {} NUM_THREADS: {} Total rows: {} Base: {} Offset: {}'.format(
            x, NUM_THREADS, total_rows, rows_per_thread, offset
        ))
        time.sleep(3) # allow seeing stats
        for y in range(NUM_THREADS):
            thread = threading.Thread(target=thread_function, args=(x, rows_per_thread, offset))
            threads.append(thread)
            thread.start()
            offset += rows_per_thread
        
        for y in threads:
            y.join()

    connection.close()



if __name__ == '__main__':
	main()
