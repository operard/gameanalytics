import threading
import time
import os
import yaml
import cx_Oracle
import argparse
import os
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("../config.yaml") as file:
		return yaml.safe_load(file)



# wallet location (default is HOME/wallets/wallet_X)
os.environ['TNS_ADMIN'] = '{}/{}'.format(home, process_yaml()['WALLET_DIR'])
print(os.environ['TNS_ADMIN'])

parser = argparse.ArgumentParser()
parser.add_argument('-t', '--threads', help='Number of threads', required=True)
parser.add_argument('-l', '--language-mode', help='Language mode, either online or offline', choices=['online', 'offline'], required=True)
parser.add_argument('-m', '--mode', help='Mode to execute', choices=['twitter', 'twitch'], required=False)
args = parser.parse_args() 

global TABLES
# We only want to parallelize these 2 tables as they are the ones giving us problems.
if args.mode == 'twitter':
    TABLES = ['public_sentiment', 'user_sentiment']
elif args.mode == 'twitch':
    TABLES = ['twitch.chat', 'twitch.vod_chat']
else:
    TABLES =  ['public_sentiment', 'user_sentiment']

NUM_THREADS = int(args.threads)



def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



def thread_function(table, base, offset, mode):
    os.system('python calculate_sentiment.py --table "{}" --offset "{}" --base "{}" --language-mode "{}"'.format(table, offset, base, mode))



def find_groups(connection, table_name):
    group_list = list()
    cursor = connection.cursor()
    sql_query = """select distinct search_filter from {}""".format(table_name)
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('@{}: {}'.format(find_groups.__name__, e))
    
    result = cursor.fetchall()
    for x in result:
        group_list.append(x[0])
    return group_list



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
            thread = threading.Thread(target=thread_function, args=(x, rows_per_thread, offset, args.language_mode))
            threads.append(thread)
            thread.start()
            offset += rows_per_thread
        
        for y in threads:
            y.join()

    connection.close()



if __name__ == '__main__':
	main()
