import threading
import time
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
parser.add_argument('-m', '--mode', help='Mode to execute', choices=['twitch'], required=False)
args = parser.parse_args() 

global TABLES
if args.mode == 'twitch':
    TABLES = ['twitch.osint']

NUM_THREADS = int(args.threads)



def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



def thread_function(table, base, offset):
    os.system('python osint.py --table "{}" --offset "{}" --base "{}"'.format(table, offset, base))



def find_users(connection):
    cursor = connection.cursor()
    sql_query = """select count(*) from (select distinct author_name from twitch.chat_users where osint_processed='0')"""
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('@{}: {}'.format(find_users.__name__, e))
    
    result = cursor.fetchall()
    print(result[0][0])
    print('Obtained {} users to process'.format(result[0][0]))
    return int(result[0][0])



def main():
    data = process_yaml()
    # Get cursor.
    connection = init_db_connection(data)
    for x in TABLES:
        offset = 0
        threads = list()
        total_rows = find_users(connection)
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
