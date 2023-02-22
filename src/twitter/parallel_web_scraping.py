import threading
import time
import os
import yaml
import cx_Oracle
import os
from pathlib import Path
home = str(Path.home())

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



def thread_function(query_type, word):
    os.system('python web_scraping.py --{} "{}"'.format(query_type, word))



def read_params_from_db(connection):
    keyword_list = list()
    at_list = list()

    sql_query_1 = """select distinct keyword from keywords"""
    sql_query_2 = """select distinct at from ats"""

    cursor = connection.cursor()

    try:
        cursor.execute(sql_query_1)
    except Exception as e:
        print('@{}: Exception: {}'.format(read_params_from_db.__name__), e)

    rows = cursor.fetchall()
    if rows:
        for x in rows:
            keyword_list.append(x[0])

    try:
        cursor.execute(sql_query_2)
    except Exception as e:
        print('@{}: Exception: {}'.format(read_params_from_db.__name__), e)

    rows = cursor.fetchall()
    if rows:
        for x in rows:
            at_list.append(x[0])

    return keyword_list, at_list



def main():
    data = process_yaml()
    # Get cursor.
    connection = init_db_connection(data)

    keywords, ats = read_params_from_db(connection)

    all_threads = list()
    for x in keywords:
        thread = threading.Thread(target=thread_function, args=('keyword', x,))
        all_threads.append(thread)
        thread.start()

    for x in ats:
        thread = threading.Thread(target=thread_function, args=('at', x,))
        all_threads.append(thread)
        thread.start()

    for x in all_threads:
        x.join()


    connection.close()



if __name__ == '__main__':
	main()
