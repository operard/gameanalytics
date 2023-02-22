import threading
import time
import os
import yaml
import cx_Oracle
import os
from pathlib import Path
home = str(Path.home())

'''
This code extracts data from TWITCH.VODS table and processes them 
'''


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



def thread_function(vod_url):
    os.system('python online.py --url "{}" --mode "vod"'.format(vod_url))



def find_vods(connection):
    vod_list = list()
    cursor = connection.cursor()
    sql_query = """select distinct vod_url from {} where processed='0'""".format('twitch.vods')
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('{}'.format(e))
    
    result = cursor.fetchall()
    for x in result:
        vod_list.append(x[0])
    return vod_list



def main():
    data = process_yaml()
    # Get cursor.
    connection = init_db_connection(data)
    vods = find_vods(connection)
    print('[DBG] Accounts: {}'.format(vods))
    for x in vods:
        threads = list()
        thread = threading.Thread(target=thread_function, args=(x,))
        threads.append(thread)
        thread.start()
        
    for x in threads:
        x.join()

    connection.close()



if __name__ == '__main__':
	main()
