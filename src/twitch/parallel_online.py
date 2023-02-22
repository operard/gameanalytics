import threading
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
parser.add_argument('-g', '--group', help='Account interest group to process', required=False)
args = parser.parse_args() 



def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



def thread_function(username):
    os.system('python online.py --url "{}" --mode "online"'.format(username))



def find_accounts(connection, account_interest=None):
    account_list = list()
    cursor = connection.cursor()
    sql_query = str()
    if account_interest:
        sql_query = """select distinct account_name from {} where account_interest = '{}'""".format('twitch.accounts', account_interest)
    else:
        sql_query = """select distinct account_name from {}""".format('twitch.accounts')
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('{}'.format(e))
    
    result = cursor.fetchall()
    for x in result:
        account_list.append(x[0])
    return account_list



def main():
    data = process_yaml()
    # Get cursor.
    connection = init_db_connection(data)
    accounts = list()
    if args.group:
        accounts = find_accounts(connection, args.group)
    else:
        accounts = find_accounts(connection)
    print('[DBG] Accounts: {}'.format(accounts))
    for x in accounts:
        threads = list()
        thread = threading.Thread(target=thread_function, args=(x,))
        threads.append(thread)
        thread.start()
        
    for x in threads:
        x.join()

    connection.close()



if __name__ == '__main__':
	main()
