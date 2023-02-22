'''
1. Get users from twitch chat and and vod_chat not present in twitch.OSINT
2. For each user, do OSINT analysis
3. Save in twitch.OSINT table
'''

import cx_Oracle
import yaml
import argparse
from importlib import import_module
import os
from pathlib import Path
home = str(Path.home())

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--offset', help='Offset from where to start to fetch rows', required=False)
parser.add_argument('-b', '--base', help='Number of rows to fetch upon execution', required=False)
parser.add_argument('-t', '--table', help='Table to process' , required=False) # not used atm. useful for scaling later.
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



def find_twitch_users(connection, base=None, offset=None):
    user_list = list()
    cursor = connection.cursor()
    sql_query = str()
    if not offset and not base:
        sql_query = """select distinct author_name from twitch.chat_users where osint_processed='0'"""
    else:
        sql_query = """select distinct author_name from twitch.chat_users where osint_processed='0' OFFSET {} ROWS FETCH NEXT {} ROWS ONLY""".format(offset, base)
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('{}'.format(e))
    
    result = cursor.fetchall()
    for x in result:
        user_list.append(x[0])

    print('[DBG] UNPROCESSED {}'.format(len(user_list)))
    return user_list



def process_osint(username):
    '''Modify sherlock path if it's not exactly in this relative path'''
    os.popen('python ../../../sherlock/sherlock/sherlock.py {} --timeout "5" --print-found --output "../osint.list"'.format(username)).read()
    print('Finished OSINT call')
    # When it's finished, read from the file line by line.
    f = open('../osint.list', 'r+')
    found_sites = list()
    lines = f.readlines()
    for line in lines:
        found_sites.append(line.rstrip())
    # Pop last element (not a URL) "[Total Websites Username Detected On : 45]"
    found_sites.pop()
    print('[DBG] USER {} SITES {}'.format(username, len(found_sites)))
    return found_sites




def process_social_analyzer(username):
    SocialAnalyzer = import_module("social-analyzer").SocialAnalyzer(silent=True)
    results = SocialAnalyzer.run_as_object(username=username, silent=True, logs=False)
    list_of_sites = list()
    for x in results.get('detected'):
        if ('wiki' not in x.get('link')) and ('User:') not in x.get('link'):
            list_of_sites.append(x.get('link'))
    print('[DBG] USER {} SITES {}'.format(username, len(list_of_sites)))
    return list_of_sites



def process_sites(connection, username, sites_list):
    cursor = connection.cursor()
    for x in sites_list:
        row = [
            username, x
        ]
        try:
            cursor.execute("insert into twitch.osint values (:1, :2)", row)
            print('[DBG] INSERT {}'.format(x))
        except Exception:
            print('[DBG] SKIP {}'.format(x))



def update_processed_bit(connection, username):
    cursor = connection.cursor()
    try:
        cursor.execute("update twitch.chat_users set osint_processed='1' where author_name = '{}'".format(username))
        print('[DBG] UPDATE BIT {}'.format(username))
    except Exception:
        print('[DBG] SKIP UPDATE BIT {}'.format(username))



def main():
    data = process_yaml()
    connection = init_db_connection(data)

    user_list = find_twitch_users(connection)
    # For every user, extract OSINT
    for x in user_list:
        print('[DBG] PROCESSING {}'.format(x))
        #sites_list = process_osint(x)
        sites_list = process_social_analyzer(x)
        process_sites(connection, x, sites_list)
        update_processed_bit(connection, x)
    

    connection.close()



if __name__ == '__main__':
    main()