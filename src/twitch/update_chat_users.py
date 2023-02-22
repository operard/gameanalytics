'''
This code updates the table TWITCH.CHAT_USERS every time it's executed with missing values.

insert into twitch.chat_users (select distinct author_name, '0' from twitch.chat where author_name not in (select author_name from twitch.chat_users));
insert into twitch.chat_users (select distinct author_name, '0' from twitch.vod_chat where author_name not in (select author_name from twitch.chat_users));
'''
import cx_Oracle
import yaml
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



def update(connection):
    cursor = connection.cursor()
    sql_1 = """insert into twitch.chat_users (select distinct author_name, '0' from twitch.chat where author_name not in (select author_name from twitch.chat_users))"""
    sql_2 = """insert into twitch.chat_users (select distinct author_name, '0' from twitch.vod_chat where author_name not in (select author_name from twitch.chat_users))"""
    try:
        cursor.execute(sql_1)
        print('[DBG] UPDATE_CHAT_USERS OK')
    except Exception:
        print('[DBG] UPDATE_CHAT_USERS ERR')
    try:
        cursor.execute(sql_2)
        print('[DBG] UPDATE_CHAT_USERS OK')
    except Exception:
        print('[DBG] UPDATE_CHAT_USERS ERR')



def main():
    data = process_yaml()
    connection = init_db_connection(data)
    update(connection)
    connection.close()



if __name__ == '__main__':
    main()