from facebook_scraper import get_posts
import yaml
import cx_Oracle
import os
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("config.yaml") as file:
		return yaml.safe_load(file)



# get login info from config.yaml
data = process_yaml()
USERNAME = data['username']
PASSWORD = data['password']
# wallet location (default is HOME/wallets/wallet_X)
os.environ['TNS_ADMIN'] = '{}/{}'.format(home, data['WALLET_DIR'])
print(os.environ['TNS_ADMIN'])



def init_db_connection(data):
    connection = cx_Oracle.connect(data['db']['username'], data['db']['password'], data['db']['dsn'])
    print('Connection successful.')
    connection.autocommit = True
    return connection



options = {
    'comments': 1000000,
    'reactors': 1000000,
    'progress': False
}



def main():
    # This will trigger the application asking for a login approval
    for post in get_posts('nintendo', credentials=(USERNAME, PASSWORD), options=options):
        print(post)



if __name__ == '__main__':
    main()