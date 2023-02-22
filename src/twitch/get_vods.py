import requests
import json
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



## Its the name you see when you browse to the twitch url of the streamer
def get_users_from_db(connection):
    # Get all users from the database

    user_list = list()
    cursor = connection.cursor()
    sql_query = """select distinct account_name from {}""".format('twitch.accounts')
    try:
        cursor.execute(sql_query)
    except Exception as e:
        print('{}'.format(e))
    
    result = cursor.fetchall()
    for x in result:
        user_list.append(x[0])
    return user_list



def get_vods(USER_ID, CLIENT_ID, SECRET):
    '''Returns JSON object with all vods.'''

    ## First get a local access token. 
    secretKeyURL = "https://id.twitch.tv/oauth2/token?client_id={}&client_secret={}&grant_type=client_credentials".format(CLIENT_ID, SECRET)
    response = requests.post(secretKeyURL)
    accessTokenData = response.json()

    ## Then figure out the user id. 
    userIDURL = "https://api.twitch.tv/helix/users?login=%s"%USER_ID
    response = requests.get(userIDURL, headers={"Client-ID":CLIENT_ID,
        'Authorization': "Bearer " + accessTokenData["access_token"]})
    userID = response.json()["data"][0]["id"]


    ## Now you can request the video clip data.
    findVideoURL = "https://api.twitch.tv/helix/videos?user_id=%s"%userID
    response= requests.get(findVideoURL, headers={"Client-ID":CLIENT_ID,
        'Authorization': "Bearer " + accessTokenData["access_token"]})

    vod_obj = response.json()
    #print(json.dumps(vod_obj, indent = 4))
    return vod_obj



def process_vods(json_obj):
    data = json_obj.get('data')
    urls = list()
    for x in data:
        urls.append(x.get('url'))
    return urls



def insert_urls_db(connection, user, url_list):  
    cursor = connection.cursor()
    for x in url_list:
        row = [
            user, x, 0
        ]
        try:
            cursor.execute("insert into twitch.vods values (:1, :2, :3)", row)
            print('[DBG] INSERT {}'.format(x))
        except Exception:
            print('[DBG] SKIP {}'.format(x))



def main():
    data = process_yaml()
    connection = init_db_connection(data)
    # Get all users from db from which to get the VODs.

    users = get_users_from_db(connection)
    for user in users:
        vod_obj = get_vods(user.split('https://twitch.tv/')[1], data['twitch']['client_id'], data['twitch']['secret'])
        url_list = process_vods(vod_obj)
        print('[DBG] USER {} URLS {}'.format(user, url_list))
        insert_urls_db(connection, user, url_list)



if __name__ == '__main__':
    main()