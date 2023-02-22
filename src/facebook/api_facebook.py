from src.oracledb import OracleATPDatabaseConnection
from pyfacebook import GraphAPI
import yaml
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("config.yaml") as file:
		return yaml.safe_load(file)

dbhandler = OracleATPDatabaseConnection()
print(process_yaml()['oauth_access_token'])
api = GraphAPI(process_yaml()['oauth_access_token'])
# Initialize the Graph API with a valid access token (optional,
# but will allow you to do all sorts of fun stuff).

#526963855355684 post id.
'''
for x in graph.search('mortal kombat', 'place', page=True, retry=3):
    print(x)
'''

api.get_object(object_id="20531316728")
api.get_object(object_id="526963855355684")

