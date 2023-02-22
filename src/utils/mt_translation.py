# Copyright (c) 2021 Oracle and/or its affiliates.
import requests
import yaml
import base64
import time
import urllib.parse
from oracledb import OracleATPDatabaseConnection
# Python equivalent to the file get_oauth2.sh. Done in Python for convenience purposes in the implementation

# Twitter language codes
TWITTER_LANGUAGES = [
	'fr', 'ja', 'zh', 'in', 'cs', 'fi', 'ca', 'tr',	'te', 'sv', 'bn', 'ckb', 'ka', 'sd', 'fa', 'ta', 'am', 'pt', 'cy', 'da',
	'uk', 'ko', 'hu', 'no', 'sl', 'iw', 'th', 'mr', 'pa', 'si', 'hy', 'dv', 'hi', 'lv', 'pl', 'is', 'ur', 'my', 'ps', 'ar', 
	'nl', 'kn', 'el', 'vi', 'bg', 'gu', 'es', 'it', 'tl', 'lt', 'ml', 'lo', 'bo', 'de', 'ro', 'or', 'ne', 'ug', 'en', 'ru',
	'et', 'ht', 'eu', 'sr', 'km'
]

# Load config file
def load_config_file():
	with open('config.yaml') as file:
		return yaml.safe_load(file)

# CURRENTLY SUPPORTED LANGUAGE CODES
LANGUAGES = [
	'en', 'fr-CA', 'pl', 'sv', 'ar', 'de', 'ro', 'zh-TW', 'pt-BR', 'it', 'ru', 'nl', 'ja', 'zh-CN', 'fr', 'ko', 'es-ww'
]
# Coming in FY22Q3: Norwegian, Danish, Czech, Finish, Turkish & in FY22Q4: Greek, Hebrew, Thai, Ukrainian, Croatian 


TABLES = [
	{
		'table_name': 'admin.user_sentiment',
		'translation_object': 'tweet'
	},
	{
		'table_name': 'admin.public_sentiment',
		'translation_object': 'tweet'
	},
	{
		'table_name': 'twitch.chat',
		'translation_object': 'message'
	},
	{
		'table_name': 'twitch.vod_chat',
		'translation_object': 'message'
	}
]


# Defining variables
IDCSSERVER = 'https://idcs-49a260143f4b4a309c5adc9e8a25de61.identity.oraclecloud.com'
SCOPE = 'urn:opc:idm:__myscopes__'
# Production endpoint
BASEURL = 'https://translation-api.us.oracle.com'
ENDPOINT = '{}/translation/api'.format(BASEURL)
#REALTIME_ENDPOINT = 'https://mt.oraclecorp.com/api'
REALTIME_ENDPOINT = 'https://mt.translation.oci.oraclecloud.com/api'
NLP_ENDPOINT = 'https://mt.oraclecorp.com/nlp-api'




def get_access_token():

	data = load_config_file()
	basic_authorization = '{}:{}'.format(
		data['mt_translation']['MT_CLIENT_ID'], data['mt_translation']['MT_CLIENT_SECRET']
	)
	basic_authorization = base64.b64encode(basic_authorization.encode('ascii')).decode('ascii')
	#print('Basic authorization string: {}'.format(basic_authorization))

	request_url = '{}/oauth2/v1/token/'.format(IDCSSERVER)
	request_headers = {
		'Authorization': 'Basic {}'.format(basic_authorization),
		'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
		'x-resource-identity-domain-name':'idcs-oracle'
	}
	request_data = 'grant_type=client_credentials&scope={}'.format(SCOPE)

	response = requests.post(request_url, data=request_data, headers=request_headers)

	# print(response.status_code, response.json())

	api_token = response.json().get('access_token')
	#print('Obtained api token: {}'.format(api_token))
	return api_token



def real_time_translation(api_token, data, source_language_code):
	assert type(data) == type(str())
	request_url = '{}/translate/{}/{}?s={}'.format(REALTIME_ENDPOINT, source_language_code, 'en', urllib.parse.quote(data))
	request_headers = {
		'Authorization': 'Bearer {}'.format(api_token)
	}
	#print(request_url)

		
	response = requests.get(request_url, headers=request_headers)
	#print(response.status_code, response.content)
	print(response.status_code, response.text)



def batch_translation(api_token, file_path, file_name, source_language_code):
	request_url = '{}/files?service={}&sourceLang={}&scope={}'.format(ENDPOINT, 'mt', source_language_code, 'en')
	request_headers = {
		'Authorization': 'Bearer {}'.format(api_token)
	}
	#print(request_url, request_headers)
	files = {
		'file': (file_name, open(file_path, 'rb'))
	}

	print('Request URL: {}'.format(request_url))
	#print('Request headers: {}'.format(request_headers))
	print('Request file: {}'.format(dict(file=files['file'])))

	response = requests.post(request_url, headers=request_headers, files=dict(file=files['file']))
	response_json = response.json()
	print(response.status_code, response.json())
	#response_json.get('pipeline').get('status')
	#response_json.get('pipeline').get('wordcounts')

	drop = None
	while drop is None:
		print('Waiting for translated file to be ready...')
		try:
			response = requests.get(response_json.get('pipeline').get('status'))
		except Exception as e:
			print(e)
	
		try:
			print(response.json())
			drop = response.json().get('map').get('en_drop') # varies with language. I used 'en' as English in this example.
			if drop is not None:
				print('File result in {}'.format(drop))
		except KeyError:
			print('Could not find the drop.')
		
		time.sleep(10)
	



def run():
	api_token = get_access_token()
	db = OracleATPDatabaseConnection()
	
	for x in TABLES:
		x['table_name']
		x['translation_object']

		sql_query = """select distinct {}, language from {} where language != 'en'""".format(x['translation_object'], x['table_name'])
		messages_to_translate = db.select(sql_query)



		for x in messages_to_translate:
			print(x[0])
			real_time_translation(api_token, x[0], x[1])

	db.close_pool()




def main():
	run()
	
	

if __name__ == '__main__':
	main()
