from oracledb import OracleATPDatabaseConnection
import requests
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--offset', help='Offset from where to start to fetch rows', required=False)
parser.add_argument('-b', '--base', help='Number of rows to fetch upon execution', required=False)
parser.add_argument('-t', '--table', help='Table to process' , required=True)
args = parser.parse_args()


TABLES = [
	{
		'table_name': 'admin.user_sentiment',
		'translation_object': 'tweet',
		'identifier': 'tweet_id'
	},
	{
		'table_name': 'admin.public_sentiment',
		'translation_object': 'tweet',
		'identifier': 'tweet_id'
	},
	{
		'table_name': 'twitch.chat',
		'translation_object': 'message',
		'identifier': 'message_id'
	},
	{
		'table_name': 'twitch.vod_chat',
		'translation_object': 'message',
		'identifier': 'message_id'
	}
]



def translate(text, source_language, destination_language):

	response = requests.post('http://129.146.152.113:5000/translate', json = {
		'source': source_language,
		'target': destination_language,
		'format': 'text',
		'q': text
	}, headers={'Content-Type': 'application/json'})

	#print(response.status_code, response.json())
	resulting_text = str()
	translated_bit = int()
	final_language = source_language # default value is original without change.
	if response.status_code == 200:
		resulting_text = response.json().get('translatedText')
		print('[DBG] TRANSLATE {} OK'.format(resulting_text))
		translated_bit = 1
		final_language = destination_language # update the language
	elif response.status_code == 400:
		print('[DBG] TRANSLATE {} {}'.format(text, response.status_code))
		resulting_text = text # unchanged
		translated_bit = 1 # if we can't process the text, we assume it's already translated.
	else:
		print('[DBG] TRANSLATE ERR {}'.format(response.status_code))
		resulting_text = text
		translated_bit = 0
	return resulting_text, translated_bit, final_language



def find_table_info(table_name):
	for x in TABLES:
		if x['table_name'] == table_name:
			return x
	return None # if none was found.



def run():
	db = OracleATPDatabaseConnection()
	# verify input
	table_object = find_table_info(args.table)
	if not table_object:
		print('[DBG] COULD NOT FIND TABLE NAME {}'.format(args.table))
		return


	if not args.offset and not args.base:
		sql_query = """select distinct {}, language, {} from {} where language not in ('en', 'UNKNOWN', 'und') and translated=0""".format(table_object['translation_object'], table_object['identifier'], table_object['table_name'])
	else:
		sql_query = """select distinct {}, language, {} from {} where language not in ('en', 'UNKNOWN', 'und') and translated=0 OFFSET {} ROWS FETCH NEXT {} ROWS ONLY""".format(
			table_object['translation_object'], table_object['identifier'], table_object['table_name'], args.offset, args.base)
	messages_to_translate = db.select(sql_query)

	print('[DBG] TRANSLATING...')
	for y in messages_to_translate:
		resulting_text, translated_bit, final_language = translate(y[0], y[1], 'en')

		# If we translated the text, update the database
		if translated_bit == 1:
			sql_query = """update {} set {}=:1, translated=:2, language=:3 where {}=:4""".format(table_object['table_name'], table_object['translation_object'], table_object['identifier'])
			row = [
				resulting_text, translated_bit, final_language, y[2]
			]
			#print(sql_query)
			db.update(sql_query, row, table_object['table_name'])
 

	db.close_pool()



def main():
    run()



if __name__ == '__main__':
    main()
